import os
import pandas as pd
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, ForeignKey, Enum, Table, Boolean
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship, aliased
from datetime import datetime
import enum

Base = declarative_base()

class FinlType(enum.Enum):
    EOP = "End-of-Period"
    FLOW = "Flow"

class ReportFormEnum(enum.Enum):
    FORM_031 = "031"
    FORM_041 = "041"
    FORM_051 = "051"

class ReportForm(Base):
    __tablename__ = 'report_forms'
    id = Column(Integer, primary_key=True)
    form_number = Column(String, unique=True, nullable=False)


field_report_form = Table('field_report_form', Base.metadata,
    Column('field_id', Integer, ForeignKey('fields.id')),
    Column('report_form_id', Integer, ForeignKey('report_forms.id'))
)

class Institution(Base):
    __tablename__ = 'institutions'
    
    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)
    rssd_id = Column(String, unique=True, nullable=False)
    
    reports = relationship("Report", back_populates="institution")

class Report(Base):
    __tablename__ = 'reports'
    
    id = Column(Integer, primary_key=True)
    Institution_id = Column(Integer, ForeignKey('institutions.id'), nullable=False)
    Date = Column(DateTime, nullable=False)
    form_id = Column(Integer, ForeignKey('report_forms.id'), nullable=False)
    
    institution = relationship("Institution", back_populates="reports")
    form = relationship("ReportForm")
    data_points = relationship("DataPoint", back_populates="report")

class Field(Base):
    __tablename__ = 'fields'
    id = Column(Integer, primary_key=True)
    MDRM_Item = Column(String, unique=True, nullable=False)
    Item_Name = Column(String, nullable=False)
    Start_Date = Column(DateTime)
    End_Date = Column(DateTime)
    Confidential = Column(Boolean)
    Datatype = Column(String)
    Unit = Column(String)
    Decimals = Column(Integer)
    report_form_id = Column(Integer, ForeignKey('report_forms.id'))
    
    Report_form = relationship("ReportForm")

ReportForm.fields = relationship("Field", secondary=field_report_form, back_populates="report_forms")


class DataPoint(Base):
    __tablename__ = 'data_points'
    
    id = Column(Integer, primary_key=True)
    report_id = Column(Integer, ForeignKey('reports.id'), nullable=False)
    field_id = Column(Integer, ForeignKey('fields.id'), nullable=False)
    Value = Column(Float)
    As_of_date = Column(DateTime, nullable=False)
    Start_date = Column(DateTime)
    End_date = Column(DateTime)
    
    report = relationship("Report", back_populates="data_points")
    field = relationship("Field")

class DatabaseHandler:
    def __init__(self, folder_path, db_filename='call_reports.db'):
        os.makedirs(folder_path, exist_ok=True)
        db_path = os.path.abspath(os.path.join(folder_path, db_filename))
        self.engine = create_engine(f'sqlite:///{db_path}')
        Base.metadata.create_all(self.engine)
        self.Session = sessionmaker(bind=self.engine)

    def add_institution(self, name, rssd_id):
        with self.Session() as session:
            institution = Institution(name=name, rssd_id=rssd_id)
            session.add(institution)
            session.commit()
            return institution.id

    def add_report(self, institution_id, date, form):
        with self.Session() as session:
            report = Report(institution_id=institution_id, date=date, form=form)
            session.add(report)
            session.commit()
            return report.id

    def add_field(self, mdrm, name, description, definition, series_description, 
                data_type, finl_type, report_forms):
        with self.Session() as session:
            field = Field(mdrm=mdrm, name=name, description=description, 
                        definition=definition, series_description=series_description,
                        data_type=data_type, finl_type=finl_type)
            
            # Get or create ReportForm objects
            for form in report_forms:
                report_form = session.query(ReportForm).filter_by(name=form).first()
                if not report_form:
                    report_form = ReportForm(name=form)
                    session.add(report_form)
                field.report_forms.append(report_form)
            
            session.add(field)
            session.commit()
            return field.id

    def add_data_point(self, report_id, field_id, value, as_of_date, start_date=None, end_date=None):
        with self.Session() as session:
            field = session.query(Field).get(field_id)
            if field.finl_type == FinlType.EOP and (start_date or not end_date):
                raise ValueError("EOP items must not have start dates")
            if field.finl_type == FinlType.FLOW and (not start_date or not end_date):
                raise ValueError("Flow items must have both start and end dates")
            
            data_point = DataPoint(report_id=report_id, field_id=field_id, value=value,
                                   as_of_date=as_of_date, start_date=start_date, end_date=end_date)
            session.add(data_point)
            session.commit()
            return data_point.id

    def get_institution_by_rssd(self, rssd_id):
        with self.Session() as session:
            return session.query(Institution).filter_by(rssd_id=rssd_id).first()

    def get_reports_for_institution(self, institution_id):
        with self.Session() as session:
            return session.query(Report).filter_by(institution_id=institution_id).all()

    def get_data_points_for_report(self, report_id):
        with self.Session() as session:
            return session.query(DataPoint).filter_by(report_id=report_id).all()

    def get_field_by_mdrm(self, mdrm):
        with self.Session() as session:
            return session.query(Field).filter_by(mdrm=mdrm).first()

    def update_data_point(self, data_point_id, new_value, new_as_of_date=None, 
                          new_start_date=None, new_end_date=None):
        with self.Session() as session:
            data_point = session.query(DataPoint).get(data_point_id)
            if data_point:
                data_point.value = new_value
                if new_as_of_date:
                    data_point.as_of_date = new_as_of_date
                if new_start_date:
                    data_point.start_date = new_start_date
                if new_end_date:
                    data_point.end_date = new_end_date
                session.commit()
                return True
            return False

    def get_eop_data_points(self, report_id):
        with self.Session() as session:
            return session.query(DataPoint).join(Field).filter(
                DataPoint.report_id == report_id,
                Field.finl_type == FinlType.EOP
            ).all()

    def get_flow_data_points(self, report_id):
        with self.Session() as session:
            return session.query(DataPoint).join(Field).filter(
                DataPoint.report_id == report_id,
                Field.finl_type == FinlType.FLOW
            ).all()

    def get_fields_by_report_form(self, report_form):
        with self.Session() as session:
            return session.query(Field).filter(Field.report_forms.contains(report_form)).all()

    ################

    def process_mdrm_dict(self, df):
        with self.Session() as session:
            for _, row in df.iterrows():
                report_form = session.query(ReportForm).filter_by(form_number=row['Reporting_Form_Num']).first()
                if not report_form:
                    report_form = ReportForm(form_number=row['Reporting_Form_Num'])
                    session.add(report_form)
                    session.flush()

                # Handle start date
                start_date = pd.to_datetime(row['Start_Date']).to_pydatetime() if pd.notna(row['Start_Date']) else None

                # Handle end date
                if pd.isna(row['End_Date']):
                    end_date = None
                elif row['End_Date'] == '9999-12-31':
                    end_date = datetime(9999, 12, 31)
                else:
                    end_date = pd.to_datetime(row['End_Date']).to_pydatetime()

                field = Field(
                    MDRM_Item=row['MDRM_Item'],
                    Item_Name=row['Item_Name'],
                    Start_Date=start_date,
                    End_Date=end_date,
                    Confidential=row['Confidential'],
                    Datatype=row['Datatype'],
                    Unit=row['Unit'],
                    Decimals=row['Decimals'],
                    Report_Form=report_form
                )
                session.add(field)
            
            session.commit()


    def process_call_report(self, df):
        with self.Session() as session:
            # Assume all rows in this df are for the same institution and report date
            rssd_id = df['RSSD_ID'].iloc[0]
            report_date = pd.to_datetime(df['ReportPeriodEndDate'].iloc[0])
            report_form_num = df['Reporting_Form_Num'].iloc[0]

            institution = session.query(Institution).filter_by(rssd_id=rssd_id).first()
            if not institution:
                institution = Institution(rssd_id=rssd_id, name=f"Institution {rssd_id}")
                session.add(institution)
                session.flush()

            report_form = session.query(ReportForm).filter_by(number=report_form_num).first()
            if not report_form:
                report_form = ReportForm(number=report_form_num)
                session.add(report_form)
                session.flush()

            report = Report(institution=institution, report_date=report_date, report_form=report_form)
            session.add(report)
            session.flush()

            for _, row in df.iterrows():
                field = session.query(Field).filter_by(mdrm_item=row['MDRM_Item']).first()
                if not field:
                    # If field doesn't exist, create it (this should be rare if MDRM_Dict was processed first)
                    field = Field(
                        mdrm_item=row['MDRM_Item'],
                        item_name=row['Item_Name'],
                        confidential=row['Confidential'],
                        report_form=report_form
                    )
                    session.add(field)
                    session.flush()

                data_point = DataPoint(
                    report=report,
                    field=field,
                    value=float(row['Value']) if pd.notna(row['Value']) else None
                )
                session.add(data_point)

            session.commit()

    def recreate_database(self):
        Base.metadata.drop_all(self.engine)
        Base.metadata.create_all(self.engine)