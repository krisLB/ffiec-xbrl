import os
import pandas as pd
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, ForeignKey, Enum, Table
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
from datetime import datetime
import enum

Base = declarative_base()

class FinlType(enum.Enum):
    EOP = "End-of-Period"
    FLOW = "Flow"

class ReportForm(enum.Enum):
    FORM_031 = "031"
    FORM_041 = "041"
    FORM_051 = "051"

field_report_form = Table('field_report_form', Base.metadata,
    Column('field_id', Integer, ForeignKey('fields.id')),
    Column('report_form', Enum(ReportForm))
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
    institution_id = Column(Integer, ForeignKey('institutions.id'), nullable=False)
    date = Column(DateTime, nullable=False)
    form = Column(Enum(ReportForm), nullable=False)
    
    institution = relationship("Institution", back_populates="reports")
    data_points = relationship("DataPoint", back_populates="report")

class Field(Base):
    __tablename__ = 'fields'
    
    id = Column(Integer, primary_key=True)
    mdrm = Column(String, unique=True, nullable=False)
    name = Column(String, nullable=False)
    description = Column(String)
    definition = Column(String)
    series_description = Column(String)
    data_type = Column(String, nullable=False)
    finl_type = Column(Enum(FinlType), nullable=False)
    report_forms = relationship("ReportForm", secondary=field_report_form)

class DataPoint(Base):
    __tablename__ = 'data_points'
    
    id = Column(Integer, primary_key=True)
    report_id = Column(Integer, ForeignKey('reports.id'), nullable=False)
    field_id = Column(Integer, ForeignKey('fields.id'), nullable=False)
    value = Column(Float)
    as_of_date = Column(DateTime, nullable=False)
    start_date = Column(DateTime)
    end_date = Column(DateTime)
    
    report = relationship("Report", back_populates="data_points")
    field = relationship("Field")

class DatabaseHandler:
    def __init__(self, folder_path, db_filename='call_reports.db'):
        os.makedirs(folder_path, exist_ok=True)
        db_path = os.path.abspath(os.path.join(folder_path, db_filename))
        db_url = f"sqlite:///{db_path}"
        self.engine = create_engine(db_url)
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
            field.report_forms = report_forms
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

    def process_dataframe(self, df):
        with self.Session() as session:
            # Cache for institution and field IDs
            institution_cache = {}
            field_cache = {}

            # Group by institution and date to create reports
            for (rssd_id, report_date), group in df.groupby(['RSSD_ID', 'ReportPeriodEndDate']):
                # Get or create institution
                if rssd_id not in institution_cache:
                    institution = self.get_institution_by_rssd(rssd_id)
                    if not institution:
                        institution_id = self.add_institution(f"Institution {rssd_id}", rssd_id)
                    else:
                        institution_id = institution.id
                    institution_cache[rssd_id] = institution_id
                else:
                    institution_id = institution_cache[rssd_id]

                # Create report
                report_id = self.add_report(institution_id, datetime.strptime(report_date, '%Y-%m-%d'), group['Form'].iloc[0])

                # Process each row in the group
                for _, row in group.iterrows():
                    mdrm = row['MDRM']
                    if mdrm not in field_cache:
                        field = self.get_field_by_mdrm(mdrm)
                        if not field:
                            field_id = self.add_field(
                                mdrm=mdrm,
                                name=row['Field Name'],
                                description=row['Description'],
                                definition=row['Definition'],
                                series_description=row['Series Description'],
                                data_type=row['Data Type'],
                                finl_type=FinlType.EOP if row['Finl Type'] == 'EOP' else FinlType.FLOW,
                                report_forms=[ReportForm(form) for form in row['Report Forms'].split(',')]
                            )
                        else:
                            field_id = field.id
                        field_cache[mdrm] = field_id
                    else:
                        field_id = field_cache[mdrm]

                    # Add data point
                    try:
                        self.add_data_point(
                            report_id=report_id,
                            field_id=field_id,
                            value=row['Value'],
                            as_of_date=datetime.strptime(report_date, '%Y-%m-%d'),
                            start_date=datetime.strptime(row['Start Date'], '%Y-%m-%d') if pd.notna(row['Start Date']) else None,
                            end_date=datetime.strptime(row['End Date'], '%Y-%m-%d') if pd.notna(row['End Date']) else None
                        )
                    except ValueError as e:
                        print(f"Error adding data point for MDRM {mdrm}: {str(e)}")

            session.commit()