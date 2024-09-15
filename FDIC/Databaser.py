from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, ForeignKey, Enum
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
from datetime import datetime
import enum

Base = declarative_base()

class ItemType(enum.Enum):
    EOP = "End-of-Period"
    FLOW = "Flow"

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
    
    institution = relationship("Institution", back_populates="reports")
    data_points = relationship("DataPoint", back_populates="report")

class Field(Base):
    __tablename__ = 'fields'
    
    id = Column(Integer, primary_key=True)
    mdrm = Column(String, unique=True, nullable=False)
    name = Column(String, nullable=False)
    description = Column(String)
    data_type = Column(String, nullable=False)
    item_type = Column(Enum(ItemType), nullable=False)

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
    def __init__(self, db_url):
        self.engine = create_engine(db_url)
        Base.metadata.create_all(self.engine)
        self.Session = sessionmaker(bind=self.engine)

    def add_institution(self, name, rssd_id):
        with self.Session() as session:
            institution = Institution(name=name, rssd_id=rssd_id)
            session.add(institution)
            session.commit()
            return institution.id

    def add_report(self, institution_id, date):
        with self.Session() as session:
            report = Report(institution_id=institution_id, date=date)
            session.add(report)
            session.commit()
            return report.id

    def add_field(self, mdrm, name, description, data_type, item_type):
        with self.Session() as session:
            field = Field(mdrm=mdrm, name=name, description=description, 
                          data_type=data_type, item_type=item_type)
            session.add(field)
            session.commit()
            return field.id

    def add_data_point(self, report_id, field_id, value, as_of_date, start_date=None, end_date=None):
        with self.Session() as session:
            field = session.query(Field).get(field_id)
            if field.item_type == ItemType.EOP and (start_date or end_date):
                raise ValueError("EOP items should not have start or end dates")
            if field.item_type == ItemType.FLOW and (not start_date or not end_date):
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
                Field.item_type == ItemType.EOP
            ).all()

    def get_flow_data_points(self, report_id):
        with self.Session() as session:
            return session.query(DataPoint).join(Field).filter(
                DataPoint.report_id == report_id,
                Field.item_type == ItemType.FLOW
            ).all()