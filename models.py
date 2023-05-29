from sqlalchemy import Integer, String, DateTime, Column
from database import Base

schemaName = "central_data_test"

class DimSupplier(Base):
    __tablename__ = 'dim_supplier'
    __table_args__ = {'schema': 'central_data_test'}

    id = Column(Integer, primary_key=True)
    name = Column(String(255), nullable=True)
    created_date = Column(DateTime, nullable=True)
    created_by = Column(String(50), nullable=True)
    updated_date = Column(DateTime, nullable=True)
    updated_by = Column(String(50), nullable=True)
    ap_supplier_id = Column(String(50), nullable=True)