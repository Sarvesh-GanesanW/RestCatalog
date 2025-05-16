from sqlalchemy import Column, Integer, String, JSON, ForeignKey, UniqueConstraint, ARRAY
from sqlalchemy.orm import declarative_base, relationship

Base = declarative_base()

class NamespaceModel(Base):
    __tablename__ = "iceberg_namespaces"

    id = Column(Integer, primary_key=True, index=True)
    levels = Column(ARRAY(String), nullable=False, unique=True) # e.g., ["db1"], ["db1", "schema1"]
    properties = Column(JSON, nullable=True)

    tables = relationship("TableModel", back_populates="namespace", cascade="all, delete-orphan")

class TableModel(Base):
    __tablename__ = "iceberg_tables"

    id = Column(Integer, primary_key=True, index=True)
    namespaceId = Column(Integer, ForeignKey("iceberg_namespaces.id"), nullable=False)
    name = Column(String, nullable=False)
    metadataLocation = Column(String, nullable=False, unique=True)
    # Catalog-specific properties, not the full Iceberg table properties from metadata.json
    properties = Column(JSON, nullable=True) 

    namespace = relationship("NamespaceModel", back_populates="tables")

    __table_args__ = (UniqueConstraint('namespaceId', 'name', name='_namespace_table_uc'),)