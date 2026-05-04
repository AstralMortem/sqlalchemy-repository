import pytest
from sqlalchemy import Column, Integer, String, ForeignKey
from sqlalchemy.orm import DeclarativeBase, relationship


class Base(DeclarativeBase):
    pass


class ModelC(Base):
    __tablename__ = "c"

    id = Column(Integer, primary_key=True)
    name = Column(String)


class ModelB(Base):
    __tablename__ = "b"
    id = Column(Integer, primary_key=True)
    c_id = Column(Integer, ForeignKey("c.id"))
    year = Column(Integer, default=0)
    qty = Column(Integer, default=0)

    c = relationship("ModelC")


class ModelA(Base):
    __tablename__ = "a"
    id = Column(Integer, primary_key=True)
    b_id = Column(Integer, ForeignKey("b.id"))
    name = Column(String)
    b = relationship("ModelB")
