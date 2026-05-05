import pytest
from sqlalchemy import Column, Integer, String, ForeignKey
from sqlalchemy.orm import DeclarativeBase, relationship, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class ModelC(Base):
    __tablename__ = "c"

    id = Column(Integer, primary_key=True)
    name = Column(String)
    b_list: Mapped[list["ModelB"]] = relationship(back_populates="c")


class ModelB(Base):
    __tablename__ = "b"
    id = Column(Integer, primary_key=True)
    c_id = Column(Integer, ForeignKey("c.id"))
    year = Column(Integer, default=0)
    qty = Column(Integer, default=0)

    c: Mapped[ModelC] = relationship(back_populates="b_list")


class ModelA(Base):
    __tablename__ = "a"
    id = Column(Integer, primary_key=True)
    b_id = Column(Integer, ForeignKey("b.id"))
    name = Column(String)
    b = relationship("ModelB")
