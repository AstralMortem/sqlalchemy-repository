import pytest
from sqlalchemy_repository.utils.lookups import apply_lookup, split_lookup
from sqlalchemy.orm import declarative_base
from sqlalchemy import BinaryExpression, Column, Integer, String, DateTime

Base = declarative_base()


class Dummy(Base):
    __tablename__ = "dummy"

    id = Column(Integer, primary_key=True)
    name = Column(String)
    age = Column(Integer)
    created_at = Column(DateTime)


def test_split_lookup():
    path, lookup = split_lookup("name")
    assert path == ["name"]
    assert lookup == "exact"


def test_split_lookup_with_lookup():
    path, lookup = split_lookup("name__icontains")

    assert path == ["name"]
    assert lookup == "icontains"


def test_split_lookup_nested():
    path, lookup = split_lookup("generation__model__name__icontains")

    assert path == ["generation", "model", "name"]
    assert lookup == "icontains"


def test_split_lookup_alias():
    path, lookup = split_lookup("age__range")

    assert path == ["age"]
    assert lookup == "range"  # alias resolved later


def test_apply_lookup_exact():
    expr = apply_lookup(Dummy.age, "exact", 10)

    assert isinstance(expr, BinaryExpression)
    assert str(expr.right.compile(compile_kwargs={"literal_binds": True})) == "10"


def test_apply_lookup_icontains():
    expr = apply_lookup(Dummy.name, "icontains", "bmw")

    sql = str(expr.compile(compile_kwargs={"literal_binds": True}))
    assert (
        "ILIKE" in sql.upper() or "LIKE" in sql.upper()
    )  # Not all backend support ILIKE
    assert "%bmw%" in sql


def test_apply_lookup_in():
    expr = apply_lookup(Dummy.age, "in", [1, 2, 3])

    sql = str(expr)
    assert "IN" in sql.upper()


def test_apply_lookup_not_in():
    expr = apply_lookup(Dummy.age, "not_in", [1, 2])

    sql = str(expr)
    assert "NOT" in sql.upper()
    assert "IN" in sql.upper()


def test_apply_lookup_isnull_true():
    expr = apply_lookup(Dummy.name, "isnull", True)

    assert "IS NULL" in str(expr).upper()


def test_apply_lookup_isnull_false():
    expr = apply_lookup(Dummy.name, "isnull", False)

    assert "IS NOT NULL" in str(expr).upper()


def test_apply_lookup_between():
    expr = apply_lookup(Dummy.age, "between", (10, 20))

    sql = str(expr)
    assert "BETWEEN" in sql.upper()


def test_apply_lookup_range_alias():
    expr = apply_lookup(Dummy.age, "range", (1, 5))

    sql = str(expr)
    assert "BETWEEN" in sql.upper()


def test_apply_lookup_gt():
    expr = apply_lookup(Dummy.age, "gt", 18)

    assert ">" in str(expr)


def test_apply_lookup_date():
    import datetime

    expr = apply_lookup(Dummy.created_at, "date", datetime.date(2024, 1, 1))

    sql = str(expr)
    assert "CAST" in sql.upper()


def test_apply_lookup_year():
    expr = apply_lookup(Dummy.created_at, "year", 2024)

    sql = str(expr)
    assert "EXTRACT" in sql.upper()


def test_apply_lookup_unknown():
    with pytest.raises(ValueError) as exc:
        apply_lookup(Dummy.age, "unknown_lookup", 1)

    assert "Unknown lookup" in str(exc.value)
