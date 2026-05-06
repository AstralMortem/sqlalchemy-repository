import pytest
from sqlalchemy import column

from sqlalchemy_repository.expressions.f import F, _coerce


# =========================
# HELPERS
# =========================


def test_coerce_with_f():
    f = F("price")
    resolved = _coerce(f)

    assert resolved is not None


def test_coerce_passthrough():
    assert _coerce(10) == 10
    assert _coerce("abc") == "abc"


# =========================
# BASIC RESOLVE
# =========================


def test_f_resolve_column():
    f = F("price")
    expr = f._resolve()

    assert hasattr(expr, "name") or expr.name == "price"


def test_f_repr():
    f = F("price")
    assert "price" in repr(f)


# =========================
# ARITHMETIC OPERATIONS
# =========================


def test_f_add():
    f = F("price") + 10
    assert isinstance(f, F)


def test_radd():
    f = 10 + F("price")
    assert isinstance(f, F)


def test_f_sub():
    f = F("price") - 5
    assert isinstance(f, F)


def test_rsub():
    f = 100 - F("price")
    assert isinstance(f, F)


def test_mul():
    f = F("price") * 2
    assert isinstance(f, F)


def test_truediv():
    f = F("price") / 2
    assert isinstance(f, F)


def test_neg():
    f = -F("price")
    assert isinstance(f, F)


# =========================
# CHAINING EXPRESSIONS
# =========================


def test_chain_operations():
    f = (F("price") + 10) * 2 - 5
    assert isinstance(f, F)


def test_nested_f_coercion():
    f1 = F("price")
    f2 = F("discount")

    expr = f1 + f2
    assert isinstance(expr, F)


# =========================
# ORDERING
# =========================


def test_asc():
    f = F("price")
    expr = f.asc()

    assert expr is not None


def test_desc():
    f = F("price")
    expr = f.desc()

    assert expr is not None


# =========================
# EDGE CASES
# =========================


def test_multiple_operations():
    f = F("price")

    expr = f + 10 - 2 * 3 / 2
    assert isinstance(expr, F)


def test_neg_chain():
    f = -(-F("price"))
    assert isinstance(f, F)


# =========================
# RESOLVE STABILITY
# =========================


def test_resolve_consistency():
    f = F("price")

    a = f._resolve()
    b = f._resolve()

    # should be stable column reference
    assert a.name == b.name


def test_expr_preserved():
    base = column("price")
    f = F("price", base)

    assert f._resolve() is base


# =========================
# MIXED TYPES
# =========================


def test_f_with_literal_math():
    f = F("price")

    expr = f + 100
    assert isinstance(expr, F)

    expr2 = f * 1.5
    assert isinstance(expr2, F)


def test_large_expression_tree():
    f = F("price")

    expr = (((f + 10) * 2) / 3) - 5
    assert isinstance(expr, F)
