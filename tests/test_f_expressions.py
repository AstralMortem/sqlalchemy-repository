from utils import ModelA, ModelB
from sqlalchemy_repository.expressions import F


def test_f_resolve_simple():
    f = F("name")

    expr = f.resolve(ModelA)

    assert expr.key == "name"


def test_f_resolve_nested():
    f = F("b__c__name")

    expr = f.resolve(ModelA)

    assert expr.key == "name"


# ------------------------
# arithmetic
# ------------------------


def test_f_add_constant():
    f = F("year") + 10

    expr = f.resolve(ModelB)

    sql = str(expr)
    assert "+" in sql
    assert "year" in sql


def test_f_sub_constant():
    f = F("year") - 5

    expr = f.resolve(ModelB)

    assert "-" in str(expr)


def test_f_mul_constant():
    f = F("year") * 2

    expr = f.resolve(ModelB)

    assert "*" in str(expr)


def test_f_div_constant():
    f = F("year") / 2

    expr = f.resolve(ModelB)

    assert "/" in str(expr)


# # ------------------------
# # F with F
# # ------------------------


def test_f_with_f_add():
    f = F("year") + F("qty")
    expr = f.resolve(ModelB)

    sql = str(expr)
    assert "year" in sql
    assert "qty" in sql
    assert "+" in sql


def test_f_with_f_mul():
    f = F("year") * F("qty")

    expr = f.resolve(ModelB)

    sql = str(expr)
    assert "*" in sql


# # ------------------------
# # chaining
# # ------------------------


def test_f_chaining_operations():
    f = (F("year") + 10) * 2

    expr = f.resolve(ModelB)

    sql = str(expr)

    # ensures composition order applied
    assert "year" in sql
    assert "+" in sql
    assert "*" in sql


# # ------------------------
# # right-hand operations
# # ------------------------


def test_f_radd():
    f = 10 + F("year")

    expr = f.resolve(ModelB)

    sql = str(expr)
    assert "+" in sql


def test_f_rsub():
    f = 100 - F("year")

    expr = f.resolve(ModelB)

    sql = str(expr)
    assert "-" in sql


def test_f_rmul():
    f = 3 * F("year")

    expr = f.resolve(ModelB)

    assert "*" in str(expr)


def test_f_rdiv():
    f = 100 / F("year")

    expr = f.resolve(ModelB)

    assert "/" in str(expr)


# # ------------------------
# # negation
# # ------------------------


def test_f_negation():
    f = -F("year")

    expr = f.resolve(ModelB)

    sql = str(expr)
    assert "-" in sql


# # ------------------------
# # edge cases
# # ------------------------


def test_f_chain_with_nested_field():
    f = F("b__year") * 2

    expr = f.resolve(ModelA)

    sql = str(expr)
    assert "year" in sql
    assert "*" in sql
