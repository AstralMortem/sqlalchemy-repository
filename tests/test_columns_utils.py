import pytest
from sqlalchemy_repository.utils.columns import resolve_column, make_order_expr

from utils import ModelA, ModelB, ModelC


def test_resolve_simpl_column():
    col, joins = resolve_column(ModelB, ["year"])

    assert col.key == "year"
    assert len(joins) == 0


def test_resolve_column_nested_relationship():
    col, joins = resolve_column(ModelA, ["b", "c", "name"])

    assert col.key == "name"

    assert len(joins) == 2
    assert joins[0][0] == ModelA
    assert joins[0][1].key == "b"
    assert joins[1][0] == ModelB
    assert joins[1][1].key == "c"


def test_resolve_column_invalid_relationship():
    with pytest.raises(AttributeError) as exc:
        resolve_column(ModelA, ["wrong_rel", "name"])


def test_resolve_column_invalid_attribute():
    with pytest.raises(AttributeError) as exc:
        resolve_column(ModelA, ["year", "wrong_attr"])


def test_make_order_expr_asc():
    from sqlalchemy.sql.operators import asc_op

    expr = make_order_expr(ModelB, "year")

    assert expr.element.key == "year"
    assert expr.modifier is asc_op


def test_make_order_expr_desc():
    from sqlalchemy.sql.operators import desc_op

    expr = make_order_expr(ModelB, "-year")

    assert expr.element.key == "year"
    assert expr.modifier is desc_op


def test_make_order_expr_nested():
    from sqlalchemy.sql.operators import desc_op

    expr = make_order_expr(ModelA, "-b__year")

    assert expr.element.key == "year"
    assert expr.modifier is desc_op
