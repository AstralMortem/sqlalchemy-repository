from sqlalchemy_repository.expressions import Q
from utils import ModelA


def test_q_simple_exact():
    q = Q(name="test")
    joins = []
    expr = q.resolve(ModelA, joins)
    assert "name" in str(expr)
    assert "=" in str(expr)
    assert len(joins) == 0


def test_q_multiple_and():
    q = Q(name="test", id=1)

    expr = q.resolve(ModelA)

    sql = str(expr)
    assert "AND" in sql


def test_q_or_operator():
    q = Q(name="a") | Q(name="b")

    expr = q.resolve(ModelA)

    sql = str(expr)
    assert "OR" in sql


def test_q_and_operator():
    q = Q(name="a") & Q(id=1)

    expr = q.resolve(ModelA)

    sql = str(expr)
    assert "AND" in sql


def test_q_negation():
    q = ~Q(name="a")
    expr = q.resolve(ModelA)

    sql = str(expr)
    assert "NOT" in sql or "!=" in sql  # wierd


def test_q_nested_relationship():
    q = Q(b__c__name="hello")

    joins = []
    expr = q.resolve(ModelA, joins)

    assert "name" in str(expr)

    # should collect 2 joins: A->b, B->c
    assert len(joins) == 2
    assert joins[0][1].key == "b"
    assert joins[1][1].key == "c"


def test_q_join_deduplication():
    q = Q(b__c__name="a") & Q(b__c__id=1)

    joins = []
    q.resolve(ModelA, joins)

    # should NOT duplicate joins
    assert len(joins) == 2


def test_q_complex_tree():
    q = (Q(name="a") & Q(b__c__name="x")) | Q(id=5)

    joins = []
    expr = q.resolve(ModelA, joins)

    sql = str(expr)

    assert "OR" in sql
    assert "AND" in sql

    # joins still deduplicated
    assert len(joins) == 2


def test_q_empty():
    q = Q()

    expr = q.resolve(ModelA)

    # should produce TRUE expression
    assert "true" in str(expr).lower()


def test_q_nested_negation():
    q = ~(Q(name="a") | Q(name="b"))

    expr = q.resolve(ModelA)

    sql = str(expr)
    assert "NOT" in sql
    assert "OR" in sql
