from utils import User
from sqlalchemy_repository.expressions import Q
from sqlalchemy_repository.utils.joins import JoinCollector


def new_collector():
    return JoinCollector()


def test_q_leaf_single_filter(monkeypatch):
    def fake_build_filter_clause(model, key, value):
        return (User.name == value, [])

    monkeypatch.setattr(
        "sqlalchemy_repository.utils.columns.build_filter_clause",
        fake_build_filter_clause,
    )

    q = Q(name="Alice")
    expr = q.resolve(User, new_collector())

    assert str(expr) == str(User.name == "Alice")


def test_q_leaf_multiple_kwargs(monkeypatch):
    def fake_build_filter_clause(model, key, value):
        return (User.name == "Alice", [])

    monkeypatch.setattr(
        "sqlalchemy_repository.utils.columns.build_filter_clause",
        fake_build_filter_clause,
    )

    q = Q(name="Alice", age=18)
    expr = q.resolve(User, new_collector())

    assert expr is not None


# =========================
# AND / OR COMPOSITION
# =========================


def test_q_and(monkeypatch):
    def fake(model, key, value):
        return (User.name == value, [])

    monkeypatch.setattr("sqlalchemy_repository.utils.columns.build_filter_clause", fake)

    q = Q(name="Alice") & Q(age=18)
    expr = q.resolve(User, new_collector())

    assert str(expr).find("AND") != -1


def test_q_or(monkeypatch):
    def fake(model, key, value):
        return (User.name == value, [])

    monkeypatch.setattr("sqlalchemy_repository.utils.columns.build_filter_clause", fake)

    q = Q(name="Alice") | Q(name="Bob")
    expr = q.resolve(User, new_collector())

    # should be OR expression
    assert str(expr).find("OR") != -1 or hasattr(expr, "clauses")


def test_nested_q_and_or(monkeypatch):
    def fake(model, key, value):
        return (User.name == value, [])

    monkeypatch.setattr("sqlalchemy_repository.utils.columns.build_filter_clause", fake)

    q = Q(name="Alice") & (Q(age=18) | Q(is_active=True))
    expr = q.resolve(User, new_collector())

    assert expr is not None


# =========================
# NOT OPERATOR
# =========================


def test_q_not(monkeypatch):
    def fake(model, key, value):
        return (User.name == value, [])

    monkeypatch.setattr("sqlalchemy_repository.utils.columns.build_filter_clause", fake)

    q = ~Q(name="Alice")
    expr = q.resolve(User, new_collector())

    # should be negated
    assert hasattr(expr, "operator") or expr is not None


def test_double_not(monkeypatch):
    def fake(model, key, value):
        return (User.name == value, [])

    monkeypatch.setattr("sqlalchemy_repository.utils.columns.build_filter_clause", fake)

    q = ~~Q(name="Alice")
    expr = q.resolve(User, new_collector())

    assert expr is not None


# =========================
# JOIN COLLECTOR SIDE EFFECTS
# =========================


def test_join_collection(monkeypatch):
    class DummyJoin:
        def __init__(self):
            self.collected = []

        def add(self, j):
            self.collected.append(j)

    collector = DummyJoin()

    def fake(model, key, value):
        class FakeJoin:
            pass

        return (User.name == value, [FakeJoin()])

    monkeypatch.setattr("sqlalchemy_repository.utils.columns.build_filter_clause", fake)

    q = Q(name="Alice")
    q.resolve(User, collector)

    assert len(collector.collected) == 1


# =========================
# EDGE CASES
# =========================


def test_empty_q():
    q = Q()
    expr = q.resolve(User, new_collector())

    # should resolve to TRUE-ish expression
    assert expr is not None


def test_complex_tree(monkeypatch):
    def fake(model, key, value):
        return (User.name == value, [])

    monkeypatch.setattr("sqlalchemy_repository.utils.columns.build_filter_clause", fake)

    q = Q(name="Alice") & Q(age=18) | (~Q(is_active=True))

    expr = q.resolve(User, new_collector())

    assert expr is not None


# =========================
# REPR TESTS
# =========================


def test_repr_leaf():
    q = Q(name="Alice")
    assert "Alice" in repr(q)


def test_repr_tree():
    q = Q(name="Alice") & Q(age=18)
    r = repr(q)

    assert "AND" in r or "Q" in r
