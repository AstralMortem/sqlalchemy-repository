import pytest
from sqlalchemy_repository.utils.columns import *
from utils import Profile, User, Post

# =========================
# resolve_column
# =========================


def test_resolve_column_direct():
    col = resolve_column(Profile, "age")
    assert col.key == "age"


def test_resolve_column_nested():
    col = resolve_column(User, "profile__age")
    assert col.key == "age"


def test_resolve_column_deep():
    col = resolve_column(Post, "author__profile__age")
    assert col.key == "age"


# =========================
# PK helpers
# =========================


def test_resolve_pk_name():
    assert resolve_pk_name(User) == "id"


def test_resolve_pk_fields():
    fields = resolve_pk_fields(User)
    assert len(fields) == 1


def test_resolve_pk_column():
    col = resolve_pk_column(User)
    assert col.key == "id"


# =========================
# resolve_path_with_joins
# =========================


def test_resolve_path_with_joins_simple():
    col, joins = resolve_path_with_joins(User, "profile__age")

    assert col.key == "age"
    assert len(joins) == 1
    assert joins[0].target_model == Profile


def test_resolve_path_with_joins_deep():
    col, joins = resolve_path_with_joins(Post, "author__profile__age")

    assert col.key == "age"
    assert len(joins) == 2


def test_resolve_path_no_duplicate_joins():
    # same path should not duplicate
    _, joins = resolve_path_with_joins(Post, "author__profile__age")
    deduped = deduplicate_joins(joins)

    assert len(joins) == len(deduped)


# =========================
# deduplicate_joins
# =========================


def test_deduplicate_joins():
    j1 = JoinSpec(User, "x", isouter=True)
    j2 = JoinSpec(User, "x", isouter=True)  # duplicate

    result = deduplicate_joins([j1, j2])

    assert len(result) == 1


def test_deduplicate_different_joins():
    j1 = JoinSpec(User, "x", isouter=True)
    j2 = JoinSpec(Profile, "y", isouter=True)

    result = deduplicate_joins([j1, j2])

    assert len(result) == 2


# =========================
# build_filter_clause
# =========================


def test_build_filter_clause_exact():
    clause, joins = build_filter_clause(User, "name", "Alice")

    assert joins == []


def test_build_filter_clause_nested():
    clause, joins = build_filter_clause(User, "profile__age__gte", 18)

    assert len(joins) >= 1


# =========================
# build_loader_option
# =========================


def test_build_loader_option_selectin():
    def fake_selectinload(attr):
        class Opt:
            def selectinload(self, x):
                return self

        return Opt()

    opt = build_loader_option(User, "profile", fake_selectinload)

    assert opt is not None


def test_build_loader_option_deep():
    def fake_selectinload(attr):
        class Opt:
            def joinedload(self, x):
                return self

        return Opt()

    opt = build_loader_option(Post, "author__profile", fake_selectinload)

    assert opt is not None


# =========================
# edge cases
# =========================


def test_resolve_column_invalid_path():
    with pytest.raises(AttributeError):
        resolve_column(User, "profile__does_not_exist")
