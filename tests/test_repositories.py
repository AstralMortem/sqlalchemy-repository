import pytest
from unittest.mock import AsyncMock, MagicMock

from sqlalchemy_repository.repository import (
    BaseRepository,
    BaseReadRepository,
    BaseWriteRepository,
)

from sqlalchemy_repository.expressions import F


# =========================
# Fake model
# =========================


class User:
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)


# =========================
# FIXTURE: mocked session
# =========================


@pytest.fixture
def session():
    s = AsyncMock()
    s.add = MagicMock()
    s.add_all = MagicMock()
    s.flush = AsyncMock()
    s.commit = AsyncMock()
    s.delete = AsyncMock()
    s.get = AsyncMock(return_value=None)
    return s


# =========================
# FIXTURE: repository
# =========================


class UserRepo(BaseRepository):
    model = User


@pytest.fixture
def repo(session):
    return UserRepo(session)


# =========================
# RETRIEVE MIXIN
# =========================


@pytest.mark.asyncio
async def test_get_by_pk(session, repo):
    session.get.return_value = User(id=1)

    obj = await repo.get_by_pk(1)

    session.get.assert_called_once_with(User, 1)
    assert obj.id == 1


@pytest.mark.asyncio
async def test_get_by_field(repo, session, monkeypatch):
    qs = MagicMock()
    qs.filter.return_value.first = AsyncMock(return_value=User(name="Alice"))

    monkeypatch.setattr(UserRepo, "objects", qs)

    obj = await repo.get_by_field("name", "Alice")

    assert obj.name == "Alice"


@pytest.mark.asyncio
async def test_get(repo, session, monkeypatch):
    qs = MagicMock()
    qs.filter.return_value.first = AsyncMock(return_value=User(id=1))

    monkeypatch.setattr(UserRepo, "objects", qs)

    obj = await repo.get(name="Alice")

    assert obj.id == 1


@pytest.mark.asyncio
async def test_filter(repo, monkeypatch):
    qs = MagicMock()
    qs.filter.return_value.all = AsyncMock(return_value=[User(id=1), User(id=2)])

    monkeypatch.setattr(UserRepo, "objects", qs)

    res = await repo.filter(name="Alice")

    assert len(res) == 2


@pytest.mark.asyncio
async def test_paginate(repo, monkeypatch):
    qs = MagicMock()
    qs.filter.return_value.paginate = AsyncMock(return_value=([User(id=1)], 1))

    monkeypatch.setattr(UserRepo, "objects", qs)

    items, total = await repo.paginate(1, 10)

    assert total == 1
    assert len(items) == 1


@pytest.mark.asyncio
async def test_all(repo, monkeypatch):
    qs = MagicMock()
    qs.all = AsyncMock(return_value=[User(id=1)])

    monkeypatch.setattr(UserRepo, "objects", qs)

    res = await repo.all()

    assert len(res) == 1


# =========================
# CREATE MIXIN
# =========================


@pytest.mark.asyncio
async def test_create(repo, session):
    obj = await repo.create({"name": "Alice"}, _commit=False)

    session.add.assert_called_once()
    session.flush.assert_called_once()
    assert obj.name == "Alice"


@pytest.mark.asyncio
async def test_create_with_f_expression(repo, session):
    f = F("price", 100)

    obj = await repo.create({"price": f})

    assert obj.price == 100


@pytest.mark.asyncio
async def test_bulk_create(repo, session):
    objs = await repo.bulk_create([{"name": "A"}, {"name": "B"}], _commit=False)

    session.add_all.assert_called_once()
    assert len(objs) == 2


@pytest.mark.asyncio
async def test_get_or_create_existing(repo, monkeypatch):
    qs = MagicMock()
    qs.filter.return_value.first = AsyncMock(return_value=User(id=1))

    monkeypatch.setattr(UserRepo, "objects", qs)

    obj, created = await repo.get_or_create({"name": "Alice"})

    assert created is False
    assert obj.id == 1


@pytest.mark.asyncio
async def test_get_or_create_new(repo, session, monkeypatch):
    qs = MagicMock()
    qs.filter.return_value.first = AsyncMock(return_value=None)

    monkeypatch.setattr(UserRepo, "objects", qs)

    obj, created = await repo.get_or_create({"name": "Alice"})

    assert created is True
    session.add.assert_called_once()


# =========================
# UPDATE MIXIN
# =========================


@pytest.mark.asyncio
async def test_update(repo, session):
    obj = User(name="Old")

    updated = await repo.update(obj, {"name": "New"})

    assert updated.name == "New"
    session.flush.assert_called_once()


@pytest.mark.asyncio
async def test_update_with_f(repo, session):
    f = F("price", 200)

    obj = User(price=0)

    updated = await repo.update(obj, {"price": f})

    assert updated.price == 200


@pytest.mark.asyncio
async def test_bulk_update(repo, session):
    objs = [User(name="A"), User(name="B")]

    res = await repo.bulk_update(objs, {"name": "X"})

    assert all(o.name == "X" for o in res)


@pytest.mark.asyncio
async def test_raw_save(repo, session):
    obj = User()

    res = await repo.raw_save(obj)

    assert res is obj
    session.flush.assert_called_once()


# =========================
# DELETE MIXIN
# =========================


@pytest.mark.asyncio
async def test_delete(repo, session):
    obj = User(id=1)

    res = await repo.delete(obj)

    session.delete.assert_called_once_with(obj)
    assert res is None


@pytest.mark.asyncio
async def test_bulk_delete(repo, session):
    objs = [User(id=1), User(id=2)]

    res = await repo.bulk_delete(objs)

    assert session.delete.call_count == 2
    assert res is None


# =========================
# BASE REPO
# =========================


def test_repr(repo):
    r = repr(repo)

    assert "UserRepo" in r or "model" in r
