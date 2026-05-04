import pytest
from sqlalchemy_repository.repository import BaseRepository
from sqlalchemy_repository.queryset import QuerySet
from utils import ModelB


@pytest.fixture
def repo(session, init_models):
    class TestRepo(BaseRepository[ModelB, int]):
        model = ModelB

    return TestRepo(session)


@pytest.mark.asyncio
async def test_qs(repo):
    qs = repo.objects
    assert isinstance(qs, QuerySet)
    assert qs._model is repo.model


@pytest.mark.asyncio
class TestRead:
    async def test_get_by_pk(self, repo):
        b1 = await repo.get_by_pk(1)
        assert b1.year == 2004

    async def test_get_by_pk_none(self, repo):
        b1 = await repo.get_by_pk("1000")
        assert b1 is None

    async def test_get_by_field(self, repo):
        b1 = await repo.get_by_field("year", 2004)
        assert b1.year == 2004

    async def test_get_by_field_none(self, repo):
        b1 = await repo.get_by_field("year", 1000)
        assert b1 is None

    async def get_filter(self, repo):
        b1 = await repo.get(year=2005)
        assert b1.year == 2005

    async def get_many_by_filter(self, repo):
        items = await repo.filter(year__gt=2004)
        assert len(items) == 2

    async def get_all(self, repo):
        items = await repo.all()
        assert len(items) == 3

    async def get_paginated(self, repo):
        items, total = await repo.paginate(1, 2)
        assert len(items) == 2
        assert total == 3


@pytest.mark.asyncio
class TestCreate:
    async def test_create(self, repo):
        await repo.create(dict(year=2008, qty=4), _commit=True)

        b4 = await repo.get(year=2008)
        assert b4.year == 2008
        assert b4.qty == 4

    async def test_bulk_create(self, repo):
        payload = [
            dict(year=1000, qty=4),
            dict(year=1001, qty=5),
        ]

        await repo.bulk_create(payload, _commit=True)

        items = await repo.filter(year__lt=1002)
        assert len(items) == 2

    async def test_get_or_create_retrieve(self, repo):

        obj, created = await repo.get_or_create(dict(year=2004, qty=1))
        assert created is False
        assert obj.id == 1

    async def test_get_or_create_creation(self, repo):
        obj, created = await repo.get_or_create(dict(year=2005, qty=1))
        assert created is True
        assert obj.year == 2005
        assert obj.qty == 1


@pytest.mark.asyncio
class TestUpdate:
    async def test_update(self, repo):
        obj = await repo.get_by_field("year", 2004)
        await repo.update(obj, dict(year=2005))
        obj = await repo.filter(year=2005)
        assert len(obj) == 2

    async def test_bulk_update(self, repo):
        objects = await repo.filter(year__gte=2004)
        await repo.bulk_update(objects, dict(qty=1000))
        items = await repo.filter(qty__gte=1000)
        assert len(items) == 3

    async def test_raw_save(self, repo):

        obj = await repo.get(year=2004)

        obj.year = 2000
        await repo.raw_save(obj, _commit=True)

        obj = await repo.get(year=2000)
        assert obj is not None


@pytest.mark.asyncio
class TestDelete:
    async def test_delete(self, repo):
        obj = await repo.get(year=2004)
        await repo.delete(obj, _commit=True)
        obj = await repo.get(year=2004)
        assert obj is None

    async def test_bulk_delete(self, repo):
        objects = await repo.filter(year__gte=2004)
        await repo.bulk_delete(objects, _commit=True)
        items = await repo.filter(year__gte=2004)
        assert len(items) == 0
