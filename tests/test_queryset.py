import pytest
from sqlalchemy_repository.queryset import QuerySet
from utils import ModelB
from sqlalchemy_repository.expressions import Q, F


@pytest.fixture
def qs(session, init_models):
    return QuerySet(ModelB, session)


@pytest.mark.asyncio
class TestBasicQueries:
    async def test_all(self, qs):
        items = await qs.all()
        assert len(items) == 3

    async def test_first(self, qs):
        item = await qs.first()
        assert item.year == 2004

    async def test_last(self, qs):
        item = await qs.last()
        assert item.year == 2006

    async def test_count(self, qs):
        count = await qs.count()
        assert count == 3

    async def test_exist(self, qs):
        assert await qs.filter(year=2004).exist() is True
        # assert await qs.filter(year=1000).exist() is False


@pytest.mark.asyncio
class TestFilterQueries:
    async def test_filter_simple(self, qs):
        items = await qs.filter(year=2004).all()
        assert len(items) == 1

    async def test_filter_q(self, qs):
        items = await qs.filter(Q(year__gte=2005)).all()
        assert len(items) == 2

    async def test_exclude(self, qs):
        items = await qs.exclude(year=2004).all()
        assert len(items) == 2

    async def test_filter_related(self, qs):
        items = await qs.filter(Q(c__name="c1")).all()
        assert len(items) == 2


@pytest.mark.asyncio
async def test_order_by(qs):
    items = await qs.order_by("-year").all()
    assert items[0].year == 2006

    items = await qs.order_by("year").all()
    assert items[0].year == 2004


@pytest.mark.asyncio
async def test_pagination(qs):
    items, total = await qs.paginate(1, 2)
    assert len(items) == 2
    assert total == 3


@pytest.mark.asyncio
async def test_annotate_many(qs):
    rows = await qs.annotate(total=F("qty") * F("year")).all()
    assert len(rows) == 3
    assert hasattr(rows[0], "total")
    assert rows[0].total == rows[0].qty * rows[0].year


@pytest.mark.asyncio
async def test_annotate_one(qs):
    row = await qs.filter(year=2004).annotate(total=F("qty") * F("year")).first()
    assert hasattr(row, "total")
    assert row.total == row.qty * row.year


@pytest.mark.asyncio
async def test_aggregate(qs):
    from sqlalchemy import func

    res = await qs.aggregate(total=func.sum(ModelB.qty))
    assert res["total"] == 6


@pytest.mark.asyncio
async def test_select_related(qs):
    items = await qs.select_related("c").all()
    assert items[0].c.name in {"c1", "c2"}


@pytest.mark.asyncio
async def test_prefetch_related(qs):
    items = await qs.prefetch_related("c").all()
    assert items[0].c.name in {"c1", "c2"}


@pytest.mark.asyncio
async def test_only(qs):
    items = await qs.only("year").all()
    assert hasattr(items[0], "year")


@pytest.mark.asyncio
async def test_complex_query(qs):
    items = (
        await qs.filter(Q(year__gt=2004) & Q(c__name="c1"))
        .annotate(total=F("year") * F("qty"))
        .order_by("-total")
        .all()
    )
    assert len(items) == 1
    assert items[0].year == 2005
    assert items[0].total == items[0].year * items[0].qty
