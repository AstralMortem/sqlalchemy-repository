import pytest
from sqlalchemy_repository.queryset import QuerySet
from sqlalchemy_repository.aggregations import Count, Max, Min, Sum, Avg
from utils import ModelB, ModelC


@pytest.fixture
def qs(session, init_models):
    return QuerySet(ModelB, session)


@pytest.fixture
def qs_c(session, init_models):
    return QuerySet(ModelC, session)


@pytest.mark.asyncio
class TestAggregations:
    async def test_simple_max(self, qs):
        a = await qs.annotate(max_year=Max("year")).first()
        assert a.max_year == 2006

    async def test_simple_min(self, qs):
        a = await qs.annotate(min_year=Min("year")).first()
        assert a.min_year == 2004

    async def test_simple_sum(self, qs):
        a = await qs.all()
        assert len(a) == 3
        test_sum = sum(item.qty for item in a)

        a = await qs.annotate(sum_qty=Sum("qty")).first()
        assert a.sum_qty == test_sum

    async def test_simple_avg(self, qs):
        a = await qs.all()
        assert len(a) == 3
        test_sum = sum(item.qty for item in a)
        test_avg = test_sum / len(a)

        a = await qs.annotate(avg_qty=Avg("qty")).first()
        assert a.avg_qty == test_avg

    async def test_simple_count(self, qs):
        a = await qs.annotate(count=Count("id")).first()
        assert a.count == 3


@pytest.mark.asyncio
class TestFilteredAggregations:
    async def test_simple_max(self, qs):
        a = await qs.filter(c__name="c1").annotate(max_year=Max("year")).first()
        assert a.max_year == 2005

    async def test_simple_min(self, qs):
        a = await qs.filter(c__name="c1").annotate(min_year=Min("year")).first()
        assert a.min_year == 2004

    async def test_simple_sum(self, qs):
        a = await qs.filter(c__name="c1").annotate(sum_qty=Sum("qty")).first()
        assert a.sum_qty == 3


@pytest.mark.asyncio
class TestJoinedAggregations:
    async def test_joined_count(self, qs_c):
        c = await qs_c.filter(name="c1").annotate(count=Count("b_list__id")).first()
        assert c.count == 2

    async def test_joined_sum(self, qs_c):
        c = await qs_c.filter(name="c1").annotate(sum_qty=Sum("b_list__qty")).first()
        assert c.sum_qty == 3
