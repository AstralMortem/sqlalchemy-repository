import pytest

from sqlalchemy_repository.expressions.aggregations import Count, Max, Min
from sqlalchemy_repository.queryset import QuerySet
from utils import Post


@pytest.fixture
def qs(session, data):
    return QuerySet(Post, session)


@pytest.mark.asyncio
class TestValuesList:
    async def test_returns_list_of_tuples(self, qs):
        results = await qs.values_list("id", "title").all()
        assert isinstance(results, list)
        assert all(isinstance(r, tuple) for r in results)

    async def test_tuple_length_matches_fields(self, qs):
        results = await qs.values_list("id", "title", "rating").all()
        assert all(len(r) == 3 for r in results)

    async def test_single_field_tuple(self, qs):
        results = await qs.values_list("title").all()
        assert all(len(r) == 1 for r in results)
        assert all(isinstance(r[0], str) for r in results)

    async def test_all_rows_returned(self, qs):
        results = await qs.values_list("id").all()
        assert len(results) == 5

    async def test_correct_values(self, qs):
        results = await qs.filter(id=1).values_list("title", "rating").all()
        assert results == [("A", 4.0)]

    async def test_respects_filter(self, qs):
        results = await qs.filter(author_id=1).values_list("title").all()
        titles = {r[0] for r in results}
        assert titles == {"A", "B"}

    async def test_respects_order_by(self, qs):
        results = await qs.order_by("rating").values_list("rating").all()
        ratings = [r[0] for r in results]
        assert ratings == sorted(ratings)

    async def test_respects_limit_offset(self, qs):
        results = await qs.order_by("id").offset(1).limit(3).values_list("id").all()
        assert [r[0] for r in results] == [2, 3, 4]

    # ── flat=True ────────────────────────────────────────────────────────────

    async def test_flat_returns_plain_list(self, qs):
        results = await qs.values_list("title", flat=True).all()
        assert isinstance(results, list)
        assert all(isinstance(r, str) for r in results)

    async def test_flat_returns_correct_count(self, qs):
        results = await qs.values_list("id", flat=True).all()
        assert len(results) == 5

    async def test_flat_correct_values(self, qs):
        results = await qs.filter(author_id=2).order_by("id").values_list("id", flat=True).all()
        assert results == [3, 4, 5]

    async def test_flat_respects_order_by(self, qs):
        results = await qs.order_by("-rating").values_list("rating", flat=True).all()
        assert results == sorted(results, reverse=True)

    async def test_flat_multiple_fields_raises(self, qs):
        with pytest.raises(ValueError, match="flat=True requires exactly one field"):
            await qs.values_list("id", "title", flat=True).all()

    # ── values_list + annotate ────────────────────────────────────────────────

    async def test_values_list_with_annotate_count(self, qs):
        results = await (
            qs.values_list("author_id").annotate(n=Count("id")).order_by("author_id").all()
        )
        # Each row is a tuple (author_id, n)
        assert len(results) == 2
        by_cat = {r[0]: r[1] for r in results}
        assert by_cat[1] == 2
        assert by_cat[2] == 3

    async def test_values_list_with_annotate_min_max(self, qs):
        results = await (
            qs.values_list("author_id")
            .annotate(min_rating=Min("rating"), max_rating=Max("rating"))
            .order_by("author_id")
            .all()
        )
        # Tuple order: (author_id, min_rating, max_rating)
        electronics = next(r for r in results if r[0] == 1)
        assert electronics[1] == pytest.approx(4.0)
        assert electronics[2] == pytest.approx(5.0)

    async def test_values_list_first(self, qs):
        result = await qs.order_by("id").values_list("id", "title").first()
        assert result == (1, "A")

    async def test_values_list_flat_first(self, qs):
        result = await qs.order_by("id").values_list("id", flat=True).first()
        assert result == 1

    async def test_values_list_immutability(self, qs):
        vl_qs = qs.values_list("id")
        # Original still yields ORM objects
        orm_results = await qs.all()
        assert all(isinstance(r, Post) for r in orm_results)
