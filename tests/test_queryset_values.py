import pytest
from sqlalchemy_repository.expressions.aggregations import Avg, Count, Sum
from sqlalchemy_repository.queryset import QuerySet
from utils import Post

@pytest.fixture
def qs(session, data):
    return QuerySet(Post, session)

@pytest.mark.asyncio
class TestValues:
    async def test_returns_list_of_dicts(self, qs):
        results = await qs.values("id", "title").all()
        assert isinstance(results, list)
        assert all(isinstance(r, dict) for r in results)
 
    async def test_dict_has_exactly_requested_keys(self, qs):
        results = await qs.values("title", "rating").all()
        assert all(set(r.keys()) == {"title", "rating"} for r in results)
 
    async def test_single_field(self, qs):
        results = await qs.values("title").all()
        assert all(set(r.keys()) == {"title"} for r in results)
        assert len(results) == 5
 
    async def test_all_rows_returned(self, qs):
        results = await qs.values("id").all()
        assert len(results) == 5
 
    async def test_correct_values(self, qs):
        results = await qs.filter(id=1).values("title", "rating").all()
        assert results == [{"title": "A", "rating": 4.0}]
 
    async def test_respects_filter(self, qs):
        results = await qs.filter(author_id=1).values("title").all()
        titles = {r["title"] for r in results}
        assert titles == {"A", "B"}
 
    async def test_respects_order_by_asc(self, qs):
        results = await qs.order_by("rating").values("title").all()
        ratings_via_title = [r["title"] for r in results]
        assert ratings_via_title[0] == "C"   # cheapest
        assert ratings_via_title[-1] == "B"   # most expensive
 
    async def test_respects_order_by_desc(self, qs):
        results = await qs.order_by("-rating").values("rating").all()
        ratings = [r["rating"] for r in results]
        assert ratings == sorted(ratings, reverse=True)
 
    async def test_respects_limit(self, qs):
        results = await qs.order_by("id").limit(3).values("id").all()
        assert [r["id"] for r in results] == [1, 2, 3]
 
    async def test_respects_offset(self, qs):
        results = await qs.order_by("id").offset(3).values("id").all()
        assert [r["id"] for r in results] == [4, 5]
 
    async def test_limit_and_offset(self, qs):
        results = await qs.order_by("id").offset(2).limit(2).values("id").all()
        assert [r["id"] for r in results] == [3, 4]
 
    async def test_immutability_of_original_qs(self, qs):
        """Calling values() must not mutate the original queryset."""
        values_qs = qs.values("title")
        all_results = await qs.all()
        # Original qs returns ORM objects, not dicts
        assert all(isinstance(r, Post) for r in all_results)
        values_results = await values_qs.all()
        assert all(isinstance(r, dict) for r in values_results)
 
    async def test_values_with_annotate_count(self, qs):
        """values("category_id").annotate(n=Count("id")) groups by category_id."""
        results = await (
            qs.values("author_id")
            .annotate(n=Count("id"))
            .order_by("author_id")
            .all()
        )
        assert len(results) == 2
        # Electronics has 3 products, Books 2, Clothing 2
        counts = {r["author_id"]: r["n"] for r in results}
        assert counts[1] == 2
        assert counts[2] == 3
 
    async def test_values_with_annotate_avg(self, qs):
        results = await (
            qs.values("author_id")
            .annotate(total_rating=Avg("rating"))
            .order_by("author_id")
            .all()
        )
        totals = {r["author_id"]: r["total_rating"] for r in results}
        assert totals[1] == (4.0 + 5.0) / 2
        assert totals[2] == (1.0 + 2.0 + 3.0) / 3   # 50+30
 
    async def test_values_with_annotate_filtered(self, qs):
        results = await (
            qs.filter(author_id=1)
            .values("author_id")
            .annotate(total=Sum("rating"))
            .all()
        )
        assert len(results) == 1
        assert results[0]["total"] == 4.0 + 5.0
 
    async def test_values_first(self, qs):
        result = await qs.order_by("id").values("id", "title").first()
        assert result == {"id": 1, "title": "A"}
 
    async def test_values_first_empty(self, qs):
        result = await qs.filter(id=9999).values("id").first()
        assert result is None
 
    async def test_values_with_exclude(self, qs):
        results = await qs.exclude(author_id=1).values("author_id").all()
        cat_ids = {r["author_id"] for r in results}
        assert 1 not in cat_ids
        assert cat_ids == {2}