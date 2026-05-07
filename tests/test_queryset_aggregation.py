from sqlalchemy_repository import QuerySet, Count, Sum, Avg, Max, Min
import pytest
from utils import Post


@pytest.fixture
def qs(session, data) -> QuerySet:
    return QuerySet(Post, session)


_TOTAL_RATING = sum([1.0 * i + 1 for i in range(5)])

# ── Basic aggregate() results ─────────────────────────────────────────────────


class TestAggregateBasic:
    async def test_count_star(self, qs):
        result = await qs.aggregate(n=Count())
        assert result["n"] == 5

    async def test_count_field(self, qs):
        result = await qs.aggregate(n=Count("id"))
        assert result["n"] == 5

    async def test_sum(self, qs):
        result = await qs.aggregate(total=Sum("rating"))
        assert result["total"] == _TOTAL_RATING

    async def test_avg(self, qs):
        result = await qs.aggregate(avg=Avg("rating"))
        expected = _TOTAL_RATING / 5
        assert result["avg"] == pytest.approx(expected, rel=1e-4)

    async def test_min(self, qs):
        result = await qs.aggregate(lo=Min("rating"))
        assert result["lo"] == pytest.approx(1.0)

    async def test_max(self, qs):
        result = await qs.aggregate(hi=Max("rating"))
        assert result["hi"] == pytest.approx(5.0)


# ── Multiple aggregates in one call ──────────────────────────────────────────


class TestAggregateMultiple:
    async def test_all_in_one_call(self, qs):
        result = await qs.aggregate(
            total=Count(),
            total_rating=Sum("rating"),
            avg_rating=Avg("rating"),
            min_rating=Min("rating"),
            max_rating=Max("rating"),
        )
        assert set(result.keys()) == {
            "total",
            "total_rating",
            "avg_rating",
            "min_rating",
            "max_rating",
        }
        assert result["total"] == 5
        assert result["total_rating"] == _TOTAL_RATING
        assert result["min_rating"] == pytest.approx(1.0)
        assert result["max_rating"] == pytest.approx(5.0)
        assert result["avg_rating"] == _TOTAL_RATING / 5

    async def test_returns_dict(self, qs):
        result = await qs.aggregate(n=Count())
        assert isinstance(result, dict)

    async def test_zero_args_returns_empty_dict(self, qs):
        result = await qs.aggregate()
        assert result == {}


# # ── aggregate() respects filters ─────────────────────────────────────────────


class TestAggregateFiltered:
    async def test_count_with_filter(self, qs):
        result = await qs.filter(author_id=1).aggregate(n=Count())
        assert result["n"] == 2

    async def test_sum_with_filter(self, qs):
        result = await qs.filter(author_id=2).aggregate(total=Sum("rating"))
        assert result["total"] == 1.0 + 2.0 + 3.0

    async def test_avg_with_filter(self, qs):
        result = await qs.filter(author_id=2).aggregate(avg=Avg("rating"))
        expected = (1.0 + 2.0 + 3.0) / 3
        assert result["avg"] == pytest.approx(expected, rel=1e-4)

    async def test_min_with_filter(self, qs):
        result = await qs.filter(author_id=1).aggregate(lo=Min("rating"))
        assert result["lo"] == pytest.approx(4.0)

    async def test_max_with_filter(self, qs):
        result = await qs.filter(author_id=1).aggregate(hi=Max("rating"))
        assert result["hi"] == pytest.approx(5.0)

    async def test_exclude(self, qs):
        result = await qs.exclude(author_id=1).aggregate(n=Count())
        assert result["n"] == 3

    async def test_chained_filters(self, qs):
        result = await qs.filter(author_id=2).filter(rating__gt=2.0).aggregate(n=Count())
        assert result["n"] == 1

    async def test_rating_range_filter(self, qs):
        result = await qs.filter(rating__gte=2.0).aggregate(n=Count(), hi=Max("rating"))
        # Laptop(999.99), Phone(499.99), Headphones(149.99)
        assert result["n"] == 4
        assert result["hi"] == pytest.approx(5.0)


# # ── aggregate() ignores limit / offset / order_by ────────────────────────────


class TestAggregateStripsQueryModifiers:
    async def test_strips_limit(self, qs):
        """limit(2) must not reduce the aggregate to only 2 rows."""
        result = await qs.limit(2).aggregate(n=Count())
        assert result["n"] == 5

    async def test_strips_offset(self, qs):
        result = await qs.offset(5).aggregate(n=Count())
        assert result["n"] == 5

    async def test_strips_order_by(self, qs):
        # order_by should have zero effect on aggregate results
        r1 = await qs.order_by("rating").aggregate(total=Sum("rating"))
        r2 = await qs.order_by("-rating").aggregate(total=Sum("rating"))
        assert r1["total"] == r2["total"]

    async def test_strips_existing_annotations(self, qs):
        """annotate() on the qs must not interfere with aggregate()."""
        result = await qs.annotate(dummy=Count("id")).aggregate(n=Count())
        assert result["n"] == 5


# # ── Empty queryset ────────────────────────────────────────────────────────────


class TestAggregateEmpty:
    async def test_count_empty(self, qs):
        result = await qs.filter(id=9999).aggregate(n=Count())
        # COUNT on empty set returns 0, not None
        assert result["n"] == 0

    async def test_sum_empty_returns_none(self, qs):
        result = await qs.filter(id=9999).aggregate(total=Sum("rating"))
        # SUM on empty set is NULL → None in Python
        assert result["total"] is None

    async def test_avg_empty_returns_none(self, qs):
        result = await qs.filter(id=9999).aggregate(avg=Avg("rating"))
        assert result["avg"] is None

    async def test_min_empty_returns_none(self, qs):
        result = await qs.filter(id=9999).aggregate(lo=Min("rating"))
        assert result["lo"] is None

    async def test_max_empty_returns_none(self, qs):
        result = await qs.filter(id=9999).aggregate(hi=Max("rating"))
        assert result["hi"] is None

    async def test_multiple_on_empty(self, qs):
        result = await qs.filter(id=9999).aggregate(
            n=Count(), total=Sum("rating"), avg=Avg("rating")
        )
        assert result["n"] == 0
        assert result["total"] is None
        assert result["avg"] is None


# # ── Count-specific behaviour ──────────────────────────────────────────────────


class TestCountAggregate:
    async def test_count_star_default(self, qs):
        result = await qs.aggregate(n=Count())
        assert result["n"] == 5

    async def test_count_field_same_as_star_when_no_nulls(self, qs):
        """All our rows have non-null ids, so COUNT(id) == COUNT(*)."""
        r_star = await qs.aggregate(n=Count())
        r_field = await qs.aggregate(n=Count("id"))
        assert r_star["n"] == r_field["n"]

    async def test_count_distinct(self, qs):
        result = await qs.aggregate(n=Count("author_id", distinct=True))
        assert result["n"] == 2  # three unique categories

    async def test_count_distinct_vs_non_distinct(self, qs):
        r_all = await qs.aggregate(n=Count("author_id"))
        r_dist = await qs.aggregate(n=Count("author_id", distinct=True))
        assert r_all["n"] == 5
        assert r_dist["n"] == 2


# # ── resolve_subquery on Aggregate subclasses ──────────────────────────────────


class TestResolveSubquery:
    """
    Unit-test the resolve_subquery hook in isolation by building a tiny
    subquery from the real session and checking the SQL expression type.
    """

    async def test_sum_resolve_subquery(self, session):
        from sqlalchemy import select
        from sqlalchemy.sql.elements import Label

        subq = select(
            Post.id.label("id"),
            Post.rating.label("rating"),
        ).subquery()

        agg = Sum("rating")
        expr = agg.resolve_subquery(subq)
        # Should be a func expression; wrapping in label() should not raise
        labeled = expr.label("result")
        assert labeled is not None

    async def test_count_star_resolve_subquery(self, session):
        from sqlalchemy import select

        subq = select(Post.id.label("id")).subquery()
        agg = Count()  # field="*"
        expr = agg.resolve_subquery(subq)
        assert expr is not None

    async def test_count_distinct_resolve_subquery(self, session):
        from sqlalchemy import select

        subq = select(
            Post.author_id.label("author_id"),
        ).subquery()

        agg = Count("author_id", distinct=True)
        expr = agg.resolve_subquery(subq)
        # distinct() applied — just check it doesn't raise and returns something
        assert expr is not None

    async def test_unknown_field_raises(self, session):
        from sqlalchemy import select

        subq = select(Post.id.label("id")).subquery()
        agg = Sum("nonexistent_field")
        with pytest.raises(ValueError):
            agg.resolve_subquery(subq)

    async def test_count_star_unknown_field_does_not_raise(self, session):
        """COUNT(*) must never hit the column-lookup path."""
        from sqlalchemy import select

        # subquery has no "nonexistent" col, but field="*" skips lookup
        subq = select(Post.id.label("id")).subquery()
        agg = Count()  # field="*"
        expr = agg.resolve_subquery(subq)  # must not raise
        assert expr is not None


# # ── aggregate() after values() ────────────────────────────────────────────────


class TestAggregateAfterValues:
    """
    aggregate() must strip values_fields from the subquery so it runs a
    plain SELECT of all columns rather than a narrow column select.
    """

    async def test_aggregate_strips_values_fields(self, qs):
        result = await qs.values("name").aggregate(n=Count())
        # Should count all 7 products, not fail on stripped column set
        assert result["n"] == 5

    async def test_aggregate_strips_values_list_fields(self, qs):
        result = await qs.values_list("name").aggregate(n=Count())
        assert result["n"] == 5
