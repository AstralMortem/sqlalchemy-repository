import pytest
from sqlalchemy_repository.expressions.aggregations import Count, Max, Min, Sum
from sqlalchemy_repository.queryset import QuerySet
from sqlalchemy_repository.utils.columns import resolve_traversal_field
from utils import Post, User


@pytest.fixture
async def qs(session, data) -> QuerySet:
    return QuerySet(Post, session)


# ── resolve_traversal_field() unit tests ──────────────────────────────────────


class TestResolveTraversalField:
    def test_single_segment_returns_none(self):
        result = resolve_traversal_field(Post, "title")
        assert result is None

    def test_direct_fk_field_returns_none(self):
        # "author_id" is a plain column, not a relationship traversal
        result = resolve_traversal_field(Post, "author_id")
        assert result is None

    def test_valid_traversal_returns_traversal_column(self):
        result = resolve_traversal_field(Post, "author__name")
        assert result is not None

    def test_col_expr_label_matches_dunder_key(self):
        result = resolve_traversal_field(Post, "author__name")
        assert result.col_expr.key == "author__name"

    def test_join_spec_is_outer(self):
        result = resolve_traversal_field(Post, "author__name")
        assert result.join_spec is not None
        assert result.join_spec.isouter is True

    def test_join_spec_target_is_related_model(self):
        result = resolve_traversal_field(Post, "author__name")
        assert result.join_spec.target_model is User

    def test_group_col_is_raw_unlabelled(self):
        result = resolve_traversal_field(Post, "author__name")
        # raw column has no label set (key == col name on the remote table)
        assert result.group_col.key == "name"

    def test_unknown_relationship_returns_none(self):
        # "nonexistent" is not a relationship on Post, so we get None
        result = resolve_traversal_field(Post, "nonexistent__field")
        assert result is None

    def test_different_traversal_fields_same_relation(self):
        r1 = resolve_traversal_field(Post, "author__name")
        r2 = resolve_traversal_field(Post, "author__id")
        assert r1 is not None and r2 is not None
        # Both join the same target model
        assert r1.join_spec.target_model is r2.join_spec.target_model
        # But select different columns
        assert r1.group_col.key != r2.group_col.key


# ── values() with traversal ───────────────────────────────────────────────────


class TestValuesTraversal:
    async def test_traversal_field_appears_in_dict(self, qs):
        results = await qs.filter(id=1).values("title", "author__name").all()
        assert len(results) == 1
        assert "author__name" in results[0]
        assert results[0]["author__name"] == "Alice"

    async def test_direct_and_traversal_mix(self, qs):
        results = await qs.filter(id=1).values("id", "author_id", "author__name").all()
        r = results[0]
        assert set(r.keys()) == {"id", "author_id", "author__name"}
        assert r["id"] == 1
        assert r["author_id"] == 1
        assert r["author__name"] == "Alice"

    async def test_left_join_keeps_orphan_row(self, qs):
        """Products with author_id=None must still appear (LEFT JOIN)."""
        results = await qs.filter(id=1).values("id", "author__name").all()
        assert len(results) == 1
        assert results[0]["id"] == 1
        assert results[0]["author__name"] == "Alice"

    async def test_traversal_with_count_annotate(self, qs):
        """The core use-case: group by FK + related fields, count products."""
        results = await (
            qs.values("author_id", "author__name")
            .annotate(count=Count("id", distinct=True))
            .order_by("author_id")
            .debug()
            .all()
        )
        # Orphan (author_id=None) appears as its own group
        by_cat = {r["author_id"]: r for r in results if r["author_id"] is not None}
        assert by_cat[1]["count"] == 2  # Electronics
        assert by_cat[2]["count"] == 3  # Books
        assert by_cat[1]["author__name"] == "Alice"

    async def test_order_by_annotation_desc(self, qs):
        """order_by('-count') after traversal + annotate."""
        results = await (
            qs.filter(author_id__in=[1, 2, 3])  # exclude orphan
            .values("author_id", "author__name")
            .annotate(count=Count("id", distinct=True))
            .order_by("-count")
            .all()
        )
        counts = [r["count"] for r in results]
        assert counts == sorted(counts, reverse=True)
        # Highest count first = Electronics (3)
        assert results[0]["author__name"] == "Bob"

    async def test_sum_annotate_with_traversal(self, qs):
        results = await (
            qs.filter(author_id__in=[1, 2, 3])
            .values("author_id", "author__name")
            .annotate(total_rating=Sum("rating"))
            .order_by("author_id")
            .all()
        )
        by_cat = {r["author_id"]: r["total_rating"] for r in results}
        assert by_cat[1] == 4.0 + 5.0
        assert by_cat[2] == 1.0 + 2.0 + 3.0

    async def test_min_max_with_traversal(self, qs):
        results = await (
            qs.filter(author_id=1)
            .values("author_id", "author__name")
            .annotate(lo=Min("rating"), hi=Max("rating"))
            .all()
        )
        assert len(results) == 1
        assert results[0]["lo"] == pytest.approx(4.0)
        assert results[0]["hi"] == pytest.approx(5.0)

    async def test_only_one_join_for_repeated_traversal(self, qs):
        """Using two fields from the same relation must not generate two JOINs."""
        # If duplicate JOINs were added, SQLAlchemy or the DB would error.
        results = await qs.filter(author_id=2).values("author__name").annotate(n=Count("id")).all()
        assert len(results) == 1
        assert results[0]["author__name"] == "Bob"
        assert results[0]["n"] == 3

    async def test_filter_still_applies(self, qs):
        results = await (
            qs.filter(rating__gte=2.0)
            .values("author__name")
            .annotate(n=Count("id"))
            .order_by("author__name")
            .all()
        )
        assert len(results) == 2
        assert results[0]["author__name"] == "Alice"
        assert results[0]["n"] == 2

    async def test_values_first_with_traversal(self, qs):
        result = await (
            qs.filter(author_id__in=[1, 2, 3])
            .values("author_id", "author__name")
            .annotate(count=Count("id", distinct=True))
            .order_by("-count")
            .first()
        )
        assert result is not None
        assert result["author__name"] == "Bob"
        assert result["count"] == 3

    async def test_exact_target_use_case(self, qs):
        """Mirrors the docstring example exactly."""
        rows = await (
            qs.filter(author_id__in=[1, 2, 3])
            .values("author_id", "author__name")
            .annotate(count=Count("id", distinct=True))
            .order_by("-count")
            .all()
        )
        output = [
            {
                "id": r["author_id"],
                "name": r["author__name"],
                "count": r["count"],
            }
            for r in rows
        ]
        assert output[0] == {"id": 2, "name": "Bob", "count": 3}


# # ── values_list() with traversal ─────────────────────────────────────────────


class TestValuesListTraversal:
    async def test_tuple_contains_traversal_value(self, qs):
        results = await qs.filter(id=1).values_list("title", "author__name").all()
        assert results == [("A", "Alice")]

    async def test_tuple_position_matches_field_order(self, qs):
        results = await qs.filter(id=3).values_list("id", "author__name").all()
        assert results == [(3, "Bob")]

    async def test_flat_with_traversal_field(self, qs):
        results = await qs.filter(author_id=2).values_list("author__name", flat=True).all()
        # Both Clothing products share the same author__name
        assert set(results) == {"Bob"}
        assert len(results) == 3

    async def test_values_list_traversal_with_annotate(self, qs):
        results = await (
            qs.filter(author_id__in=[1, 2, 3])
            .values_list("author_id", "author__name")
            .annotate(n=Count("id", distinct=True))
            .order_by("-n")
            .all()
        )
        # Each row: (author_id, author__name, n)
        assert results[0][1] == "Bob"
        assert results[0][2] == 3
        counts = [r[2] for r in results]
        assert counts == sorted(counts, reverse=True)

    async def test_left_join_orphan_in_values_list(self, qs):
        results = await qs.filter(id=5).values_list("id", "author__name").all()
        assert results == [(5, "Bob")]
