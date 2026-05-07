from sqlalchemy_repository.queryset import QuerySet
from sqlalchemy_repository.expressions.aggregations import Count, Avg, Max, Min
from sqlalchemy_repository.expressions import Q
from utils import User, Post
import pytest


@pytest.mark.asyncio
async def test_filter_simple(session, data):
    qs = QuerySet(User, session)
    result = await qs.filter(name="Alice").all()

    assert len(result) == 1
    assert result[0].name == "Alice"


@pytest.mark.asyncio
async def test_filter_related(session, data):
    qs = QuerySet(User, session)
    result = await qs.filter(profile__age__gte=26).all()

    assert len(result) == 1
    assert result[0].name == "Bob"


@pytest.mark.asyncio
async def test_exclude(session, data):
    qs = QuerySet(User, session)
    result = await qs.exclude(name="Alice").all()

    assert len(result) == 2
    assert result[0].name == "Bob"


@pytest.mark.asyncio
async def test_q_objects(session, data):
    qs = QuerySet(User, session)
    result = await qs.filter(Q(name="Alice") | Q(name="Bob")).all()

    assert len(result) == 2


@pytest.mark.asyncio
async def test_ordering(session, data):
    qs = QuerySet(Post, session)
    result = await qs.order_by("-rating").all()

    assert result[0].rating >= result[1].rating


@pytest.mark.asyncio
async def test_limit_offset(session, data):
    qs = QuerySet(Post, session)

    result = await qs.order_by("id").limit(2).offset(1).all()

    assert len(result) == 2
    assert result[0].title == "B"


@pytest.mark.asyncio
async def test_get(session, data):
    qs = QuerySet(User, session)

    user = await qs.get(name="Alice")
    assert user.name == "Alice"

    with pytest.raises(ValueError):
        await qs.get(name="unknown")


@pytest.mark.asyncio
async def test_first_last(session, data):
    qs = QuerySet(Post, session).order_by("id")

    first = await qs.first()
    last = await qs.last()

    assert first.id < last.id


@pytest.mark.asyncio
async def test_count_exists(session, data):
    qs = QuerySet(Post, session)

    assert await qs.count() == 5
    assert await qs.exists() is True


@pytest.mark.asyncio
async def test_paginate(session, data):
    qs = QuerySet(Post, session)

    items, total = await qs.paginate(page=1, size=2)

    assert total == 5
    assert len(items) == 2


@pytest.mark.asyncio
async def test_select_related(session, data):
    qs = QuerySet(Post, session).select_related("author")

    posts = await qs.all()

    # access without lazy load error
    assert posts[0].author is not None


@pytest.mark.asyncio
async def test_prefetch_related(session, data):
    qs = QuerySet(Post, session).prefetch_related("comments")

    posts = await qs.all()

    assert len(posts[0].comments) >= 0


@pytest.mark.asyncio
async def test_nested_prefetch(session, data):
    qs = QuerySet(User, session).prefetch_related("posts__comments")

    users = await qs.all()

    assert isinstance(users[0].posts, list)
    if users[0].posts:
        assert isinstance(users[0].posts[0].comments, list)


@pytest.mark.asyncio
async def test_annotate_count(session, data):
    qs = QuerySet(Post, session).annotate(comments_count=Count("comments__id"))

    posts = await qs.all()

    for post in posts:
        assert hasattr(post, "comments_count")


@pytest.mark.asyncio
async def test_annotate_multiple(session, data):
    qs = QuerySet(Post, session).annotate(
        comments_count=Count("comments__id"),
        avg_rating=Avg("comments__id"),  # dummy but tests multi agg
    )

    posts = await qs.all()

    for post in posts:
        assert hasattr(post, "comments_count")
        assert hasattr(post, "avg_rating")


@pytest.mark.asyncio
async def test_annotation_with_prefetch(session, data):
    qs = (
        QuerySet(Post, session)
        .annotate(comments_count=Count("comments__id"))
        .prefetch_related("comments")
    )

    posts = await qs.all()

    assert posts
    assert hasattr(posts[0], "comments_count")
    assert isinstance(posts[0].comments, list)


@pytest.mark.asyncio
async def test_async_iter(session, data):
    qs = QuerySet(Post, session)

    collected = []
    async for item in qs:
        collected.append(item)

    assert len(collected) == 5


@pytest.mark.asyncio
async def test_values_not_break(session, data):
    qs = QuerySet(User, session).values("id", "name")

    result = await qs.all()

    assert isinstance(result, list)


@pytest.mark.asyncio
async def test_distinct(session, data):
    qs = QuerySet(User, session).distinct()

    users = await qs.all()

    assert len(users) == 3


@pytest.mark.asyncio
async def test_complex_query(session, data):
    qs = (
        QuerySet(Post, session)
        .filter(author__profile__age__gte=20)
        .annotate(comments_count=Count("comments__id"))
        .prefetch_related("comments")
        .order_by("-rating")
    )

    posts = await qs.all()

    assert posts
    assert hasattr(posts[0], "comments_count")


@pytest.mark.asyncio
async def test_complex_query2(session, data):

    qs1 = (
        QuerySet(User, session)
        .filter(name="Alice")
        .annotate(comments_count=Count("posts__comments__id"))
        .prefetch_related("posts", "posts__comments")
    )
    qs2 = (
        QuerySet(Post, session)
        .filter(author__name="Alice")
        .annotate(comments_count=Count("comments__id"))
        .prefetch_related("comments")
    )

    item1 = await qs1.first()
    item2 = await qs2.first()

    assert item1.comments_count == 3
    assert item2.comments_count == 2


def by_id(users: list[User]) -> dict[int, User]:
    return {u.id: u for u in users}


@pytest.mark.asyncio
async def test_annotate_min_max_basic(session, data):
    """Min/Max over posts.rating should return correct values per user."""
    qs = QuerySet(User, session)
    users = await qs.annotate(
        min_rating=Min("posts__rating"),
        max_rating=Max("posts__rating"),
    ).all()

    result = by_id(users)

    assert result[1].min_rating == pytest.approx(4.0)
    assert result[1].max_rating == pytest.approx(5.0)

    assert result[2].min_rating == pytest.approx(1.0)
    assert result[2].max_rating == pytest.approx(3.0)


# ---------------------------------------------------------------------------
# Test 2 – THE REPORTED BUG: prefetch_related + annotate must not raise
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_prefetch_related_with_annotate_does_not_raise(session, data):
    """
    This is the exact query that triggered the InvalidRequestError.
    It must complete without raising.
    """
    qs = QuerySet(User, session)

    # Must NOT raise sqlalchemy.exc.InvalidRequestError
    users = await (
        qs.prefetch_related("posts")
        .annotate(
            min_rating=Min("posts__rating"),
            max_rating=Max("posts__rating"),
        )
        .all()
    )

    assert len(users) == 3


# ---------------------------------------------------------------------------
# Test 3 – prefetch_related + annotate: values are still correct
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_prefetch_related_with_annotate_correct_values(session, data):
    """After the fix, annotation values must match expectations."""
    qs = QuerySet(User, session)
    users = await (
        qs.prefetch_related("posts")
        .annotate(
            min_rating=Min("posts__rating"),
            max_rating=Max("posts__rating"),
        )
        .all()
    )

    result = by_id(users)

    assert result[1].min_rating == pytest.approx(4.0)
    assert result[1].max_rating == pytest.approx(5.0)
    assert result[2].min_rating == pytest.approx(1.0)
    assert result[2].max_rating == pytest.approx(3.0)


# ---------------------------------------------------------------------------
# Test 4 – annotate does NOT break prefetch_related data loading
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_prefetch_data_intact_after_annotate(session, data):
    """posts should still be eagerly loaded after .annotate()."""
    qs = QuerySet(User, session)
    users = await qs.prefetch_related("posts").annotate(min_rating=Min("posts__rating")).all()

    result = by_id(users)

    # Verify that the prefetched relationship is populated (not lazy-load needed)
    assert len(result[1].posts) == 2
    assert len(result[2].posts) == 3
    assert len(result[3].posts) == 0


# ---------------------------------------------------------------------------
# Test 5 – NULL handling: user with no related rows gets None
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_annotate_null_for_user_without_posts(session, data):
    """carol has no posts → min/max should be None, not raise."""
    qs = QuerySet(User, session)
    users = await qs.annotate(
        min_rating=Min("posts__rating"),
        max_rating=Max("posts__rating"),
    ).all()

    result = by_id(users)
    assert result[3].min_rating is None
    assert result[3].max_rating is None


# ---------------------------------------------------------------------------
# Test 6 – aggregation is scoped per owner (no cross-user leakage)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_annotate_no_cross_user_leakage(session, data):
    """Each user's aggregation must reflect only their own posts."""
    qs = QuerySet(User, session)
    users = await qs.annotate(
        min_rating=Min("posts__rating"),
        max_rating=Max("posts__rating"),
    ).all()

    result = by_id(users)

    # alice's max must not include bob's rating=3.0
    assert result[1].max_rating == pytest.approx(5.0)
    # bob's min must not include alice's rating=4.0
    assert result[2].min_rating == pytest.approx(1.0)


# ---------------------------------------------------------------------------
# Test 7 – annotate combined with .filter()
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_annotate_with_filter(session, data):
    """filter() before annotate() should narrow the result set correctly."""
    qs = QuerySet(User, session)
    users = await (
        qs.filter(name="Alice")
        .annotate(
            min_rating=Min("posts__rating"),
            max_rating=Max("posts__rating"),
        )
        .all()
    )

    assert len(users) == 1
    assert users[0].name == "Alice"
    assert users[0].min_rating == pytest.approx(4.0)
    assert users[0].max_rating == pytest.approx(5.0)


# ---------------------------------------------------------------------------
# Test 8 – multiple independent aggregation aliases in one .annotate() call
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_annotate_multiple_aliases(session, data):
    """Two Min/Max aliases over the same path must both be set correctly."""
    qs = QuerySet(User, session)
    users = await qs.annotate(
        lowest=Min("posts__rating"),
        highest=Max("posts__rating"),
    ).all()

    result = by_id(users)

    assert hasattr(result[1], "lowest")
    assert hasattr(result[1], "highest")
    assert result[1].lowest == pytest.approx(4.0)
    assert result[1].highest == pytest.approx(5.0)


# ---------------------------------------------------------------------------
# Test 9 – select_related + annotate (another eager-load variant)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_select_related_with_annotate_does_not_raise(session, data):
    """select_related("profile") + annotate over posts must not raise."""
    qs = QuerySet(User, session)
    users = await qs.select_related("profile").annotate(max_rating=Max("posts__rating")).all()

    result = by_id(users)
    assert result[1].max_rating == pytest.approx(5.0)
    # profile should still be loaded
    assert result[1].profile.age == 25


# ---------------------------------------------------------------------------
# Test 10 – annotate over a 2-level path (posts → comments count proxy)
#            Uses Min on comment id as a smoke test for deep path resolution
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_annotate_two_level_path(session, data):
    """
    Post → comments is a 2-level join from User.
    Min("posts__comments__id") should work without ambiguous FROM.
    """
    qs = QuerySet(User, session)
    users = await qs.annotate(
        first_comment_id=Min("posts__comments__id"),
    ).all()

    result = by_id(users)

    # alice has comments with id 1, 2, 3 → min = 1
    assert result[1].first_comment_id == 1
    # bob's posts have no comments → None
    assert result[2].first_comment_id is None


@pytest.mark.asyncio
async def test_annotate_many_with_joined_filter(session, data):

    qs = QuerySet(Post, session)
    posts = (
        await qs.filter(author__name="Alice")
        .annotate(min_rating=Min("comments__rating"), max_rating=Max("comments__rating"))
        .all()
    )

    assert len(posts) == 2
    assert posts[0].title == "A"
    assert posts[0].min_rating == 3
    assert posts[0].max_rating == 4


@pytest.mark.asyncio
async def test_annotate_many_with_paginations(session, data):

    qs = QuerySet(Post, session)
    posts, total = (
        await qs.filter(author__name="Alice")
        .annotate(min_rating=Min("comments__rating"), max_rating=Max("comments__rating"))
        .paginate(1, 2)
    )

    assert total == 2
    assert posts[0].title == "A"
    assert posts[0].min_rating == 3
    assert posts[0].max_rating == 4
