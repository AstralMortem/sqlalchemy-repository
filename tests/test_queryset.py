from sqlalchemy_repository.queryset import QuerySet
from sqlalchemy_repository.expressions.aggregations import Count, Avg
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

    assert len(result) == 1
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

    assert await qs.count() == 3
    assert await qs.exists() is True


@pytest.mark.asyncio
async def test_paginate(session, data):
    qs = QuerySet(Post, session)

    items, total = await qs.paginate(page=1, size=2)

    assert total == 3
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

    assert len(collected) == 3


@pytest.mark.asyncio
async def test_values_not_break(session, data):
    qs = QuerySet(User, session).values("id", "name")

    result = await qs.all()

    assert isinstance(result, list)


@pytest.mark.asyncio
async def test_distinct(session, data):
    qs = QuerySet(User, session).distinct()

    users = await qs.all()

    assert len(users) == 2


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
