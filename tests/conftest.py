from utils import Base, User, Profile, Post, Comment
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
import pytest
import pytest_asyncio


# =========================
# FIXTURES
# =========================


@pytest_asyncio.fixture
async def engine():
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield engine
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)


@pytest_asyncio.fixture
async def session(engine):
    async_session = async_sessionmaker(
        engine, class_=AsyncSession, expire_on_commit=False
    )

    async with async_session() as session:
        yield session


@pytest_asyncio.fixture
async def data(session: AsyncSession):
    u1 = User(name="Alice",id=1)
    u2 = User(name="Bob", id=2)
    u3 = User(name="Carol",id=3)

    p1 = Profile(age=25, user=u1)
    p2 = Profile(age=30, user=u2)
    p3 = Profile(age=22, user=u3)

    post1 = Post(title="A", rating=4.0, author=u1, id=1)
    post2 = Post(title="B", rating=5.0, author=u1, id=2)
    post3 = Post(title="C", rating=1.0, author=u2, id=3)
    post4 = Post(title="D", rating=2.0, author=u2, id=4)
    post5 = Post(title="E", rating=3.0, author=u2, id=5)

    comments = [
        Comment(text="c1", post=post1, profile=p2, rating=4),
        Comment(text="c2", post=post1, profile=p3, rating=3),
        Comment(text="c3", post=post2, profile=p2, rating=2),
    ]

    session.add_all([u1, u2, u3])
    await session.commit()

    return {
        "users": [u1, u2, u3],
        "posts": [post1, post2, post3, post4, post5],
    }
