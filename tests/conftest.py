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
    u1 = User(name="Alice")
    u2 = User(name="Bob")

    p1 = Profile(age=25, user=u1)
    p2 = Profile(age=30, user=u2)

    post1 = Post(title="A", rating=4.5, author=u1)
    post2 = Post(title="B", rating=3.0, author=u1)
    post3 = Post(title="C", rating=5.0, author=u2)

    comments = [
        Comment(text="c1", post=post1),
        Comment(text="c2", post=post1),
        Comment(text="c3", post=post2),
    ]

    session.add_all([u1, u2])
    await session.commit()

    return {
        "users": [u1, u2],
        "posts": [post1, post2, post3],
    }
