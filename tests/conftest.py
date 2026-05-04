from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker
from utils import Base, ModelB, ModelA, ModelC
import pytest_asyncio


@pytest_asyncio.fixture()
async def engine():
    engine = create_async_engine("sqlite+aiosqlite://")

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield engine
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)


@pytest_asyncio.fixture()
async def session(engine):
    LocalSession = async_sessionmaker(engine, expire_on_commit=False)
    async with LocalSession() as session:
        yield session


@pytest_asyncio.fixture
async def init_models(session):
    c1 = ModelC(name="c1")
    c2 = ModelC(name="c2")

    b1 = ModelB(year=2004, qty=1, c=c1)
    b2 = ModelB(year=2005, qty=2, c=c1)
    b3 = ModelB(year=2006, qty=3, c=c2)

    a1 = ModelA(name="a1", b=b1)
    a2 = ModelA(name="a2", b=b2)

    session.add_all([c1, c2, b1, b2, b3, a1, a2])

    await session.flush()
    await session.commit()

    return {"a": [a1, a2], "b": [b1, b2, b3], "c": [c1, c2]}
