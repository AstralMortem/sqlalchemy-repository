from typing import Any, Generic

from sqlalchemy_repository.expressions import F
from .types import FilterExpr, ModelT, PK
from .queryset import QuerySet
from sqlalchemy.ext.asyncio import AsyncSession


class _Base(Generic[ModelT]):
    model: type[ModelT]
    session: AsyncSession

    @property
    def objects(self) -> QuerySet[ModelT]:
        return QuerySet(self.model, self.session)


class RetrieveMixin(Generic[ModelT, PK], _Base[ModelT]):
    async def get_by_pk(self, pk: PK) -> ModelT | None:
        return await self.session.get(self.model, pk)

    async def get_by_field(self, field: str, value: Any) -> ModelT | None:
        return await self.objects.filter(**{field: value}).first()

    async def get(self, *args: FilterExpr, **kwargs: Any) -> ModelT | None:
        return await self.objects.filter(*args, **kwargs).first()

    async def filter(self, *args: FilterExpr, **kwargs: Any) -> list[ModelT]:
        return await self.objects.filter(*args, **kwargs).all()

    async def paginate(
        self, page: int, page_size: int, *args: FilterExpr, **kwargs: Any
    ) -> tuple[list[ModelT], int]:
        return await self.objects.filter(*args, **kwargs).paginate(page, page_size)

    async def all(self) -> list[ModelT]:
        return await self.objects.all()


class CreateMixin(_Base[ModelT]):
    async def create(self, payload: dict, _commit: bool = False):
        resolved = {
            k: v.resolve(self.model) if isinstance(v, F) else v
            for k, v in payload.items()
        }
        obj = self.model(**resolved)
        self.session.add(obj)
        await self.session.flush()
        if _commit:
            await self.session.commit()
        return obj

    async def bulk_create(self, payloads: list[dict], _commit: bool = False):
        objects = [self.model(**row) for row in payloads]
        self.session.add_all(objects)
        await self.session.flush()
        if _commit:
            await self.session.commit()
        return objects

    async def get_or_create(
        self,
        payload: dict,
        defaults: dict[str, Any] | None = None,
        _commit: bool = False,
    ) -> tuple[ModelT, bool]:
        obj = await self.objects.filter(**payload).first()
        if obj is not None:
            return obj, False

        create_kw = {**payload, **(defaults or {})}
        obj = self.model(**create_kw)
        self.session.add(obj)
        await self.session.flush()
        if _commit:
            await self.session.commit()
        return obj, True


class UpdateMixin(_Base[ModelT]):
    async def update(self, obj: ModelT, payload: dict, _commit: bool = False):
        for k, v in payload.items():
            setattr(obj, k, v.resolve(self.model) if isinstance(v, F) else v)
        await self.session.flush()
        if _commit:
            await self.session.commit()
        return obj

    async def bulk_update(
        self, objects: list[ModelT], payload: dict, _commit: bool = False
    ):
        for obj in objects:
            for k, v in payload.items():
                setattr(obj, k, v.resolve(self.model) if isinstance(v, F) else v)
        await self.session.flush()
        if _commit:
            await self.session.commit()
        return objects

    async def raw_save(self, obj: ModelT, _commit: bool = False):
        await self.session.flush()
        if _commit:
            await self.session.commit()
        return obj


class DeleteMixin(_Base[ModelT]):
    async def delete(self, obj: ModelT, _commit: bool = False):
        await self.session.delete(obj)
        await self.session.flush()
        if _commit:
            await self.session.commit()
        return None

    async def bulk_delete(self, objects: list[ModelT], _commit: bool = False):
        for obj in objects:
            await self.session.delete(obj)
        await self.session.flush()
        if _commit:
            await self.session.commit()
        return None


class BaseRepository(
    Generic[ModelT, PK],
    RetrieveMixin[ModelT, PK],
    CreateMixin[ModelT],
    UpdateMixin[ModelT],
    DeleteMixin[ModelT],
):
    def __init__(self, session: AsyncSession):
        self.session = session

    def __repr__(self):
        return f"<{self.__class__.__name__} model={self.model.__name__}>"


class BaseReadRepository(Generic[ModelT, PK], RetrieveMixin[ModelT, PK]):
    def __init__(self, session: AsyncSession):
        self.session = session

    def __repr__(self):
        return f"<{self.__class__.__name__} model={self.model.__name__}>"


class BaseWriteRepository(
    Generic[ModelT], CreateMixin[ModelT], UpdateMixin[ModelT], DeleteMixin[ModelT]
):
    def __init__(self, session: AsyncSession):
        self.session = session

    def __repr__(self):
        return f"<{self.__class__.__name__} model={self.model.__name__}>"
