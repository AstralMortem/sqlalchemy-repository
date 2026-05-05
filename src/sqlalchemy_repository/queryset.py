from sqlalchemy import Select, func, select
from .types import ModelT, FilterExpr
from .expressions import F, Q
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import joinedload, subqueryload, selectinload
from typing import Any, Generic
from .utils.columns import make_order_expr, apply_group_by
from .utils.relations import build_loader_tree
from .aggregations import Aggregate


class QuerySet(Generic[ModelT]):
    def __init__(
        self,
        model: type[ModelT],
        session: AsyncSession,
        *,
        _stmt: Select | None = None,
        _joins: list[tuple[Any, Any]] | None = None,
        _loader_opts: list[Any] | None = None,
        _result_cache: list[ModelT] | None = None,
    ):
        self._model = model
        self._session = session
        self._stmt = _stmt if _stmt is not None else select(model)
        self._joins = _joins or []
        self._loader_opts = _loader_opts or []
        self._result_cache = _result_cache or []
        self._annotations = {}

    def _clone(self, **kw):
        clone = QuerySet.__new__(QuerySet)
        clone._model = self._model
        clone._session = self._session
        clone._stmt = kw.get("_stmt", self._stmt)
        clone._joins = kw.get("_joins", self._joins)
        clone._loader_opts = kw.get("_loader_opts", self._loader_opts)
        clone._result_cache = None
        clone._annotations = kw.get("_annotations", self._annotations)
        return clone

    def _add_joins(self, *joins: list[tuple[Any, Any]]):
        existing = {id(j[1]) for j in self._joins}
        new_joins = list(self._joins)
        for pair in joins:
            if id(pair[1]) not in existing:
                new_joins.append(pair)
                existing.add(id(pair[1]))
        return self._clone(_joins=new_joins)

    def _build_stmt(self):
        stmt = self._stmt
        for _parent, rel_attr in self._joins:
            stmt = stmt.join(rel_attr, isouter=True)
        for opt in self._loader_opts:
            stmt = stmt.options(opt)

        if self._joins:
            stmt = stmt.distinct()
        return stmt

    def filter(self, *args: FilterExpr, **kwargs: Any) -> "QuerySet[ModelT]":
        collected_joins: list[tuple[Any, Any]] = []
        clauses = []
        for arg in args:
            if isinstance(arg, Q):
                clauses.append(arg.resolve(self._model, collected_joins))
            else:
                clauses.append(arg)
        if kwargs:
            clauses.append(Q(**kwargs).resolve(self._model, collected_joins))
        qs = self._add_joins(*collected_joins)
        qs._stmt = qs._stmt.where(*clauses)
        return qs

    def exclude(self, *args: FilterExpr, **kwargs: Any) -> "QuerySet[ModelT]":
        collected_joins: list[tuple[Any, Any]] = []
        clauses = []
        for arg in args:
            if isinstance(arg, Q):
                clauses.append((~arg).resolve(self._model, collected_joins))
            else:
                from sqlalchemy import not_

                clauses.append(not_(arg))
        if kwargs:
            clauses.append((~Q(**kwargs)).resolve(self._model, collected_joins))
        qs = self._add_joins(*collected_joins)
        qs._stmt = qs._stmt.where(*clauses)
        return qs

    def order_by(self, *fields: str | Any) -> "QuerySet[ModelT]":
        exprs = []

        for f in fields:
            if isinstance(f, str):
                desc = f.startswith("-")
                name = f.lstrip("-")

                if name in self._annotations:
                    col = self._annotations[name]
                    exprs.append(col.desc() if desc else col.asc())
                else:
                    exprs.append(make_order_expr(self._model, f))
            else:
                exprs.append(f)
        qs = self._clone()
        qs._stmt = qs._stmt.order_by(*exprs)
        qs._annotations = self._annotations
        return qs

    def limit(self, n: int) -> "QuerySet[ModelT]":
        qs = self._clone()
        qs._stmt = qs._stmt.limit(n)
        return qs

    def offset(self, n: int) -> "QuerySet[ModelT]":
        qs = self._clone()
        qs._stmt = qs._stmt.offset(n)
        return qs

    def select_related(
        self, *paths: str, strategy: str = "joined"
    ) -> "QuerySet[ModelT]":
        fns = {
            "joined": (joinedload, lambda p, a: p.joinedload(a)),
            "subquery": (subqueryload, lambda p, a: p.subqueryload(a)),
            "selectin": (selectinload, lambda p, a: p.selectinload(a)),
        }

        loader_fn, chained_fn = fns.get(strategy, fns["joined"])
        opts = build_loader_tree(self._model, paths, loader_fn, chained_fn)
        qs = self._clone()
        qs._loader_opts = list(self._loader_opts) + opts
        return qs

    def prefetch_related(self, *paths) -> "QuerySet[ModelT]":
        return self.select_related(*paths, strategy="selectin")

    def only(self, *fields: str) -> "QuerySet[ModelT]":
        from sqlalchemy.orm import load_only

        attrs = [getattr(self._model, f) for f in fields]
        qs = self._clone()
        qs._stmt = qs._stmt.options(load_only(*attrs))
        return qs

    def annotate(self, **kwargs: Any) -> "QuerySet[ModelT]":
        qs = self._clone()

        collected_joins: list[tuple[Any, Any]] = []
        add_cols = []

        for alias, expr in kwargs.items():
            if isinstance(expr, Aggregate):
                col_expr = expr.resolve(self._model, collected_joins)
            elif isinstance(expr, F):
                col_expr = expr.resolve(self._model)
            else:
                col_expr = expr
            labeled = col_expr.label(alias)
            self._annotations[alias] = labeled
            add_cols.append(labeled)

        qs = self._add_joins(*collected_joins)
        qs._stmt = qs._stmt.add_columns(*add_cols)

        return qs

    # Main methods

    async def execute(self):
        result = await self._session.execute(self._build_stmt())
        return result.all()

    async def all(self) -> list[ModelT]:
        result = await self._session.execute(self._build_stmt())

        if not getattr(self, "_annotations", None):
            return list(result.scalars().unique().all())

        rows = result.all()
        objects = []
        for row in rows:
            obj = row[0]
            for i, alias in enumerate(self._annotations.keys(), start=1):
                setattr(obj, alias, row[i])
            objects.append(obj)

        return objects

    async def first(self) -> ModelT | None:
        stmt = self._build_stmt().limit(1)
        result = await self._session.execute(stmt)
        if not getattr(self, "_annotations", None):
            return result.scalar_one_or_none()

        row = result.one_or_none()
        if row is not None:
            obj = row[0]
            for i, alias in enumerate(self._annotations.keys(), start=1):
                setattr(obj, alias, row[i])
            return obj
        return None

    async def last(self) -> ModelT | None:
        pk_col = list(self._model.__table__.primary_key.columns)[0]
        qs = self.order_by(f"-{pk_col.name}").limit(1)
        result = await self._session.execute(qs._build_stmt())
        return result.scalar_one_or_none()

    async def count(self) -> int:
        subq = self._build_stmt().order_by(None).subquery()
        stmt = select(func.count()).select_from(subq)
        result = await self._session.execute(stmt)
        return result.scalar() or 0

    async def exist(self) -> bool:

        from sqlalchemy import exists as sa_exists

        subq = self._build_stmt().limit(1)
        stmt = select(sa_exists(subq))
        result = await self._session.execute(stmt)
        return bool(result.scalar())

    async def paginate(self, page: int, page_size: int) -> tuple[list[ModelT], int]:
        total = await self.count()
        items = await self.offset((page - 1) * page_size).limit(page_size).all()
        return items, total

    async def aggregate(self, **kwargs) -> dict[str, Any]:
        collected_joins: list[tuple[Any, Any]] = []
        cols = []

        for _, expr in kwargs.items():
            if isinstance(expr, Aggregate):
                col = expr.resolve(self._model, collected_joins)
            else:
                col = expr
            cols.append(col)

        qs = self._add_joins(*collected_joins)
        stmt = qs._build_stmt().with_only_columns(*cols).order_by(None)

        row = (await self._session.execute(stmt)).one()
        return dict(zip(kwargs.keys(), row))

    def __repr__(self):
        return f"<QuerySet model={self._model.__name__}>"

    def compile(self):
        return self._stmt.compile(compile_kwargs={"literal_binds": True})
