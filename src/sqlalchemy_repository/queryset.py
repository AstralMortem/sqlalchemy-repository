from collections import defaultdict
import copy
import logging
from sqlalchemy import func, not_, select
from .types import ModelT
from typing import Any, AsyncIterator, Generic, Self
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import joinedload, selectinload
from .expressions import Q, F
from .expressions.aggregations import Aggregate, group_aggregates
from .utils.joins import JoinCollector
from .utils.columns import (
    build_filter_clause,
    build_loader_option,
    resolve_column,
    resolve_pk_column,
    resolve_pk_fields,
    resolve_pk_name,
    resolve_path_with_joins,
)

log = logging.getLogger(__name__)


class QuerySet(Generic[ModelT]):
    def __init__(
        self,
        model: type[ModelT],
        session: AsyncSession,
        *,
        _copy_from: "QuerySet[ModelT] | None" = None,
    ):
        self._model = model
        self._session = session
        self._filters: list[Any] = []
        self._excludes: list[Any] = []
        self._joins: JoinCollector = JoinCollector()
        self._order_fields: list[Any] = []
        self._annotations: dict[str, Any] = {}
        self._select_related: list[str] = []
        self._prefetch: list[str] = []
        self._limit_val: int | None = None
        self._offset_val: int | None = None
        self._distinct_on: bool = False
        self._values_fields: list[str] | None = None
        self._values_list_fields: list[str] | None = None
        self._values_list_flat: bool = False
        self._debug: bool = False

        if _copy_from is not None:
            self._copy(_copy_from)

    def _copy(self, source: "QuerySet[ModelT]"):
        self._model = source._model
        self._session = source._session
        self._filters = list(source._filters)
        self._excludes = list(source._excludes)
        self._joins = copy.deepcopy(source._joins)
        self._order_fields = list(source._order_fields)
        self._annotations = dict(source._annotations)
        self._select_related = list(source._select_related)
        self._prefetch = list(source._prefetch)
        self._limit_val = source._limit_val
        self._offset_val = source._offset_val
        self._distinct_on = source._distinct_on
        self._values_fields = source._values_fields
        self._values_list_fields = source._values_list_fields
        self._values_list_flat = source._values_list_flat
        self._debug = source._debug

    def _clone(self) -> "QuerySet[ModelT]":
        return QuerySet(self._model, self._session, _copy_from=self)

    def _add_filters_from_kw(
        self, kw: dict[str, Any], qs: "QuerySet[ModelT]", negate: bool = False
    ):
        for k, v in kw.items():
            clause, joins = build_filter_clause(self._model, k, v)
            for j in joins:
                qs._joins.add(j)
            if negate:
                clause = not_(clause)
            qs._filters.append(clause)
        return qs

    def _add_q_obj(
        self, q_objs: tuple["Q", ...], qs: "QuerySet[ModelT]", negate: bool = False
    ):
        for q in q_objs:
            if negate:
                q = ~q
            clause = q.resolve(self._model, qs._joins)
            qs._filters.append(clause)
        return qs

    def _build_select(self):
        if self._annotations:
            annotation_exprs = {
                alias: (
                    agg.resolve(self._model)
                    if isinstance(agg, Aggregate)
                    else agg._resolve()
                    if isinstance(agg, F)
                    else agg
                )
                for alias, agg in self._annotations.items()
            }
            stmt = select(
                self._model,
                *[expr.label(alias) for alias, expr in annotation_exprs.items()],
            )
        else:
            stmt = select(self._model)

        # Joins
        for spec in self._joins.joins:
            stmt = stmt.join(spec.target_model, spec.on_clause, isouter=spec.isouter)

        # Filters
        for clause in self._filters:
            stmt = stmt.where(clause)
        for clause in self._excludes:
            stmt = stmt.where(clause)

        # Group By
        if self._annotations:
            pks = resolve_pk_fields(self._model)
            stmt = stmt.group_by(*pks)

        # Order by
        for order_field in self._order_fields:
            stmt = stmt.order_by(self._parse_order_field(order_field))

        # Distinct
        if self._distinct_on:
            stmt = stmt.distinct()

        # Limit/Offset
        if self._limit_val is not None:
            stmt = stmt.limit(self._limit_val)

        if self._offset_val is not None:
            stmt = stmt.offset(self._offset_val)

        # Relation loading
        options = []
        for path in self._select_related:
            options.append(build_loader_option(self._model, path, joinedload))

        for path in self._prefetch:
            options.append(build_loader_option(self._model, path, selectinload))

        if options:
            stmt = stmt.options(*options)

        return stmt

    def _parse_order_field(self, field: str):
        if isinstance(field, F):
            return field._resolve()

        if field.startswith("-"):
            col = resolve_column(self._model, field[1:])
            return col.desc()

        col = resolve_column(self._model, field)
        return col.asc()

    def filter(self, *q: Q, **kw) -> Self:
        qs = self._clone()
        qs = self._add_q_obj(q, qs)
        qs = self._add_filters_from_kw(kw, qs)
        return qs

    def exclude(self, *q: Q, **kw) -> Self:
        qs = self._clone()
        qs = self._add_q_obj(q, qs, negate=True)
        qs = self._add_filters_from_kw(kw, qs, negate=True)
        return qs

    def order_by(self, *fields: str | F) -> Self:
        qs = self._clone()
        qs._order_fields = list(fields)
        return qs

    def distinct(self) -> Self:
        qs = self._clone()
        qs._distinct_on = True
        return qs

    def limit(self, n: int) -> Self:
        qs = self._clone()
        qs._limit_val = n
        return qs

    def offset(self, n: int) -> Self:
        qs = self._clone()
        qs._offset_val = n
        return qs

    def annotate(self, **kw: Aggregate | F | Any) -> Self:
        qs = self._clone()
        qs._annotations = {**qs._annotations, **kw}
        return qs

    def values(self, *fields: str) -> Self:
        qs = self._clone()
        qs._values_fields = list(fields)
        return qs

    def values_list(self, *fields: str, flat: bool = False):
        qs = self._clone()
        qs._values_list_fields = list(fields)
        qs._values_list_flat = flat
        return qs

    def select_related(self, *fields: str) -> Self:
        qs = self._clone()
        qs._select_related = list(fields)
        return qs

    def prefetch_related(self, *fields: str) -> Self:
        qs = self._clone()
        qs._prefetch = list(fields)
        return qs

    def debug(self) -> Self:
        qs = self._clone()
        qs._debug = True
        return qs

    # Execution
    async def _execute(self):
        stmt = self._build_select()
        if self._debug:
            log.debug("QuerySet SQL:\n%s", stmt)
        return await self._session.execute(stmt)

    async def _execute_with_aggregation(self):
        # Run without annotations
        base_qs = self._clone()
        base_qs._annotations = {}

        result = await base_qs._execute()
        instances = result.scalars().all()
        if not instances:
            return []

        pk_name = resolve_pk_name(self._model)
        ids = [getattr(i, pk_name) for i in instances]
        grouped = group_aggregates(self._annotations)

        # Process each aggregations
        for path, aggs in grouped.items():
            result_map = await self._run_aggregation_group(ids, path, aggs)
            for obj in instances:
                obj_id = getattr(obj, pk_name)
                values = result_map.get(obj_id, {})
                for alias, _ in aggs:
                    setattr(obj, alias, values.get(alias))
        return instances

    async def _run_aggregation_group(
        self, ids, path: str, aggs: list[tuple[str, Aggregate]]
    ):
        _, joins = resolve_path_with_joins(self._model, path)

        parent_pk = resolve_pk_column(self._model)

        # Start from root model
        stmt = select(parent_pk.label("parent_id"))

        for spec in joins:
            stmt = stmt.join(spec.target_model, spec.on_clause, isouter=spec.isouter)

        for alias, agg in aggs:
            col_expr = agg.resolve(self._model)
            stmt = stmt.add_columns(col_expr.label(alias))

        stmt = stmt.where(parent_pk.in_(ids))
        stmt = stmt.group_by(parent_pk)
        result = await self._session.execute(stmt)

        output = defaultdict(dict)
        for row in result:
            pid = row.parent_id
            for alias, _ in aggs:
                output[pid][alias] = getattr(row, alias)
        return output
    
    def _is_annotated(self) -> bool:
        return self._annotations and any(isinstance(v, Aggregate) for v in self._annotations.values())
    
    async def all(self) -> list[ModelT]:
        if self._is_annotated():
            return await self._execute_with_aggregation()
        result = await self._execute()
        return result.scalars().all()

    async def first(self) -> ModelT | None:
        if self._is_annotated():
            qs = self._clone()
            qs._limit_val = 1
            res = await qs._execute_with_aggregation()
            return res[0] if res else None
        stmt = self._build_select().limit(1)
        if self._debug:
            log.debug("QuerySet SQL (first):\n%s", stmt)
        result = await self._session.execute(stmt)
        return result.scalar_one_or_none()
    
    async def last(self) -> ModelT | None:
        qs = self._clone()
        if qs._order_fields:
            reversed_fields = []
            for f in qs._order_fields:
                if isinstance(f, str):
                    if f.startswith("-"):
                        reversed_fields.append(f[1:])
                    else:
                        reversed_fields.append(f"-{f}")
                else:
                    reversed_fields.append(f)
            qs._order_fields = reversed_fields
        else:
            # Fallback to pk
            pk_col = resolve_pk_name(self._model)
            qs._order_fields = [f"-{pk_col}"]

        return await qs.first()

    async def get(self, **kw) -> ModelT:
        qs = self.filter(**kw)
        result = await qs.limit(2).all()
        if not result:
            raise ValueError(f"{self._model.__name__} mathing query does not exist")
        if len(result) > 1:
            raise ValueError(f"get() returned more than onre {self._model.__name__}")
        return result[0]

    async def count(self) -> int:
        stmt = select(func.count()).select_from(self._build_select().subquery())
        if self._debug:
            log.debug(f"QuerySet SQL (count):\n%s", stmt)
        result = await self._session.execute(stmt)
        return result.scalar_one() or 0

    async def exists(self) -> bool:
        from sqlalchemy import literal

        stmt = select(literal(1)).select_from(self._build_select().limit(1).subquery())
        if self._debug:
            log.debug(f"QuerySet SQL (exists):\n%s", stmt)
        result = await self._session.execute(stmt)
        return result.first() is not None

    async def paginate(self, page: int, size: int) -> tuple[list[ModelT], int]:
        total = await self.count()
        items = await self.offset((page - 1) * size).limit(size).all()
        return items, total

    async def explain(self) -> str:
        from sqlalchemy import text

        stmt = self._build_select()
        compiled = stmt.compile(
            dialect=self._session.get_bind().dialect,
            compile_kwargs={"literal_binds": True},
        )
        explain_stmt = text(f"EXPLAIN {compiled}")
        result = await self._session.execute(explain_stmt)
        return "\n".join(row[0] for row in result)

    def __aiter__(self) -> AsyncIterator[ModelT]:
        return self._async_iter()

    async def _async_iter(self) -> AsyncIterator[ModelT]:
        result = await self.all()
        for obj in result:
            yield obj

    def __await__(self):
        return self.all().__await__()

    def __repr__(self) -> str:
        return (
            f"<QuerySet model={self._model.__name__} "
            f"filters={len(self._filters)} "
            f"annotations={list(self._annotations.keys())}"
        )
