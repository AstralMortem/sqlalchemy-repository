from collections import defaultdict
from dataclasses import dataclass, field as dataclass_field
from typing import Any, Generic
from ..utils.columns import resolve_column
from sqlalchemy import distinct, func
from ..types import ModelT


def group_aggregates(annotations: dict[str, Any]):
    grouped = defaultdict(list)

    for alias, agg in annotations.items():
        if isinstance(agg, Aggregate):
            grouped[agg.field].append((alias, agg))
    return grouped


@dataclass
class Aggregate(Generic[ModelT]):
    """Base class for SQL aggregate functions."""

    field: str
    _sa_func: Any = dataclass_field(default=None, repr=False, init=False)

    def _get_column(self, model: type[ModelT]):
        return resolve_column(model, self.field)

    def _get_expr(self, model):
        col = self._get_column(model)
        return self._sa_func(col)

    def _get_subq_column(self, subq):
        col = subq.c.get(self.field)
        if col is None:
            raise ValueError(
                f"Aggregate field {self.field!r} is not a direct column on the subquery"
            )
        return col

    def _get_subq_expr(self, subq):
        col = self._get_subq_column(subq)
        return self._sa_func(col)

    def resolve(self, model: type[ModelT]) -> Any:
        return self._get_expr(model)

    def resolve_subquery(self, subq):
        return self._get_subq_expr(subq)


class Count(Aggregate[ModelT]):
    _sa_func = func.count

    def __init__(self, field: str = "*", distinct: bool = False) -> None:
        self.field = field
        self.distinct = distinct

    def _get_subq_expr(self, subq):
        if self.field == "*":
            return self._sa_func(None)
        col = super()._get_subq_column(subq)
        if self.distinct:
            return self._sa_func(distinct(col))
        return self._sa_func(col)

    def _get_expr(self, model):
        if self.field == "*":
            col = self._sa_func(None)

        col = super()._get_column(model)
        if self.distinct:
            self._sa_func(distinct(col))
        return self._sa_func(col)


class Sum(Aggregate[ModelT]):
    _sa_func = func.sum


class Avg(Aggregate[ModelT]):
    _sa_func = func.avg


class Min(Aggregate[ModelT]):
    _sa_func = func.min


class Max(Aggregate[ModelT]):
    _sa_func = func.max
