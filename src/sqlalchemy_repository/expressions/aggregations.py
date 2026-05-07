from collections import defaultdict
from dataclasses import dataclass, field as dataclass_field
from typing import Any, Generic
from ..utils.columns import resolve_column, resolve_path_with_joins
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
    
    def _get_sub_expr(self, model):
        col, joins = resolve_path_with_joins(model, self.field)
        return self._sa_func(col), joins

    def resolve(self, model: type[ModelT]) -> Any:
        return self._get_expr(model)

    def resolve_subquery(self, model: type[ModelT]):
        return self._get_sub_expr(model)


class Count(Aggregate[ModelT]):
    _sa_func = func.count

    def __init__(self, field: str = "*", distinct: bool = False) -> None:
        self.field = field
        self.distinct = distinct

    def _get_expr(self, model):
        if self.field == "*":
            col = None
        else:
            col = super()._get_column(model)
        if self.distinct:
            return self._sa_func(distinct(col))
        return self._sa_func(col)

    def _get_sub_expr(self, model):
        joins = []
        if self.field == "*":
            col = None
        else:
            col, joins = resolve_path_with_joins(model, self.field)
        
        if self.distinct:
            return self._sa_func(distinct(col)), joins
        return self._sa_func(col), joins


class Sum(Aggregate[ModelT]):
    _sa_func = func.sum


class Avg(Aggregate[ModelT]):
    _sa_func = func.avg


class Min(Aggregate[ModelT]):
    _sa_func = func.min


class Max(Aggregate[ModelT]):
    _sa_func = func.max
