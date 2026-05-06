from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any
from ..utils.columns import resolve_column
from sqlalchemy import func


def group_aggregates(annotations: dict[str, Any]):
    grouped = defaultdict(list)

    for alias, agg in annotations.items():
        if isinstance(agg, Aggregate):
            grouped[agg.field].append((alias, agg))
    return grouped


@dataclass
class Aggregate:
    """Base class for SQL aggregate functions."""

    field: str
    _sa_func: Any = field(default=None, repr=False, init=False)

    def _get_column(self, model: type):
        return resolve_column(model, self.field)

    def _get_expr(self, model):
        col = self._get_column(model)
        return self._sa_func(col)

    def resolve(self, model: type) -> Any:
        return self._get_expr(model)


class Count(Aggregate):
    _sa_func = func.count

    def __init__(self, field: str = "*", distinct: bool = False) -> None:
        self.field = field
        self.distinct = distinct

    def _get_column(self, model):
        if self.field == "*":
            return None
        return super()._get_column(model)

    def _get_expr(self, model):
        col = super()._get_expr(model)
        if self.distinct:
            return col.distinct()
        return col


class Sum(Aggregate):
    _sa_func = func.sum


class Avg(Aggregate):
    _sa_func = func.avg


class Min(Aggregate):
    _sa_func = func.min


class Max(Aggregate):
    _sa_func = func.max
