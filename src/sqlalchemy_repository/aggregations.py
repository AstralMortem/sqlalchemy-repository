from typing import Callable
from sqlalchemy import func
from .utils.columns import resolve_column


class Aggregate:
    SQLALCHEMY_FUNC: Callable

    def __init__(self, field: str | None):
        self.field = field

    def _resolve_col(self, model: type, joins: list = []):
        if self.field is None:
            return None
        path = self.field.split("__")
        col, new_joins = resolve_column(model, path)
        joins.extend(new_joins)
        return col

    def _modify_column(self, col):
        return col

    def resolve(self, model: type, collected_joins: list):
        col = self._resolve_col(model, collected_joins)
        if col is None:
            return self.SQLALCHEMY_FUNC()
        return self.SQLALCHEMY_FUNC(self._modify_column(col))

    def __repr__(self):
        parts = []
        if self.field:
            parts.append(repr(self.field))
        return f"{self.__class__.__name__}({', '.join(parts)})"


class Count(Aggregate):
    SQLALCHEMY_FUNC = func.count

    def __init__(self, field: str | None, distinct: bool = False):
        super().__init__(field)
        self.distinct = distinct

    def _modify_column(self, col):
        return col.distinct() if self.distinct else col


class Sum(Aggregate):
    SQLALCHEMY_FUNC = func.sum

    def __init__(self, field: str | None, distinct: bool = False):
        super().__init__(field)
        self.distinct = distinct

    def _modify_column(self, col):
        return col.distinct() if self.distinct else col


class Avg(Aggregate):
    SQLALCHEMY_FUNC = func.avg

    def __init__(self, field: str | None, distinct: bool = False):
        super().__init__(field)
        self.distinct = distinct

    def _modify_column(self, col):
        return col.distinct() if self.distinct else col


class Min(Aggregate):
    SQLALCHEMY_FUNC = func.min


class Max(Aggregate):
    SQLALCHEMY_FUNC = func.max
