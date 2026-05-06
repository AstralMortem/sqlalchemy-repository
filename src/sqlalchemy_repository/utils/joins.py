from dataclasses import dataclass
from typing import Any
from ..types import ModelT


@dataclass
class JoinSpec:
    """Describes a single JOIN to apply to a query."""

    target_model: type[ModelT]  # the model to JOIN to
    on_clause: Any  # SQLAlchemy ON expression
    isouter: bool = False  # LEFT OUTER vs INNER

    @property
    def dedup_key(self) -> str:
        return f"{self.target_model.__tablename__}"


class JoinCollector:
    """Tracks required JOINs, deduplicating by (left_table, right_table, condition_key)."""

    def __init__(self) -> None:
        self._seen: set[str] = set()
        self._joins: list[Any] = []

    def add(self, join: "JoinSpec") -> None:
        key = join.dedup_key
        if key not in self._seen:
            self._seen.add(key)
            self._joins.append(join)

    @property
    def joins(self) -> list[Any]:
        return list(self._joins)
