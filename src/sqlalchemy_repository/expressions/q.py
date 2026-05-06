from typing import Any
from ..utils.joins import JoinCollector
from sqlalchemy import ClauseElement, and_, not_, or_


class Q:
    """Composable filter expression with &, |, ~ support.

    Example:
        Q(name="Alice") & (Q(age__gt=18) | Q(is_active=True))
    """

    AND = "AND"
    OR = "OR"
    NOT = "NOT"

    def __init__(self, **kwargs: Any) -> None:
        # Each Q node is either a leaf (kwargs) or a composite (children + connector)
        self._kwargs: dict[str, Any] = kwargs
        self._children: list["Q"] = []
        self._connector: str = Q.AND
        self._negated: bool = False

    # ── operators ─────────────────────────────
    def __and__(self, other: "Q") -> "Q":
        return self._combine(other, Q.AND)

    def __or__(self, other: "Q") -> "Q":
        return self._combine(other, Q.OR)

    def __invert__(self) -> "Q":
        clone = self._clone()
        clone._negated = not clone._negated
        return clone

    def _combine(self, other: "Q", connector: str) -> "Q":
        node = Q()
        node._children = [self, other]
        node._connector = connector
        return node

    def _clone(self) -> "Q":
        node = Q(**self._kwargs)
        node._children = list(self._children)
        node._connector = self._connector
        node._negated = self._negated
        return node

    def resolve(self, model: type, join_collector: JoinCollector) -> ClauseElement:
        """Translate this Q-tree into a SQLAlchemy clause element."""
        from ..utils.columns import build_filter_clause

        if self._children:
            parts = [c.resolve(model, join_collector) for c in self._children]
            if self._connector == Q.AND:
                expr = and_(*parts)
            else:
                expr = or_(*parts)
        else:
            # leaf: translate each kwarg lookup
            parts = []
            for key, value in self._kwargs.items():
                clause, joins = build_filter_clause(model, key, value)
                for j in joins:
                    join_collector.add(j)
                parts.append(clause)
            expr = and_(*parts) if parts else and_(True)

        if self._negated:
            expr = not_(expr)
        return expr

    def __repr__(self) -> str:
        if self._children:
            op = f" {self._connector} "
            inner = op.join(repr(c) for c in self._children)
            return f"({'NOT ' if self._negated else ''}{inner})"
        return f"Q({self._kwargs})"
