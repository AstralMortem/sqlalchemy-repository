from typing import Any, Callable, Union
from sqlalchemy_repository.utils.lookups import split_lookup, apply_lookup
from sqlalchemy_repository.utils.columns import resolve_column
from sqlalchemy import ClauseElement, and_, or_, not_
import operator as _op


class Q:
    """
    Encapsulates one or more filter conditions.

    Parameters
    ----------
    **kwargs
        Django-style lookup kwargs, e.g. name="BMW", year__gte=2020,
        generation__model__name__icontains="Series".

    negate : bool
        Internal flag used by ``~Q(...)``; do not pass directly.

    connector : "AND" | "OR"
        How children are combined; used internally by ``&`` and ``|``.
    """

    AND = "AND"
    OR = "OR"

    def __init__(
        self,
        *children: "Q",
        connector: str = AND,
        negate: bool = False,
        **kwargs: Any,
    ) -> None:
        self.connector = connector
        self.negate = negate
        self.children: list[Union["Q", tuple[str, Any]]] = list(children)
        for key, value in kwargs.items():
            self.children.append((key, value))

    # ---- operators ---------------------------------------------------------

    def __and__(self, other: "Q") -> "Q":
        if not isinstance(other, Q):
            return NotImplemented
        node = Q(connector=self.AND)
        node.children = [self, other]
        return node

    def __or__(self, other: "Q") -> "Q":
        if not isinstance(other, Q):
            return NotImplemented
        node = Q(connector=self.OR)
        node.children = [self, other]
        return node

    def __invert__(self) -> "Q":
        clone = Q(connector=self.connector)
        clone.children = list(self.children)
        clone.negate = not self.negate
        return clone

    # ---- resolution --------------------------------------------------------

    def resolve(
        self,
        model: type,
        collected_joins: list | None = None,
    ) -> ClauseElement:
        """
        Translate this Q tree into a SQLAlchemy ``ClauseElement``.

        Side-effect: appends required (model, rel_attr) join pairs to
        *collected_joins* (a list shared with the QuerySet/caller) so the
        caller can apply them once to the query.
        """
        if collected_joins is None:
            collected_joins = []

        clauses: list[ClauseElement] = []

        for child in self.children:
            if isinstance(child, Q):
                clauses.append(child.resolve(model, collected_joins))
            else:
                key, value = child
                path, lookup = split_lookup(key)
                col_attr, joins = resolve_column(model, path)

                # Register joins (deduplicate by identity of rel_attr)
                seen_attrs = {id(j[1]) for j in collected_joins}
                for join_pair in joins:
                    if id(join_pair[1]) not in seen_attrs:
                        collected_joins.append(join_pair)
                        seen_attrs.add(id(join_pair[1]))

                clauses.append(apply_lookup(col_attr, lookup, value))

        if not clauses:
            from sqlalchemy import true

            expr = true()
        elif self.connector == self.AND:
            expr = and_(*clauses)
        else:
            expr = or_(*clauses)

        return not_(expr) if self.negate else expr

    def __repr__(self) -> str:  # pragma: no cover
        return (
            f"Q(connector={self.connector!r}, "
            f"negate={self.negate}, "
            f"children={self.children!r})"
        )


class F:
    """
    References a model field/column by name for use in expressions.

    Supports arithmetic: ``F("price") * 1.2``, ``F("stock") - 1``, etc.

    Usage
    -----
        # In update
        repo.filter(Q(active=True)).update(price=F("price") * 1.1)

        # In annotations / order_by
        qs.annotate(total=F("qty") * F("unit_price")).order_by("-total")
    """

    def __init__(self, field: str) -> None:
        self.field = field
        self._expr: Any = None  # resolved lazily

    def resolve(self, model: type) -> ClauseElement:
        """Return the SQLAlchemy column expression for this field."""
        path = self.field.split("__")
        col_attr, _ = resolve_column(model, path)
        if self._expr is not None:
            # wrap the pending arithmetic
            return self._expr(col_attr)
        return col_attr

    # ---- arithmetic operators ----------------------------------------------

    def _make_op(self, op: Callable, other: Any, *, right: bool = False) -> "F":
        clone = F(self.field)
        prev_expr = self._expr

        def operand(col: Any):
            if isinstance(other, F):
                return other.resolve(col.class_ if hasattr(col, "class_") else col)
            return other

        def composed(col: Any) -> Any:
            base = prev_expr(col) if prev_expr else col
            if right:
                return op(operand(col), base)
            return op(base, operand(col))

        clone._expr = composed
        return clone

    def __add__(self, other: Any) -> "F":
        return self._make_op(_op.add, other)

    def __radd__(self, other: Any) -> "F":
        return self._make_op(_op.add, other, right=True)

    def __sub__(self, other: Any) -> "F":
        return self._make_op(_op.sub, other)

    def __rsub__(self, other: Any) -> "F":
        return self._make_op(_op.sub, other, right=True)

    def __mul__(self, other: Any) -> "F":
        return self._make_op(_op.mul, other)

    def __rmul__(self, other: Any) -> "F":
        return self._make_op(_op.mul, other, right=True)

    def __truediv__(self, other: Any) -> "F":
        return self._make_op(_op.truediv, other)

    def __rtruediv__(self, other: Any) -> "F":
        return self._make_op(_op.truediv, other, right=True)

    def __neg__(self) -> "F":
        clone = F(self.field)
        prev = self._expr

        def neg_expr(col: Any) -> Any:
            base = prev(col) if prev else col
            return -base

        clone._expr = neg_expr
        return clone

    def __repr__(self) -> str:  # pragma: no cover
        return f"F({self.field!r})"
