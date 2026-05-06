from typing import Any
from sqlalchemy import column


def _coerce(value: Any) -> Any:
    """Unwrap F to its inner expression; leave everything else as-is."""
    return value._resolve() if isinstance(value, F) else value


class F:
    """Reference to a model field, usable in update/annotate/order_by.

    Example:
        qs.update(price=F("price") * 1.2)
        qs.order_by(F("score").desc())
    """

    __slots__ = ("_name", "_expr")

    def __init__(self, name: str, _expr: Any = None) -> None:
        self._name = name
        self._expr = _expr  # wrapped SA column expression (resolved lazily)

    # ── arithmetic ────────────────────────────
    def _wrap(self, op_result: Any) -> "F":
        return F(self._name, op_result)

    def __add__(self, other: Any) -> "F":
        return self._wrap(self._resolve() + _coerce(other))

    def __radd__(self, other: Any) -> "F":
        return self._wrap(_coerce(other) + self._resolve())

    def __sub__(self, other: Any) -> "F":
        return self._wrap(self._resolve() - _coerce(other))

    def __rsub__(self, other: Any) -> "F":
        return self._wrap(_coerce(other) - self._resolve())

    def __mul__(self, other: Any) -> "F":
        return self._wrap(self._resolve() * _coerce(other))

    def __truediv__(self, other: Any) -> "F":
        return self._wrap(self._resolve() / _coerce(other))

    def __neg__(self) -> "F":
        return self._wrap(-self._resolve())

    # ── ordering helpers ───────────────────────
    def asc(self) -> Any:
        return self._resolve().asc()

    def desc(self) -> Any:
        return self._resolve().desc()

    def _resolve(self) -> Any:
        if self._expr is not None:
            return self._expr
        # Return a placeholder column; real resolution happens in QuerySet
        return column(self._name)

    def __repr__(self) -> str:
        return f"F({self._name!r})"
