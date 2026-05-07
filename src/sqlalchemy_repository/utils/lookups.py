from enum import StrEnum
from typing import Any

from sqlalchemy import ClauseElement, Column
from sqlalchemy.orm import InstrumentedAttribute


class Lookup(StrEnum):
    EXACT = "exact"
    IEXACT = "iexact"
    CONTAINS = "contains"
    ICONTAINS = "icontains"
    STARTSWITH = "startswith"
    ISTARTSWITH = "istartswith"
    ENDSWITH = "endswith"
    IENDSWITH = "iendswith"
    IN = "in"
    NOT_IN = "not_in"
    ISNULL = "isnull"
    RANGE = "range"  # alias for between
    BETWEEN = "between"
    GT = "gt"
    GTE = "gte"
    LT = "lt"
    LTE = "lte"
    REGEX = "regex"
    IREGEX = "iregex"
    DATE = "date"
    YEAR = "year"
    MONTH = "month"
    DAY = "day"


_LOOKUP_ALIASES: dict[str, str] = {
    "range": "between",
}

ALL_LOOKUPS = {e.value for e in Lookup} | set(_LOOKUP_ALIASES)


def apply_lookup(column: InstrumentedAttribute, lookup: str, value: Any) -> ClauseElement:
    """Apply a lookup string to a SQLAlchemy column attribute."""
    lookup = _LOOKUP_ALIASES.get(lookup, lookup)

    match lookup:
        case "exact":
            return column == value
        case "iexact":
            return column.ilike(value)
        case "contains":
            return column.contains(value)
        case "icontains":
            return column.ilike(f"%{value}%")
        case "startswith":
            return column.startswith(value)
        case "istartswith":
            return column.ilike(f"{value}%")
        case "endswith":
            return column.endswith(value)
        case "iendswith":
            return column.ilike(f"%{value}")
        case "in":
            return column.in_(value)
        case "not_in":
            return column.not_in(value)
        case "isnull":
            return column.is_(None) if value else column.is_not(None)
        case "between" | "range":
            lo, hi = value
            return column.between(lo, hi)
        case "gt":
            return column > value
        case "gte":
            return column >= value
        case "lt":
            return column < value
        case "lte":
            return column <= value
        case "regex":
            return column.regexp_match(value)
        case "iregex":
            return column.regexp_match(value, flags="i")
        case "date":
            from sqlalchemy import cast, Date

            return cast(column, Date) == value
        case "year":
            from sqlalchemy import extract

            return extract("year", column) == value
        case "month":
            from sqlalchemy import extract

            return extract("month", column) == value
        case "day":
            from sqlalchemy import extract

            return extract("day", column) == value
        case _:
            raise ValueError(f"Unknown lookup '{lookup}'. Available: {sorted(ALL_LOOKUPS)}")
