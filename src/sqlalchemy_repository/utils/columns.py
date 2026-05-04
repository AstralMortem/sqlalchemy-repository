from typing import Any
from sqlalchemy import inspect as sa_inspect, UnaryExpression
from sqlalchemy.orm import RelationshipProperty


def resolve_column(model: type, path: list[str]) -> tuple[Any, list[tuple[Any, Any]]]:
    """
    Walk a model attribute path and return:
        (column_attribute, [(parent_model, relationship_attr), ...])

    """
    joins: list[tuple[Any, Any]] = []
    current_model = model

    for i, part in enumerate(path[:-1]):
        mapper = sa_inspect(current_model)
        try:
            rel: RelationshipProperty = mapper.relationships[part]
        except KeyError:
            available = list(mapper.relationships.keys())
            raise AttributeError(
                f"'{current_model.__name__}' has no relationship '{part}'. "
                f"Available: {available}"
            )
        rel_attr = getattr(current_model, part)
        next_model = rel.mapper.class_
        joins.append((current_model, rel_attr))
        current_model = next_model

    # Final part is the column
    col_name = path[-1]
    try:
        col_attr = getattr(current_model, col_name)
    except AttributeError:
        mapper = sa_inspect(current_model)
        cols = [c.key for c in mapper.columns]
        raise AttributeError(
            f"'{current_model.__name__}' has no column '{col_name}'. "
            f"Available columns: {cols}"
        )

    return col_attr, joins


def make_order_expr(model: type, token: str) -> UnaryExpression:
    """
    "-year"  → Model.year.desc()
    "year"   → Model.year.asc()
    "-generation__model__name" → (with join) related_col.desc()
    """
    desc = token.startswith("-")
    field = token.lstrip("-")
    path = field.split("__")
    col_attr, _joins = resolve_column(model, path)
    return col_attr.desc() if desc else col_attr.asc()
