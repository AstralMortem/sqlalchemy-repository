from dataclasses import dataclass
from typing import Any, Callable, Tuple
from sqlalchemy import ColumnClause, Label, inspect as sa_inspect, ColumnElement
from sqlalchemy.orm import RelationshipProperty, selectinload, mapped_column, InstrumentedAttribute
from sqlalchemy.orm.strategy_options import _AbstractLoad
from .joins import JoinSpec
from .lookups import ALL_LOOKUPS, apply_lookup
from ..types import ModelT


def resolve_column(model: type[ModelT], path: str) -> InstrumentedAttribute:
    """
    Resolve a dotted / dunder path to a SQLAlchemy column.

    Example:
        resolve_column(User, "profile__country")   → Profile.country
        resolve_column(Order, "user__profile__age") → Profile.age
    """
    parts = path.split("__")
    current_model = model

    for part in parts[:-1]:
        mapper = sa_inspect(current_model)
        rel: RelationshipProperty = mapper.relationships[part]
        current_model = rel.mapper.class_

    col_name = parts[-1]
    return getattr(current_model, col_name)


def resolve_pk_fields(model: type[ModelT]) -> Tuple[ColumnElement[Any], ...]:
    return sa_inspect(model).primary_key


def resolve_pk_name(model: type[ModelT]) -> str:
    pks = resolve_pk_fields(model)
    return pks[0].name


def resolve_pk_column(model: type) -> InstrumentedAttribute:
    pk_name = resolve_pk_name(model)
    return resolve_column(model, pk_name)


def deduplicate_joins(joins: list[JoinSpec]) -> list[JoinSpec]:
    seen = set()
    result = []

    for j in joins:
        key = (j.target_model, str(j.on_clause))
        if key in seen:
            continue
        seen.add(key)
        result.append(j)

    return result


def resolve_path_with_joins(
    model: type[ModelT],
    path: str,
) -> tuple[InstrumentedAttribute, list[JoinSpec]]:
    """
    Walk a dunder path, collect required JOINs, return (column, joins).

    Example:
        path = "profile__country"
        → joins=[JoinSpec(Profile, User.profile_id == Profile.id)]
        → col = Profile.country
    """

    parts = path.split("__")
    joins: list[JoinSpec] = []
    current_model = model

    for part in parts[:-1]:
        mapper = sa_inspect(current_model)
        rel: RelationshipProperty = mapper.relationships[part]
        next_model = rel.mapper.class_

        # Build ON clause from the relationship's local/remote pairs
        local_col, remote_col = next(iter(rel.synchronize_pairs))
        on_clause = local_col == remote_col

        joins.append(
            JoinSpec(
                target_model=next_model,
                on_clause=on_clause,
                isouter=True,
            )
        )
        current_model = next_model

    col: InstrumentedAttribute = getattr(current_model, parts[-1])
    return col, deduplicate_joins(joins)


def build_filter_clause(
    model: type[ModelT],
    key: str,
    value: Any,
) -> tuple[Any, list["JoinSpec"]]:
    """
    Translate a Django-style filter kwarg into (SA clause, [JoinSpec]).

    Example:
        build_filter_clause(User, "profile__age__gte", 18)
        → (Profile.age >= 18, [JoinSpec(Profile, ...)])
    """
    parts = key.split("__")

    # Extract the trailing lookup suffix if present
    if parts[-1] in ALL_LOOKUPS:
        lookup = parts[-1]
        field_path = "__".join(parts[:-1])
    else:
        lookup = "exact"
        field_path = key

    col, joins = resolve_path_with_joins(model, field_path)
    clause = apply_lookup(col, lookup, value)
    return clause, joins


def build_loader_option(model: type[ModelT], path: str, loader: Callable) -> _AbstractLoad:

    parts = path.split("__")
    attr: InstrumentedAttribute = getattr(model, parts[0])

    option = loader(attr)

    current = option
    current_model = attr.property.mapper.class_
    for part in parts[1:]:
        rel_attr = getattr(current_model, part)
        current = (
            current.selectinload(rel_attr)
            if loader is selectinload
            else current.joinedload(rel_attr)
        )
        current_model = rel_attr.property.mapper.class_
    return option


@dataclass
class TraversalColumn:
    col_expr: Label
    join_spec: JoinSpec | None
    group_col: ColumnElement


def resolve_traversal_field(model: type[ModelT], path: str) -> "TraversalColumn | None":
    parts = path.split("__")
    if len(parts) == 1:
        return None

    current_model = model
    pending_joins: list[JoinSpec] = []
    for rel_name in parts[:-1]:
        mapper = sa_inspect(current_model)
        if rel_name not in mapper.relationships:
            return None
        rel = mapper.relationships[rel_name]
        target_model = rel.mapper.class_

        local_cols = list(rel.local_columns)
        remote_cols = list(rel.remote_side)
        if len(local_cols) == 1 and len(remote_cols) == 1:
            on_clause = getattr(current_model, local_cols[0].name) == getattr(
                target_model, remote_cols[0].name
            )
        else:
            from sqlalchemy import and_

            on_clause = and_(
                *(
                    getattr(current_model, lc.name) == getattr(target_model, rc.name)
                    for lc, rc in zip(local_cols, remote_cols)
                )
            )

        pending_joins.append(JoinSpec(target_model=target_model, on_clause=on_clause, isouter=True))
        current_model = target_model

    col_name = parts[-1]
    raw_col: ColumnElement = getattr(current_model, col_name)
    labelled = raw_col.label(path)
    join_spec = pending_joins[0] if pending_joins else None
    return TraversalColumn(col_expr=labelled, join_spec=join_spec, group_col=raw_col)
