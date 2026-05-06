from typing import Any
from sqlalchemy import inspect as sa_inspect
from sqlalchemy.orm import RelationshipProperty, selectinload
from .joins import JoinSpec
from .lookups import ALL_LOOKUPS, apply_lookup


def resolve_column(model: type, path: str) -> Any:
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


def resolve_pk_fields(model: type):
    return sa_inspect(model).primary_key


def resolve_pk_name(model: type):
    pks = resolve_pk_fields(model)
    return pks[0].name


def resolve_pk_column(model: type):
    pk_name = resolve_pk_name(model)
    return resolve_column(model, pk_name)


def resolve_parent_fk(model: type, joins: list[JoinSpec]):
    if not joins:
        raise ValueError("Aggregation requires at least one relation")

    first_join = joins[0]
    _, remote = first_join.on_clause.left, first_join.on_clause.right
    return remote


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
    model: type,
    path: str,
) -> tuple[Any, list[JoinSpec]]:
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

    col = getattr(current_model, parts[-1])
    return col, deduplicate_joins(joins)


def build_filter_clause(
    model: type,
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


def build_loader_option(model: type, path: str, loader):

    parts = path.split("__")
    attr = getattr(model, parts[0])

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
