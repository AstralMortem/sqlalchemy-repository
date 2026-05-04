from typing import Any
from sqlalchemy import Select, Sequence, inspect as sa_inspect
from sqlalchemy.orm import RelationshipProperty, Query


def resolve_relationship(mapper: Any, attr_name: str) -> RelationshipProperty:
    try:
        return mapper.relationships[attr_name]
    except KeyError:
        available = list(mapper.relationships.keys())
        raise AttributeError(
            f"'{mapper.class_.__name__}' has no relationship '{attr_name}'. "
            f"Available: {available}"
        )


def get_root_model(model_or_query: Any) -> type:
    if isinstance(model_or_query, (Query, Select)):
        return model_or_query.column_descriptions[0]["entity"]
    return model_or_query


def build_loader_tree(
    root_model: type,
    relation_paths: Sequence[str],
    loader_fn: Any,
    chained_fn: Any,
) -> list[Any]:
    """
    Build a minimal, de-duplicated list of SQLAlchemy loader options from
    Django-style double-underscore paths.

    Paths sharing a prefix (e.g. "a__b" and "a__c") reuse the same
    parent loader option object so the JOIN/selectin is not duplicated.
    """
    OPT = "__opt__"
    tree: dict[str, Any] = {}

    for raw_path in relation_paths:
        parts = raw_path.split("__")
        node = tree
        current_model = root_model

        for i, part in enumerate(parts):
            if part not in node:
                mapper = sa_inspect(current_model)
                rel = resolve_relationship(mapper, part)
                next_model = rel.mapper.class_
                attr = getattr(current_model, part)

                if i == 0:
                    opt = loader_fn(attr)
                else:
                    parent_opt = node[OPT]
                    opt = chained_fn(parent_opt, attr)

                node[part] = {OPT: opt}
            else:
                next_model = resolve_relationship(
                    sa_inspect(current_model), part
                ).mapper.class_

            node = node[part]
            current_model = next_model

    return [v[OPT] for k, v in tree.items()]
