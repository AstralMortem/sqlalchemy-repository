import pytest
from sqlalchemy_repository.utils.relations import (
    resolve_relationship,
    get_root_model,
    build_loader_tree,
)
from sqlalchemy.orm import declarative_base, relationship, joinedload
from sqlalchemy import Column, Integer, ForeignKey, select

Base = declarative_base()


class ModelC(Base):
    __tablename__ = "c"
    id = Column(Integer, primary_key=True)


class ModelB(Base):
    __tablename__ = "b"
    id = Column(Integer, primary_key=True)
    c_id = Column(Integer, ForeignKey("c.id"))

    c = relationship("ModelC")


class ModelA(Base):
    __tablename__ = "a"
    id = Column(Integer, primary_key=True)
    b_id = Column(Integer, ForeignKey("b.id"))

    b = relationship("ModelB")


def test_resolve_relationship_ok():
    mapper = ModelA.__mapper__

    rel = resolve_relationship(mapper, "b")
    assert rel.key == "b"
    assert rel.mapper.class_ == ModelB


def test_resolve_relationship_invalid():
    mapper = ModelA.__mapper__

    with pytest.raises(AttributeError) as exc:
        resolve_relationship(mapper, "wrong")


def test_get_root_model_from_model():
    assert get_root_model(ModelA) is ModelA


def test_get_root_model_from_query(session):
    # session fixture expected
    q = select(ModelA)

    root = get_root_model(q)

    assert root is ModelA


def test_build_loader_tree_single_path():
    opts = build_loader_tree(
        ModelA,
        ["b"],
        loader_fn=joinedload,
        chained_fn=lambda opt, attr: opt.joinedload(attr),
    )

    assert len(opts) == 1
    assert "Load" in type(opts[0]).__name__


def test_build_loader_tree_nested():
    opts = build_loader_tree(
        ModelA,
        ["b__c"],
        loader_fn=joinedload,
        chained_fn=lambda opt, attr: opt.joinedload(attr),
    )

    assert len(opts) == 1


def test_build_loader_tree_deduplication():
    """
    Critical test:
    "b__c" and "b" should NOT produce duplicate root loaders.
    """
    opts = build_loader_tree(
        ModelA,
        ["b", "b__c"],
        loader_fn=joinedload,
        chained_fn=lambda opt, attr: opt.joinedload(attr),
    )

    # Only ONE root loader for "b"
    assert len(opts) == 1


def test_build_loader_tree_multiple_branches():
    """
    "b__c" and (hypothetical) "b__d" should share root "b"
    """

    class ModelD(Base):
        __tablename__ = "d"
        id = Column(Integer, primary_key=True)
        b_id = Column(ForeignKey("b.id"))

    # monkey patch relationship for test
    ModelB.d = relationship("ModelD")

    opts = build_loader_tree(
        ModelA,
        ["b__c", "b__d"],
        loader_fn=joinedload,
        chained_fn=lambda opt, attr: opt.joinedload(attr),
    )

    assert len(opts) == 1  # still one root loader


def test_build_loader_tree_invalid_path():
    with pytest.raises(AttributeError):
        build_loader_tree(
            ModelA,
            ["b__wrong"],
            loader_fn=joinedload,
            chained_fn=lambda opt, attr: opt.joinedload(attr),
        )
