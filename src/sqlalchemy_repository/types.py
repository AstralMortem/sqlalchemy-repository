from typing import TYPE_CHECKING, Any, TypeVar, Union
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy import ColumnElement

if TYPE_CHECKING:
    from sqlalchemy_repository.expressions import Q


ModelT = TypeVar("ModelT", bound=DeclarativeBase)
PK = TypeVar("PK")

FilterExpr = Union[ColumnElement[bool], "Q"]

ColRef = Union[str, ColumnElement[Any]]


class _Asc:
    def __repr__(self) -> str:
        return "ASC"


class _Desc:
    def __repr__(self) -> str:
        return "DESC"


ASC = _Asc()
DESC = _Desc()
