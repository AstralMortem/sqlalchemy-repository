# SQLAlchemy Repository

This repository provides a set of base classes for building SQLAlchemy-based repositories. It includes functionality for performing CRUD operations, querying, and updating database records.

## Installation

To use this repository, you need to have SQLAlchemy installed in your environment. You can install it using pip:

```shell
pip install sqlalchemy-repository
```

## Usage
To use the repository, you need to create a subclass of 
`BaseWriteRepository`, `BaseReadRepository` or `BaseRepository`
 for each table you want to manage in your database. Here's an example:

```python
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import sessionmaker, declarative_base, Mapped, mapped_column
from sqlalchemy_repository import BaseRepository

Base = declarative_base()

class MyTable(Base):
    id: Mapped[int] = mapped_column(primary_key=True)
    title: Mapped[str]

class MyTableRepository(BaseRepository[MyTable, int]):
    model = MyTable

    # Add any additional custom methods here
```

To use the repository, you need to provide an instance of AsyncSession when creating an instance of the repository class. You can use the sessionmaker class from SQLAlchemy to create the session:
from sqlalchemy_repository.repository import BaseWriteRepository, BaseReadRepository

```python
session = sessionmaker(expire_on_commit=False, autocommit=False, autoflush=False, bind=engine)
repo = MyTableRepository(session())
```

## Features
 - full CRUD, 
 - Django-like queryset in every repository class
 - support Django-like Q, F filtering
 - annotation and aggregation
 - prefetch_related and select_related

## Contributing
Contributions are welcome! If you find any issues or have suggestions for improvements, please open an issue or submit a pull request.

## License
This project is licensed under the MIT License. See the LICENSE file for more details.