# 📦 sqlalchemy-repository

> Django-like Repository & QuerySet layer for SQLAlchemy (async-first)

`sqlalchemy-repository` brings a familiar, expressive, and powerful **QuerySet API** on top of SQLAlchemy, inspired by Django ORM — but designed for **modern async Python**.

It helps you write cleaner, composable database logic with support for:

* 🔍 Django-style filtering (`Q`, `F`)
* 🔗 Relation traversal via `__` syntax
* 📊 Aggregations (`Count`, `Min`, `Max`, etc.)
* ⚡ Async-first design
* 🧱 Repository patterns
* 🧠 Type-safe and extensible

---

## 🚀 Features

* **QuerySet API**

  ```python
  await repo.objects.filter(Q(price__gt=100) & Q(active=True)).all()
  ```

* **Django-style lookups**

  ```python
  Q(name__icontains="bmw")
  Q(category__parent__id=1)
  ```

* **F expressions (field references)**

  ```python
  await repo.objects.update(price=F("price") * 1.1)
  ```

* **Aggregations**

  ```python
  await repo.objects.annotate(total=Count("id"), max_price=Max("price"))
  ```

* **Annotations**

  ```python
  await repo.objects.annotate(total=F("qty") * F("unit_price"))
  ```

* **Async support (SQLAlchemy 2.0+)**

* **Composable query expressions**

* **Repository pattern abstraction**

---

## 📦 Installation

```bash
pip install sqlalchemy-repository
```

---

## ⚙️ Requirements

* Python **3.11+**
* SQLAlchemy **2.0+**


## 🏗 Basic Usage

### Define repository

```python
from sqlalchemy_repository import BaseRepository

class ProductRepository(BaseRepository[Product]):
    model = Product
```

---

### Querying

```python
products = await repo.objects.filter(
    Q(price__gt=100),
    Q(category__name="Engine")
).order_by("-price").all()
```

---

### Get single object

```python
product = await repo.objects.get(id=1)
```

---

### Create

```python
await repo.create(
    name="Brake Pad",
    price=50
)
```

---

### Update with F expressions

```python
await repo.objects.filter(id=1).update(
    price=F("price") * 1.2
)
```

---

### Delete

```python
await repo.objects.filter(price__lt=10).delete()
```

---

## 🔗 Relationships via `__`

```python
await repo.objects.filter(
    category__parent__name="Auto Parts"
)
```

No manual joins needed — handled internally.

---

## 🔍 Q Expressions

```python
from sqlalchemy_repository import Q

query = Q(price__gt=100) & (Q(stock__lt=5) | Q(discount=True))

await repo.objects.filter(query).all()
```

---

## 🧮 Aggregations

```python
from sqlalchemy_repository import Count, Max

result = await repo.objects.annotate(
    total=Count("id"),
    max_price=Max("price")
)
```

---

## 🏷 Annotations

```python
qs = repo.objects.annotate(
    total_price=F("quantity") * F("unit_price")
)

data = await qs.all()
```


## 🛠 Development

Install dev dependencies:

```bash
pip install -e .[dev]
```

Run linters:

```bash
ruff check .
mypy .
```


## 🤝 Contributing

Contributions are welcome.

1. Fork the repo
2. Create a feature branch
3. Write tests
4. Open a PR

---

## 📄 License

MIT License
