---
layout: blue-title-slide
---

## Module 1
# Ingest Data

---
layout: blue-sidebar
---

::header::

## Catalog Data
(books.csv)

::content::

| ISBN          | Title               | Author              | Genre   | Price |
|----------------|---------------------|---------------------|----------|-------|
| 9780743273565 | The Great Gatsby    | F. Scott Fitzgerald | Fiction  | 12.99 |
| 9780061096525 | To Kill a Mockingbird | Harper Lee        | Fiction  | 13.99 |
| 9780743477550 | 1984                | George Orwell       | Fiction  | 11.99 |

---
layout: blue-sidebar
---

::header::

## Connections (Recap)

::content::


1. A named, encrypted credential stored in Airflow. 
2. Your DAG code refers to it by ID 
3. The actual credentials host, port, and password live in the Airflow DB, not in your code.
4. You can also use external secret managers

<br/>

> We can use the same credential across multiple dags

---
layout: blue-sidebar
---

::header::

## Hooks with Connection

::content::

```python
from airflow.providers.postgres.hooks.postgres import PostgresHook

hook = PostgresHook(postgres_conn_id="bookshop_postgres")

# Run a query, get results as list of tuples
rows = hook.get_records("SELECT isbn, title FROM books")

# Bulk insert rows
hook.insert_rows(
    table="books",
    rows=[("978...", "Dune", "Frank Herbert", "Sci-Fi", 16.99)],
    target_fields=["isbn", "title", "author", "genre", "price"],
)
```

<v-clicks>

- `get_records(sql)` — returns a list of tuples
- `get_df(sql)` — returns a pandas DataFrame
- `insert_rows(...)` — bulk insert with optional upsert support
- No need to manage connections manually — the hook handles open/close

</v-clicks>

---
layout: blue-title-slide
---

# Exercise 1

Complete `ingest_books` Dag

---
layout: blue-sidebar
---

::header::

## Idempotency

::content::

- Idempotency ensures that running a data pipeline multiple times with the same input produces the exact same output, preventing duplicates and data corruption

- Consistent Results
- Duplicate Prevention
- Fault tolerance

---
layout: blue-sidebar
---

::header::

## Full-Load

::content::

- The act of replacing the old data with full-set of new data is called Full-load. 
- Also known as `bulk refresh`
- The alternative is incremental load
- Good for small datasets, dimension tables.



---
layout: blue-sidebar
---

::header::

# Ingestion 101: Reflections

::content::

<ul class="check-list">
  <li>Providers and Hooks 201</li>
  <li>Wrote your own DAG</li>
  <li>Idempotency</li>
  <li>Full Load</li>
</ul>



---
layout: blue-sidebar
---

::header::

## Next Goal

::content::

<div class="concept-step warning">
  <strong>What if the publisher sends a new catalog every week?</strong>
  <p>Each file may contain old books with updated prices and new books that were not there before.</p>
</div>

```
books-01-05-2026.csv
books-07-05-2026.csv
books-14-05-2026.csv
```

<div class="concept-shell" style="margin-top:0.75rem">
  <div class="concept-step" v-click>
    <strong>One day, someone wipes the prod database.</strong>
    <p>How do you recover? How do you ensure the same pipelines fetch the same data, in the right order, keeping the catalog up to date?</p>
  </div>
  <div class="concept-step action" v-click>
    <strong>That is our next exercise.</strong>
  </div>
</div>
