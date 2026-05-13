---
layout: blue-title-slide
---

## Module 2
# Daily Sales Ingest
(Incremental)
---
layout: blue-sidebar
---

::header::

## Scheduling in Airflow

::content::

![Daily sales data inside data/sales folder](/sales-data.png)

---
layout: blue-sidebar
---

::header::

## Scheduling in Airflow

::content::

```python
@dag(
    schedule="@daily",        # run once per day
    start_date=datetime(2026, 5, 1),
    catchup=True,             # backfill all days since start_date
)
```

### Common schedule values

<v-clicks>

- `None` — manual trigger only
- `"@daily"` — once per day at midnight
- `"@hourly"` — once per hour
- `"0 6 * * *"` — cron: 6am every day

</v-clicks>

---
layout: blue-sidebar
---

::header::

## Catchup


::content::


```python
@dag(
    schedule="@daily",        # run once per day
    start_date=datetime(2026, 5, 1),
    catchup=True,             # backfill all days since start_date to today
)
```

<v-click>

1. Start date = 1 May 2026
2. Dag written and activated on - 13th May 2026
3. Schedule = Daily
4. No of catchup DagRuns = 13

</v-click>

<br/>

<v-click>

> if it's scheduled weekly?

</v-click>

<br/>

<v-click>

### Catchup let's your DagRuns catchup(run) on all past schedules
Works only if `schedule != None`

</v-click>

---
layout: blue-sidebar
---

::header::

## Logical Date

::content::

- The date the run <em>represents</em>, not when it actually ran.
- Eg., A dagrun scheduled at midnight might start running at `12:03`, but logical datetime will be `12:00`
- At a task level we can access them as parameters

```python
@task
def load_file(ds=None):
    path = REPO_ROOT / "data" / "sales" / f"{ds}.json"
    #                                          ^ "2026-05-03" for the May 3 run
    return json.loads(path.read_text())
```

- At Operator level we can access them as jinja templates

```python
echo_date = BashOperator(
  task_id="echo_date",
  bash_command="echo {{ ds }}",
)
```

---
layout: blue-title-slide
---

# Mini-Exercise

### Explore other template fields

https://airflow.apache.org/docs/apache-airflow/stable/templates-ref.html


---
layout: blue-sidebar
---

::header::

## XComs

::content::

XComs are an airflow construct to share small data between tasks. 


#### Passing Values Between Tasks

```python
@task
def insert_sales(ds=None):
    hook = PostgresHook(postgres_conn_id="bookshop_postgres")
    rows = ...                 # Assume we fetched the rows here
    return len(rows)           # pushed to XCom automatically

@task
def log_summary(count, ds=None):
    print(f"Date: {ds} | Inserted: {count} records")
    #                       ^ pulled from XCom automatically & passed as argument

# Passing xcom while chaining
# denotes that the output `insert_sales` is input to `log_summary`
log_summary(insert_sales())
```

<v-clicks>

- Return a value from `@task` and it is saved to XCom in the metadata DB
- Pass that return value as an argument to the next task — Airflow wires the pull automatically
- Keep XCom values small: strings, numbers, short lists. Not DataFrames or large files.

</v-clicks>

---
layout: blue-title-slide
---

# Exercise 2
### Ingest Daily Sales

Load one JSON file per day using `ds`. Enable catchup and watch 7 backfill runs trigger.

`dags/02_daily_sales_starter.py`

---
layout: blue-sidebar
---

::header::

# Incremental Ingestion

::content::

<ul class="check-list">
  <li>Incremental Loading</li>
  <li>Backfill Data </li>
  <li>Scheduling Dags </li>
  <li>Logical Date</li>
</ul>

---
layout: blue-sidebar
---

::header::

## Next Goal

::content::

<div class="concept-step">
  <strong>What if we want to validate the data as and when we complete ingestion?</strong>
  <p>How will we schedule the downstream dag based on upstream's completion</p>
</div>
