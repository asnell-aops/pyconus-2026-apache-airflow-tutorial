---
layout: title-slide
---

# Apache Airflow 101

---
layout: title-slide
---

# Hello World
## Core Airflow Concepts in One DAG

<div class="section-note">
Before the real pipeline, build one DAG that exercises every concept you will rely on.
</div>

---
layout: plain
---

# Airflow 101 Dag

<div style="width: 100%; max-width: 1100px; margin: 0 auto; margin-right: 1em ;">
<div style="transform-origin: top left;">
  
```mermaid
flowchart LR
    A["greet\n@task"] --> B["check_db_with_hook\n@task"]
    A --> C["check_db\n@task"]
    C --> D["show_env\n@task\nreads Variable"]
    B --> E["branch\nBranchPythonOperator"]
    D --> E
    E -->|"env=dev"| F["path_dev\nEmptyOperator"]
    E -->|"env=prod"| G["path_prod\nEmptyOperator"]
    F --> H["done"]
    G --> H
```

</div>
</div>

---
layout: blue-title-slide
---

# Exercise 0
### Run the Hello World DAG

Trigger it, Some tasks will fail, check why


`dags/00_hello_world.py`

---
layout: blue-sidebar
---

::header::

# The DAG

::content::

```python
@dag(
    dag_id="00_hello_world",
    start_date=datetime(2026, 1, 1),
    schedule=None,       # manual trigger only
    catchup=False,
    tags=["workshop", "hello-world"],
)
def hello_world():
    ...

hello_world()            # registers the DAG with Airflow
```

<v-clicks>

- A DAG is a Python function decorated with `@dag`
- `schedule=None` means the DAG only runs when you click **Trigger DAG**
- `catchup=False` prevents Airflow from backfilling historical runs on first load
- The function call at the bottom instantiates and registers the DAG object

</v-clicks>

---
layout: blue-sidebar
---

::header::

# Tasks and Operators

::content::

<div class="balanced-cols">

<v-clicks>
<div>

### `@task` — Python shorthand

```python
@task
def say_hello() -> str:
    message = "Hello, Airflow 3!"
    print(message)
    return message

hello_task = say_hello()
```
</div>

<div>

### Equvivalent `PythonOperator`

```python

def say_hello() -> str:
    message = "Hello, Airflow 3!"
    print(message)
    return message

hello_task = PythonOperator(
    task_id="check_date",
    python_callable=say_hello()
)
```
</div>
</v-clicks>

</div>

<v-click>

### Other SDK Alternatives

- `@task.bash`
- `@task.branch`
- `@task.virutalenv`

</v-click>


---
layout: blue-sidebar
---

::header::

# Task Chaining

::content::

```python
greet_task = greet()
hook_task = check_db_with_hook()

greet_task >> hook_task >> branch
greet_task >> check_db() >> show_env() >> branch
branch >> [path_dev, path_prod]
[path_dev, path_prod] >> done
```

<v-clicks>

- `>>` declares a dependency: left must complete before right starts
- Putting tasks in a list `[a, b] >> c` means both `a` and `b` must finish
- Airflow resolves the full dependency graph before scheduling any task

</v-clicks>

---
layout: blue-sidebar
---

::header::

# BranchPythonOperator

::content::

```python
def _pick_branch() -> str:
    env = Variable.get("bookshop_env", default_var="dev")
    return "path_prod" if env == "prod" else "path_dev"

branch = BranchPythonOperator(
    task_id="branch",
    python_callable=_pick_branch,
)

branch >> [path_dev, path_prod]
```

<v-clicks>

- Returns the `task_id` (or a list) of the branch to follow
- Tasks on the unchosen path are **skipped**, not failed
- Real use cases: environment checks, feature flags, data-driven routing

</v-clicks>


---
layout: blue-sidebar
---

::header::

# Trigger Rules

::content::

```python
join = EmptyOperator(
    task_id="join",
    trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
)
```

<div class="balanced-cols" style="margin-top:1rem">
<div>

### Default: `ALL_SUCCESS`

<v-clicks>

- Every upstream task must succeed
- A skipped branch **blocks** the downstream task
- Works fine when there is no branching

</v-clicks>

</div>
<div>

### `NONE_FAILED_MIN_ONE_SUCCESS`

<v-clicks>

- No upstream task failed
- At least one upstream task succeeded
- Skipped tasks are acceptable — the join proceeds

</v-clicks>

</div>
</div>

<div class="exercise-why" v-click>
After a branch, always set <code>trigger_rule</code> on the join task, or Airflow will wait for a success that will never arrive.
</div>

---
layout: blue-sidebar
---

::header::

## Connections

::content::


1. A named, encrypted credential stored in Airflow. Your DAG code refers to it by ID — the actual host, port, and password live in the UI, not in the code
2. To create it, Go to Airflow UI → Admin → Connections → + Add. Set Conn ID to <code>bookshop_postgres</code>, Conn Type to <code>Postgres</code>

<br/>

<v-click>

<div class="exercise-why" v-click>
Used when: The same DAG file works in dev and prod because the connection ID stays the same — only the credentials change per environment.
</div>

</v-click>

---
layout: blue-sidebar
---

::header::

## Providers

::content::

- Providers are python packages that follow airflow specs
- This enables airflow to connect, run task, pull data from an external system
- Common providers include `standard`, `postgres`, `aws`, `google`


---
layout: blue-title-slide
---

# Mini-Exercise

### Explore Provider Packages

https://airflow.apache.org/docs/#providers-packages




---
layout: blue-sidebar
---

::header::

# Airflow Architecture

- Scheduler
- API Server
- Executor/Worker
- Triggerers
- Dag Processor
- Metadata DB


::content::

![Airflow Architecture](/airflow-architecture.png)

---
layout: blue-sidebar
---

::header::

# Airflow Architecture

![Airflow Architecture](/airflow-architecture.png)

::content::

<v-clicks>

- **Scheduler** — continuously parses DAG files and queues tasks that are ready to run
- **API Server** — serves the UI and the REST API; reads state from the metadata DB
- **Executor** — decides where tasks run (same process, separate process, or remote worker)
- **Triggerer** — like worker, but for deferred tasks so it doesn't block other work
- **Dag processor** — serializes DAGs and makes them available to other components
- **Metadata DB** — the source of truth for all run history, connections, and variables

</v-clicks>

---
layout: blue-sidebar
---

::header::

# Executors

::content::

<div class="exercise-why">
This workshop uses <strong>SequentialExecutor</strong> — one task at a time, no infrastructure setup.
</div>

<v-clicks>

- **SequentialExecutor** — one task at a time; default for `airflow standalone`
- **LocalExecutor** — parallel tasks on a single machine; good for team development
- **CeleryExecutor / KubernetesExecutor** — distributed workers for production scale

</v-clicks>

<div class="exercise-why" v-click>
Changing the executor will not change how to Dag Runs.
</div>

---
layout: blue-sidebar
---

::header::

# Airflow 101: Reflections

::content::

<ul class="check-list">
  <li>Airflow pipeline concetps - Dag, Task, Variables, Connections...</li>
  <li>DAG Code Concepts - Chaining, Branching, Trigger Rules</li>
  <li>Airflow Infrastructure</li>
  <li>Airflow is still an orchestration engine, not a transformation engine</li>
</ul>
