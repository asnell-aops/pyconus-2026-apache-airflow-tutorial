# BookShop Workshop — Exercises

Ensure you've followed the setup guide from Readme.md file. 

---

## Exercise 0 — Hello World

**What you will learn:** Airflow UI, DAG anatomy, `@task` decorator, task dependencies, Variables, branching in the UI.

**No code changes needed.** This DAG is already complete.

### Steps

1. Open the Airflow UI at [http://localhost:8080](http://localhost:8080)
2. Find click on `Dags` on left and find `00_hello_world`
3. On the left corner of dag screen, Go to the Graph View, Absorb what the pipeline looks like for a moment

![Hello World DAG graph view](./assets/images/hello-world-dag-page.png)

4. On the right, There will be a toggle to `enable the DAG`, so that we can run it
5. On the right top, click trigger. Ignore the con
6. Something fails? 
    - `show_env` 
    - `check_db_with_hook`
    - Why?
    - How did `check_db` pass?
    - Let's check the logs
7. Click on the `failed` task, then logs, to see what's going wrong

![Hello World logs page](./assets/images/hello-world-logs-page.png)

8. To fix `show_env` : Go to **Admin > Variables** and create a new variable:
   - Key: `bookshop_env`
   - Value: `prod`
9. To fix `check_db_with_hook` Set up the `bookshop_postgres` connection in the Airflow UI: Go to **Admin > Connections > + Add**
10. Fill in:
    - **Conn ID**: `bookshop_postgres`
    - **Conn Type**: `Postgres`
    - **Host**: `postgres`
    - **Database**: `bookops`
    - **Login**: `airflow`
    - **Password**: `airflow`
    - **Port**: `5432`
11. Click **Save**

![Airflow connections page](./assets/images/airflow-connections.png)
13. You can `Test Connection` are right by clicking on the pulse like icon next to the newly created connection
12. Trigger the DAG again 
    - check `show_env` logs — it should now print `prod`
    - All the `check_db_with_hook` errors would've vanished
13. Notice which branch runs (`path_dev` or `path_prod`) and which is skipped (pink in the UI)
14. Click on [code tab](http://localhost:8080/dags/00_hello_world/code)  and try to associate the code on the right to the graph on the left

### What to look for in the UI

- **Task colors**: green = success, pink = skipped, red = failed
- **Graph view**: shows the dependency structure and which path was taken
- **Logs tab**: every `print()` in your task code appears here

### Key concepts

| Concept | Where you see it |
|---|---|
| `@dag` decorator | Top of `00_hello_world.py` |
| `@task` decorator | `greet`, `check_db`, `show_env` functions |
| `BranchPythonOperator` | The `branch` task |
| `>>` dependency | `greet() >> check_db() >> show_env()` |
| `Variable.get()` | Inside `show_env` |
| `TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS` | The `done` task |
| `Providers & Hooks` | The imports at the top |

---

## Exercise 1 — Ingest the Books Catalog

**What you will learn:** Connections, Hooks, Ingestion, Idempotency

**File to edit:** `dags/01_ingest_books_starter.py`

### Before you start

Ensure `bookshop_postgres` connection in the Airflow UI: Go to **Admin > Connections**

### Steps

1. Open `data/books.csv` and explore it's structure
2. Open `dags/01_ingest_books_starter.py`
3. Scan the code file and make a note of dag, task and it's chaining
4. Open `schema.sql` to check what tables we are creating.
5. **TODO 1** — Complete `load_books` such that the code reads `data/books.csv` and build a list of row tuples:

```python
    with open(REPO_ROOT / "data" / "books.csv") as f:
        reader = csv.reader(f)
        next(reader)  # skip the header
        rows = [tuple(row) for row in reader]
```

6. **TODO 2** — Insert the rows using `hook.insert_rows`:

```python
    hook.insert_rows(
        table="books",
        rows=rows,
        target_fields=["isbn", "title", "author", "genre", "price"],
    )
```

7. Trigger the DAG. Check the `reconcile` task log — it should print the row count.
8. Run the DAG again, what happens? Something is failing? Check why?
9. Let's say tomorrow the seller sends the books.csv with updated price info, how do we update them?
10. Update the task with `replace=True`

```python
    @task
    def load_books():
        ...
        ...
        hook.insert_rows(
            table="books",
            ...
            replace=True,
            replace_index="isbn",
        )
```
11. Open [http://localhost:8501/](http://localhost:8501/) See the `Books Catalog` tab populated with 24 books.

---

## Exercise 2 — Daily Sales Ingest

**What you will learn:** `schedule`, `catchup`, Jinja templating with `{{ ds }}`, logical date, XCom.

**File to edit:** `dags/02_daily_sales_starter.py`

### Steps

1. Open `dags/02_daily_sales_starter.py`

2. **TODO 1** — Add a task that logs the logical date and timestamp so you can confirm each run knows its own date:

```python
@task
def log_date(ds=None, ts=None):
    print(f"Logical date: {ds} | Triggered at: {ts}")
```

3. **TODO 2** — Add a branch that skips insertion when no sales file exists for that date. You used `BranchPythonOperator` in Exercise 0 — same pattern here, different condition:

> This is an sdk way to create a branch operator

```python
@task.branch
def check_file(ds=None):
    path = REPO_ROOT / "data" / "sales" / f"{ds}.json"
    return "insert_sales" if path.exists() else "no_file"
```

4. **TODO 3** — Inside `insert_sales`, load the JSON file using the logical date and delete any existing rows for that date before inserting (so re-runs are safe):

   ```python
   path = REPO_ROOT / "data" / "sales" / f"{ds}.json"
   records = json.loads(path.read_text())
   hook.run("DELETE FROM raw_sales WHERE sale_date = %s", parameters=[ds])
   ```

5. **TODO 4** — Log the summary using XCom:

   ```python
   def log_summary(summary_dict):
       print(f"Date: {summary_dict['date']} | Inserted: {summary_dict['count']} records into raw_sales")
   ```

   The `summary_dict` argument is the return value from `insert_sales`, passed through XCom automatically.

6. Wire the tasks together using `>>`

7. Enable the DAG in the UI (toggle it on). Because `catchup=True` and `start_date=2026-05-01`, Airflow will create one run per day from May 1 to today.

8. Watch the runs complete. Click on any run and check the `log_date` log — confirm the date matches.

9. Open [http://localhost:8501/](http://localhost:8501/) See the `Daily Sales` tab

### Verify

```sql
SELECT sale_date, COUNT(*) FROM raw_sales GROUP BY sale_date ORDER BY sale_date;
-- Shows one row per date that has a sales file (May 1-7); later dates hit the no_file branch
```

### XCom in the UI

After a run completes, click a task > **XCom** tab to see what value it pushed. Try clicking `insert_sales` — you will see the integer row count stored there.

![XCom page](./assets/images/xcom-page.png)

---

## Exercise 3a — Asset Handoff

**What you will learn:** How producer DAGs attach metadata to asset events and how consumer DAGs read that metadata when triggered.

**File to edit:** `dags/03a_asset_handoff_starter.py`

### Context

When one DAG emits an asset, a downstream DAG can be triggered automatically. But how does the consumer DAG know *what* the producer processed? It reads the asset event's `extra` field -- a dict the producer attaches when the event fires.

This exercise wires that handoff with no database or validation involved: pure asset plumbing.

### Steps

1. Open `dags/03a_asset_handoff_starter.py`. It contains two DAGs in one file: `03a_producer` and `03a_consumer`.

2. **TODO 1** -- In `emit_asset`, attach extra data to the asset event:

   ```python
   context["outlet_events"][raw_sales_asset].extra = {"date": ds, "count": 42}
   ```

3. **TODO 2** -- In `print_event`, read the triggering asset events and print the extra:

   ```python
   events = context["triggering_asset_events"].get(raw_sales_asset, [])
   for event in events:
       print(f"raw_sales fired | extra: {event.extra}")
   ```

4. Enable both DAGs in the UI. Trigger `03a_producer_starter` manually (it has `schedule=None`).

5. Watch `03a_consumer_starter` fire automatically. Click on the `print_event` task log -- you should see the extra dict printed.

### What you should see in the logs

```
raw_sales fired | extra: {'date': '2026-05-10', 'count': 42}
```

### Key concepts

| Concept | Where you see it |
|---|---|
| `outlets=[Asset(...)]` | `03a_producer` -- marks what data this task produces |
| `context["outlet_events"][asset].extra` | How the producer attaches metadata to the event |
| `schedule=Asset(...)` | `03a_consumer` -- triggers when the asset is updated |
| `context["triggering_asset_events"]` | How the consumer reads the incoming event and its extra |

---

## Exercise 3b — Validate Sales + Human-in-the-Loop

**What you will learn:** Data quality validation, quarantine pattern, Human-in-the-Loop (HITL), Sharing data across assets with Airflow.

**File to edit:** `dags/03b_validate_sales_starter.py`

### Context

DAG 02 emits the `raw_sales` asset via `outlet_events` and stores the processed date in `extra`. DAG 03b is scheduled on `Asset("raw_sales")` — so every time DAG 02 completes a run, DAG 03b wakes up automatically and reads the date from the event. You practiced exactly this handoff in Exercise 3a.

Each daily sales file has 10 records. Two of them are deliberately bad:

- One record has `quantity: -2` (invalid — you cannot sell negative books)
- One record has an ISBN that does not exist in the `books` table (orphan reference)

The fix is not to drop bad records silently. Instead:
1. Insert the valid records immediately so downstream reports are not blocked
2. Quarantine the bad records and return a formatted table as XCom
3. Pause at `ApprovalOperator` — Airflow renders the quarantine table in the UI for a human to review, then approve or reject

This is the **Human-in-the-Loop** pattern using Airflow 3.2's built-in `ApprovalOperator`. The task pauses the DAG and shows a review form directly in the Airflow UI. No external tools or manual "Mark Success" needed.

### Steps

1. Open `dags/03b_validate_sales_starter.py` and read through the full file before writing any code. Pay attention to how `ApprovalOperator` is wired — its `body` uses `{{ ti.xcom_pull(task_ids='validate_and_insert') }}` to render whatever your task returns.

2. **TODO 1** — Implement `validate_and_insert`. The function receives `**context`. Work through it in four sub-steps:

   **1a** — Read the date from the triggering asset event (same pattern as Exercise 3a):
   ```python
   events = context["triggering_asset_events"].get(Asset("raw_sales"), [])
   ds = events[0].extra["date"]
   ```

   **1b** — Query `raw_sales` for that date and build a known ISBNs set:
   ```python
   hook = PostgresHook(postgres_conn_id="bookshop_postgres")
   raw_rows = hook.get_records(
       "SELECT isbn, sale_date, quantity, total FROM raw_sales WHERE sale_date = %s", parameters=[ds]
   )
   records = [{"isbn": r[0], "sale_date": str(r[1]), "quantity": r[2], "total": float(r[3])} for r in raw_rows]
   known_isbns = {row[0] for row in hook.get_records("SELECT isbn FROM books")}
   ```

   **1c** — Split records, delete existing rows for this date (idempotency), then insert:
   ```python
   valid_rows, bad_rows = [], []
   for rec in records:
       if rec["quantity"] <= 0:
           bad_rows.append((json.dumps(rec), f"invalid quantity: {rec['quantity']}"))
       elif rec["isbn"] not in known_isbns:
           bad_rows.append((json.dumps(rec), f"unknown isbn: {rec['isbn']}"))
       else:
           valid_rows.append((rec["isbn"], rec["sale_date"], rec["quantity"], rec["total"]))
   hook.run("DELETE FROM daily_sales WHERE sale_date = %s", parameters=[ds])
   hook.run("DELETE FROM sales_quarantine WHERE raw->>'sale_date' = %s", parameters=[ds])
   hook.insert_rows("daily_sales", valid_rows, target_fields=["isbn", "sale_date", "quantity", "total"])
   if bad_rows:
       hook.insert_rows("sales_quarantine", bad_rows, target_fields=["raw", "reason"])
   ```

   **1d** — Return a markdown table so `ApprovalOperator` can display it:
   ```python
   if not bad_rows:
       return "No bad records."
   lines = ["| Reason | Raw Data |", "|--------|----------|"]
   for raw, reason in bad_rows:
       lines.append(f"| {reason} | {raw} |")
   return "\n".join(lines)
   ```

   > The return value becomes an XCom. `ApprovalOperator` pulls it via `{{ ti.xcom_pull(...) }}` in its `body`.

3. The `ApprovalOperator` is already wired up in the starter. It shows the quarantine table in the Airflow UI and waits for a human to click **Approve** or **Reject**. Approving emits the `daily_sales` asset and triggers DAG 04.

4. Enable the DAG. Nothing will happen or trigger. Since we missed the past asset events
5. To simulate an asset based trigger click on `Assets -> Raw Sales` click on `Play` icon at the top
6. Paste the following and press create event.

```json
{
  "date": "2026-05-10",
  "count": 10
}
```

7. Go back to the [DAG page](http://localhost:8080/dags/03b_validate_sales_starter) you should see a new DagRun.

8. **Play the human reviewer role:**
   - In the Airflow UI, click on the `approve_or_reject` task
   - The **HITL** panel shows the quarantine table rendered from XCom
   - Click **Approve** to proceed — the `daily_sales` asset fires and DAG 04 starts
   - Click **Reject** to stop the run for that date
   - > Note one can remove approval operator and replace it with a Email or Slack. 

9. Open [http://localhost:8501/](http://localhost:8501/) See the `Daily Sales` tab, it will have 8 sales from one day
10. To simulate the whole run from sales ingestion to validation. We can simulate a backfill run on [Daily Sales](http://localhost:8080/dags/02_daily_sales_starter) DAG.
11. Go to Daily Sales dag page, click trigger, on the dialog box, click `Backfill`. Fill in the date range from 01 May to 13 May, you'll see 12 runs.
12. Click `Run backfill`
13. New asset events will be triggered and new dag runs will queue up in `3b_validate_sales_starter` dag page

![triggering backfill from UI](./assets/images/triggering-backfill-from-ui.png)

### Verify

```sql
SELECT reason, COUNT(*) FROM sales_quarantine GROUP BY reason;
-- Should show rows for "invalid quantity" and "unknown isbn"

SELECT COUNT(*) FROM daily_sales WHERE quantity <= 0;
-- Expected: 0
```

### Why HITL?

Automated pipelines cannot always decide what to do with bad data. `ApprovalOperator` makes the problem visible and blocks downstream processing until a human confirms it is safe to continue. The human sees the actual quarantine data in the Airflow UI and clicks Approve or Reject — no external tools or "Mark Success" workarounds needed.

---

## Exercise 4 — Genre Report

**What you will learn:** Dynamic Task Mapping with `.expand()`, Airflow Assets, event-driven scheduling.

**File to edit:** `dags/04_genre_report_starter.py`

### Context

The business wants a daily breakdown of revenue and units sold per genre. There are 5 genres in the catalog. Instead of aggregating all of them in a single task, we will map one task instance per genre — they run in parallel, and each has its own log and retry.

DAG 04 should run automatically every time DAG 03 finishes inserting valid sales — not on a clock schedule.

### Steps

1. Open `dags/04_genre_report_starter.py`

2. **TODO 1** — Implement `build_genre_report`. It receives one genre string and returns aggregated rows:

   ```python
   @task
   def build_genre_report(genre):
       hook = PostgresHook(postgres_conn_id="bookshop_postgres")
       rows = hook.get_records(
           """
           SELECT ds.sale_date, b.genre, SUM(ds.quantity), SUM(ds.total)
           FROM daily_sales ds
           JOIN books b ON ds.isbn = b.isbn
           WHERE b.genre = %s
           GROUP BY ds.sale_date, b.genre
           """,
           parameters=(genre,),
       )
       return [(row[0], row[1], row[2], row[3]) for row in rows]
   ```

3. **TODO 2** — Implement `merge_results`. Flatten the nested list and upsert:

   ```python
   @task(outlets=[Asset("daily_report")])
   def merge_results(genre_rows):
       hook = PostgresHook(postgres_conn_id="bookshop_postgres")
       all_rows = [row for rows in genre_rows for row in rows]
       hook.insert_rows(
           table="daily_report",
           rows=all_rows,
           target_fields=["report_date", "genre", "books_sold", "revenue"],
           replace=True,
           replace_index=["report_date", "genre"],
       )
       print(f"Upserted {len(all_rows)} rows into daily_report")
   ```

4. **TODO 3** — Switch `build_genre_report(genres)` to `build_genre_report.expand(genre=genres)`:

   ```python
   genres = get_genres()
   genre_rows = build_genre_report.expand(genre=genres)
   merge_results(genre_rows)
   ```

5. Trigger DAG 03 manually. Watch the **DAGs** page — DAG 04 should start automatically within seconds.

### Verify

```sql
SELECT * FROM daily_report ORDER BY report_date, genre;
-- Should show 5 genres per day

SELECT genre, SUM(revenue) AS total FROM daily_report GROUP BY genre ORDER BY total DESC;
```

### See Dynamic Task Mapping in the UI

Click on a completed `04_genre_report_starter` run > **Graph** view > expand the `build_genre_report` node. You will see 5 mapped instances: `build_genre_report[0]` through `build_genre_report[4]`, one per genre.

### See Assets in the UI

Go to **Assets** at the left navigation. You will see the `daily_sales` asset and a dependency arrow pointing from DAG 03 to DAG 04.

---

## Reference

### Schema

![Database tables in DBeaver](./assets/images/dbeaver-tables.png)

```sql
books            (isbn PK, title, author, genre, price)
raw_sales        (sale_id PK, isbn, sale_date, quantity, total)
daily_sales      (sale_id PK, isbn, sale_date, quantity, total)
sales_quarantine (raw JSONB, reason, quarantined_at)
daily_report     (report_date, genre, books_sold, revenue) PK(report_date, genre)
```

### Useful queries

```sql
-- How many books per genre?
SELECT genre, COUNT(*) FROM books GROUP BY genre;

-- What is in quarantine?
SELECT reason, raw FROM sales_quarantine ORDER BY quarantined_at DESC;

-- Full pipeline output
SELECT report_date, genre, books_sold, revenue FROM daily_report ORDER BY report_date, revenue DESC;
```

### Reset everything

```bash
# Drop all tables and start fresh
psql postgresql://airflow:airflow@localhost:5432/bookops -f sql/reset.sql
```
