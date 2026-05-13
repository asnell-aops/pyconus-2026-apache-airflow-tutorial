---
layout: blue-sidebar
---

::header::

# What You Learned

::content::

<ul>
  <li>DAG anatomy: <code>@dag</code>, <code>@task</code>, <code>>></code>, manual trigger, Airflow UI</li>
  <li>Connections and PostgresHook — credentials out of code</li>
  <li>Scheduling, catchup, Jinja <code v-pre>{{ ds }}</code>, logical date, XCom</li>
  <li>Branching with <code>@task.branch</code>, trigger rules, quarantine pattern</li>
  <li>Dynamic Task Mapping with <code>.expand()</code> for parallel processing</li>
  <li>Assets - event-driven scheduling between decoupled DAGs</li>
  <li>Full-load, incremental data ingestion, validation, reporting</li>
</ul>

---
layout: blue-sidebar
---

::header::

# Explore Next

::content::

<div class="balanced-cols">
<div>

### Go deeper on Airflow

<v-click>

- **TaskGroups** — visually group related tasks inside a DAG
- **Sensors** — wait for a file, a table, or an external condition
- **KubernetesExecutor** — run each task in its own pod at scale
- **Deferrable operators** — async waiting without blocking a worker slot

</v-click>

</div>

<div>

### Grow the system

<v-click>

- dbt for SQL transform ownership
- Great Expectations for richer data quality contracts
- Asset-aware orchestration with multiple upstream dependencies
- AI pipelines and operators

</v-click>

</div>
</div>

---
layout: title-slide
---

# Thank You

Questions, ideas, and extensions are welcome.


https://www.linkedin.com/in/bhavanicodes
<br/>
bhavanicodes@gmail.com

---
layout: blue-sidebar
---

::header::

# Appendix — Hosting Options

::content::

| Option | Best for | Tradeoff |
|---|---|---|
| **`airflow standalone`** | Local dev, workshops | Single process, not for production |
| **Docker Compose** | Team dev, CI | Easy setup, executor limited to Local |
| **Self-hosted Kubernetes** | Full control, large scale | High ops overhead |
| **Amazon MWAA** | AWS-native shops | Managed, but version lag and limited config |
| **Google Cloud Composer** | GCP-native shops | Managed, expensive at small scale |
| **Astronomer** | Enterprise, any cloud | Fully managed + support, paid |
