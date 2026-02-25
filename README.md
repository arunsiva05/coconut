# Airflow 3 — Databricks SQL Sensor + Job Runner

An Airflow 3.1.6 solution that polls Databricks SQL endpoints and triggers
Databricks Jobs when data conditions are satisfied.  AND / OR multi-sensor
logic is configured via a single YAML file.

---

## How it works

```
┌───────────────────────────────────────────────────────────────┐
│  dbx_sql_sensor_dag  (triggered manually or on a schedule)    │
│                                                               │
│   resolve_date ──┬──► sensor__sales_data_ready      ─► Asset │
│                  └──► sensor__inventory_data_ready  ─► Asset │
└──────────────────────────────────┬────────────────────────────┘
                                   │  Airflow Assets
                    (AND → AssetAll / OR → AssetAny)
                                   ▼
┌───────────────────────────────────────────────────────────────┐
│  dbx_job_runner_dag  (auto-triggered by assets)               │
│                                                               │
│   resolve_job_date ──┬──► run__process_sales                  │
│                      └──► run__generate_daily_report          │
└───────────────────────────────────────────────────────────────┘
```

### DAG 1 — `dbx_sql_sensor_dag`

- Resolves the run date (Today / Yesterday / Custom)
- Runs one `DatabricksSqlSensor` per entry in `sensors.checks`
- Each sensor replaces `{YYYYMMDD}` in the SQL at execution time
- On success every sensor emits its Airflow **Asset**

### DAG 2 — `dbx_job_runner_dag`

- Airflow auto-triggers it when assets become available:
  - `operation: AND` → **all** sensors must succeed (`AssetAll`)
  - `operation: OR`  → **any** sensor succeeding is enough (`AssetAny`)
- Runs each configured Databricks job (in parallel) via `DatabricksRunNowOperator`
- Replaces `{YYYYMMDD}` in `notebook_params` / `python_params`

---

## Repository layout

```
coconut/
├── config/
│   └── dag_config.yaml          # Single source of truth — edit here
├── dags/
│   └── dbx_sql_sensor_and_jobs.py   # Both DAGs defined in one file
├── requirements.txt
└── README.md
```

---

## Configuration (`config/dag_config.yaml`)

```yaml
date_mode: "Yesterday"   # Today | Yesterday | Custom
custom_date:             # YYYYMMDD — only for date_mode: Custom

sensors:
  operation: "AND"       # AND → AssetAll | OR → AssetAny

  checks:
    - name: "sales_data_ready"
      connection_id: "databricks_default"
      sql: >
        SELECT COUNT(*) FROM main.default.sales
        WHERE sale_date = '{YYYYMMDD}'
        HAVING COUNT(*) > 0
      asset_name: "sales_data_ready"
      poke_interval: 60   # seconds between polls
      timeout: 7200       # max wait in seconds

jobs:
  - name: "process_sales"
    connection_id: "databricks_default"
    job_id: "123456789"
    notebook_params:
      date: "{YYYYMMDD}"
```

### Date modes

| `date_mode`  | Resolved date                                    |
|--------------|--------------------------------------------------|
| `Today`      | Current calendar date                            |
| `Yesterday`  | Yesterday (default)                              |
| `Custom`     | Value of `custom_date` field (format YYYYMMDD)   |

The sensor DAG also accepts **per-run overrides** via Airflow params at
trigger time:

```json
{
  "date_mode":   "Custom",
  "custom_date": "20240315"
}
```

---

## Setup

### 1. Install dependencies

```bash
pip install -r requirements.txt
```

### 2. Configure the Databricks connection in Airflow

In the Airflow UI (Admin → Connections) create a connection of type
**Databricks** named `databricks_default`:

| Field         | Value                                         |
|---------------|-----------------------------------------------|
| Conn Type     | Databricks                                    |
| Host          | `<your-workspace>.azuredatabricks.net`        |
| Extra (JSON)  | `{"token": "<personal-access-token>"}`        |

Or via the CLI:

```bash
airflow connections add databricks_default \
  --conn-type databricks \
  --conn-host <workspace-url> \
  --conn-extra '{"token":"<pat>"}'
```

### 3. Copy DAGs and config

Place the `dags/` folder and `config/` folder where Airflow can find them
(i.e. under `AIRFLOW_HOME/dags/` or the path set in `dags_folder`).

### 4. Trigger the sensor DAG

```bash
# Default date_mode from config
airflow dags trigger dbx_sql_sensor_dag

# Override to a custom date
airflow dags trigger dbx_sql_sensor_dag \
  --conf '{"date_mode":"Custom","custom_date":"20240315"}'
```

The job runner DAG (`dbx_job_runner_dag`) starts automatically once the
configured asset conditions are satisfied.

---

## Adding sensors or jobs

Edit `config/dag_config.yaml` only — no Python changes required.

- **New sensor:** add an entry under `sensors.checks`
- **New job:** add an entry under `jobs`
- **Change AND/OR logic:** set `sensors.operation: OR` (or back to `AND`)
- **Change date:** set `date_mode` or trigger with `custom_date`
