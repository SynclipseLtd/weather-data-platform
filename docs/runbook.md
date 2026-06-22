# Weather Data Platform Runbook

## Purpose

This runbook provides operational guidance for monitoring, troubleshooting, maintaining and recovering the Weather Data Platform.

---

## Architecture Overview

Open Meteo API

↓

Bronze Layer (Raw JSON)

↓

Silver Layer (Cleaned Hourly Weather Data)

↓

Gold Layer (Daily Weather Summary)

↓

PostgreSQL

↓

Power BI Dashboard

---

## Components

### Data Ingestion

File:

scripts/ingest_weather_api.py

Purpose:

Calls the Open Meteo API and stores raw JSON files in the Bronze layer.

Output:

data/bronze/open_meteo/raw/ingestion_date=YYYY_MM_DD/

---

### Data Transformation

File:

spark_jobs/bronze_to_silver_weather.py

Purpose:

Transforms Bronze data into Silver and Gold layers.

Responsibilities:

* Parse raw JSON
* Flatten nested structures
* Create hourly weather records
* Perform data quality checks
* Create daily weather summaries
* Load Gold data into PostgreSQL
* Write audit records

---

### Orchestration

File:

dags/weather_pipeline_dag.py

Tasks:

* ingest_weather_api
* run_spark_pipeline

Schedule:

Daily

---

### Reporting

Tool:

Power BI

Data Source:

PostgreSQL

Table:

```sql
SELECT *
FROM daily_weather_summary_prod;
```

---

## Monitoring

### Check Airflow DAG Status

Verify both tasks completed successfully:

* ingest_weather_api
* run_spark_pipeline

Expected:

Green Success Status

---

### Check Audit Log

Connect to PostgreSQL and run:

```sql
SELECT
    environment,
    pipeline_name,
    start_time,
    end_time,
    bronze_row_count,
    silver_row_count,
    gold_row_count,
    status
FROM pipeline_audit_log
ORDER BY start_time DESC;
```

Expected:

Status = SUCCESS

---

### Check Failure Details

```sql
SELECT
    start_time,
    status,
    error_message
FROM pipeline_audit_log
WHERE status = 'FAILED'
ORDER BY start_time DESC;
```

---

### Check Gold Data

```sql
SELECT *
FROM daily_weather_summary_prod;
```

Expected:

Latest weather summary available.

---

## Data Quality Checks

### Row Count Validation

Ensures datasets contain records.

Validation Function:

```python
validate_row_count()
```

---

### Null Validation

Required columns:

* weather_timestamp
* temperature_2m
* relative_humidity_2m

Validation Function:

```python
validate_not_null()
```

---

### Duplicate Validation

Duplicate Keys:

* source
* weather_timestamp
* environment

Validation Function:

```python
validate_no_duplicates()
```

---

## Common Failures

### Bronze Data Missing

Symptoms:

```text
Row count = 0
```

Checks:

* Verify API availability
* Verify ingestion script completed
* Verify Bronze files exist

---

### Duplicate Records

Symptoms:

```text
Duplicate records found using keys
```

Checks:

* Verify Bronze data
* Review deduplication logic

---

### Null Values

Symptoms:

```text
Null validation failure
```

Checks:

* Review API response
* Review transformation logic

---

### PostgreSQL Load Failure

Symptoms:

* Connection refused
* Authentication failure

Checks:

* Verify Docker is running
* Verify PostgreSQL container is running
* Verify POSTGRES_PASSWORD environment variable

---

### CI Pipeline Failure

Checks:

GitHub Actions → Weather Data Platform CI

Review:

* Syntax Check
* Ruff Linting
* Secret Scan
* Unit Tests

---

## Recovery Procedure

### Step 1

Identify the failure:

```sql
SELECT *
FROM pipeline_audit_log
WHERE status = 'FAILED';
```

### Step 2

Review logs from:

* Airflow
* GitHub Actions
* Spark Job Output

### Step 3

Fix the root cause.

### Step 4

Rerun the Airflow DAG.

### Step 5

Verify successful execution:

```sql
SELECT *
FROM pipeline_audit_log
ORDER BY start_time DESC;
```

Expected:

Status = SUCCESS

---

## Useful PowerShell Commands

### Navigate to Project Folder

```powershell
cd "C:\Users\syncl\Desktop\Data Engineering\Projects\weather_data_platform"
```

### Check Git Status

```powershell
git status
```

### View Recent Commits

```powershell
git log --oneline -10
```

### Commit and Push Changes

```powershell
git add .
git commit -m "Your commit message"
git push
```

### Start PostgreSQL Container

```powershell
docker compose -f docker-compose.postgres.yaml up -d
```

### Stop PostgreSQL Container

```powershell
docker compose -f docker-compose.postgres.yaml down
```

### Check Running Containers

```powershell
docker ps
```

### Connect to PostgreSQL

```powershell
docker exec -it weather_postgres psql -U weather_user -d weather_dev
```

### Check Audit Log

```sql
SELECT *
FROM pipeline_audit_log
ORDER BY start_time DESC;
```

### Run API Ingestion Manually

```powershell
python scripts/ingest_weather_api.py --env dev
```

### Run Spark Pipeline Manually

```powershell
docker run --rm --user root -w /app -v "${PWD}:/app" apache/spark:3.5.0 bash -c "pip install pyyaml && /opt/spark/bin/spark-submit --packages org.postgresql:postgresql:42.7.3 spark_jobs/bronze_to_silver_weather.py --env dev"
```

### Run Spark Pipeline For Specific Date

```powershell
docker run --rm --user root -w /app -v "${PWD}:/app" apache/spark:3.5.0 bash -c "pip install pyyaml && /opt/spark/bin/spark-submit --packages org.postgresql:postgresql:42.7.3 spark_jobs/bronze_to_silver_weather.py --env dev --ingestion-date 2026_06_18"
```

### Run Unit Tests

```powershell
$env:PYTHONPATH="."
pytest tests
```

### Run Ruff Linting

```powershell
ruff check scripts spark_jobs tests
```

### Search For Hardcoded Passwords

```powershell
git grep "weather_password"
git grep "postgres_password"
git grep "POSTGRES_PASSWORD"
```

### Open Airflow Project

```powershell
cd "C:\Users\syncl\Desktop\Data Engineering\airflow_project"
```

### Check Airflow Containers

```powershell
docker compose ps
```

### Restart Airflow

```powershell
docker compose restart
```

---

## CI/CD

### CI Workflow

Workflow File:

.github/workflows/ci.yml

Checks Performed:

* Python Syntax Validation
* Ruff Linting
* Secret Scanning
* Unit Testing

Trigger:

Every push to main branch.

---

### CD Workflow

Workflow File:

.github/workflows/cd.yml

Purpose:

Deploy to Development Environment.

Trigger:

Successful completion of CI workflow.

---

## Technology Stack

* Python
* PySpark
* Apache Airflow
* Docker
* PostgreSQL
* Power BI
* GitHub Actions
* Pytest
* Ruff
* Git
* GitHub

---

## Project Owner

Seye Sampson

Repository:

weather-data-platform
