# Weather Data Platform Architecture

## Overview

This project demonstrates a production style data engineering pipeline using a lakehouse inspired Bronze, Silver and Gold pattern.

## Data Flow

1. Open Meteo API is called by a Python ingestion script.
2. Raw JSON is stored in the Bronze layer partitioned by ingestion date.
3. PySpark transforms raw nested JSON into a cleaned Silver hourly weather dataset.
4. Silver data is validated using data quality checks.
5. Gold data is created as a daily weather summary.
6. Gold data is loaded into PostgreSQL as the serving layer.
7. Power BI connects to PostgreSQL for dashboard reporting.
8. Airflow orchestrates the ingestion and transformation pipeline.
9. GitHub Actions validates code quality through CI and triggers a CD workflow.

## Layers

### Bronze

Stores raw API responses exactly as received.

### Silver

Stores cleaned, typed and deduplicated hourly weather observations.

### Gold

Stores business ready daily weather summaries for reporting.

## Production Engineering Features

Audit logging captures pipeline status and row counts.

Data quality checks validate row counts, nulls, duplicates and sensible value ranges.

Incremental loading processes data by ingestion date.

Metadata configuration supports reusable pipeline settings.

CI/CD validates code through syntax checks, linting, secret scanning and unit testing.

## Reporting

Power BI connects to PostgreSQL and visualises daily weather trends.