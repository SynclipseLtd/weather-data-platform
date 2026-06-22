# Weather Data Platform

A production style data engineering project that ingests weather API data, processes it through Bronze, Silver and Gold layers, loads the curated output into PostgreSQL, and supports reporting in Power BI.

## Architecture

Open Meteo API  
↓  
Bronze raw JSON landing  
↓  
Silver cleaned hourly weather data  
↓  
Gold daily weather summary  
↓  
PostgreSQL serving layer  
↓  
Power BI dashboard  

## Technology Stack

Python  
PySpark  
Apache Airflow  
Docker  
PostgreSQL  
Power BI  
GitHub Actions  

## Key Engineering Features

API ingestion  
Bronze, Silver, Gold data lake pattern  
Incremental loading by ingestion date  
Metadata driven configuration  
Data quality checks  
Audit logging  
PostgreSQL serving table  
Power BI dashboard  
CI/CD with GitHub Actions  
Unit testing with pytest  
Secret scanning  
Linting with Ruff  

## CI/CD

On every push to main, GitHub Actions runs:

Python syntax checks  
Ruff linting  
Secret scanning  
Unit tests  

After CI succeeds, a CD workflow is triggered to simulate deployment to the Dev environment.