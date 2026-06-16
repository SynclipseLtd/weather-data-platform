from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator


with DAG(
    dag_id="weather_data_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["weather", "spark", "postgres"],
) as dag:

    ingest_weather_api = BashOperator(
        task_id="ingest_weather_api",
        bash_command=(
            "cd '/opt/airflow/weather_data_platform' && "
            "python scripts/ingest_weather_api.py"
        ),
    )

    run_spark_pipeline = BashOperator(
        task_id="run_spark_pipeline",
        bash_command=(
            "cd '/opt/airflow/weather_data_platform' && "
            "docker run --rm --user root "
            "-w /app "
            "-v $(pwd):/app "
            "apache/spark:3.5.0 "
            "bash -c "
            "\"pip install pyyaml && "
            "/opt/spark/bin/spark-submit "
            "--packages org.postgresql:postgresql:42.7.3 "
            "spark_jobs/bronze_to_silver_weather.py "
            "--env dev\""
            "--ingestion-date {{ ds_nodash[:4] }}_{{ ds_nodash[4:6] }}_{{ ds_nodash[6:8] }}\""
        ),
    )

    ingest_weather_api >> run_spark_pipeline