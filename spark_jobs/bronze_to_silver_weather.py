import argparse
import os
import traceback
from datetime import datetime
from pathlib import Path

import yaml
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, lit, max, posexplode, round, sum, to_date
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from scripts.ingest_weather_api import load_pipeline_config


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Transform Bronze weather data to Silver"
    )

    parser.add_argument(
        "--env",
        choices=["dev", "test", "prod"],
        default="dev",
        help="Environment to run the transformation for",
    )

    parser.add_argument(
        "--ingestion-date",
        default=None,
        help="Optional ingestion date to process, format YYYY_MM_DD",
    )

    parser.add_argument(
        "--pipeline-name",
        default="weather",
        help="Pipeline name to run from pipelines.yaml",
    )

    return parser.parse_args()


def load_config(config_path: str) -> dict:
    with open(config_path, "r") as f:
        return yaml.safe_load(f)

def load_metadata(metadata_path: str) -> dict:
    with open(metadata_path, "r") as file:
        return yaml.safe_load(file)


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("BronzeToSilverWeather")
        .getOrCreate()
    )


def read_bronze_weather_data(
    spark: SparkSession,
    bronze_path: Path,
    source_name: str,
    ingestion_date: str = None
):
    if ingestion_date:
        input_path = bronze_path / source_name / "raw" / f"ingestion_date={ingestion_date}"
    else:
        input_path = bronze_path / source_name / "raw" / "*"

    print(f"Reading bronze data from: {input_path}")

    df = spark.read.option("multiline", "true").json(str(input_path))

    return df

def transform_bronze_to_silver(bronze_df, env: str):
    df = bronze_df.select(
        col("ingestion_timestamp_utc"),
        col("source"),
        col("payload.hourly.time").alias("time"),
        col("payload.hourly.temperature_2m").alias("temperature"),
        col("payload.hourly.relative_humidity_2m").alias("humidity"),
        col("payload.hourly.precipitation").alias("precipitation"),
        col("payload.hourly.wind_speed_10m").alias("wind_speed"),
    )

    df = df.select(
        col("ingestion_timestamp_utc"),
        col("source"),
        posexplode(col("time")).alias("idx", "weather_timestamp"),
        col("temperature"),
        col("humidity"),
        col("precipitation"),
        col("wind_speed"),
    )

    df = df.select(
        col("ingestion_timestamp_utc"),
        col("source"),
        col("weather_timestamp"),
        col("temperature")[col("idx")].alias("temperature_2m"),
        col("humidity")[col("idx")].alias("relative_humidity_2m"),
        col("precipitation")[col("idx")].alias("precipitation"),
        col("wind_speed")[col("idx")].alias("wind_speed_10m"),
    )

    df = (
    df.withColumn("environment", lit(env))
      .withColumn("weather_date", to_date(col("weather_timestamp")))
)

    return df

def validate_row_count(df):
    row_count = df.count()

    print(f"Row count = {row_count}")

    if row_count == 0:
        raise ValueError("Dataset contains zero rows")

    return row_count

def validate_not_null(df, required_columns):
    for column_name in required_columns:
        null_count = df.filter(col(column_name).isNull()).count()

        print(f"{column_name} null count = {null_count}")

        if null_count > 0:
            raise ValueError(f"Column {column_name} contains null values")

def validate_no_duplicates(df, key_columns):
    duplicate_count = (
        df.groupBy(key_columns)
        .count()
        .filter(col("count") > 1)
        .count()
    )

    print(f"Duplicate count = {duplicate_count}")

    if duplicate_count > 0:
        raise ValueError(f"Duplicate records found using keys: {key_columns}")

def write_silver_data(silver_df, silver_path: Path, source_name: str) -> None:
    output_path = silver_path / source_name / "weather"

    writer_df = silver_df.repartition(1)

    print("Partitions before write:")
    print(writer_df.rdd.getNumPartitions())

    (
        writer_df.write
        .mode("overwrite")
        .partitionBy("weather_date")
        .parquet(str(output_path))
    )

    print(f"Silver data written to: {output_path}")

def validate_silver_data(spark: SparkSession, silver_path: Path, source_name: str) -> None:
    output_path = silver_path / source_name / "weather"

    silver_df = spark.read.parquet(str(output_path))

    print("Validated Silver schema:")
    silver_df.printSchema()

    print("Validated Silver row count:")
    print(silver_df.count())

    print("Validated Silver sample:")
    silver_df.show(10, truncate=False)
def transform_silver_to_gold(silver_df, gold_aggregations):

    aggregation_expressions = []

    for output_column, aggregation_config in gold_aggregations.items():

        source_column = aggregation_config["source_column"]
        aggregation_type = aggregation_config["aggregation"]

        if aggregation_type == "avg":
            expression = round(
                avg(source_column),
                2
            ).alias(output_column)

        elif aggregation_type == "sum":
            expression = round(
                sum(source_column),
                2
            ).alias(output_column)

        elif aggregation_type == "max":
            expression = round(
                max(source_column),
                2
            ).alias(output_column)

        else:
            raise ValueError(
                f"Unsupported aggregation type: {aggregation_type}"
            )

        aggregation_expressions.append(expression)

    gold_df = (
        silver_df.groupBy(
            "environment",
            "source",
            "weather_date"
        )
        .agg(*aggregation_expressions)
    )

    return gold_df

def write_gold_data(gold_df, gold_path: Path, source_name: str) -> None:
    output_path = gold_path / source_name / "daily_weather_summary"

    (
        gold_df.coalesce(1)
        .write
        .mode("overwrite")
        .partitionBy("weather_date")
        .parquet(str(output_path))
    )

    print(f"Gold data written to: {output_path}")

def write_gold_to_postgres(gold_df, config: dict) -> None:
    jdbc_url = (
        f"jdbc:postgresql://{config['postgres_host']}:"
        f"{config['postgres_port']}/{config['postgres_database']}"
    )

    (
        gold_df.write
        .format("jdbc")
        .mode("overwrite")
        .option("url", jdbc_url)
        .option("dbtable", config["postgres_table"])
        .option("user", config["postgres_user"])
        .option("password", os.environ["POSTGRES_PASSWORD"])
        .option("driver", "org.postgresql.Driver")
        .save()
    )

    print(f"Gold data written to PostgreSQL table: {config['postgres_table']}")
def write_audit_log(
    spark,
    environment,
    start_time,
    end_time,
    bronze_count,
    silver_count,
    gold_count,
    status,
    error_message=None
):
    schema = StructType([
        StructField("environment", StringType(), True),
        StructField("pipeline_name", StringType(), True),
        StructField("start_time", StringType(), True),
        StructField("end_time", StringType(), True),
        StructField("bronze_row_count", IntegerType(), True),
        StructField("silver_row_count", IntegerType(), True),
        StructField("gold_row_count", IntegerType(), True),
        StructField("status", StringType(), True),
        StructField("error_message", StringType(), True),
    ])

    audit_data = [
        (
            str(environment),
            "weather_pipeline",
            start_time.isoformat(),
            end_time.isoformat(),
            int(bronze_count),
            int(silver_count),
            int(gold_count),
            str(status),
            "" if error_message is None else str(error_message)
        )
    ]

    audit_df = spark.createDataFrame(
        audit_data,
        schema=schema
    )

    (
        audit_df.write
        .format("jdbc")
        .option("url", "jdbc:postgresql://host.docker.internal:5432/weather_dev")
        .option("dbtable", "pipeline_audit_log")
        .option("user", "weather_user")
        .option("password", os.environ["POSTGRES_PASSWORD"])
        .option("driver", "org.postgresql.Driver")
        .mode("append")
        .save()
    )

    print("Audit log written successfully")

def validate_value_ranges(df, validation_rules):
    for column_name, rules in validation_rules.items():
        min_value = rules.get("min")
        max_value = rules.get("max")

        invalid_count = df.filter(
            (col(column_name) < min_value) |
            (col(column_name) > max_value)
        ).count()

        print(f"{column_name} invalid range count = {invalid_count}")

        if invalid_count > 0:
            raise ValueError(
                f"Column {column_name} has {invalid_count} values outside range {min_value} to {max_value}"
            )


def run_data_quality_checks(bronze_df, silver_df, gold_df) -> None:

    validate_row_count(bronze_df)

    validate_row_count(silver_df)

    validate_row_count(gold_df)

    null_weather_date_count = silver_df.filter(
        col("weather_date").isNull()
    ).count()

    print(f"weather_date null count = {null_weather_date_count}")

    if null_weather_date_count > 0:
        raise ValueError(
            f"Data quality failed: {null_weather_date_count} rows have null weather_date"
        )

    print("Data quality checks passed")

def main() -> None:
    args = parse_args()
    start_time = datetime.now()

    bronze_count = 0
    silver_count = 0
    gold_count = 0

    bronze_df = None
    silver_df = None
    gold_df = None
    spark = None

    try:
        config_path = f"config/{args.env}/config.yaml"
        config = load_config(config_path)

        pipeline_config_path = f"config/{args.env}/pipelines.yaml"
        pipeline_config = load_pipeline_config(
        pipeline_config_path,
        args.pipeline_name
        )

        metadata = load_metadata("config/metadata/source_metadata.yaml")
        source_metadata = metadata["sources"][pipeline_config["source_name"]]
        

        bronze_path = Path(config["bronze_path"])
        silver_path = Path(config["silver_path"])
        gold_path = Path(config["gold_path"])
        source_name = pipeline_config["source_name"]

        spark = create_spark_session()
        spark.sparkContext.setLogLevel("ERROR")

        bronze_df = read_bronze_weather_data(
            spark,
            bronze_path,
            source_name,
            args.ingestion_date
        )

        bronze_count = bronze_df.count()

        print("Bronze schema:")
        bronze_df.printSchema()

        print("Bronze sample:")
        bronze_df.show(truncate=False)

        silver_df = transform_bronze_to_silver(
            bronze_df,
            args.env
        )

        silver_df = silver_df.dropDuplicates([
            "source",
            "weather_timestamp",
            "environment"
        ])

        validate_not_null(
        silver_df,
        source_metadata["required_columns"]
        )

        validate_no_duplicates(
        silver_df,
        source_metadata["primary_key"]
       )
        
        validate_value_ranges(
        silver_df,
        source_metadata["validation_rules"]
       )

        silver_count = validate_row_count(silver_df)

        print("Silver schema:")
        silver_df.printSchema()

        print("Silver sample:")
        silver_df.show(truncate=False)

        write_silver_data(
            silver_df,
            silver_path,
            source_name
        )

        validate_silver_data(
            spark,
            silver_path,
            source_name
        )

        validated_silver_df = spark.read.parquet(
            str(silver_path / source_name / "weather")
        )

        gold_df = transform_silver_to_gold(
            validated_silver_df,
            source_metadata["gold_aggregations"]
        )

        gold_count = gold_df.count()

        run_data_quality_checks(
            bronze_df,
            silver_df,
            gold_df
        )

        print("Gold schema:")
        gold_df.printSchema()

        print("Gold sample:")
        gold_df.show(truncate=False)

        write_gold_data(
            gold_df,
            gold_path,
            source_name
        )

        write_gold_to_postgres(
            gold_df,
            config
        )

        end_time = datetime.now()

        write_audit_log(
            spark=spark,
            environment=args.env,
            start_time=start_time,
            end_time=end_time,
            bronze_count=bronze_count,
            silver_count=silver_count,
            gold_count=gold_count,
            status="SUCCESS"
        )

        print(f"Environment: {args.env}")
        print(f"Bronze path: {bronze_path}")
        print(f"Silver path: {silver_path}")
        print(f"Gold path: {gold_path}")

    except Exception:
        end_time = datetime.now()
        error_message = traceback.format_exc()

        print("Pipeline failed")
        print(error_message)

        if spark is not None:
            write_audit_log(
                spark=spark,
                environment=args.env,
                start_time=start_time,
                end_time=end_time,
                bronze_count=bronze_count,
                silver_count=silver_count,
                gold_count=gold_count,
                status="FAILED",
                error_message=error_message
            )

        raise

    finally:
        if spark is not None:
            spark.stop()

if __name__ == "__main__":
    main()