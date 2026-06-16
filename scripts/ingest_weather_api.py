import argparse
import json
import os
from datetime import UTC, datetime
from pathlib import Path

import requests
import yaml


def load_config(config_path: str) -> dict:
    with open(config_path, "r") as f:
        return yaml.safe_load(f)

def load_pipeline_config(config_path: str, pipeline_name: str) -> dict:
    with open(config_path, "r") as f:
        pipeline_config = yaml.safe_load(f)

    return pipeline_config["pipelines"][pipeline_name]


def build_output_dir(bronze_path: str, source_name: str) -> Path:
    ingestion_date = datetime.now(UTC).strftime("%Y_%m_%d")
    return Path(bronze_path) / source_name / "raw" / f"ingestion_date={ingestion_date}"


def get_weather_data() -> dict:
    url = "https://api.open-meteo.com/v1/forecast"

    params = {
        "latitude": 51.5072,
        "longitude": -0.1276,
        "hourly": "temperature_2m,relative_humidity_2m,precipitation,wind_speed_10m",
        "forecast_days": 1,
    }

    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()

    return {
        "ingestion_timestamp_utc": datetime.now(UTC).isoformat(),
        "source": "open_meteo",
        "request_params": params,
        "payload": response.json(),
    }


def write_raw_json(record: dict, output_dir: Path) -> None:
    os.makedirs(output_dir, exist_ok=True)

    file_name = f"weather_raw_{datetime.now(UTC).strftime('%Y%m%dT%H%M%S')}.json"
    output_file = output_dir / file_name

    with open(output_file, "w") as f:
        json.dump(record, f, indent=2)

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Ingest weather API data into the Bronze layer")
    parser.add_argument(
        "--env",
        choices=["dev", "test", "prod"],
        default="dev",
        help="Environment to run the ingestion for",
    )
    return parser.parse_args()



def main() -> None:
    args = parse_args()

    config_path = f"config/{args.env}/config.yaml"
    config = load_config(config_path)

    bronze_path = config["bronze_path"]
    source_name = config["source_name"]

    output_dir = build_output_dir(bronze_path, source_name)
    record = get_weather_data()
    write_raw_json(record, output_dir)

    print(f"Environment: {args.env}")
    print(f"Raw weather data written to: {output_dir}")


if __name__ == "__main__":
    main()