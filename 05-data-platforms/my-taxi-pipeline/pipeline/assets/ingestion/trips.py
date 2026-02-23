"""@bruin

# TODO: Set the asset name (recommended pattern: schema.asset_name).
# - Convention in this module: use an `ingestion.` schema for raw ingestion tables.
name: ingestion.trips

# TODO: Set the asset type.
# Docs: https://getbruin.com/docs/bruin/assets/python
type: python

# TODO: Pick a Python image version (Bruin runs Python in isolated environments).
# Example: python:3.11
image: python:3.11

# TODO: Set the connection.
connection: duckdb-default

# TODO: Choose materialization (optional, but recommended).
# Bruin feature: Python materialization lets you return a DataFrame (or list[dict]) and Bruin loads it into your destination.
# This is usually the easiest way to build ingestion assets in Bruin.
# Alternative (advanced): you can skip Bruin Python materialization and write a "plain" Python asset that manually writes
# into DuckDB (or another destination) using your own client library and SQL. In that case:
# - you typically omit the `materialization:` block
# - you do NOT need a `materialize()` function; you just run Python code
# Docs: https://getbruin.com/docs/bruin/assets/python#materialization
materialization:
  # TODO: choose `table` or `view` (ingestion generally should be a table)
  type: table
  # TODO: pick a strategy.
  # suggested strategy: append
  strategy: append
@bruin"""

import os
import json
from datetime import datetime

import pandas as pd

# NYC TLC trip data: parquet files per taxi type per month
BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/"


def _parse_vars():
    """Read pipeline variables from BRUIN_VARS (JSON)."""
    raw = os.environ.get("BRUIN_VARS", "{}")
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return {}


def _month_range(start_date: str, end_date: str):
    """Yield (year, month) for each month in [start_date, end_date] (inclusive)."""
    from dateutil.relativedelta import relativedelta

    start = datetime.strptime(start_date[:10], "%Y-%m-%d").date()
    end = datetime.strptime(end_date[:10], "%Y-%m-%d").date()
    current = start.replace(day=1)
    end_first = end.replace(day=1)
    while current <= end_first:
        yield current.year, current.month
        current += relativedelta(months=1)


def materialize():
    """
    Ingest NYC taxi trip data from TLC parquet endpoint.

    Uses BRUIN_START_DATE / BRUIN_END_DATE for the run window and the
    `taxi_types` pipeline variable (e.g. ["yellow"], ["yellow", "green"]).
    Keeps data raw; adds extracted_at for lineage.
    """
    start_date = os.environ.get("BRUIN_START_DATE", "")
    end_date = os.environ.get("BRUIN_END_DATE", "")
    if not start_date or not end_date:
        raise ValueError("BRUIN_START_DATE and BRUIN_END_DATE must be set")

    vars_ = _parse_vars()
    taxi_types = vars_.get("taxi_types", ["yellow"])
    if not isinstance(taxi_types, list):
        taxi_types = [taxi_types]

    extracted_at = datetime.utcnow().isoformat() + "Z"
    frames = []

    for year, month in _month_range(start_date, end_date):
        for taxi_type in taxi_types:
            filename = f"{taxi_type}_tripdata_{year}-{month:02d}.parquet"
            url = f"{BASE_URL}{filename}"
            try:
                df = pd.read_parquet(url)
                # Normalize to unified schema so staging works for yellow-only or yellow+green
                renames = {}
                if "tpep_pickup_datetime" in df.columns:
                    renames["tpep_pickup_datetime"] = "pickup_datetime"
                    renames["tpep_dropoff_datetime"] = "dropoff_datetime"
                elif "lpep_pickup_datetime" in df.columns:
                    renames["lpep_pickup_datetime"] = "pickup_datetime"
                    renames["lpep_dropoff_datetime"] = "dropoff_datetime"
                if "PULocationID" in df.columns:
                    renames["PULocationID"] = "pickup_location_id"
                    renames["DOLocationID"] = "dropoff_location_id"
                if renames:
                    df = df.rename(columns=renames)
                df["taxi_type"] = taxi_type
                df["extracted_at"] = extracted_at
                frames.append(df)
            except Exception as e:
                # Skip missing or unreadable files (e.g. future months, 404)
                print(f"Skipping {url}: {e}")
                continue

    if not frames:
        return pd.DataFrame()

    return pd.concat(frames, ignore_index=True)
