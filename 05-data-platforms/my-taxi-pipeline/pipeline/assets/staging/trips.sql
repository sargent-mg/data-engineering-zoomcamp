/* @bruin

# Docs:
# - Materialization: https://getbruin.com/docs/bruin/assets/materialization
# - Quality checks (built-ins): https://getbruin.com/docs/bruin/quality/available_checks
# - Custom checks: https://getbruin.com/docs/bruin/quality/custom

name: staging.trips
type: duckdb.sql

depends:
  - ingestion.trips
  - ingestion.payment_lookup

materialization:
  type: table
  strategy: time_interval
  incremental_key: pickup_datetime
  time_granularity: timestamp

columns:
  - name: pickup_datetime
    type: timestamp
    description: "When the trip started"
    primary_key: true
    checks:
      - name: not_null
  - name: dropoff_datetime
    type: timestamp
    description: "When the trip ended"
    primary_key: true
    checks:
      - name: not_null
  - name: pickup_location_id
    type: integer
    description: "TLC Taxi Zone where meter was engaged"
    primary_key: true
  - name: dropoff_location_id
    type: integer
    description: "TLC Taxi Zone where meter was disengaged"
    primary_key: true
  - name: fare_amount
    type: float
    description: "Time-and-distance fare calculated by the meter"
    primary_key: true
    checks:
      - name: non_negative
  - name: taxi_type
    type: string
    description: "Type of taxi (yellow, green)"
    checks:
      - name: not_null
      - name: accepted_values
        value: ["yellow", "green"]
  - name: payment_type_id
    type: integer
    description: "Payment method code"
    checks:
      - name: not_null
  - name: payment_type_name
    type: string
    description: "Payment method name (from lookup)"
    checks:
      - name: not_null
  - name: passenger_count
    type: integer
    description: "Number of passengers"
    checks:
      - name: non_negative
  - name: trip_distance
    type: float
    description: "Trip distance in miles"
    checks:
      - name: non_negative
  - name: total_amount
    type: float
    description: "Total amount charged to passenger"
    checks:
      - name: non_negative
  - name: extracted_at
    type: timestamp
    description: "When the row was extracted from source"

custom_checks:
  - name: row_count_positive
    description: Ensures the table is not empty
    query: SELECT COUNT(*) > 0 FROM staging.trips
    value: 1

@bruin */

WITH normalized_trips AS (
  SELECT
    -- Ingestion outputs unified pickup_datetime, dropoff_datetime, pickup_location_id, dropoff_location_id
    pickup_datetime,
    dropoff_datetime,
    pickup_location_id,
    dropoff_location_id,
    fare_amount,
    COALESCE(total_amount, fare_amount + COALESCE(extra, 0) + COALESCE(mta_tax, 0) + COALESCE(tip_amount, 0) + COALESCE(tolls_amount, 0)) AS total_amount,
    taxi_type,
    payment_type AS payment_type_id,
    passenger_count,
    trip_distance,
    extracted_at
  FROM ingestion.trips
  WHERE pickup_datetime >= '{{ start_datetime }}'
    AND pickup_datetime < '{{ end_datetime }}'
    AND pickup_datetime IS NOT NULL
    AND dropoff_datetime IS NOT NULL
    AND pickup_location_id IS NOT NULL
    AND dropoff_location_id IS NOT NULL
    AND fare_amount >= 0
    AND passenger_count >= 0
    AND trip_distance >= 0
    AND (COALESCE(total_amount, fare_amount + COALESCE(extra, 0) + COALESCE(mta_tax, 0) + COALESCE(tip_amount, 0) + COALESCE(tolls_amount, 0)) >= 0)
),
deduplicated AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY pickup_datetime, dropoff_datetime, pickup_location_id, dropoff_location_id, fare_amount
      ORDER BY extracted_at DESC
    ) AS rn
  FROM normalized_trips
)
SELECT
  d.pickup_datetime,
  d.dropoff_datetime,
  d.pickup_location_id,
  d.dropoff_location_id,
  d.fare_amount,
  d.taxi_type,
  d.payment_type_id,
  COALESCE(pl.payment_type_name, 'Unknown') AS payment_type_name,
  d.passenger_count,
  d.trip_distance,
  d.total_amount,
  d.extracted_at
FROM deduplicated d
LEFT JOIN ingestion.payment_lookup pl
  ON d.payment_type_id = pl.payment_type_id
WHERE d.rn = 1;  -- Keep only the first (most recent) record per composite key
