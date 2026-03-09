/* @bruin

# Docs:
# - SQL assets: https://getbruin.com/docs/bruin/assets/sql
# - Materialization: https://getbruin.com/docs/bruin/assets/materialization
# - Quality checks: https://getbruin.com/docs/bruin/quality/available_checks

name: reports.trips_report

type: duckdb.sql

depends:
  - staging.trips

materialization:
  type: table
  strategy: time_interval
  incremental_key: trip_date
  time_granularity: date

columns:
  - name: trip_date
    type: date
    description: "Date of the trip pickup"
    primary_key: true
    checks:
      - name: not_null
  - name: taxi_type
    type: string
    description: "Type of taxi (yellow, green)"
    primary_key: true
    checks:
      - name: not_null
      - name: accepted_values
        value: ["yellow", "green"]
  - name: payment_type_name
    type: string
    description: "Payment method name"
    primary_key: true
    checks:
      - name: not_null
  - name: trip_count
    type: bigint
    description: "Number of trips"
    checks:
      - name: not_null
      - name: positive
  - name: total_revenue
    type: float
    description: "Total revenue in USD"
    checks:
      - name: not_null
      - name: non_negative
  - name: avg_fare_amount
    type: float
    description: "Average fare amount per trip"
    checks:
      - name: not_null
      - name: non_negative
  - name: avg_trip_distance
    type: float
    description: "Average trip distance in miles"
    checks:
      - name: not_null
      - name: non_negative
  - name: total_passengers
    type: bigint
    description: "Total number of passengers"
    checks:
      - name: not_null
      - name: non_negative
  - name: avg_passengers_per_trip
    type: float
    description: "Average number of passengers per trip"
    checks:
      - name: not_null
      - name: non_negative

custom_checks:
  - name: row_count_positive
    description: Ensures the report table is not empty
    query: SELECT COUNT(*) > 0 FROM reports.trips_report
    value: 1

@bruin */

SELECT
  DATE(pickup_datetime) AS trip_date,
  taxi_type,
  payment_type_name,
  COUNT(*) AS trip_count,
  SUM(total_amount) AS total_revenue,
  AVG(fare_amount) AS avg_fare_amount,
  AVG(trip_distance) AS avg_trip_distance,
  SUM(passenger_count) AS total_passengers,
  AVG(CAST(passenger_count AS DOUBLE)) AS avg_passengers_per_trip
FROM staging.trips
WHERE pickup_datetime >= '{{ start_datetime }}'
  AND pickup_datetime < '{{ end_datetime }}'
GROUP BY
  DATE(pickup_datetime),
  taxi_type,
  payment_type_name
ORDER BY
  trip_date DESC,
  taxi_type,
  payment_type_name
