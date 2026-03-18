import os
from pyflink.table import EnvironmentSettings, TableEnvironment

# 1. Initialize the Table Environment
env_settings = EnvironmentSettings.in_streaming_mode()
table_env = TableEnvironment.create(env_settings)

# Important: Homework says topic has 1 partition, so set parallelism to 1
table_env.get_config().set("parallelism.default", "1")

# 2. Create Source Table (Kafka/Redpanda)
source_ddl = """
    CREATE TABLE green_trips (
        lpep_pickup_datetime STRING,
        PULocationID INT,
        trip_distance DOUBLE,
        tip_amount DOUBLE,
        event_timestamp AS TO_TIMESTAMP(lpep_pickup_datetime, 'yyyy-MM-dd HH:mm:ss'),
        WATERMARK FOR event_timestamp AS event_timestamp - INTERVAL '5' SECOND
    ) WITH (
        'connector' = 'kafka',
        'topic' = 'green-trips',
        'properties.bootstrap.servers' = 'redpanda:29092', 
        'properties.group.id' = 'flink-homework-4-v2', 
        'scan.startup.mode' = 'earliest-offset',
        'format' = 'json'
    )
"""
table_env.execute_sql(source_ddl)

# 3. Create Sink Table (PostgreSQL)
sink_ddl = """
    CREATE TABLE trip_counts_sink (
        window_start TIMESTAMP(3),
        PULocationID INT,
        num_trips BIGINT
    ) WITH (
        'connector' = 'jdbc',
        'url' = 'jdbc:postgresql://postgres:5432/postgres',
        'table-name' = 'trip_counts',
        'username' = 'postgres',
        'password' = 'postgres',
        'driver' = 'org.postgresql.Driver'
    )
"""
table_env.execute_sql(sink_ddl)

# 4. The Aggregation Query (Tumbling Window TVF)
# This groups by window and PULocationID
query = """
    INSERT INTO trip_counts_sink
    SELECT 
        window_start, 
        PULocationID, 
        COUNT(*) as num_trips
    FROM TABLE(
        TUMBLE(TABLE green_trips, DESCRIPTOR(event_timestamp), INTERVAL '5' MINUTES)
    )
    GROUP BY window_start, window_end, PULocationID
"""

table_env.execute_sql(query).wait()