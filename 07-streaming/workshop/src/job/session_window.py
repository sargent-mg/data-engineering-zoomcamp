import os
from pyflink.table import EnvironmentSettings, TableEnvironment

# 1. Initialize the Table Environment
env_settings = EnvironmentSettings.in_streaming_mode()
table_env = TableEnvironment.create(env_settings)

# Important: Homework says topic has 1 partition, so set parallelism to 1
table_env.get_config().set("parallelism.default", "1")

# ... (standard imports and env setup same as Q4)

# 1. Source DDL (Use the 29092 port we fixed!)
source_ddl = """
    CREATE TABLE green_trips (
        lpep_pickup_datetime STRING,
        PULocationID INT,
        event_timestamp AS TO_TIMESTAMP(lpep_pickup_datetime, 'yyyy-MM-dd HH:mm:ss'),
        -- Tighten the watermark to 0 seconds for this batch-to-stream homework
        WATERMARK FOR event_timestamp AS event_timestamp
    ) WITH (
        'connector' = 'kafka',
        'topic' = 'green-trips',
        'properties.bootstrap.servers' = 'redpanda:29092',
        'properties.group.id' = 'flink-homework-5-final-v3', -- NEW GROUP ID
        'scan.startup.mode' = 'earliest-offset',
        'format' = 'json'
    )
"""
table_env.execute_sql(source_ddl)

# 2. Sink Table for Sessions
table_env.execute_sql("""
    CREATE TABLE session_results (
        window_start TIMESTAMP(3),
        window_end TIMESTAMP(3),
        PULocationID INT,
        num_trips BIGINT,
        PRIMARY KEY (window_start, window_end, PULocationID) NOT ENFORCED
    ) WITH (
        'connector' = 'jdbc',
        'url' = 'jdbc:postgresql://postgres:5432/postgres',
        'table-name' = 'session_counts',
        'username' = 'postgres',
        'password' = 'postgres',
        'driver' = 'org.postgresql.Driver'
    )
""")

# 3. Session Window Query (UPDATED to match the sink columns)
query = """
    INSERT INTO session_results
    SELECT 
        window_start,
        window_end,
        PULocationID, 
        COUNT(*) as num_trips
    FROM TABLE(
        SESSION(TABLE green_trips, DESCRIPTOR(event_timestamp), INTERVAL '5' MINUTES)
    )
    GROUP BY window_start, window_end, PULocationID
"""
table_env.execute_sql(query).wait()