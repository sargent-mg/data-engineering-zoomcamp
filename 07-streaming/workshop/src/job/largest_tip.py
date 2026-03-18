import os
from pyflink.table import EnvironmentSettings, TableEnvironment

# 1. Initialize
env_settings = EnvironmentSettings.in_streaming_mode()
table_env = TableEnvironment.create(env_settings)
table_env.get_config().set("parallelism.default", "1")

# 2. Source DDL
source_ddl = """
    CREATE TABLE green_trips (
        lpep_pickup_datetime STRING,
        tip_amount DOUBLE,
        event_timestamp AS TO_TIMESTAMP(lpep_pickup_datetime, 'yyyy-MM-dd HH:mm:ss'),
        WATERMARK FOR event_timestamp AS event_timestamp - INTERVAL '5' SECOND
    ) WITH (
        'connector' = 'kafka',
        'topic' = 'green-trips',
        'properties.bootstrap.servers' = 'redpanda:29092',
        'properties.group.id' = 'flink-homework-6',
        'scan.startup.mode' = 'earliest-offset',
        'format' = 'json'
    )
"""
table_env.execute_sql(source_ddl)

# 3. Sink Table (Primary Key on window_start to satisfy the JDBC connector)
table_env.execute_sql("""
    CREATE TABLE tip_results (
        window_start TIMESTAMP(3),
        total_tip DOUBLE,
        PRIMARY KEY (window_start) NOT ENFORCED
    ) WITH (
        'connector' = 'jdbc',
        'url' = 'jdbc:postgresql://postgres:5432/postgres',
        'table-name' = 'hourly_tips',
        'username' = 'postgres', 'password' = 'postgres',
        'driver' = 'org.postgresql.Driver'
    )
""")

# 4. Aggregation Query (1-Hour Tumbling Window)
query = """
    INSERT INTO tip_results
    SELECT 
        window_start, 
        SUM(tip_amount) as total_tip
    FROM TABLE(
        TUMBLE(TABLE green_trips, DESCRIPTOR(event_timestamp), INTERVAL '1' HOURS)
    )
    GROUP BY window_start, window_end
"""
table_env.execute_sql(query).wait()