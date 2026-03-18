import dataclasses
import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import pandas as pd
from kafka import KafkaProducer
from models import Trip, trip_from_row
import numpy as np

# 1. Read data

url = "../data/green_tripdata_2025-10.parquet"
columns = ['lpep_pickup_datetime', 'lpep_dropoff_datetime', 'PULocationID', 'DOLocationID', 'passenger_count', 'trip_distance', 'tip_amount', 'total_amount']

df = pd.read_parquet(url, columns=columns)

# 2. Setup Producer
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

topic_name = 'green-trips'

t0 = time.time()

for _, row in df.iterrows():
    # Convert row to dict and handle datetime strings
    payload = row.to_dict()
    payload['lpep_pickup_datetime'] = payload['lpep_pickup_datetime'].strftime('%Y-%m-%d %H:%M:%S')
    payload['lpep_dropoff_datetime'] = payload['lpep_dropoff_datetime'].strftime('%Y-%m-%d %H:%M:%S')

    payload = {k: (None if isinstance(v, float) and np.isnan(v) else v) for k, v in payload.items()}
    
    producer.send(topic_name, value=payload)

producer.flush()
t1 = time.time()

print(f'took {(t1 - t0):.2f} seconds')