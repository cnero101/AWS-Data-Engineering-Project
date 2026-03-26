import boto3
import json
import time
import random
from datetime import datetime

kinesis = boto3.client('kinesis', region_name='us-east-1')

stream_name = "data608-project-sensordata"

#  NEW: Define multiple pipe IDs
PIPE_IDS = ["P-101", "P-102", "P-103", "P-104", "P-105"]

# ---------- VALUE GENERATION ----------
def generate_normal():
    return {
        "pressure_MPa": round(random.uniform(4.5, 6.0), 2),
        "temperature_C": round(random.uniform(10, 45), 2),
        "flow_rate_percent": round(random.uniform(-5, 5), 2),
    }

def generate_anomaly():
    choice = random.choice(["pressure", "temperature", "flow"])

    if choice == "pressure":
        pressure = random.choice([
            random.uniform(2.5, 4.5),
            random.uniform(6.0, 7.0)
        ])
        temperature = random.uniform(10, 45)
        flow = random.uniform(-5, 5)

    elif choice == "temperature":
        temperature = random.choice([
            random.uniform(0, 5),
            random.uniform(46, 84)
        ])
        pressure = random.uniform(4.5, 6.0)
        flow = random.uniform(-5, 5)

    else:
        flow = random.choice([
            random.uniform(-25, -5),
            random.uniform(5, 25)
        ])
        pressure = random.uniform(4.5, 6.0)
        temperature = random.uniform(10, 45)

    return {
        "pressure_MPa": round(pressure, 2),
        "temperature_C": round(temperature, 2),
        "flow_rate_percent": round(flow, 2),
    }

# ---------- STREAM LOOP ----------
start_time = time.time()

while True:
    elapsed = time.time() - start_time

    if elapsed <= 30:
        sensor_data = generate_normal()
    elif elapsed <= 50:
        sensor_data = random.choice([
            generate_normal(),
            generate_anomaly()
        ])
    else:
        start_time = time.time()
        continue

    # ✅ NEW: randomly pick pipe_id
    pipe_id = random.choice(PIPE_IDS)

    data = {
        "datetime": datetime.utcnow().isoformat(),
        "pipe_id": pipe_id,
        "section": f"S-{random.randint(1, 50)}",
        "pressure_MPa": sensor_data["pressure_MPa"],
        "temperature_C": sensor_data["temperature_C"],
        "flow_rate_percent": sensor_data["flow_rate_percent"]
    }

    kinesis.put_record(
        StreamName=stream_name,
        Data=json.dumps(data),
        # ✅ IMPROVED: use pipe_id as partition key
        PartitionKey=pipe_id
    )

    print(f"[{round(elapsed,1)}s] Sent:", data)

    time.sleep(1)
