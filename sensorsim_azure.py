import json
import time
import random
from datetime import datetime
from azure.eventhub import EventHubProducerClient, EventData

EVENT_HUB_CONN_STR = os.getenv("EVENT_HUB_CONN_STR")
EVENT_HUB_NAME = os.getenv("EVENT_HUB_NAME", "pipeline-sensor-hub")

PIPES = ["P-101", "P-102", "P-103", "P-104", "P-105"]

def generate_reading(pipe_id):
    return {
        "pipe_id":           pipe_id,
        "timestamp":         datetime.utcnow().isoformat(),
        "pressure_MPa":      round(random.uniform(5.0, 6.0), 3),
        "temperature_C":     round(random.uniform(20.0, 40.0), 2),
        "flow_rate_percent": round(random.uniform(-10.0, 10.0), 2),
    }

def main():
    producer = EventHubProducerClient.from_connection_string(
        conn_str=EVENT_HUB_CONN_STR,
        eventhub_name=EVENT_HUB_NAME
    )
    print("Streaming to Azure Event Hubs... (Ctrl+C to stop)")
    try:
        while True:
            batch = producer.create_batch()
            for pipe_id in PIPES:
                reading = generate_reading(pipe_id)
                batch.add(EventData(json.dumps(reading)))
            producer.send_batch(batch)
            print(f"[{datetime.utcnow().isoformat()}] Sent {len(PIPES)} readings")
            time.sleep(1)
    except KeyboardInterrupt:
        print("Stopped.")
    finally:
        producer.close()

if __name__ == "__main__":
    main()