import pandas as pd
import random
from datetime import datetime, timedelta

# Configuration
NUM_RECORDS = 50000
PIPES = [f"P-{i}" for i in range(1, 6)]
SECTIONS = [f"S-{i}" for i in range(1, 51)]

# Class distribution
CLASSES = ["normal", "anomaly", "critical"]
WEIGHTS = [0.83, 0.15, 0.02]

# Generate values based on class
def generate_values(status):
    # PRESSURE (MPa)
    # Normal: 4.5 - 6
    # Anomaly: 2.5 - 4.5 OR 6 - 7
    # Critical: > 7

    # TEMPERATURE (°C)
    # Normal: 10 - 45
    # Anomaly: <5 OR 46 - 84
    # Critical: <0 OR >=85

    # FLOW RATE (%)
    # Normal: -5 to 5
    # Anomaly: -25 to -5 OR 5 to 25
    # Critical: < -25 OR > 25

    if status == "normal":
        pressure = random.uniform(4.5, 6.0)
        temperature = random.uniform(10, 45)
        flow = random.uniform(-5, 5)

    elif status == "anomaly":
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

        else:  # flow anomaly
            flow = random.choice([
                random.uniform(-25, -5),
                random.uniform(5, 25)
            ])
            pressure = random.uniform(4.5, 6.0)
            temperature = random.uniform(10, 45)

    else:  # critical
        choice = random.choice(["pressure", "temperature", "flow"])

        if choice == "pressure":
            pressure = random.uniform(7.0, 8.5)
            temperature = random.uniform(10, 45)
            flow = random.uniform(-5, 5)

        elif choice == "temperature":
            temperature = random.choice([
                random.uniform(-10, 0),
                random.uniform(85, 100)
            ])
            pressure = random.uniform(4.5, 6.0)
            flow = random.uniform(-5, 5)

        else:  # flow critical
            flow = random.choice([
                random.uniform(-40, -25),
                random.uniform(25, 40)
            ])
            pressure = random.uniform(4.5, 6.0)
            temperature = random.uniform(10, 45)

    return round(pressure, 2), round(temperature, 2), round(flow, 2)


# Generate dataset
data = []
start_time = datetime.now()

for i in range(NUM_RECORDS):
    pipe_id = random.choice(PIPES)
    section = random.choice(SECTIONS)

    # Select class with imbalance
    status = random.choices(CLASSES, weights=WEIGHTS, k=1)[0]

    pressure, temperature, flow = generate_values(status)

    timestamp = start_time + timedelta(seconds=i)

    data.append({
        "datetime": timestamp.strftime("%Y-%m-%d %H:%M:%S"),
        "pipe_id": pipe_id,
        "section": section,
        "pressure_MPa": pressure,
        "temperature_C": temperature,
        "flow_rate_percent": flow,
        "status": status
    })

# Convert to DataFrame
df = pd.DataFrame(data)

# Save to CSV
df.to_csv("pipeline_sensor_data_50000.csv", index=False)

# Show distribution
print("Dataset generated: pipeline_sensor_data_50000.csv")
print("\nClass distribution (%):")
print((df["status"].value_counts(normalize=True) * 100).round(2))

print("\nSample data:")
print(df.head())