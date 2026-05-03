import boto3
import json
import time
import random
import signal
import os
from datetime import datetime, timezone

# ---------- CONFIG ----------
STREAM_NAME = "data608-project-sensordata"
REGION = "us-east-1"
ALL_PIPES    = [f"P-{i}" for i in range(1, 6)]
ALL_SECTIONS = [f"S-{i}" for i in range(1, 51)]
ANOMALY_TRIGGER_SEC  = 20
ANOMALY_DURATION_SEC = 3

kinesis = boto3.client("kinesis", region_name=REGION)

# ---------- STATE ----------
start_time    = time.time()
total_sent    = 0
running       = True

# Anomaly lifecycle:
#   None            = not yet triggered
#   dict            = currently sending anomaly
anomaly_sensor  = None   # {"pipe": ..., "section": ..., "field": ...}
anomaly_start   = None   # time anomaly began
anomaly_done    = False  # True after the 3s window has passed (sensor goes silent)
silent_sensor   = None   # {"pipe": ..., "section": ...} kept for display

def handle_exit(sig, frame):
    global running
    running = False

signal.signal(signal.SIGINT, handle_exit)
signal.signal(signal.SIGTERM, handle_exit)

# ---------- VALUE GENERATORS ----------
def gen_normal():
    return {
        "pressure_MPa":    round(random.uniform(4.5, 6.0), 2),
        "temperature_C":   round(random.uniform(10, 45), 2),
        "flow_rate_percent": round(random.uniform(-5, 5), 2),
    }

def gen_anomaly(field):
    pressure    = round(random.uniform(4.5, 6.0), 2)
    temperature = round(random.uniform(10, 45), 2)
    flow        = round(random.uniform(-5, 5), 2)

    if field == "pressure":
        pressure = round(random.choice([
            random.uniform(2.5, 4.49),
            random.uniform(6.01, 7.0),
        ]), 2)
    elif field == "temperature":
        temperature = round(random.choice([
            random.uniform(0, 5),
            random.uniform(46, 84),
        ]), 2)
    else:
        flow = round(random.choice([
            random.uniform(-25, -5.01),
            random.uniform(5.01, 25),
        ]), 2)

    return {
        "pressure_MPa":    pressure,
        "temperature_C":   temperature,
        "flow_rate_percent": flow,
    }

# ---------- SEND ----------
def send_record(pipe_id, section, values):
    record = {
        "datetime":          datetime.now(timezone.utc).isoformat(),
        "pipe_id":           pipe_id,
        "section":           section,
        **values,
    }
    try:
        kinesis.put_record(
            StreamName=STREAM_NAME,
            Data=json.dumps(record),
            PartitionKey=pipe_id,
        )
    except Exception:
        pass
    return record

# ---------- TERMINAL UI ----------
def clear():
    os.system("cls" if os.name == "nt" else "clear")

def draw(elapsed, record, note, status_line):
    clear()
    width = 80
    sep   = "─" * width

    print(f"\033[96m{'SCADA PIPELINE SENSOR SIMULATOR':^{width}}\033[0m")
    print(f"\033[96m{'DATA608: Group 1 Project':^{width}}\033[0m")
    print(f"\033[37m{'Kinesis Stream: ' + STREAM_NAME:^{width}}\033[0m")
    print(sep)
    print(f"  Elapsed: {elapsed:>5.0f}s   Total records sent: {total_sent:>6,}")
    print(f"  {status_line}")
    print(sep)
    print(f"  \033[97m{'PIPE':<8}{'SECTION':<10}"
          f"{'PRESSURE(MPa)':<16}{'TEMP(°C)':<14}{'FLOW(%)':<10}{'NOTE'}\033[0m")
    print(sep)

    if record:
        is_anom = note == "ANOMALY"
        color   = "\033[93m" if is_anom else "\033[0m"
        tag     = "⚠ ANOMALY" if is_anom else "ok"
        print(
            f"  {color}{record['pipe_id']:<8}{record['section']:<10}"
            f"{record['pressure_MPa']:<16.2f}{record['temperature_C']:<14.2f}"
            f"{record['flow_rate_percent']:<10.2f}{tag}\033[0m"
        )
    else:
        print(f"  \033[90m(no record sent this second — anomalous sensor is silent)\033[0m")

    print(sep)
    print(f"  \033[96mPress Ctrl+C to stop\033[0m")
    print(f"  \033[96mRio Sibuea - Alejandro Alvarado - Ifeanyi Njoku\033[0m")

# ---------- MAIN LOOP ----------
print("Starting SCADA simulator... press Ctrl+C to stop.")
time.sleep(1)

while running:
    elapsed = time.time() - start_time

    # --- Phase: trigger anomaly at 20s ---
    if elapsed >= ANOMALY_TRIGGER_SEC and anomaly_sensor is None and not anomaly_done:
        anomaly_sensor = {
            "pipe":    random.choice(ALL_PIPES),
            "section": random.choice(ALL_SECTIONS),
            "field":   random.choice(["pressure", "temperature", "flow"]),
        }
        anomaly_start  = time.time()
        silent_sensor  = {"pipe": anomaly_sensor["pipe"], "section": anomaly_sensor["section"]}

    # --- Phase: end anomaly after 3s, go silent ---
    if anomaly_sensor and (time.time() - anomaly_start) >= ANOMALY_DURATION_SEC:
        anomaly_sensor = None
        anomaly_done   = True

    # --- Decide what to send this second ---
    if anomaly_sensor:
        # Send anomaly from the locked sensor
        values = gen_anomaly(anomaly_sensor["field"])
        record = send_record(anomaly_sensor["pipe"], anomaly_sensor["section"], values)
        total_sent += 1
        note   = "ANOMALY"
        status = (f"\033[93m⚠  Simulated anomaly data sent — "
                  f"{anomaly_sensor['pipe']} / {anomaly_sensor['section']}   "
                  f"Field: {anomaly_sensor['field']}\033[0m")

    elif anomaly_done:
        # Pick any pipe/section EXCEPT the silent one
        candidates = [
            (p, s)
            for p in ALL_PIPES
            for s in ALL_SECTIONS
            if not (p == silent_sensor["pipe"] and s == silent_sensor["section"])
        ]
        pipe, section = random.choice(candidates)
        values = gen_normal()
        record = send_record(pipe, section, values)
        total_sent += 1
        note   = "normal"
        status = (f"\033[37mAnomaly window closed — "
                  f"{silent_sensor['pipe']}/{silent_sensor['section']} is silent. "
                  f"Other sensors resuming normal data.\033[0m")

    else:
        # Normal phase — any random pipe/section
        pipe    = random.choice(ALL_PIPES)
        section = random.choice(ALL_SECTIONS)
        values  = gen_normal()
        record  = send_record(pipe, section, values)
        total_sent += 1
        note   = "normal"
        countdown = max(0, ANOMALY_TRIGGER_SEC - int(elapsed))
        status = f"\033[37mNormal data — anomaly simulation starts in {countdown}s\033[0m"

    draw(elapsed, record, note, status)
    time.sleep(1)

# ---------- EXIT ----------
clear()
print(f"\n\033[92mSimulator stopped.\033[0m")
print(f"Total records sent : {total_sent:,}")
print(f"Runtime            : {time.time() - start_time:.0f}s")
if silent_sensor:
    print(f"Anomalous sensor   : {silent_sensor['pipe']} / {silent_sensor['section']} (now silent)")
print()
