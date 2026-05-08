import json
import os
import uuid
import logging
import pandas as pd
import azure.functions as func
from azure.storage.blob import BlobServiceClient
from io import BytesIO
import joblib
import urllib.request
from datetime import datetime, timedelta

app = func.FunctionApp()

STORAGE_CONN_STR = os.environ["STORAGE_CONN_STR"]
DATA_CONTAINER   = "sensordata"
MODEL_CONTAINER  = "modelstore"
MODEL_NAME       = "random_forest_model.joblib"
LOGIC_APP_URL    = os.environ.get("LOGIC_APP_URL", "")

# Cooldown tracker — stores last alert time per pipe
# Resets when the Function cold starts, which is fine for a student project
_last_alert = {}
COOLDOWN_MINUTES = 5

_model = None

def load_model():
    global _model
    if _model is None:
        client = BlobServiceClient.from_connection_string(STORAGE_CONN_STR)
        blob   = client.get_blob_client(MODEL_CONTAINER, MODEL_NAME)
        data   = blob.download_blob().readall()
        _model = joblib.load(BytesIO(data))
        logging.info("Model loaded from Blob Storage")
    return _model

def classify(row):
    features = [[row["pressure_MPa"], row["temperature_C"], row["flow_rate_percent"]]]
    pred = load_model().predict(features)[0]
    return {0: "Normal", 1: "Anomaly", 2: "Critical"}.get(int(pred), "Normal")

def save_to_adls(records):
    df  = pd.DataFrame(records)
    buf = BytesIO()
    df.to_parquet(buf, index=False)
    buf.seek(0)
    pipe_id = records[0].get("pipe_id", "UNKNOWN")
    ts      = pd.Timestamp.utcnow()
    path    = (
        f"processed_data/pipe_id={pipe_id}/"
        f"year={ts.year}/month={ts.month:02d}/day={ts.day:02d}/"
        f"{uuid.uuid4()}.parquet"
    )
    client = BlobServiceClient.from_connection_string(STORAGE_CONN_STR)
    client.get_blob_client(DATA_CONTAINER, path).upload_blob(buf.read(), overwrite=True)
    logging.info(f"Saved Parquet to ADLS: {path}")

def should_send_alert(pipe_id, risk_level):
    """Returns True only if cooldown has passed for this pipe."""
    if risk_level == "Normal":
        return False
    now = datetime.utcnow()
    last = _last_alert.get(pipe_id)
    if last and (now - last) < timedelta(minutes=COOLDOWN_MINUTES):
        logging.info(f"Cooldown active for {pipe_id} — skipping alert")
        return False
    _last_alert[pipe_id] = now
    return True

def send_alert(pipe_id, risk_level, record):
    if not LOGIC_APP_URL:
        return

    actions = {
        "Anomaly":  "Please dispatch inspection team.",
        "Critical": "IMMEDIATE ACTION REQUIRED. Shut down pipeline section and dispatch maintenance team."
    }

    batch_id  = str(uuid.uuid4())
    pressure  = record.get("pressure_MPa", "N/A")
    temp      = record.get("temperature_C", "N/A")
    flow      = record.get("flow_rate_percent", "N/A")
    timestamp = record.get("timestamp", "N/A")
    action    = actions.get(risk_level, "Monitor pipeline.")

    html_body = f"""
    <div style="font-family: Arial, sans-serif; max-width: 600px; border: 1px solid #ccc; padding: 0;">
      <div style="background-color: #1a1a2e; padding: 16px 20px;">
        <h2 style="color: #ffffff; margin: 0; font-size: 16px; letter-spacing: 1px;">
          PIPELINE EARLY WARNING ALERT
        </h2>
      </div>
      <div style="padding: 20px; background-color: #f9f9f9;">
        <p style="margin: 4px 0; font-size: 13px; color: #333;">
          <strong>Batch ID:</strong> {batch_id}
        </p>
        <p style="margin: 4px 0; font-size: 13px; color: #333;">
          <strong>Timestamp:</strong> {timestamp}
        </p>
        <p style="margin: 4px 0 16px 0; font-size: 13px; color: #333;">
          <strong>Anomalies:</strong> 1 of 1 records
        </p>
        <table style="width: 100%; border-collapse: collapse; font-size: 13px;">
          <tr style="background-color: #e8e8e8;">
            <td style="padding: 8px 12px; border: 1px solid #ccc;">
              <strong>[{risk_level}]</strong> {pipe_id}
              &nbsp;&nbsp;—&nbsp;&nbsp;
              P={pressure} MPa
              &nbsp;&nbsp;
              T={temp} °C
              &nbsp;&nbsp;
              Flow={flow} %
            </td>
          </tr>
        </table>
        <p style="margin: 20px 0 4px 0; font-size: 13px; color: #333;">
          {action}
        </p>
      </div>
    </div>
    """

    payload = json.dumps({
        "pipe_id":    pipe_id,
        "risk_level": risk_level,
        "timestamp":  timestamp,
        "pressure":   pressure,
        "temperature": temp,
        "flow_rate":  flow,
        "action":     action,
        "message":    html_body
    }).encode()

    req = urllib.request.Request(
        LOGIC_APP_URL,
        data=payload,
        headers={"Content-Type": "application/json"}
    )
    urllib.request.urlopen(req)
    logging.info(f"Alert sent for {pipe_id} — {risk_level}")

@app.event_hub_message_trigger(
    arg_name="events",
    event_hub_name="pipeline-sensor-hub",
    connection="EVENT_HUB_CONN_STR",
    cardinality="many",
    consumer_group="$Default"
)
def pipeline_processor(events: func.EventHubEvent) -> None:
    records = []
    for event in events:
        record = json.loads(event.get_body().decode("utf-8"))
        record["risk_level"] = classify(record)
        records.append(record)

        if should_send_alert(record["pipe_id"], record["risk_level"]):
            send_alert(record["pipe_id"], record["risk_level"], record)
            logging.warning(f"ALERT sent: {record['pipe_id']} = {record['risk_level']}")

    if records:
        save_to_adls(records)
        logging.info(f"Processed {len(records)} events")