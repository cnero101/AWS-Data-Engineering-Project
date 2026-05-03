import base64
import json
import boto3
import logging
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from io import BytesIO
from datetime import datetime, timezone
import joblib

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# AWS clients
s3 = boto3.client('s3', region_name='us-east-1')
sns_client = boto3.client('sns', region_name='us-east-1')

# Constants
SNS_TOPIC_ARN = 'arn:aws:sns:us-east-1:787118717748:topic2'
S3_BUCKET = 'data608-project-sensordata'
MODEL_BUCKET = 'data608-project-model'
MODEL_KEY = 'random_forest_model.joblib'
LOCAL_MODEL_PATH = '/tmp/random_forest_model.joblib'

# Load ML model once per container (cold start)
clf = None
def load_model():
    global clf
    if clf is None:
        try:
            logger.info("Downloading ML model from S3...")
            s3.download_file(MODEL_BUCKET, MODEL_KEY, LOCAL_MODEL_PATH)
            clf = joblib.load(LOCAL_MODEL_PATH)
            logger.info("ML model loaded successfully.")
        except Exception as e:
            logger.error(f"Failed to load model: {e}")
            raise
    return clf

# Lambda handler
def lambda_handler(event, context):
    logger.info(f"Event ID: {context.aws_request_id}")

    model = load_model()

    # Group records by pipe_id so each pipe gets its own Parquet file
    records_by_pipe = {}

    if 'Records' not in event:
        return {'status': 'no records'}

    for record in event['Records']:
        try:
            payload = base64.b64decode(record['kinesis']['data']).decode('utf-8')
            data = json.loads(payload)

            # Add timestamp
            data['processed_timestamp'] = datetime.now(timezone.utc).isoformat()

            # ----- MACHINE LEARNING PREDICTION -----
            features = pd.DataFrame([{
                'pressure_MPa':      data['pressure_MPa'],
                'temperature_C':     data['temperature_C'],
                'flow_rate_percent': data['flow_rate_percent']
            }])
            label_map = {0: 'normal', 1: 'anomaly', 2: 'critical'}
            prediction = model.predict(features)[0]
            data['status'] = label_map[int(prediction)]

            logger.info(f"Prediction: pipe={data['pipe_id']} status={data['status']}")

            # Group by pipe_id for partitioned writes
            pipe_id = data['pipe_id']
            if pipe_id not in records_by_pipe:
                records_by_pipe[pipe_id] = []
            records_by_pipe[pipe_id].append(data)

            # SNS alert if anomaly or critical
            if data['status'] in ['anomaly', 'critical']:
                message = f"""
🚨 PIPELINE ALERT 🚨

Status: {data['status'].upper()}

Pipe ID: {data['pipe_id']}
Section: {data['section']}

Pressure (MPa):   {data['pressure_MPa']}
Temperature (°C): {data['temperature_C']}
Flow Rate (%):    {data['flow_rate_percent']}

Processed Time (UTC): {data['processed_timestamp']}

Normal Threshold Guidance:
Pressure (MPa): 4.5MPa - 6MPa
Temperature (°C): 10°C - 45°C
Flow Rate (%): -5% - 5%
"""
                sns_client.publish(
                    TopicArn=SNS_TOPIC_ARN,
                    Message=message,
                    Subject=f"Pipeline Alert - {data['status'].upper()}"
                )
                logger.info(f"SNS alert sent for pipe={data['pipe_id']} status={data['status']}")

        except Exception as e:
            logger.error(f"Failed to process record: {e}")
            continue

    # Write one Parquet file per pipe_id, partitioned by date underneath
    total_records = 0
    now = datetime.now(timezone.utc)

    for pipe_id, records in records_by_pipe.items():
        try:
            df = pd.DataFrame(records)
            table = pa.Table.from_pandas(df)

            buffer = BytesIO()
            pq.write_table(table, buffer)

            # Partition pattern: pipe_id → year → month → day
            key = (
                f"processed_data/"
                f"pipe_id={pipe_id}/"
                f"year={now.year}/"
                f"month={now.month:02d}/"
                f"day={now.day:02d}/"
                f"{context.aws_request_id}.parquet"
            )

            s3.put_object(
                Bucket=S3_BUCKET,
                Key=key,
                Body=buffer.getvalue()
            )
            logger.info(f"Saved {len(records)} records to {key}")
            total_records += len(records)

        except Exception as e:
            logger.error(f"Failed to write Parquet for pipe_id={pipe_id}: {e}")
            continue

    return {
        'status': 'success',
        'records_processed': total_records
    }