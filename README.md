# Real-Time Energy Pipeline Early Warning and Monitoring System

![Python](https://img.shields.io/badge/Python-3.11-blue?logo=python)
![AWS](https://img.shields.io/badge/AWS-Kinesis%20%7C%20Lambda%20%7C%20S3-orange?logo=amazonaws)
![Streamlit](https://img.shields.io/badge/Streamlit-Dashboard-red?logo=streamlit)
![ML](https://img.shields.io/badge/ML-Scikit--Learn-green?logo=scikit-learn)
![Status](https://img.shields.io/badge/Project-Completed-brightgreen)

**Data Engineering - Machine Learning - Cloud Computing**

University of Calgary | Master of Data Science and Analytics | March 2026

---

## Project Team

**Rio Sibuea | Alejandro Alvarado | Ifeanyi Njoku**

---

## Overview

Alberta's energy sector operates one of North America's most extensive pipeline networks. Pipeline failures — leaks, pressure anomalies, and blockages — carry severe environmental, safety, and economic consequences.

This project implements a **cloud-native, real-time pipeline early warning system** that:

- Streams live sensor telemetry from a SCADA simulator running on EC2
- Detects anomalies in real time using a Random Forest machine learning model
- Sends early warning email alerts to the Operations/Security Team via Amazon SNS
- Visualizes pipeline health on a live Streamlit monitoring dashboard

---

## System Architecture

```
EC2 Simulator → Kinesis → Lambda → S3 Parquet + SNS Email → Streamlit Dashboard
```

| Layer | Technology | Role |
|---|---|---|
| Sensor / EC2 | Python + boto3 | SCADA simulator streams sensor readings every second |
| Ingestion | Amazon Kinesis | Buffers real-time telemetry — 1 shard, us-east-1 |
| Processing | AWS Lambda (Python 3.11) | Runs ML model, classifies risk, triggers SNS, writes to S3 |
| Alerting | Amazon SNS | Emails operations team on Anomaly or Critical detection |
| Storage | S3 + PyArrow Parquet | Partitioned by pipe_id / year / month / day |
| ML Model | scikit-learn Random Forest | Classifies Normal / Anomaly / Critical |
| Dashboard | Streamlit + Plotly | Live monitoring UI reading directly from S3 |

---

## Repository Structure

```
AWS-Data-Engineering-Project/
├── DASHBOARD/
│   └── dashboard_1.1.py
├── DATASET_GENERATOR/
│   └── dataset-creator.py
├── LAMBDA_CONTAINER/
│   ├── lambda_function.py
│   ├── Dockerfile
│   └── requirements.txt
├── ML_MODEL/
│   └── pipeline_anomaly_detection_1.ipynb
├── SENSOR_SIMULATOR/
│   └── scada_simulator_1.4.py
├── .gitignore
└── README.md
```

> The trained model (`random_forest_model.joblib`) and full dataset (`pipeline_sensor_data_50000.csv`) are excluded from this repository. The model is stored in Amazon S3.

---

## Sensor Features and Thresholds

| Feature | Normal | Anomaly | Critical |
|---|---|---|---|
| pressure_MPa | 4.5 – 6.0 MPa | 2.5–4.5 or 6.0–7.0 | > 7.0 |
| temperature_C | 10 – 45 °C | 0–5 or 46–84 | < 0 or > 85 |
| flow_rate_percent | -5 – 5% | 5 – 25% | > 25% |

Training class distribution: Normal 83% — Anomaly 15% — Critical 2% (50,000 total samples)

---

## How to Run

**1. Train the model**

Open `ML_MODEL/pipeline_anomaly_detection_1.ipynb` in Jupyter and run all cells, then upload:

```bash
aws s3 cp random_forest_model.joblib s3://data608-project-model/random_forest_model.joblib
```

**2. Deploy Lambda**

```bash
cd LAMBDA_CONTAINER
zip lambda.zip lambda_function.py
aws lambda update-function-code --function-name pipeline-processor --zip-file fileb://lambda.zip
```

**3. Run the simulator on EC2**

```bash
python3 -m venv pipeline-env
source pipeline-env/bin/activate
pip install boto3
python SENSOR_SIMULATOR/scada_simulator_1.4.py
```

**4. Run the dashboard on your local machine**

```bash
pip install streamlit plotly pandas boto3 pyarrow
streamlit run DASHBOARD/dashboard_1.1.py
```

Open http://localhost:8501

---

## AWS Resources

| Resource | Name |
|---|---|
| S3 Bucket — sensor data | data608-project-sensordata |
| S3 Bucket — ML model | data608-project-model |
| Kinesis Data Stream | data608-project-sensordata |
| Lambda Function | pipeline-processor |
| SNS Topic | Topic2 |
| EC2 Instance | Ubuntu t2.micro with LabInstanceProfile |

---

## Success Criteria

| Criterion | Status |
|---|---|
| Data flows end-to-end from sensor to dashboard | Achieved |
| ML model classifies Normal, Anomaly, Critical in real time | Achieved |
| SNS email alert delivered to Operations Team on anomaly | Achieved |

---

## References

- Canada Energy Regulator (2024). Incidents at CER-regulated pipelines and facilities. https://open.canada.ca/data/en/dataset/fd17f08f-f14d-433f-91df-c90a34e1e9a6
- Waqas, M. (2025). Pipeline dataset in oil and gas sector. Kaggle. https://www.kaggle.com/datasets/muhammadwaqas023/pipeline-dataset-in-oil-and-gas-sector
- Amazon Web Services (2024). AWS Documentation. https://docs.aws.amazon.com/
