\# Real-Time Energy Pipeline Early Warning and Monitoring System



\*\*DATA 608 – Big Data Systems | Group 1\*\*

Rio Sibuea · Alejandro Alvarado · Ifeanyi Njoku

University of Calgary · March 2026



\---



\## Overview



Alberta's energy sector operates one of North America's most extensive pipeline networks. Pipeline failures — leaks, pressure anomalies, blockages — carry severe environmental, safety, and economic consequences.



This project implements a cloud-native, real-time pipeline early warning system that ingests live sensor telemetry, applies machine learning anomaly detection, and alerts operations teams before incidents escalate.



\---



\## System Architecture



EC2 Simulator → Kinesis → Lambda → S3 Parquet + SNS Email → Streamlit Dashboard



| Layer | Technology | Role |

|---|---|---|

| Sensor / EC2 | Python + boto3 | SCADA simulator streams sensor readings every second |

| Ingestion | Amazon Kinesis | Buffers real-time telemetry; 1 shard, us-east-1 |

| Processing | AWS Lambda (Python 3.11) | Runs ML model, classifies risk, triggers SNS, writes S3 |

| Alerting | Amazon SNS | Emails operations team on Anomaly or Critical detection |

| Storage | S3 + PyArrow Parquet | Partitioned by pipe\_id / year / month / day |

| ML Model | scikit-learn Random Forest | Classifies Normal / Anomaly / Critical |

| Dashboard | Streamlit + Plotly | Live monitoring UI reading directly from S3 |



\---



\## Repository Structure



AWS-Data-Engineering-Project/

├── DASHBOARD/

│   └── dashboard\_1.1.py

├── DATASET\_GENERATOR/

│   └── dataset-creator.py

├── LAMBDA\_CONTAINER/

│   ├── lambda\_function.py

│   ├── Dockerfile

│   └── requirements.txt

├── ML\_MODEL/

│   └── pipeline\_anomaly\_detection\_1.ipynb

├── SENSOR\_SIMULATOR/

│   └── scada\_simulator\_1.4.py

├── .gitignore

└── README.md



Note: random\_forest\_model.joblib and pipeline\_sensor\_data\_50000.csv are excluded — model is stored in S3.



\---



\## Sensor Features and Thresholds



| Feature | Normal | Anomaly | Critical |

|---|---|---|---|

| pressure\_MPa | 4.5 – 6.0 | 2.5–4.5 or 6.0–7.0 | > 7.0 |

| temperature\_C | 10 – 45 | 0–5 or 46–84 | < 0 or > 85 |

| flow\_rate\_percent | -5 – 5 | 5 – 25 | > 25 |



\---



\## How to Run



\### 1. Train the model

```bash

jupyter notebook ML\_MODEL/pipeline\_anomaly\_detection\_1.ipynb

aws s3 cp random\_forest\_model.joblib s3://data608-project-model/random\_forest\_model.joblib

```



\### 2. Deploy Lambda

```bash

cd LAMBDA\_CONTAINER

zip lambda.zip lambda\_function.py

aws lambda update-function-code --function-name pipeline-processor --zip-file fileb://lambda.zip

```



\### 3. Run the simulator on EC2

```bash

python3 -m venv pipeline-env

source pipeline-env/bin/activate

pip install boto3

python SENSOR\_SIMULATOR/scada\_simulator\_1.4.py

```



\### 4. Run the dashboard

```bash

pip install streamlit plotly pandas boto3 pyarrow

streamlit run DASHBOARD/dashboard\_1.1.py

```



\---



\## Team



| Member | Role |

|---|---|

| Rio Sibuea | Sensor ingestion, Kinesis streaming, EC2 |

| Alejandro Alvarado | ML model, Lambda integration |

| Ifeanyi Njoku | Streamlit dashboard, SNS alerts |



\---



\## References



\- Canada Energy Regulator: https://open.canada.ca/data/en/dataset/fd17f08f-f14d-433f-91df-c90a34e1e9a6

\- Kaggle Pipeline Dataset: https://www.kaggle.com/datasets/muhammadwaqas023/pipeline-dataset-in-oil-and-gas-sector

\- AWS Documentation: https://docs.aws.amazon.com/

