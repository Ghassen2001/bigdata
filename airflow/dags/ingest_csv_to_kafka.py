from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
from confluent_kafka import Producer
import json

CSV_PATH = "/opt/airflow/data/first-4G-cell_Query_Result_20250527.csv"  # 📂 Monte ce volume dans docker-compose
TOPIC_NAME = "csv-data-topic"
BOOTSTRAP_SERVERS = "kafka:9092"

def send_csv_to_kafka():
    df = pd.read_csv(CSV_PATH)
    producer = Producer({'bootstrap.servers': BOOTSTRAP_SERVERS})

    for _, row in df.iterrows():
        message = json.dumps(row.to_dict())
        producer.produce(TOPIC_NAME, value=message)

    producer.flush()

with DAG(
    dag_id="send_csv_to_kafka",
    start_date=datetime(2025, 6, 13),
    schedule_interval=None,
    catchup=False,
) as dag:

    send_task = PythonOperator(
        task_id="send_csv_rows_to_kafka",
        python_callable=send_csv_to_kafka
    )
