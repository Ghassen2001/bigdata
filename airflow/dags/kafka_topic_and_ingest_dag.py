from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
from confluent_kafka import Producer
import json

CSV_PATH = "/opt/airflow/data/retail_data.csv"
TOPIC_NAME = "csv-data-topic"
BOOTSTRAP_SERVERS = "kafka:9092"
CREATE_TOPIC_SCRIPT = "/opt/airflow/scripts/create_kafka_topic.py"

def send_csv_to_kafka():
    df = pd.read_csv(CSV_PATH)
    producer = Producer({'bootstrap.servers': BOOTSTRAP_SERVERS})
    for _, row in df.iterrows():
        message = json.dumps(row.to_dict())
        producer.produce(TOPIC_NAME, value=message)
    producer.flush()

with DAG(
    dag_id="kafka_topic_and_ingest_dag",
    start_date=datetime(2025, 7, 4),
    schedule_interval=None,
    catchup=False,
    description="Crée le topic Kafka puis ingère les données CSV dans ce topic",
    tags=["kafka", "ingestion"]
) as dag:

    create_topic = BashOperator(
        task_id="create_kafka_topic",
        bash_command=f"python3 {CREATE_TOPIC_SCRIPT}"
    )

    ingest_csv = PythonOperator(
        task_id="send_csv_rows_to_kafka",
        python_callable=send_csv_to_kafka
    )

    create_topic >> ingest_csv
