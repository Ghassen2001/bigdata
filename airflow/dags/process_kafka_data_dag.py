from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

SPARK_SCRIPT = "/opt/bitnami/spark/process_kafka_data.py"

with DAG(
    dag_id="process_kafka_data_dag",
    start_date=datetime(2025, 7, 4),
    schedule_interval=None,
    catchup=False,
    description="Traitement des données Kafka via PySpark",
    tags=["spark", "processing"]
) as dag:

    process_data = BashOperator(
        task_id="process_kafka_data",
        bash_command=f"docker exec spark-master spark-submit {SPARK_SCRIPT}"
    )
