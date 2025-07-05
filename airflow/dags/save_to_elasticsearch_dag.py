from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

SCRIPT_PATH = "/opt/airflow/scripts/save_to_elasticsearch.py"

with DAG(
    dag_id="save_to_elasticsearch_dag",
    start_date=datetime(2025, 7, 4),
    schedule_interval=None,
    catchup=False,
    description="Indexation des données traitées dans Elasticsearch",
    tags=["elasticsearch", "indexing"]
) as dag:

    index_data = BashOperator(
        task_id="index_processed_data",
        bash_command=f"python {SCRIPT_PATH}"
    )
