from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime


def test_function():
    print("Manual test successful!")


with DAG(
    dag_id="manual_test_dag",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    test_task = PythonOperator(
        task_id="test_task",
        python_callable=test_function,
    )
