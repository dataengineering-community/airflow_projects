from airflow.utils import dates
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.decorators import task

from airflow.operators.python import PythonOperator, BranchPythonOperator

with DAG(
    dag_id="cde",
    start_date=dates.days_ago(1),
    schedule="@daily"
) as dag:

    ERP_CHANGE_DATE = dates.days_ago(1)

    def fetch_old_data(**context):
        if context.get("task_instance").start_date < ERP_CHANGE_DATE:
            return "fetch_old"
        else:
            return "fetch_new"
    
    def _fetch_old(**context):
        print("Fetching old data")
    
    def _fetch_new(**context):
        print("Fetching new data")

    def _clean_old(**context):
        print("Cleaning old data")
    
    def _clean_new(**context):
        print("Cleaning new data")


    pick_data_source = BranchPythonOperator(
        task_id="pick_data_source",
        python_callable=fetch_old_data
    )

    fetch_old = PythonOperator(
        task_id="fetch_old",
        python_callable=_fetch_old
    )

    clean_old = PythonOperator(
        task_id="clean_old",
        python_callable=_clean_old
    )

    fetch_new = PythonOperator(
        task_id="fetch_new",
        python_callable=_fetch_new
    )

    clean_new = PythonOperator(
        task_id="clean_new",
        python_callable=_clean_new
    )

    # fetch_sales = DummyOperator(task_id="fetch_sales")
    # clean_sales = DummyOperator(task_id="clean_sales")

    fetch_weather = DummyOperator(task_id="fetch_weather")
    clean_weather = DummyOperator(task_id="clean_weather")
    
    join_datasets = DummyOperator(task_id="join_datasets", trigger_rule="none_failed")
    train_model = DummyOperator(task_id="train_model")
    deploy_model = DummyOperator(task_id="deploy_model")

    [pick_data_source, fetch_weather]
    pick_data_source >>  [fetch_old, fetch_new]
    fetch_old >> clean_old
    fetch_new >> clean_new
    fetch_weather >> clean_weather
    [clean_old, clean_new, clean_weather] >> join_datasets 
    join_datasets >> train_model >> deploy_model

