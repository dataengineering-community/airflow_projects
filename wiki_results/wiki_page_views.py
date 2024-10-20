# """
# Documentation of pageview format: https://wikitech.wikimedia.org/wiki/Analytics/Data_Lake/Traffic/Pageviews
# """

# Import neccessary classes and functions
from airflow.utils.dates import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

from wiki_results.includes.get_wiki_pageviews import _get_data, _fetch_pageviews


with DAG(
    dag_id="listing_4_20",
    start_date=datetime(2024, 10, 20),
    schedule_interval="@hourly",
    #template_searchpath="/tmp", #setting a base path for templates at the DAG-level
    max_active_runs=1,
    catchup=False,
) as dag:


    get_data = PythonOperator(
    task_id="get_data",
    python_callable=_get_data,
    #Paasing templated keyword arguments to the function _get_data
    op_kwargs={
        "year": "{{ data_interval_start.year }}",
        "month": "{{ data_interval_start.month }}",
        "day": "{{ data_interval_start.day }}",
        "hour": "{{ data_interval_start.hour }}",
        "output_path": "/tmp/wikipageviews.gz",
    },
    )


    extract_gz = BashOperator(
        task_id="extract_gz", 
        bash_command="gunzip --force /tmp/wikipageviews.gz", 
    )


    fetch_pageviews = PythonOperator(
        task_id="fetch_pageviews",
        python_callable=_fetch_pageviews,
        #Paasing templated keyword argument to the function _fetch_pageviews
        op_kwargs={"pagenames": {"Google", "Amazon", "Apple", "Microsoft", "Facebook"}},
    )


    write_to_postgres = PostgresOperator(
        task_id="write_to_postgres",
        postgres_conn_id="my_postgres", #id of the connection defined through the web UI
        sql="sql/load_pageviews.sql", #The relative path to the SQL file containing the SQL query to execute on the Postgres db.
    )


get_data >> extract_gz >> fetch_pageviews >> write_to_postgres