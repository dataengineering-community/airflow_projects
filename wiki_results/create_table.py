import airflow.utils.dates
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

dag = DAG(
    dag_id="create_table",
    start_date=airflow.utils.dates.days_ago(1),
    schedule_interval=None,
    template_searchpath="/tmp",
    max_active_runs=1,
)

create_postgres_table = PostgresOperator(
    task_id="create_postgres_table",
    postgres_conn_id="my_postgres",
    sql="""
CREATE TABLE pageview_counts (
    pagename VARCHAR(50) NOT NULL,
    pageviewcount INT NOT NULL,
    datetime TIMESTAMP NOT NULL
);
""",
    dag=dag,
)


create_postgres_table