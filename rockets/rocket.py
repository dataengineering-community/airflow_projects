import airflow
from airflow import DAG
from airflow.utils.dates import datetime
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from rockets.includes.get_picture import _get_pictures
from rockets.includes.email_sender import task_fail_alert

from airflow.models import Variable

arg = {
"on_failure_callback": task_fail_alert,
"params": {
    "environment": Variable.get("environment"),
    "dag_owner": "Najeeb"
}
}

#Another method of instatiating a Dag
# dag = DAG(
#     dag_id="rocket",
#     start_date=datetime(2024, 10, 2),
#     schedule_interval=None
# )

#Instantiate a DAG object; this is the starting point of any workflow
with DAG(
    dag_id="rocket",  #The name of the DAG
    start_date=datetime(2024, 10, 2), #The date at which the DAG should first start running
    schedule_interval=None,  #At what interval the DAG should run
    catchup=False,
    default_args=arg
    #tag="CoreDataEngineers"
) as dag:
    
    #Apply Bash to download the URL response with curl
    download_launches = BashOperator(
        task_id="download_launches",
        bash_command="curl -o /opt/airflow/dags/rockets/launches/launches.json -L https://ll.thespacedevs.com/2.0.0/launch/upcoming"
    )
    
    #Call the Python function in the DAG with a PythonOperator
    get_pictures = PythonOperator(
        task_id="get_pictures", 
        python_callable=_get_pictures
    )

    #Echo the number of images downloaded into the terminal
    notify = BashOperator(
        task_id="notify",
        bash_command='echo "There are now $(ls /opt/airflow/dags/rockets/images/ | wc -l) images"'
    )

    #Set the order of execution of tasks.
    download_launches >> get_pictures >> notify