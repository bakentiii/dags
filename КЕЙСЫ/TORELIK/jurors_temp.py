from datetime import timedelta,datetime
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow_pentaho.operators.PanOperator import PanOperator
from airflow.models.connection import Connection
from airflow.operators.dummy import DummyOperator
from airflow.models.baseoperator import chain


DAG_NAME = "TORELIK_JURORS_TEMP"
# Transformation's directories
TRANSFORMATION_PATH = "/KPM/DATA_MARTS/OTBOR_PRISYAZHNYH/postgres"                

DEFAULT_ARGS = {
    'owner': 'Bakhtiyar',
    'depends_on_past': False,
    'start_date': datetime(2023,8,4),
    'max_active_runs': 1
}


DAG_TAGS = ['torelik', 'jurors_temp']


with DAG(dag_id=DAG_NAME,
         default_args=DEFAULT_ARGS,
         schedule_interval="0 3 * * *",
         tags=DAG_TAGS) as dag:

    # lists with transformation's names
    transformation_name_list = ["JURORS_TEMP"]
    
    # new lists for tasks
    dag_tasks_list = []
    
    for task_id in transformation_name_list:
        mvd_trans = PanOperator(
            dag=dag,
            task_id=f"{task_id}",
            xcom_push=False,
            directory=TRANSFORMATION_PATH,
            level="Error",
            trans=task_id,
            params={"date": "{{ ds }}"})
        dag_tasks_list.append(mvd_trans)

    
    chain(*dag_tasks_list)