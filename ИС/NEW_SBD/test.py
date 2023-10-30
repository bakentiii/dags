import datetime



import cx_Oracle
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models.connection import Connection
from airflow.models.baseoperator import chain
from airflow.models import Variable



def get_source_conn() -> any:
    """
    Geting connection creadentials from airflow secrets and performing connection with database

    Returns:
        connection with database
    """
    try:
        conn_secrets = Connection.get_connection_from_secrets(conn_id="ENTER_CONNECTION_ID")
        dsn_data = None

        if conn_secrets.extra['sid'] is not None:
            dsn_data = cx_Oracle.makedsn(host=conn_secrets.host, port=conn_secrets.port, sid=conn_secrets.extra['sid'])
        elif conn_secrets.extra['sevice_name'] is not None:
            dsn_data = cx_Oracle.makedsn(host=conn_secrets.host, port=conn_secrets.port, service_name=conn_secrets.extra['service_name'])
        
        return cx_Oracle.connect(conn_secrets.login, password=conn_secrets.password, dsn=dsn_data)
    except Exception as ex:
        raise ex


def TEST():
    sql = """SELECT
    *
    FROM pfl."DIC_COUNTRY"
    FETCH NEXT 10 ROWS ONLY"""

    conn_source = get_source_conn()


    try:
            with conn_source.cursor() as cursor_source:

                print(cursor_source.execute(sql))
    
with DAG("test_oracle", description="test Oracle", start_date=datetime(2023, 8, 1), schedule_interval=None, catchup=False) as dag:
	hash_name = Variable.get("hash_password")

	TEST = PythonOperator(
		owner='Bakhtiyar',
 		task_id='TEST',
		python_callable=TEST,
	)