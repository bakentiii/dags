from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models.connection import Connection
from airflow.models.baseoperator import chain
from clickhouse_driver import Client, connect
import datetime


def get_17_connection():
	try:
		conn_17_secrets = Connection.get_connection_from_secrets(conn_id="Clickhouse-17")##скрытй коннекшн источника    
		return connect(host=conn_17_secrets.host, port=conn_17_secrets.port, password=conn_17_secrets.password, user=conn_17_secrets.login, connect_timeout=3600)
	except Exception as ex:
		print(f'Error connection of 17 server - {ex}')


def get_5_connection():
    try:
        conn_5_secrets = Connection.get_connection_from_secrets(conn_id="Clickhouse-5")##скрытй коннекшн источника    
        return connect(host=conn_5_secrets.host, port=conn_5_secrets.port, password=conn_5_secrets.password, user=conn_5_secrets.login, connect_timeout=3600)
    except Exception as ex:
        print(f'Error connection of 5 server - {ex}')




def insert_values(data):
    conn_17 = get_17_connection()
    try:
        sql = f"""INSERT INTO DM_ANALYTICS.NEDV_ZHIL VALUES"""
        with conn_17.cursor() as cursor:
            cursor.executemany(sql, data)
            print(f'[+] Inserted {len(data)} rows to DM_ANALYTICS.NEDV_ZHIL')
    finally:
        conn_17.close()


def truncate_table():
    conn_17 = get_17_connection()
    try:
        sql = f"""TRUNCATE TABLE DM_ANALYTICS.NEDV_ZHIL;"""
        with conn_17.cursor() as cursor:
            cursor.execute(sql)
        print(f'[+] TABLE DM_ANALYTICS.NEDV_ZHIL TRUNCATED')
    finally:
        conn_17.close()




def extract_and_load(SQL, BATCH_SIZE=5000):
    SDU_LOAD_IN_DT = datetime.datetime.now() + datetime.timedelta(hours=6)

    conn_source = get_5_connection()
    try:
        with conn_source.cursor() as cursor:
            data = cursor.execute(SQL)

            truncate_table()

            while True:
                data = cursor.fetchmany(50000)
                print(f'[+] {len(data)} rows fetched')

                if not len(data):
                    print(f'[+] breaked')
                    break

                insert_values(data)
    finally:
        conn_source.close()




def main(*args, **kwargs):
    extract_and_load(
        SQL=kwargs['sql'],
        BATCH_SIZE=10000)



with DAG(
    dag_id='DWH_CDM_RCHL_TABLES',
    default_args={'owner':'Bakhtiyar'},
    start_date=datetime.datetime(2023, 9, 22), 
    schedule_interval = '0 0 * * *',
    catchup = False,
    tags=['rchl', 'dwh_cdm']
) as dag:

    SQL = """select distinct IIN, DESC_TYPE_OF_PROPERTY, ADRESS, KATO  from DM_ZEROS.DM_MU_RN_PROPERTY_IIN_RNN
where RAZDELENIYE_IMUSH in ('16','19','20','21','22','24','26','27','3','30','32','33','34','5','7')"""

    for key, value in TABLES.items():
        
        params = {
            'sql': SQL
        }

        task_ = PythonOperator(
            task_id=f"DM_ANALYTICS.NEDV_ZHIL",
            trigger_rule='all_done',
            python_callable=main,
            op_kwargs=params
        )