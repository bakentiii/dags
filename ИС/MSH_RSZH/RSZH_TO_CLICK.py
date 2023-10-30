import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models.connection import Connection
from airflow.models.baseoperator import chain
from airflow.models import Variable
import psycopg2
from clickhouse_driver import Client, connect

def get_click_5_conn():
    try:
        conn_5_secrets = Connection.get_connection_from_secrets(conn_id="Clickhouse-5")##скрытй коннекшн источника    
        return connect(host=conn_5_secrets.host, port=conn_5_secrets.port, password=conn_5_secrets.password, user=conn_5_secrets.login, connect_timeout=3600)
    except Exception as ex:
        print(f'Error connection of 5 server - {ex}')

def get_click_17_conn():
    try:
        conn_17_secrets = Connection.get_connection_from_secrets(conn_id="Clickhouse-17")##скрытй коннекшн источника    
        return connect(host=conn_17_secrets.host, port=conn_17_secrets.port, password=conn_17_secrets.password, user=conn_17_secrets.login, connect_timeout=3600)
    except Exception as ex:
        print(f'Error connection of 5 server - {ex}')

def get_loacl_postgres_connection():
    try:
        conn_RSZH_secrets = Connection.get_connection_from_secrets(conn_id="postgres_114")##скрытй коннекшн источника    
        return psycopg2.connect(
            dbname=conn_RSZH_secrets.schema, 
            user=conn_RSZH_secrets.login,
            password=conn_RSZH_secrets.password, 
            host=conn_RSZH_secrets.host, 
            port=conn_RSZH_secrets.port)
    except Exception as ex:
        print(f'Error connection of MSH_RSZH - {ex}')

def insert_values_5(data, schema, table):
    local_conn = get_click_5_conn()

    try:
        sql = f"INSERT INTO {schema}.{table} VALUES" 

        with local_conn.cursor() as cursor:
            cursor.executemany(sql,data)
            print(f'--> Inserted {len(data)} rows to {schema}.{table}')
    finally:
        local_conn.close()

def insert_values_17(data, schema, table):
    local_conn = get_click_17_conn()

    try:
        sql = f"INSERT INTO DWH_CDM.{table} VALUES" 

        with local_conn.cursor() as cursor:
            cursor.executemany(sql,data)
            print(f'--> Inserted {len(data)} rows to {schema}.{table}')
    finally:
        local_conn.close()

def truncate_table(schema, table,flag):
    if flag == 1:
        local_conn = get_click_5_conn()
        schema = 'MSH_RSH'
    elif flag == 2:
        local_conn = get_click_17_conn()
        schema = 'DWH_CDM'

    try:
        sql = f"""TRUNCATE TABLE {schema}.{table};"""
        with local_conn.cursor() as cursor:
            cursor.execute(sql)
        print(f'--> Truncated table {table} in {schema} schema')
    finally:
        local_conn.close()
        print('Truncate exiting')

def extract_and_load_data(tagret_schema,target_table):

    postgres_conn = get_loacl_postgres_connection()

    try:
        with postgres_conn.cursor() as cursor:
            
            data = cursor.execute(f"""SELECT * FROM "{tagret_schema}"."{target_table}";""")


            truncate_table(tagret_schema, target_table,1)
            truncate_table(tagret_schema, target_table,2)

            while True:
                print('fetching')
                data = cursor.fetchmany(50000)
                print('fetched')
                data = [[*list(x)] for x in data]
                print(f'--> {len(data)} rows fetched')
                

                if not len(data):
                    print(f'--> break')
                    break

                insert_values_5(data,tagret_schema,target_table)
                insert_values_17(data,tagret_schema,target_table)
    finally:
        postgres_conn.close()

def main(*args, **kwargs):

    extract_and_load_data(
        tagret_schema = kwargs['target_schema'],
        target_table = kwargs['target_table']
    )
        
with DAG("MSH_RSZH",default_args= {'owner': 'Bakhtiyar'}, description="Load table",start_date=datetime.datetime(2023, 9, 27),schedule_interval='0 6 * * *',catchup=False, tags = ['rszh']) as dag:

    TABLES = [    
        'ASSESSMENT_OF_BREEDING_ANIMALS' ,
        'BREEDING_CERTIFICATES' ,
        'REGISTER_OF_SUBJECTS' 
    ]

    TARGET_SCHEMA = 'MSH_RSZH'

    tasks = []

    for value in TABLES:

        params = {
            'target_schema'  : TARGET_SCHEMA,
            'target_table' : value
        }

        task_ = PythonOperator(
            task_id=f"{TARGET_SCHEMA}.{value}",
            trigger_rule='all_done',
            python_callable=main,
            op_kwargs=params
        )

        tasks.append(task_)
    chain(*tasks)