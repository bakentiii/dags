import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models.connection import Connection
from airflow.models import Variable
from clickhouse_driver import Client, connect
import pandas as pd

def get_5_connection():
    try:
        conn_5_secrets = Connection.get_connection_from_secrets(conn_id="Clickhouse-5")##скрытй коннекшн источника    
        return connect(host=conn_5_secrets.host, port=conn_5_secrets.port, password=conn_5_secrets.password, user=conn_5_secrets.login, connect_timeout=3600)
    except Exception as ex:
        print(f'Error connection of 5 server - {ex}')

def is_hashed(string):

    if any(char.islower() for char in string):
        return False, "String contains lowercase letters."

    if not (any(char.isalpha() for char in string) and any(char.isdigit() for char in string)):
        return False, "String does not contain both letters and numbers."

    if ' ' in string:
        return  False, "String contains spaces."

    if len(string) < 18:
        return  False, "Length of the string is less than 18 characters."

    return True, None   #is_hashed, message

def insert_values(schema, table, column, hashed, message, data):
    local_conn = get_5_connection()

    try:
        sql = f"""INSERT INTO TEST.HASH_COLUMNS VALUES""" 

        with local_conn.cursor() as cursor:
            cursor.executemany(sql,data)
            print(f'--> Inserted {len(data)} rows to TEST.HASH_COLUMNS')
    finally:
        local_conn.close()

def check(schema, table, column):

    local_conn = get_5_connection()

    try:
        with local_conn.cursor() as cursor:

            data = cursor.execute(f"select {column} from {schema}.{table} limit 10")#data = 'ASDF13RFW42R42R2R12'

            all_hashed = True

            for val in data:
                hashed, message = is_hashed(val)

                if not hashed:
                    data_to_load = (schema, table, column, hashed, message, val)
                    insert value (data_to_load)
                    all_hashed = False
                    break

            if all_hashed:
                # all success
                for val in data:
                    data_to_load = (schema, table, column, all_hashed, None, val)
                    insert value (data_to_load)
    finally:
        local_conn.close()
        
    
    
        

def main(df: DataFrame):

    for index, row in df.iterrows():
        check(row.schema, row.table, row.column)


with DAG("5_server_tables_hash_checker",default_args= {'owner': 'Bakhtiyar'}, description="Column checker",start_date=datetime.datetime(2023, 6, 11),schedule_interval=None,catchup=False, tags = ['shd']) as dag::

    file_path = '/opt/airflow/dags/test/tables.CSV'
    df = read(file_path, delimiter=';') ['schema', 'table', 'column']

    task_1 = python_operator(
        func=main,
        op_kwargs=df
    )



