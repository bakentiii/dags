from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models.connection import Connection
from airflow.models.baseoperator import chain
from airflow.models import Variable
from clickhouse_driver import Client, connect
import pandas as pd


def get_5_connection():
    try:
        conn_5_secrets = Connection.get_connection_from_secrets(conn_id="Clickhouse-5")##скрытй коннекшн источника    
        return connect(host=conn_5_secrets.host, port=conn_5_secrets.port, password=conn_5_secrets.password, user=conn_5_secrets.login, connect_timeout=3600)
    except Exception as ex:
        print(f'Error connection of 5 server - {ex}')

def insert_values(data):
    local_conn = get_5_connection()

    try:
        #sql = f"""INSERT INTO "{schema}".{table.lower()} VALUES({', '.join(['%s' for _ in range(n_columns)])});"""
        sql = f"""INSERT INTO TEST.HASH_COLUMNS VALUES""" 

        with local_conn.cursor() as cursor:
            cursor.executemany(sql,data)
            print(f'--> Inserted {len(data)} rows to TEST.HASH_COLUMNS')
    finally:
        local_conn.close()


def check_string(string):
    if any(char.islower() for char in string):
        return "String contains lowercase letters."

    if not (any(char.isalpha() for char in string) and any(char.isdigit() for char in string)):
        return "String does not contain both letters and numbers."

    if ' ' in string:
        return "String contains spaces."

    if len(string) < 18:
        return "Length of the string is less than 18 characters."

    return "HASHED"


def extract_and_load_data(schema,table,column):
	
    flag = True

    conn_5 = get_5_connection()

    try:
        with conn_5.cursor() as cursor:

            data = cursor.execute(f"""SELECT {column} FROM {schema}.{table} limit 1;""")

            count_data = cursor.execute(f"""SELECT count(*) FROM {schema}.{table};""")

            if data is None or len(data) == 0:
                if len(count_data) == 0:
                    empty_data = ('table is empty','table is empty','table is empty','table is empty')
                    insert_values(empty_data)
                    flag = False

                empty_data = ('data is null','data is null','data is null','data is null')
                insert_values(empty_data)
                flag = False
              
            while flag:
                data = cursor.fetchone()

                is_hashed = check_string(data)

                data_with_extra_column = (*data, is_hashed)
                print('Added extra column')

                insert_values(data_with_extra_column)
                print('Data inserted')
                print(f'--> break')
                break

    finally:
        conn_5.close()

def read_csv(file_path):
    df = pd.read_csv(file_path, delimiter = ';')
    data = df.values.tolist()
    return data

def main(*args, **kwargs):
	

    extract_and_load_data(
        schema = kwargs['schema'],
        table = kwargs['table'],
        column = kwargs['column']

    )
        
with DAG("5_server_tables_hash_checker",default_args= {'owner': 'Bakhtiyar'}, description="Column checker",start_date=datetime.datetime(2023, 6, 8),schedule_interval=None,catchup=False, tags = ['shd']) as dag:

    file_path = "/home/Bakhtiyar_Temirov/Desktop/tables.CSV"
    csv_data = read_csv(file_path)


    tasks = []

    for row in csv_data:

        params = {
            'schema'  : row[0],
            'table' : row[1],
            'column' : row[2]
        }

        task_ = PythonOperator(
            task_id=f"{row[0].row[2]}",
            trigger_rule='all_done',
            #python_callable=main,
            op_kwargs=params
        )

        tasks.append(task_)
    chain(*tasks)