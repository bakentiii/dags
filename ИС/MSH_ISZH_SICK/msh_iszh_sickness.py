from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models.connection import Connection
from airflow.models import Variable

import psycopg2
import pymssql
import datetime


def get_msh_iszh_conn():
    """
    Setting up connection with MSH ISZH
    """
    try:
        msh_iszh_conn = Connection.get_connection_from_secrets(conn_id="msh_iszh_sdu_user")
        return pymssql.connect(database=msh_iszh_conn.schema, user=msh_iszh_conn.login, password=msh_iszh_conn.password, host=msh_iszh_conn.host, port=msh_iszh_conn.port)
    except Exception as ex:
        print(f'[-] ERROR: {ex}')
    

def get_local_postgres_conn():
    """
    Setting up connection with local Postgres
    """
    try:
        local_postgre_conn = Connection.get_connection_from_secrets(conn_id="local_postgres")
        return psycopg2.connect(dbname=local_postgre_conn.schema, user=local_postgre_conn.login, password=local_postgre_conn.password, host=local_postgre_conn.host, port=local_postgre_conn.port)
    except Exception as ex:
        print(f'[-] ERROR: {ex}')


def generate_date_for_search(date: any) -> str:
    """
    Generating date with format YYYY-MM-DD
    """
    return date.strftime("%Y-%m-%d")


def get_next_day(date: any) -> list:
    """
    Return next day
    return: [YYYY, MM, DD]
    """
    return date + datetime.timedelta(days=1)


def extract_and_load_data(iszh_conn, local_conn, last_max_date, animal_kind_id):
    """
    Extracting data from MSH_ISZH and loading to local_postgres
    """
    start_date = generate_date_for_search(last_max_date)
    print(f"[+] max date in sdu {start_date}")
    
    with iszh_conn.cursor() as cursor:
        try:
            cursor.callproc('GetSDUreports', (start_date, start_date,animal_kind_id))
            data = cursor.fetchall()
        except:
            return {'status': 'Failed on select'}

        if len(data): # if data is not empty
            with local_conn.cursor() as cursor:
                sql = f"""INSERT INTO "MSH_ISZH"."SICKNESS_DATA" VALUES ({', '.join(['%s' for _ in range(7)])})"""

                data = [[*list(i), start_date] for i in data]# ADDING DATE TO DATA

                cursor.executemany(sql, data)
            local_conn.commit()
            print(f"[+] data inserted")
        else:
            print(f"[+] data is empty")
        
    return {'status': 'compleated'}


def parse_data():
    # geting max date from SDU
    local_conn = get_local_postgres_conn()
    iszh_conn = get_msh_iszh_conn()

    with local_conn.cursor() as cursor:

        cursor.execute(f"""  select max("reportdate") from "MSH_ISZH"."SICKNESS_DATA"  """)
        result = cursor.fetchall()[0][0]
        print(result)
        year, month, day = [int(i) for i in result.split(' ')[0].split('-')]
        last_max_date = datetime.date(year, month, day)


    while datetime.date(2022, 2, 1) != last_max_date:
        next_date = get_next_day(last_max_date)

        animal_list = {
            1   :   'Крупный рогатый скот',
            5   :   'Лошади',
            6   :   'Свиньи',
            7   :   'Верблюды',
            8   :   'Мелко рогатый скот',
            9   :   'Козы',
            10  :   'Овцы',
            11  :   'Однокопытные',
            12  :   'Ослы',
            14  :   'Мулы',
            15  :   'Пони',
            16  :   'Зебра',
            17  :   'Кулан',
            18  :   'Маралы'
        }

        for key in animal_list.items():

            response = extract_and_load_data(iszh_conn, local_conn, last_max_date, key)

        last_max_date = next_date

        if response['status'] != 'compleated':
            print(f"[-] Error: {response['status']}")


    # closing connecitons
    iszh_conn.close()
    local_conn.close()


with DAG(
    dag_id="MSH_ISZH_SICKNESS",
    default_args={'owner': 'Bakhtiyar'},
    description="Get data from GetSDUSickness procedure",
    start_date=datetime.datetime(2023, 8, 8),
    schedule_interval='0 0 * * *',
    tags=['msh', 'iszh']
):
    task1 = PythonOperator(
        task_id="Task1",
        python_callable=parse_data,
        trigger_rule='all_done'
    )