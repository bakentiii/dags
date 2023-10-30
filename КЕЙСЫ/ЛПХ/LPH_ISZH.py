from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models.connection import Connection
from airflow.models import Variable
import datetime
import psycopg2

def get_postgres_conn():
    """
    Getting connection from Postgres 192.168.222.114

    """
    try:
        postgres_conn = Connection.get_connection_from_secrets(conn_id="postgres_114")
        return psycopg2.connect(dbname=postgres_conn.schema, user=postgres_conn.login, password=postgres_conn.password, host=postgres_conn.host, port=postgres_conn.port)
    except Exception as ex:
        print(f'--> ERROR: {ex}')

def get_click_5_conn():
    """
    Getting connection from Clickhouse 192.168.52.5
    """
    try:
        click_5_conn = Connection.get_connection_from_secrets(conn_id="Clickhouse-5")
        return connect(host=click_5_conn.host,port=click_5_conn.port,user=click_5_conn.login, password=click_5_conn.password, connect_timeout=3600)
    except Exception as ex:
        print(f'--> ERROR: {ex}')


def extract_and_load_data(clickhouse_5_conn, postgres_conn,max_sdu_load_date):
    """

    Extracting data from Postgres(114) and loading to Clickhouse-5

    """
    
    with postgres_conn.cursor() as cursor_postgres, clickhouse_5_conn.cursor() as cursor_click:
        try:#1) max_sdu_load_date
            cursor_click.execute('SELECT max(SDU_LOAD_IN_DT) FROM MSH_ISZH.DATA')
            if cursor_click.fetchone()[0] is None:
                pass
            else:
                max_sdu_load_date = cursor_click.fetchone()[0]
            print(f"--> max sdu_date is : {max_sdu_load_date}")
        except:
            return {'status': '1) max_sdu_load_date'}
        
        try:#2) Reading data from Postgres into Clickhouse
            cursor_postgres.executemany(f'select * from "MSH_ISZH"."DATA" where SDU_LOAD_IN_DT >{max_sdu_load_date}')

            if len(data):#if data is not empty
                data = cursor_postgres.fetchmany(50000)
                cursor_click.executemany('INSERT INTO MSH_ISZH.DATA VALUES',data)
                print(f'--> Inserted {len(data)} rows')
            else:
                print(f'--> Data is empty')
        
        except:
            return {'status' : '2) Reading data from Postgres into Clickhouse'}
    
    return {'status' : 'extract and load finished'}



def main():

    postgres_conn = get_postgres_conn()
    clickhouse_5_conn = get_click_5_conn()

    max_sdu_load_date = '1970-01-01 00:00:00.000'#temporary data

    transfer = extract_and_load_data(clickhouse_5_conn,postgres_conn,max_sdu_load_date)


    # closing connecitons
    postgres_conn.close()
    clickhouse_5_conn.close()

with DAG(
    dag_id="LPH",
    default_args={'owner': 'Bakhtiyar'},
    description="Transfer zero_database of LPH from Postgres to Clickhouse",
    start_date=datetime.datetime(2023, 4, 20),
    schedule_interval='0 3 * * *',
    tags=['msh', 'iszh', 'lph']
):
    task1 = PythonOperator(
        task_id="Task1",
        python_callable=main,
        trigger_rule='all_done'
    )