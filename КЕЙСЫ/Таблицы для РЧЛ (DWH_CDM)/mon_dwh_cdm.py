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




def insert_values(data, schema, table):
    conn_17 = get_17_connection()
    try:
        sql = f"""INSERT INTO {schema}.{table} VALUES"""
        with conn_17.cursor() as cursor:
            cursor.executemany(sql, data)
            print(f'[+] Inserted {len(data)} rows to {schema}.{table}')
    finally:
        conn_17.close()


def truncate_table(schema, table):
    conn_17 = get_17_connection()
    try:
        sql = f"""TRUNCATE TABLE {schema}.{table};"""
        with conn_17.cursor() as cursor:
            cursor.execute(sql)
        print(f'[+] TABLE {schema}.{table} TRUNCATED')
    finally:
        conn_17.close()




def extract_and_load(target_schema, target_table, SQL, BATCH_SIZE=5000):
    SDU_LOAD_IN_DT = datetime.datetime.now() + datetime.timedelta(hours=6)

    conn_source = get_5_connection()
    try:
        with conn_source.cursor() as cursor:
            data = cursor.execute(SQL)

            truncate_table(target_schema, target_table)

            while True:
                data = cursor.fetchmany(50000)
                print(f'[+] {len(data)} rows fetched')

                if not len(data):
                    print(f'[+] breaked')
                    break

                insert_values(data, target_schema, target_table)
    finally:
        conn_source.close()




def main(*args, **kwargs):
    extract_and_load(
        target_schema=kwargs['target_schema'],
        target_table=kwargs['target_table'],
        SQL=kwargs['sql'],
        BATCH_SIZE=10000)



with DAG(
    dag_id='DWH_CDM_RCHL_TABLES',
    default_args={'owner':'Bakhtiyar'},
    start_date=datetime.datetime(2023, 7, 19), 
    schedule_interval = '0 0 * * *',
    catchup = False,
    tags=['rchl', 'dwh_cdm']
) as dag:

    TABLES = {
        "NOBD_MP_P1_V1": {
            'sql': """
                "SELECT --TOP 15
                            distinct s.ID AS ID,
                            sa.SHIFT_ID AS KOLVO_SMEN_OBUCH
                    FROM MON_NOBD.SCHOOL s
                    INNER JOIN MON_NOBD.SCHOOL_ATTR sa ON sa.SCHOOL_ID = s.ID AND sa.EDU_PERIOD_ID = toInt32(0)
                    left JOIN MON_NOBD.SCHOOL_SPEC ss ON sa.ID = ss.SCHOOL_ATTR_ID 
                    LEFT JOIN MON_NOBD.D_SCHOOLSPEC_TYPE sst ON sst.ID = ss.SPEC_TYPE_ID
                    WHERE sa.ID = ss.SCHOOL_ATTR_ID AND ( sst.CODE LIKE '02.1.%' OR sst.CODE LIKE '02.2.%' OR sst.CODE LIKE '02.3.%' OR sst.CODE LIKE '02.4.%' OR sst.CODE LIKE '02.5.%' OR sst.CODE LIKE '07.%' OR sst.CODE IN ('02.6.1', '02.6.2', '02.6.3', '02.6.4', '08.3', '08.4', '08.5', '08.6', '09.3', '09.4') )
                    AND sa.DATE_CLOSE IS NULL"
            """
        }
        
        }

    TARGET_SCHEMA = 'DWH_CDM'

    tasks = []

    for key, value in TABLES.items():
        
        params = {
            'target_schema': TARGET_SCHEMA,
            'target_table': key,
            'sql': value['sql']
        }

        task_ = PythonOperator(
            task_id=f"{TARGET_SCHEMA}.{key}",
           # trigger_rule='all_done',
            python_callable=main,
            op_kwargs=params
        )

        tasks.append(task_)

    chain(*tasks)