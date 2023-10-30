import datetime
from typing import Union, List, Tuple, Optional

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models.connection import Connection
from airflow.models.baseoperator import chain
from airflow.models import Variable
from clickhouse_driver import Client, connect
import ibm_db_dbi as db

def get_source_conn() -> any:
    """
    Geting connection creadentials from airflow secrets and performing connection with database

    Returns:
        connection with database
    """
    try:
        conn_SUR_secrets=Connection.get_connection_from_secrets(conn_id="mf_sur_ibm")#скрытый коннекшн источника
        
        return db.connect(f"DATABASE={conn_SUR_secrets.schema};HOSTNAME={conn_SUR_secrets.host};PORT={conn_SUR_secrets.port};PROTOCOL=TCPIP;UID=nit;PWD={conn_SUR_secrets.password};", "", "")
    except Exception as ex:
        raise ex

def get_target_conn() -> any:
    """
    Geting connection creadentials from airflow secrets and performing connection with database

    Returns:
        connection with database
    """
    try:
        conn_5_secrets = Connection.get_connection_from_secrets(conn_id="Clickhouse-5")##скрытй коннекшн источника    
        return connect(host=conn_5_secrets.host, port=conn_5_secrets.port, password=conn_5_secrets.password, user=conn_5_secrets.login, connect_timeout=3600)
    except Exception as ex:
        print(f'Error connection of 5 server - {ex}')


def insert_values(data: List[List[any]], schema: str, table: str, n_columns: int, cursor: any) -> None:
    """
    Inserts data into target table

    :param data: list of tupled values
    :param schema: schema name in target database
    :param table: table name in target database `schema`
    :param n_columns: amount of columns in target database `schema`.`table`
    :param cursor: cursor of the target connection
    """
    try:
        sql = f"""INSERT INTO "{schema}"."{table}" VALUES"""
        cursor.executemany(sql, data)

    except Exception as ex:
        print('[-] ERROR: An approximate problem, the amount of columns being filled does not match')
        raise ex

# def truncate_table(schema: str, table: str, cursor: any) -> None:
#     """
#     Truncate target database.schema.table to insert new data from source database

#     :param schema: schema name in target database
#     :param table: table name in target database.schema
#     :param cursor: cursor of the target connection
#     """
#     try:
#         sql = f"""TRUNCATE TABLE "{schema}"."{table}";"""
#         print(sql)
#         cursor.execute(sql)
#     except Exception as ex:
#         raise ex


def extract_transform_load(source_schema: str, target_schema: str,
                           source_table: str, target_table: str,
                           n_columns: int,
                           sql: Union[str, None], batch_size: int = 100000) -> None:
    """
    Extracts data from source database, hashing data if needs, and load into target database
    
    :param source_schema: schema name in source database
    :param source_table: table name in source database `source_schema`
    :param target_schema: schema name in target database
    :param target_table: table name in target database `target_schema`
    :param hash_column_indexes: index of values in column for hash, index range starts from 0[inluding]
    :param n_columns: amount of columns in target database `target_schema`.`target_table`
    :param sql: custom sql query to select data from source database
    :param batch_size: data batch size to fetch from source database 
    """
    SDU_LOAD_DATE = datetime.datetime.now() + datetime.timedelta(hours=6)   # loading time into SDU

    conn_source = get_source_conn()
    conn_target = get_target_conn()

    try:
        with conn_source.cursor() as cursor_source, \
             conn_target.cursor() as cursor_target:

            cursor_source.execute(sql)

            # truncate_table(target_schema, target_table, cursor_target)
            # conn_target.commit()

            while True:
                data = cursor_source.fetchmany(batch_size)

                if not len(data): 
                    # break, if there no data
                    break

                # adding column SDU_LOAD_DATE
                data = [[*list(i), SDU_LOAD_DATE] for i in data]

                insert_values(data, target_schema, target_table, n_columns, cursor_target)
                conn_target.commit()
    except Exception as ex:
        raise ex
    finally:
        conn_source.close()


def main(*args, **kwargs):
    extract_transform_load(
        source_schema=kwargs['source_schema'],
        target_schema=kwargs['target_schema'],
        source_table=kwargs['source_table'],
        target_table=kwargs['target_table'],
        n_columns=kwargs['n_columns'],
        sql=kwargs['sql'])


def get_dag_configs(key):
    configs = {
        'dag_id': "r_mf_sur_test",
        'owner': "Abai",
        'start_date': datetime.datetime.strptime('23/8/2023', '%d/%m/%Y'),
        'schedule_interval': "0 1 * * *",
        'tags': ['mysql/mariadb', 'msh'],
    }

    return configs[key]


with DAG(
    dag_id=get_dag_configs('dag_id'),
    default_args={'owner': get_dag_configs('owner')},
    start_date=get_dag_configs('start_date'), 
    schedule_interval=get_dag_configs('schedule_interval'),
    catchup=False,
    tags=get_dag_configs('tags')
) as dag:

#   ...
#   Reglament update, truncate load case

    METADATA = [
        {
            "source_schema": "SUR",
            "target_schema": "MF_SUR",
            "tables": [
                {
                    "source_table": "NIT_RISK_10",
                    "n_columns": 50,
                    "sql": """SELECT NVL(APPNUM_UNION, '') as APPNUM_UNION, NVL(IIN_BIN, '') as IIN_BIN, PRODUCT_NUMBER_INDECL, UNIT_CODE, UNIT_DESC, CAST(UNIT_PRICE AS INT) as UNIT_PRICE, CAST(FNO_PRICE AS INT) as FNO_PRICE FROM SUR.NIT_RISK_10"""
                }
                ]
        }
    ]

    tasks = []

    for schema in METADATA:
        for table in schema['tables']:
            params = {
                'source_schema': schema['source_schema'],
                'target_schema': schema['target_schema'],
                'source_table': table['source_table'],
                'target_table': table['source_table'],
                'n_columns': table['n_columns'],
                'sql': table['sql']
            }

            task = PythonOperator(
                task_id=f"{schema['target_schema']}.{table['source_table'].upper()}",
                python_callable=main,
                op_kwargs=params
                # trigger_rule='all_done'
            )

            tasks.append(task)

    chain(*tasks)