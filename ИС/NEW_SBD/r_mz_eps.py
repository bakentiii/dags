import datetime
import hashlib
from typing import Union, List, Tuple, Optional

import psycopg2
import pymssql
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
        return pymssql.connect(database=conn_secrets.schema, host=conn_secrets.host, port=conn_secrets.port, user=conn_secrets.login, password=conn_secrets.password)
    except Exception as ex:
        raise ex


def get_target_conn() -> any:
    """
    Geting connection creadentials from airflow secrets and performing connection with database

    Returns:
        connection with database
    """
    try:
        conn_secrets = Connection.get_connection_from_secrets(conn_id="postgres_11")
        return psycopg2.connect(dbname=conn_secrets.schema, user=conn_secrets.login, password=conn_secrets.password, host=conn_secrets.host, port=conn_secrets.port)
    except Exception as ex:
        raise ex


def hash(value: any, hash_keyword: str) -> Union[str, None]:
    """
    Hashing data with SHA256 algorithm in Uppercase format or None if value is null

    :param value: value to hash
    :hash_keyword: `соль` with which we hash value

    Returns:
        hashed value or None
    """
    if value is None:
        return value

    hash_data = f"{str(value).lower()}{hash_keyword}".encode("utf-8")
    result = hashlib.sha256(hash_data).hexdigest()
    return result.upper()
    

def hash_columns(values: List[any], indexes: List[int], hash_keyword: str) -> Tuple[Union[str, None]]:
    """
    Looping columns by index to Hash

    :param values: List of values to hash
    :param indexes: index of values in column for hash, index range starts from 0[inluding]
    :param hash_keyword: `соль` with which we hash values

    Returns:
        tupled values with hashed by column index
    """
    tmp = list(values)
    for index in indexes:
        tmp[index] = hash(values[index], hash_keyword)
    return tuple(tmp)


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
        sql = f"""INSERT INTO "{schema}"."{table}" VALUES ( {', '.join(['%s']*n_columns)} )"""
        cursor.executemany(sql, data)

    except Exception as ex:
        print('[-] ERROR: An approximate problem, the amount of columns being filled does not match')
        raise ex


def truncate_table(schema: str, table: str, cursor: any) -> None:
    """
    Truncate target database.schema.table to insert new data from source database

    :param schema: schema name in target database
    :param table: table name in target database.schema
    :param cursor: cursor of the target connection
    """
    try:
        sql = f"""TRUNCATE TABLE "{schema}"."{table}";"""
        print(sql)
        cursor.execute(sql)
    except Exception as ex:
        raise ex


def extract_transform_load(source_schema: str, target_schema: str,
                           source_table: str, target_table: str,
                           hash_column_indexes: List[int], n_columns: int,
                           sql: Union[str, None], batch_size: int = 10000) -> None:
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
    HASH_KEYWORD = Variable.get("hash_password")    # `соль` with which we hash value

    if sql is None:
        # if there no custom sql query perform defualting query
        sql = f'SELECT * FROM "{source_schema}"."{source_table}"'

    conn_source = get_source_conn()
    conn_target = get_target_conn()

    try:
        with conn_source.cursor() as cursor_source, \
             conn_target.cursor() as cursor_target:

            cursor_source.execute(sql)

            truncate_table(target_schema, target_table, cursor_target)
            conn_target.commit()

            while True:
                data = cursor_source.fetchmany(batch_size)

                if not len(data): 
                    # break, if there no data
                    break

                # adding column SDU_LOAD_DATE
                data = [[*list(i), get_dag_configs('owner'), SDU_LOAD_DATE] for i in data]

                # hashing data in columns by indexes
                data = [*map(lambda row: hash_columns(row, indexes=hash_column_indexes, hash_keyword=HASH_KEYWORD), data)]

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
        hash_column_indexes=kwargs['hash_column_indexes'],
        n_columns=kwargs['n_columns'],
        sql=kwargs['sql'])


def get_dag_configs(key):
    configs = {
        'dag_id': "r_mz_eps",
        'owner': "Bakhtiyar",
        'start_date': datetime.datetime.strptime('', '%d/%m/%Y'),
        'schedule_interval': "0 0 * * *",
        'tags': ['mssql', 'mz', 'reglament'],
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
            "source_schema": "eps_dbo",
            "target_schema": "R_MZ_EPS",
            "tables": [
                {
                    "source_table": "_AccumRg1121",
                    "hash_column_indexes": [],
                    "n_columns": 12,
                    "sql": None
                },
                {
                    "source_table": "_AccumRg1210",
                    "hash_column_indexes": [],
                    "n_columns": 13,
                    "sql": None
                },
                {
                    "source_table": "_AccumRg2597",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "_AccumRg3195",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "_AccumRg663",
                    "hash_column_indexes": [],
                    "n_columns": 12,
                    "sql": None
                },
                {
                    "source_table": "_AccumRg941",
                    "hash_column_indexes": [],
                    "n_columns": 23,
                    "sql": None
                },
                {
                    "source_table": "_Chrc1323",
                    "hash_column_indexes": [],
                    "n_columns": 12,
                    "sql": None
                },
                {
                    "source_table": "_Chrc90",
                    "hash_column_indexes": [],
                    "n_columns": 12,
                    "sql": None
                },
                {
                    "source_table": "_Chrc91",
                    "hash_column_indexes": [],
                    "n_columns": 16,
                    "sql": None
                },
                {
                    "source_table": "_Chrc91_VT674",
                    "hash_column_indexes": [],
                    "n_columns": 12,
                    "sql": None
                },
                {
                    "source_table": "_Const1008",
                    "hash_column_indexes": [],
                    "n_columns": 4,
                    "sql": None
                },
                {
                    "source_table": "_Const1010",
                    "hash_column_indexes": [],
                    "n_columns": 4,
                    "sql": None
                },
                {
                    "source_table": "_Const1495",
                    "hash_column_indexes": [],
                    "n_columns": 4,
                    "sql": None
                },
                {
                    "source_table": "_Const1583",
                    "hash_column_indexes": [],
                    "n_columns": 4,
                    "sql": None
                },
                {
                    "source_table": "_Const1624",
                    "hash_column_indexes": [],
                    "n_columns": 4,
                    "sql": None
                },
                {
                    "source_table": "_Const1976",
                    "hash_column_indexes": [],
                    "n_columns": 4,
                    "sql": None
                },
                {
                    "source_table": "_Const1978",
                    "hash_column_indexes": [],
                    "n_columns": 4,
                    "sql": None
                },
                {
                    "source_table": "_Const1980",
                    "hash_column_indexes": [],
                    "n_columns": 4,
                    "sql": None
                },
                {
                    "source_table": "_Const1982",
                    "hash_column_indexes": [],
                    "n_columns": 4,
                    "sql": None
                },
                {
                    "source_table": "_Const2209",
                    "hash_column_indexes": [],
                    "n_columns": 4,
                    "sql": None
                },
                {
                    "source_table": "_Const2218",
                    "hash_column_indexes": [],
                    "n_columns": 4,
                    "sql": None
                },
                {
                    "source_table": "_Const2237",
                    "hash_column_indexes": [],
                    "n_columns": 4,
                    "sql": None
                },
                {
                    "source_table": "_Const2329",
                    "hash_column_indexes": [],
                    "n_columns": 4,
                    "sql": None
                },
                {
                    "source_table": "_Const2703",
                    "hash_column_indexes": [],
                    "n_columns": 4,
                    "sql": None
                },
                {
                    "source_table": "_Const2963",
                    "hash_column_indexes": [],
                    "n_columns": 4,
                    "sql": None
                },
                {
                    "source_table": "_Const2976",
                    "hash_column_indexes": [],
                    "n_columns": 4,
                    "sql": None
                },
                {
                    "source_table": "_Const3014",
                    "hash_column_indexes": [],
                    "n_columns": 4,
                    "sql": None
                },
                {
                    "source_table": "_Const3278",
                    "hash_column_indexes": [],
                    "n_columns": 4,
                    "sql": None
                },
                {
                    "source_table": "_Const3280",
                    "hash_column_indexes": [],
                    "n_columns": 4,
                    "sql": None
                },
                {
                    "source_table": "_Const3469",
                    "hash_column_indexes": [],
                    "n_columns": 4,
                    "sql": None
                },
                {
                    "source_table": "_Const3529",
                    "hash_column_indexes": [],
                    "n_columns": 4,
                    "sql": None
                },
                {
                    "source_table": "_Const3754",
                    "hash_column_indexes": [],
                    "n_columns": 4,
                    "sql": None
                },
                {
                    "source_table": "_Const3756",
                    "hash_column_indexes": [],
                    "n_columns": 4,
                    "sql": None
                },
                {
                    "source_table": "_Const3853",
                    "hash_column_indexes": [],
                    "n_columns": 4,
                    "sql": None
                },
                {
                    "source_table": "_Const3855",
                    "hash_column_indexes": [],
                    "n_columns": 4,
                    "sql": None
                },
                {
                    "source_table": "_Const3857",
                    "hash_column_indexes": [],
                    "n_columns": 4,
                    "sql": None
                },
                {
                    "source_table": "_Const4024",
                    "hash_column_indexes": [],
                    "n_columns": 4,
                    "sql": None
                },
                {
                    "source_table": "_Const4153",
                    "hash_column_indexes": [],
                    "n_columns": 4,
                    "sql": None
                },
                {
                    "source_table": "_Const4178",
                    "hash_column_indexes": [],
                    "n_columns": 4,
                    "sql": None
                },
                {
                    "source_table": "_Const4180",
                    "hash_column_indexes": [],
                    "n_columns": 4,
                    "sql": None
                },
                {
                    "source_table": "_Const4847",
                    "hash_column_indexes": [],
                    "n_columns": 4,
                    "sql": None
                },
                {
                    "source_table": "_Const4873",
                    "hash_column_indexes": [],
                    "n_columns": 4,
                    "sql": None
                },
                {
                    "source_table": "_Const5329",
                    "hash_column_indexes": [],
                    "n_columns": 4,
                    "sql": None
                },
                {
                    "source_table": "_Const5331",
                    "hash_column_indexes": [],
                    "n_columns": 4,
                    "sql": None
                },
                {
                    "source_table": "_Const850",
                    "hash_column_indexes": [],
                    "n_columns": 4,
                    "sql": None
                },
                {
                    "source_table": "_Const972",
                    "hash_column_indexes": [],
                    "n_columns": 4,
                    "sql": None
                },
                {
                    "source_table": "_Document1110",
                    "hash_column_indexes": [],
                    "n_columns": 12,
                    "sql": None
                },
                {
                    "source_table": "_Document1110_VT1116",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "_Document1630",
                    "hash_column_indexes": [],
                    "n_columns": 13,
                    "sql": None
                },
                {
                    "source_table": "_Document1630_VT1638",
                    "hash_column_indexes": [],
                    "n_columns": 12,
                    "sql": None
                },
                {
                    "source_table": "_Document1631",
                    "hash_column_indexes": [],
                    "n_columns": 13,
                    "sql": None
                },
                {
                    "source_table": "_Document1631_VT1651",
                    "hash_column_indexes": [],
                    "n_columns": 15,
                    "sql": None
                },
                {
                    "source_table": "_Document1632",
                    "hash_column_indexes": [],
                    "n_columns": 13,
                    "sql": None
                },
                {
                    "source_table": "_Document1632_VT1664",
                    "hash_column_indexes": [],
                    "n_columns": 29,
                    "sql": None
                },
                {
                    "source_table": "_Document1633",
                    "hash_column_indexes": [],
                    "n_columns": 13,
                    "sql": None
                },
                {
                    "source_table": "_Document1633_VT1694",
                    "hash_column_indexes": [],
                    "n_columns": 14,
                    "sql": None
                },
                {
                    "source_table": "_Document1725",
                    "hash_column_indexes": [],
                    "n_columns": 34,
                    "sql": None
                },
                {
                    "source_table": "_Document1771",
                    "hash_column_indexes": [],
                    "n_columns": 27,
                    "sql": None
                },
                {
                    "source_table": "_Document1946",
                    "hash_column_indexes": [],
                    "n_columns": 15,
                    "sql": None
                },
                {
                    "source_table": "_Document1946_VT1969",
                    "hash_column_indexes": [],
                    "n_columns": 10,
                    "sql": None
                },
                {
                    "source_table": "_Document2312",
                    "hash_column_indexes": [],
                    "n_columns": 25,
                    "sql": None
                },
                {
                    "source_table": "_Document2312_VT5210",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "_Document2365",
                    "hash_column_indexes": [],
                    "n_columns": 10,
                    "sql": None
                },
                {
                    "source_table": "_Document2365_VT2369",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "_Document2391",
                    "hash_column_indexes": [],
                    "n_columns": 16,
                    "sql": None
                },
                {
                    "source_table": "_Document2391_VT2400",
                    "hash_column_indexes": [],
                    "n_columns": 24,
                    "sql": None
                },
                {
                    "source_table": "_Document2391_VT2421",
                    "hash_column_indexes": [],
                    "n_columns": 22,
                    "sql": None
                },
                {
                    "source_table": "_Document2391_VT2441",
                    "hash_column_indexes": [],
                    "n_columns": 13,
                    "sql": None
                },
                {
                    "source_table": "_Document2391_VT2533",
                    "hash_column_indexes": [],
                    "n_columns": 20,
                    "sql": None
                },
                {
                    "source_table": "_Document2391_VT2682",
                    "hash_column_indexes": [],
                    "n_columns": 24,
                    "sql": None
                },
                {
                    "source_table": "_Document2391_VT2727",
                    "hash_column_indexes": [],
                    "n_columns": 22,
                    "sql": None
                },
                {
                    "source_table": "_Document2391_VT3713",
                    "hash_column_indexes": [],
                    "n_columns": 28,
                    "sql": None
                },
                {
                    "source_table": "_Document2391_VT4296",
                    "hash_column_indexes": [],
                    "n_columns": 24,
                    "sql": None
                },
                {
                    "source_table": "_Document2391_VT4317",
                    "hash_column_indexes": [],
                    "n_columns": 24,
                    "sql": None
                },
                {
                    "source_table": "_Document2391_VT4338",
                    "hash_column_indexes": [],
                    "n_columns": 24,
                    "sql": None
                },
                {
                    "source_table": "_Document2391_VT4359",
                    "hash_column_indexes": [],
                    "n_columns": 24,
                    "sql": None
                },
                {
                    "source_table": "_Document2391_VT4380",
                    "hash_column_indexes": [],
                    "n_columns": 24,
                    "sql": None
                },
                {
                    "source_table": "_Document2391_VT4466",
                    "hash_column_indexes": [],
                    "n_columns": 24,
                    "sql": None
                },
                {
                    "source_table": "_Document2391_VT4774",
                    "hash_column_indexes": [],
                    "n_columns": 24,
                    "sql": None
                },
                {
                    "source_table": "_Document2391_VT5212",
                    "hash_column_indexes": [],
                    "n_columns": 28,
                    "sql": None
                },
                {
                    "source_table": "_Document2391_VT5237",
                    "hash_column_indexes": [],
                    "n_columns": 18,
                    "sql": None
                },
                {
                    "source_table": "_Document2391_VT5252",
                    "hash_column_indexes": [],
                    "n_columns": 25,
                    "sql": None
                },
                {
                    "source_table": "_Document2391_VT5413",
                    "hash_column_indexes": [],
                    "n_columns": 24,
                    "sql": None
                },
                {
                    "source_table": "_Document2391_VT5434",
                    "hash_column_indexes": [],
                    "n_columns": 24,
                    "sql": None
                },
                {
                    "source_table": "_Document2391_VT5541",
                    "hash_column_indexes": [],
                    "n_columns": 21,
                    "sql": None
                },
                {
                    "source_table": "_Document2595",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "_Document2801",
                    "hash_column_indexes": [],
                    "n_columns": 22,
                    "sql": None
                },
                {
                    "source_table": "_Document2829",
                    "hash_column_indexes": [],
                    "n_columns": 24,
                    "sql": None
                },
                {
                    "source_table": "_Document3093",
                    "hash_column_indexes": [],
                    "n_columns": 33,
                    "sql": None
                },
                {
                    "source_table": "_Document3093_VT3116",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "_Document3157",
                    "hash_column_indexes": [],
                    "n_columns": 12,
                    "sql": None
                },
                {
                    "source_table": "_Document39",
                    "hash_column_indexes": [],
                    "n_columns": 49,
                    "sql": None
                },
                {
                    "source_table": "_Document39_VT1155",
                    "hash_column_indexes": [],
                    "n_columns": 19,
                    "sql": None
                },
                {
                    "source_table": "_Document39_VT1164",
                    "hash_column_indexes": [],
                    "n_columns": 18,
                    "sql": None
                },
                {
                    "source_table": "_Document39_VT1439",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "_Document39_VT243",
                    "hash_column_indexes": [],
                    "n_columns": 11,
                    "sql": None
                },
                {
                    "source_table": "_Document40",
                    "hash_column_indexes": [],
                    "n_columns": 11,
                    "sql": None
                },
                {
                    "source_table": "_Document40_VT253",
                    "hash_column_indexes": [],
                    "n_columns": 12,
                    "sql": None
                },
                {
                    "source_table": "_Document41",
                    "hash_column_indexes": [],
                    "n_columns": 21,
                    "sql": None
                },
                {
                    "source_table": "_Document41_VT267",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "_Document41_VT984",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "_Document42",
                    "hash_column_indexes": [],
                    "n_columns": 11,
                    "sql": None
                },
                {
                    "source_table": "_Document42_VT276",
                    "hash_column_indexes": [],
                    "n_columns": 12,
                    "sql": None
                },
                {
                    "source_table": "_Document42_VT5048",
                    "hash_column_indexes": [],
                    "n_columns": 11,
                    "sql": None
                },
                {
                    "source_table": "_Document42_VT5056",
                    "hash_column_indexes": [],
                    "n_columns": 16,
                    "sql": None
                },
                {
                    "source_table": "_Document43",
                    "hash_column_indexes": [],
                    "n_columns": 13,
                    "sql": None
                },
                {
                    "source_table": "_Document43_VT288",
                    "hash_column_indexes": [],
                    "n_columns": 20,
                    "sql": None
                },
                {
                    "source_table": "_Document44",
                    "hash_column_indexes": [],
                    "n_columns": 11,
                    "sql": None
                },
                {
                    "source_table": "_Document44_VT296",
                    "hash_column_indexes": [],
                    "n_columns": 15,
                    "sql": None
                },
                {
                    "source_table": "_Document45",
                    "hash_column_indexes": [],
                    "n_columns": 13,
                    "sql": None
                },
                {
                    "source_table": "_Document45_VT313",
                    "hash_column_indexes": [],
                    "n_columns": 11,
                    "sql": None
                },
                {
                    "source_table": "_Document48",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "_Document49",
                    "hash_column_indexes": [],
                    "n_columns": 33,
                    "sql": None
                },
                {
                    "source_table": "_Document4925",
                    "hash_column_indexes": [],
                    "n_columns": 14,
                    "sql": None
                },
                {
                    "source_table": "_Document4925_VT4934",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "_Document4926",
                    "hash_column_indexes": [],
                    "n_columns": 13,
                    "sql": None
                },
                {
                    "source_table": "_Document4926_VT4942",
                    "hash_column_indexes": [],
                    "n_columns": 13,
                    "sql": None
                },
                {
                    "source_table": "_Document49_VT988",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "_Document50",
                    "hash_column_indexes": [],
                    "n_columns": 11,
                    "sql": None
                },
                {
                    "source_table": "_Document5016",
                    "hash_column_indexes": [],
                    "n_columns": 11,
                    "sql": None
                },
                {
                    "source_table": "_Document5016_VT5021",
                    "hash_column_indexes": [],
                    "n_columns": 15,
                    "sql": None
                },
                {
                    "source_table": "_Document50_VT378",
                    "hash_column_indexes": [],
                    "n_columns": 24,
                    "sql": None
                },
                {
                    "source_table": "_Document51",
                    "hash_column_indexes": [],
                    "n_columns": 29,
                    "sql": None
                },
                {
                    "source_table": "_Document51_VT1018",
                    "hash_column_indexes": [],
                    "n_columns": 15,
                    "sql": None
                },
                {
                    "source_table": "_Document51_VT1025",
                    "hash_column_indexes": [],
                    "n_columns": 14,
                    "sql": None
                },
                {
                    "source_table": "_Document51_VT1174",
                    "hash_column_indexes": [],
                    "n_columns": 16,
                    "sql": None
                },
                {
                    "source_table": "_Document51_VT1351",
                    "hash_column_indexes": [],
                    "n_columns": 11,
                    "sql": None
                },
                {
                    "source_table": "_Document51_VT1559",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "_Document51_VT2189",
                    "hash_column_indexes": [],
                    "n_columns": 10,
                    "sql": None
                },
                {
                    "source_table": "_Document51_VT2203",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "_Document51_VT2250",
                    "hash_column_indexes": [],
                    "n_columns": 21,
                    "sql": None
                },
                {
                    "source_table": "_Document51_VT2260",
                    "hash_column_indexes": [],
                    "n_columns": 22,
                    "sql": None
                },
                {
                    "source_table": "_Document51_VT2458",
                    "hash_column_indexes": [],
                    "n_columns": 13,
                    "sql": None
                },
                {
                    "source_table": "_Document51_VT2491",
                    "hash_column_indexes": [],
                    "n_columns": 14,
                    "sql": None
                },
                {
                    "source_table": "_Document51_VT2549",
                    "hash_column_indexes": [],
                    "n_columns": 19,
                    "sql": None
                },
                {
                    "source_table": "_Document51_VT2611",
                    "hash_column_indexes": [],
                    "n_columns": 22,
                    "sql": None
                },
                {
                    "source_table": "_Document51_VT2662",
                    "hash_column_indexes": [],
                    "n_columns": 22,
                    "sql": None
                },
                {
                    "source_table": "_Document51_VT2708",
                    "hash_column_indexes": [],
                    "n_columns": 21,
                    "sql": None
                },
                {
                    "source_table": "_Document51_VT4128",
                    "hash_column_indexes": [],
                    "n_columns": 16,
                    "sql": None
                },
                {
                    "source_table": "_Document51_VT4193",
                    "hash_column_indexes": [],
                    "n_columns": 22,
                    "sql": None
                },
                {
                    "source_table": "_Document51_VT4212",
                    "hash_column_indexes": [],
                    "n_columns": 22,
                    "sql": None
                },
                {
                    "source_table": "_Document51_VT4231",
                    "hash_column_indexes": [],
                    "n_columns": 22,
                    "sql": None
                },
                {
                    "source_table": "_Document51_VT4250",
                    "hash_column_indexes": [],
                    "n_columns": 22,
                    "sql": None
                },
                {
                    "source_table": "_Document51_VT4269",
                    "hash_column_indexes": [],
                    "n_columns": 22,
                    "sql": None
                },
                {
                    "source_table": "_Document51_VT4439",
                    "hash_column_indexes": [],
                    "n_columns": 22,
                    "sql": None
                },
                {
                    "source_table": "_Document51_VT4748",
                    "hash_column_indexes": [],
                    "n_columns": 22,
                    "sql": None
                },
                {
                    "source_table": "_Document51_VT4859",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "_Document51_VT4891",
                    "hash_column_indexes": [],
                    "n_columns": 12,
                    "sql": None
                },
                {
                    "source_table": "_Document51_VT5069",
                    "hash_column_indexes": [],
                    "n_columns": 28,
                    "sql": None
                },
                {
                    "source_table": "_Document51_VT5094",
                    "hash_column_indexes": [],
                    "n_columns": 28,
                    "sql": None
                },
                {
                    "source_table": "_Document51_VT5119",
                    "hash_column_indexes": [],
                    "n_columns": 18,
                    "sql": None
                },
                {
                    "source_table": "_Document51_VT5134",
                    "hash_column_indexes": [],
                    "n_columns": 25,
                    "sql": None
                },
                {
                    "source_table": "_Document51_VT5361",
                    "hash_column_indexes": [],
                    "n_columns": 22,
                    "sql": None
                },
                {
                    "source_table": "_Document51_VT5380",
                    "hash_column_indexes": [],
                    "n_columns": 22,
                    "sql": None
                },
                {
                    "source_table": "_Document51_VT5514",
                    "hash_column_indexes": [],
                    "n_columns": 21,
                    "sql": None
                },
                {
                    "source_table": "_Document52",
                    "hash_column_indexes": [],
                    "n_columns": 11,
                    "sql": None
                },
                {
                    "source_table": "_Document52_VT413",
                    "hash_column_indexes": [],
                    "n_columns": 14,
                    "sql": None
                },
                {
                    "source_table": "_Document52_VT5156",
                    "hash_column_indexes": [],
                    "n_columns": 23,
                    "sql": None
                },
                {
                    "source_table": "_Document53",
                    "hash_column_indexes": [],
                    "n_columns": 122,
                    "sql": None
                },
                {
                    "source_table": "_Document53_VT1033",
                    "hash_column_indexes": [],
                    "n_columns": 20,
                    "sql": None
                },
                {
                    "source_table": "_Document53_VT1803",
                    "hash_column_indexes": [],
                    "n_columns": 28,
                    "sql": None
                },
                {
                    "source_table": "_Document53_VT2107",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "_Document53_VT3046",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "_Document53_VT3076",
                    "hash_column_indexes": [],
                    "n_columns": 23,
                    "sql": None
                },
                {
                    "source_table": "_Document53_VT3388",
                    "hash_column_indexes": [],
                    "n_columns": 21,
                    "sql": None
                },
                {
                    "source_table": "_Document53_VT4062",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "_Document53_VT4068",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "_Document53_VT4074",
                    "hash_column_indexes": [],
                    "n_columns": 11,
                    "sql": None
                },
                {
                    "source_table": "_Document53_VT4080",
                    "hash_column_indexes": [],
                    "n_columns": 14,
                    "sql": None
                },
                {
                    "source_table": "_Document53_VT4090",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "_Document53_VT4114",
                    "hash_column_indexes": [],
                    "n_columns": 10,
                    "sql": None
                },
                {
                    "source_table": "_Document53_VT4184",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "_Document53_VT4289",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "_Document53_VT440",
                    "hash_column_indexes": [],
                    "n_columns": 12,
                    "sql": None
                },
                {
                    "source_table": "_Document53_VT4459",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "_Document53_VT4530",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "_Document53_VT4768",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "_Document53_VT4865",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "_Document53_VT5189",
                    "hash_column_indexes": [],
                    "n_columns": 21,
                    "sql": None
                },
                {
                    "source_table": "_Document53_VT5401",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "_Document53_VT5407",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "_Document54",
                    "hash_column_indexes": [],
                    "n_columns": 12,
                    "sql": None
                },
                {
                    "source_table": "_Document54_VT449",
                    "hash_column_indexes": [],
                    "n_columns": 13,
                    "sql": None
                },
                {
                    "source_table": "_Document55",
                    "hash_column_indexes": [],
                    "n_columns": 131,
                    "sql": None
                },
                {
                    "source_table": "_Document55_VT1202",
                    "hash_column_indexes": [],
                    "n_columns": 10,
                    "sql": None
                },
                {
                    "source_table": "_Document55_VT2039",
                    "hash_column_indexes": [],
                    "n_columns": 12,
                    "sql": None
                },
                {
                    "source_table": "_Document55_VT2046",
                    "hash_column_indexes": [],
                    "n_columns": 23,
                    "sql": None
                },
                {
                    "source_table": "_Document55_VT3936",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "_Document55_VT4012",
                    "hash_column_indexes": [],
                    "n_columns": 11,
                    "sql": None
                },
                {
                    "source_table": "_Document55_VT492",
                    "hash_column_indexes": [],
                    "n_columns": 16,
                    "sql": None
                },
                {
                    "source_table": "_Document55_VT496",
                    "hash_column_indexes": [],
                    "n_columns": 50,
                    "sql": None
                },
                {
                    "source_table": "_Document55_VT5350",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "_Document60",
                    "hash_column_indexes": [],
                    "n_columns": 14,
                    "sql": None
                },
                {
                    "source_table": "_Document60_VT594",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "_Document61",
                    "hash_column_indexes": [],
                    "n_columns": 11,
                    "sql": None
                },
                {
                    "source_table": "_Document61_VT603",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "_Document679",
                    "hash_column_indexes": [],
                    "n_columns": 40,
                    "sql": None
                },
                {
                    "source_table": "_Document679_VT713",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "_Document679_VT717",
                    "hash_column_indexes": [],
                    "n_columns": 13,
                    "sql": None
                },
                {
                    "source_table": "_Document737",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "_Document737_VT740",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "_Document738",
                    "hash_column_indexes": [],
                    "n_columns": 20,
                    "sql": None
                },
                {
                    "source_table": "_Document738_VT1447",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "_Document738_VT751",
                    "hash_column_indexes": [],
                    "n_columns": 18,
                    "sql": None
                },
                {
                    "source_table": "_Document854",
                    "hash_column_indexes": [],
                    "n_columns": 15,
                    "sql": None
                },
                {
                    "source_table": "_Document854_VT856",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "_Document903",
                    "hash_column_indexes": [],
                    "n_columns": 15,
                    "sql": None
                },
                {
                    "source_table": "_Document903_VT1382",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "_Document903_VT904",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "_Document905",
                    "hash_column_indexes": [],
                    "n_columns": 17,
                    "sql": None
                },
                {
                    "source_table": "_Document905_VT920",
                    "hash_column_indexes": [],
                    "n_columns": 20,
                    "sql": None
                },
                {
                    "source_table": "_Document906",
                    "hash_column_indexes": [],
                    "n_columns": 21,
                    "sql": None
                },
                {
                    "source_table": "_Document906_VT1224",
                    "hash_column_indexes": [],
                    "n_columns": 10,
                    "sql": None
                },
                {
                    "source_table": "_Document906_VT1356",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "_Document906_VT1475",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "_Document906_VT932",
                    "hash_column_indexes": [],
                    "n_columns": 26,
                    "sql": None
                },
                {
                    "source_table": "_DocumentJournal608",
                    "hash_column_indexes": [],
                    "n_columns": 13,
                    "sql": None
                },
                {
                    "source_table": "_DocumentJournal614",
                    "hash_column_indexes": [],
                    "n_columns": 10,
                    "sql": None
                },
                {
                    "source_table": "_DocumentJournal971",
                    "hash_column_indexes": [],
                    "n_columns": 10,
                    "sql": None
                },
                {
                    "source_table": "_Enum1111",
                    "hash_column_indexes": [],
                    "n_columns": 4,
                    "sql": None
                },
                {
                    "source_table": "_Enum1245",
                    "hash_column_indexes": [],
                    "n_columns": 4,
                    "sql": None
                },
                {
                    "source_table": "_Enum1246",
                    "hash_column_indexes": [],
                    "n_columns": 4,
                    "sql": None
                },
                {
                    "source_table": "_Enum1247",
                    "hash_column_indexes": [],
                    "n_columns": 4,
                    "sql": None
                },
                {
                    "source_table": "_Enum1372",
                    "hash_column_indexes": [],
                    "n_columns": 4,
                    "sql": None
                },
                {
                    "source_table": "_Enum1404",
                    "hash_column_indexes": [],
                    "n_columns": 4,
                    "sql": None
                },
                {
                    "source_table": "_Enum1492",
                    "hash_column_indexes": [],
                    "n_columns": 4,
                    "sql": None
                },
                {
                    "source_table": "_Enum1493",
                    "hash_column_indexes": [],
                    "n_columns": 4,
                    "sql": None
                },
                {
                    "source_table": "_Enum1494",
                    "hash_column_indexes": [],
                    "n_columns": 4,
                    "sql": None
                },
                {
                    "source_table": "_Enum1540",
                    "hash_column_indexes": [],
                    "n_columns": 4,
                    "sql": None
                },
                {
                    "source_table": "_Enum1573",
                    "hash_column_indexes": [],
                    "n_columns": 4,
                    "sql": None
                },
                {
                    "source_table": "_Enum1722",
                    "hash_column_indexes": [],
                    "n_columns": 4,
                    "sql": None
                },
                {
                    "source_table": "_Enum1772",
                    "hash_column_indexes": [],
                    "n_columns": 4,
                    "sql": None
                },
                {
                    "source_table": "_Enum1947",
                    "hash_column_indexes": [],
                    "n_columns": 4,
                    "sql": None
                },
                {
                    "source_table": "_Enum1948",
                    "hash_column_indexes": [],
                    "n_columns": 4,
                    "sql": None
                },
                {
                    "source_table": "_Enum1949",
                    "hash_column_indexes": [],
                    "n_columns": 4,
                    "sql": None
                },
                {
                    "source_table": "_Enum1950",
                    "hash_column_indexes": [],
                    "n_columns": 4,
                    "sql": None
                },
                {
                    "source_table": "_Enum2074",
                    "hash_column_indexes": [],
                    "n_columns": 4,
                    "sql": None
                },
                {
                    "source_table": "_Enum2240",
                    "hash_column_indexes": [],
                    "n_columns": 4,
                    "sql": None
                },
                {
                    "source_table": "_Enum2392",
                    "hash_column_indexes": [],
                    "n_columns": 4,
                    "sql": None
                },
                {
                    "source_table": "_Enum2596",
                    "hash_column_indexes": [],
                    "n_columns": 4,
                    "sql": None
                },
                {
                    "source_table": "_Enum2832",
                    "hash_column_indexes": [],
                    "n_columns": 4,
                    "sql": None
                },
                {
                    "source_table": "_Enum2983",
                    "hash_column_indexes": [],
                    "n_columns": 4,
                    "sql": None
                },
                {
                    "source_table": "_Enum2984",
                    "hash_column_indexes": [],
                    "n_columns": 4,
                    "sql": None
                },
                {
                    "source_table": "_Enum3006",
                    "hash_column_indexes": [],
                    "n_columns": 4,
                    "sql": None
                },
                {
                    "source_table": "_Enum3171",
                    "hash_column_indexes": [],
                    "n_columns": 4,
                    "sql": None
                },
                {
                    "source_table": "_Enum3277",
                    "hash_column_indexes": [],
                    "n_columns": 4,
                    "sql": None
                },
                {
                    "source_table": "_Enum3352",
                    "hash_column_indexes": [],
                    "n_columns": 4,
                    "sql": None
                },
                {
                    "source_table": "_Enum3457",
                    "hash_column_indexes": [],
                    "n_columns": 4,
                    "sql": None
                },
                {
                    "source_table": "_Enum3458",
                    "hash_column_indexes": [],
                    "n_columns": 4,
                    "sql": None
                },
                {
                    "source_table": "_Enum3957",
                    "hash_column_indexes": [],
                    "n_columns": 4,
                    "sql": None
                },
                {
                    "source_table": "_Enum3958",
                    "hash_column_indexes": [],
                    "n_columns": 4,
                    "sql": None
                },
                {
                    "source_table": "_Enum4165",
                    "hash_column_indexes": [],
                    "n_columns": 4,
                    "sql": None
                },
                {
                    "source_table": "_Enum4487",
                    "hash_column_indexes": [],
                    "n_columns": 4,
                    "sql": None
                },
                {
                    "source_table": "_Enum4850",
                    "hash_column_indexes": [],
                    "n_columns": 4,
                    "sql": None
                },
                {
                    "source_table": "_Enum4927",
                    "hash_column_indexes": [],
                    "n_columns": 4,
                    "sql": None
                },
                {
                    "source_table": "_Enum5017",
                    "hash_column_indexes": [],
                    "n_columns": 4,
                    "sql": None
                },
                {
                    "source_table": "_Enum5496",
                    "hash_column_indexes": [],
                    "n_columns": 4,
                    "sql": None
                },
                {
                    "source_table": "_Enum62",
                    "hash_column_indexes": [],
                    "n_columns": 4,
                    "sql": None
                },
                {
                    "source_table": "_Enum63",
                    "hash_column_indexes": [],
                    "n_columns": 4,
                    "sql": None
                },
                {
                    "source_table": "_Enum64",
                    "hash_column_indexes": [],
                    "n_columns": 4,
                    "sql": None
                },
                {
                    "source_table": "_Enum65",
                    "hash_column_indexes": [],
                    "n_columns": 4,
                    "sql": None
                },
                {
                    "source_table": "_Enum66",
                    "hash_column_indexes": [],
                    "n_columns": 4,
                    "sql": None
                },
                {
                    "source_table": "_Enum67",
                    "hash_column_indexes": [],
                    "n_columns": 4,
                    "sql": None
                },
                {
                    "source_table": "_Enum68",
                    "hash_column_indexes": [],
                    "n_columns": 4,
                    "sql": None
                },
                {
                    "source_table": "_Enum69",
                    "hash_column_indexes": [],
                    "n_columns": 4,
                    "sql": None
                },
                {
                    "source_table": "_Enum70",
                    "hash_column_indexes": [],
                    "n_columns": 4,
                    "sql": None
                },
                {
                    "source_table": "_Enum71",
                    "hash_column_indexes": [],
                    "n_columns": 4,
                    "sql": None
                },
                {
                    "source_table": "_Enum72",
                    "hash_column_indexes": [],
                    "n_columns": 4,
                    "sql": None
                },
                {
                    "source_table": "_Enum73",
                    "hash_column_indexes": [],
                    "n_columns": 4,
                    "sql": None
                },
                {
                    "source_table": "_Enum732",
                    "hash_column_indexes": [],
                    "n_columns": 4,
                    "sql": None
                },
                {
                    "source_table": "_Enum74",
                    "hash_column_indexes": [],
                    "n_columns": 4,
                    "sql": None
                },
                {
                    "source_table": "_Enum75",
                    "hash_column_indexes": [],
                    "n_columns": 4,
                    "sql": None
                },
                {
                    "source_table": "_Enum76",
                    "hash_column_indexes": [],
                    "n_columns": 4,
                    "sql": None
                },
                {
                    "source_table": "_Enum77",
                    "hash_column_indexes": [],
                    "n_columns": 4,
                    "sql": None
                },
                {
                    "source_table": "_Enum78",
                    "hash_column_indexes": [],
                    "n_columns": 4,
                    "sql": None
                },
                {
                    "source_table": "_Enum79",
                    "hash_column_indexes": [],
                    "n_columns": 4,
                    "sql": None
                },
                {
                    "source_table": "_Enum80",
                    "hash_column_indexes": [],
                    "n_columns": 4,
                    "sql": None
                },
                {
                    "source_table": "_Enum81",
                    "hash_column_indexes": [],
                    "n_columns": 4,
                    "sql": None
                },
                {
                    "source_table": "_Enum82",
                    "hash_column_indexes": [],
                    "n_columns": 4,
                    "sql": None
                },
                {
                    "source_table": "_Enum83",
                    "hash_column_indexes": [],
                    "n_columns": 4,
                    "sql": None
                },
                {
                    "source_table": "_Enum84",
                    "hash_column_indexes": [],
                    "n_columns": 4,
                    "sql": None
                },
                {
                    "source_table": "_Enum85",
                    "hash_column_indexes": [],
                    "n_columns": 4,
                    "sql": None
                },
                {
                    "source_table": "_Enum86",
                    "hash_column_indexes": [],
                    "n_columns": 4,
                    "sql": None
                },
                {
                    "source_table": "_Enum87",
                    "hash_column_indexes": [],
                    "n_columns": 4,
                    "sql": None
                },
                {
                    "source_table": "_Enum88",
                    "hash_column_indexes": [],
                    "n_columns": 4,
                    "sql": None
                },
                {
                    "source_table": "_Enum89",
                    "hash_column_indexes": [],
                    "n_columns": 4,
                    "sql": None
                },
                {
                    "source_table": "_InfoRg1083",
                    "hash_column_indexes": [],
                    "n_columns": 10,
                    "sql": None
                },
                {
                    "source_table": "_InfoRg1090",
                    "hash_column_indexes": [],
                    "n_columns": 18,
                    "sql": None
                },
                {
                    "source_table": "_InfoRg1145",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "_InfoRg1149",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "_InfoRg1185",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "_InfoRg1189",
                    "hash_column_indexes": [],
                    "n_columns": 11,
                    "sql": None
                },
                {
                    "source_table": "_InfoRg1243",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "_InfoRg1259",
                    "hash_column_indexes": [],
                    "n_columns": 17,
                    "sql": None
                },
                {
                    "source_table": "_InfoRg1324",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "_InfoRg1330",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "_InfoRg1333",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "_InfoRg1336",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "_InfoRg1405",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "_InfoRg1410",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "_InfoRg1415",
                    "hash_column_indexes": [],
                    "n_columns": 21,
                    "sql": None
                },
                {
                    "source_table": "_InfoRg1572",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "_InfoRg1580",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "_InfoRg1585",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "_InfoRg1626",
                    "hash_column_indexes": [],
                    "n_columns": 4,
                    "sql": None
                },
                {
                    "source_table": "_InfoRg1741",
                    "hash_column_indexes": [],
                    "n_columns": 13,
                    "sql": None
                },
                {
                    "source_table": "_InfoRg1754",
                    "hash_column_indexes": [],
                    "n_columns": 11,
                    "sql": None
                },
                {
                    "source_table": "_InfoRg1788",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "_InfoRg1795",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "_InfoRg1833",
                    "hash_column_indexes": [],
                    "n_columns": 11,
                    "sql": None
                },
                {
                    "source_table": "_InfoRg1844",
                    "hash_column_indexes": [],
                    "n_columns": 43,
                    "sql": None
                },
                {
                    "source_table": "_InfoRg1873",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "_InfoRg1984",
                    "hash_column_indexes": [],
                    "n_columns": 4,
                    "sql": None
                },
                {
                    "source_table": "_InfoRg1987",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "_InfoRg2122",
                    "hash_column_indexes": [],
                    "n_columns": 48,
                    "sql": None
                },
                {
                    "source_table": "_InfoRg2153",
                    "hash_column_indexes": [],
                    "n_columns": 45,
                    "sql": None
                },
                {
                    "source_table": "_InfoRg2199",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "_InfoRg2216",
                    "hash_column_indexes": [],
                    "n_columns": 3,
                    "sql": None
                },
                {
                    "source_table": "_InfoRg2220",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "_InfoRg2231",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "_InfoRg2244",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "_InfoRg2331",
                    "hash_column_indexes": [],
                    "n_columns": 10,
                    "sql": None
                },
                {
                    "source_table": "_InfoRg2372",
                    "hash_column_indexes": [],
                    "n_columns": 23,
                    "sql": None
                },
                {
                    "source_table": "_InfoRg2636",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "_InfoRg2747",
                    "hash_column_indexes": [],
                    "n_columns": 22,
                    "sql": None
                },
                {
                    "source_table": "_InfoRg2754",
                    "hash_column_indexes": [],
                    "n_columns": 22,
                    "sql": None
                },
                {
                    "source_table": "_InfoRg2830",
                    "hash_column_indexes": [],
                    "n_columns": 17,
                    "sql": None
                },
                {
                    "source_table": "_InfoRg2876",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "_InfoRg2890",
                    "hash_column_indexes": [],
                    "n_columns": 21,
                    "sql": None
                },
                {
                    "source_table": "_InfoRg2908",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "_InfoRg2926",
                    "hash_column_indexes": [],
                    "n_columns": 13,
                    "sql": None
                },
                {
                    "source_table": "_InfoRg2965",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "_InfoRg2972",
                    "hash_column_indexes": [],
                    "n_columns": 4,
                    "sql": None
                },
                {
                    "source_table": "_InfoRg2987",
                    "hash_column_indexes": [],
                    "n_columns": 4,
                    "sql": None
                },
                {
                    "source_table": "_InfoRg2989",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "_InfoRg2995",
                    "hash_column_indexes": [],
                    "n_columns": 4,
                    "sql": None
                },
                {
                    "source_table": "_InfoRg3016",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "_InfoRg3023",
                    "hash_column_indexes": [],
                    "n_columns": 16,
                    "sql": None
                },
                {
                    "source_table": "_InfoRg3063",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "_InfoRg3131",
                    "hash_column_indexes": [],
                    "n_columns": 17,
                    "sql": None
                },
                {
                    "source_table": "_InfoRg3164",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "_InfoRg3173",
                    "hash_column_indexes": [],
                    "n_columns": 22,
                    "sql": None
                },
                {
                    "source_table": "_InfoRg3237",
                    "hash_column_indexes": [],
                    "n_columns": 43,
                    "sql": None
                },
                {
                    "source_table": "_InfoRg3282",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "_InfoRg3286",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "_InfoRg3292",
                    "hash_column_indexes": [],
                    "n_columns": 3,
                    "sql": None
                },
                {
                    "source_table": "_InfoRg3351",
                    "hash_column_indexes": [],
                    "n_columns": 12,
                    "sql": None
                },
                {
                    "source_table": "_InfoRg3367",
                    "hash_column_indexes": [],
                    "n_columns": 10,
                    "sql": None
                },
                {
                    "source_table": "_InfoRg3429",
                    "hash_column_indexes": [],
                    "n_columns": 38,
                    "sql": None
                },
                {
                    "source_table": "_InfoRg3471",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "_InfoRg3476",
                    "hash_column_indexes": [],
                    "n_columns": 14,
                    "sql": None
                },
                {
                    "source_table": "_InfoRg3495",
                    "hash_column_indexes": [],
                    "n_columns": 4,
                    "sql": None
                },
                {
                    "source_table": "_InfoRg3500",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "_InfoRg3506",
                    "hash_column_indexes": [],
                    "n_columns": 14,
                    "sql": None
                },
                {
                    "source_table": "_InfoRg3531",
                    "hash_column_indexes": [],
                    "n_columns": 23,
                    "sql": None
                },
                {
                    "source_table": "_InfoRg3547",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "_InfoRg3634",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "_InfoRg3642",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "_InfoRg3650",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "_InfoRg3705",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "_InfoRg3752",
                    "hash_column_indexes": [],
                    "n_columns": 14,
                    "sql": None
                },
                {
                    "source_table": "_InfoRg3753",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "_InfoRg3834",
                    "hash_column_indexes": [],
                    "n_columns": 15,
                    "sql": None
                },
                {
                    "source_table": "_InfoRg3852",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "_InfoRg3866",
                    "hash_column_indexes": [],
                    "n_columns": 10,
                    "sql": None
                },
                {
                    "source_table": "_InfoRg3895",
                    "hash_column_indexes": [],
                    "n_columns": 18,
                    "sql": None
                },
                {
                    "source_table": "_InfoRg3910",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "_InfoRg3916",
                    "hash_column_indexes": [],
                    "n_columns": 14,
                    "sql": None
                },
                {
                    "source_table": "_InfoRg3959",
                    "hash_column_indexes": [],
                    "n_columns": 20,
                    "sql": None
                },
                {
                    "source_table": "_InfoRg4026",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "_InfoRg4033",
                    "hash_column_indexes": [],
                    "n_columns": 11,
                    "sql": None
                },
                {
                    "source_table": "_InfoRg4041",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "_InfoRg4044",
                    "hash_column_indexes": [],
                    "n_columns": 10,
                    "sql": None
                },
                {
                    "source_table": "_InfoRg4051",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "_InfoRg4054",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "_InfoRg4105",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "_InfoRg4124",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "_InfoRg4155",
                    "hash_column_indexes": [],
                    "n_columns": 13,
                    "sql": None
                },
                {
                    "source_table": "_InfoRg4166",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "_InfoRg4417",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "_InfoRg4425",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "_InfoRg4488",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "_InfoRg4496",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "_InfoRg4504",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "_InfoRg4512",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "_InfoRg4520",
                    "hash_column_indexes": [],
                    "n_columns": 10,
                    "sql": None
                },
                {
                    "source_table": "_InfoRg4550",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "_InfoRg4579",
                    "hash_column_indexes": [],
                    "n_columns": 42,
                    "sql": None
                },
                {
                    "source_table": "_InfoRg4641",
                    "hash_column_indexes": [],
                    "n_columns": 44,
                    "sql": None
                },
                {
                    "source_table": "_InfoRg4707",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "_InfoRg4720",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "_InfoRg4726",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "_InfoRg4736",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "_InfoRg4795",
                    "hash_column_indexes": [],
                    "n_columns": 34,
                    "sql": None
                },
                {
                    "source_table": "_InfoRg4851",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "_InfoRg4875",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "_InfoRg4948",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "_InfoRg4961",
                    "hash_column_indexes": [],
                    "n_columns": 12,
                    "sql": None
                },
                {
                    "source_table": "_InfoRg5033",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "_InfoRg5302",
                    "hash_column_indexes": [],
                    "n_columns": 10,
                    "sql": None
                },
                {
                    "source_table": "_InfoRg5323",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "_InfoRg5341",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "_InfoRg5458",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "_InfoRg5467",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "_InfoRg5497",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "_InfoRg5504",
                    "hash_column_indexes": [],
                    "n_columns": 15,
                    "sql": None
                },
                {
                    "source_table": "_InfoRg621",
                    "hash_column_indexes": [],
                    "n_columns": 19,
                    "sql": None
                },
                {
                    "source_table": "_InfoRg626",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "_InfoRg628",
                    "hash_column_indexes": [],
                    "n_columns": 19,
                    "sql": None
                },
                {
                    "source_table": "_InfoRg637",
                    "hash_column_indexes": [],
                    "n_columns": 13,
                    "sql": None
                },
                {
                    "source_table": "_InfoRg647",
                    "hash_column_indexes": [],
                    "n_columns": 16,
                    "sql": None
                },
                {
                    "source_table": "_InfoRg650",
                    "hash_column_indexes": [],
                    "n_columns": 12,
                    "sql": None
                },
                {
                    "source_table": "_InfoRg655",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "_InfoRg859",
                    "hash_column_indexes": [],
                    "n_columns": 46,
                    "sql": None
                },
                {
                    "source_table": "_InfoRg900",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "_InfoRg974",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "_Reference10",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "_Reference1078",
                    "hash_column_indexes": [],
                    "n_columns": 12,
                    "sql": None
                },
                {
                    "source_table": "_Reference10_VT95",
                    "hash_column_indexes": [],
                    "n_columns": 4,
                    "sql": None
                },
                {
                    "source_table": "_Reference11",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "_Reference11_VT98",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "_Reference12",
                    "hash_column_indexes": [],
                    "n_columns": 14,
                    "sql": None
                },
                {
                    "source_table": "_Reference1244",
                    "hash_column_indexes": [],
                    "n_columns": 15,
                    "sql": None
                },
                {
                    "source_table": "_Reference13",
                    "hash_column_indexes": [],
                    "n_columns": 20,
                    "sql": None
                },
                {
                    "source_table": "_Reference1369",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "_Reference1386",
                    "hash_column_indexes": [],
                    "n_columns": 11,
                    "sql": None
                },
                {
                    "source_table": "_Reference14",
                    "hash_column_indexes": [],
                    "n_columns": 18,
                    "sql": None
                },
                {
                    "source_table": "_Reference1401",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "_Reference1402",
                    "hash_column_indexes": [],
                    "n_columns": 13,
                    "sql": None
                },
                {
                    "source_table": "_Reference1402_VT3484",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "_Reference15",
                    "hash_column_indexes": [],
                    "n_columns": 16,
                    "sql": None
                },
                {
                    "source_table": "_Reference1539",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "_Reference16",
                    "hash_column_indexes": [],
                    "n_columns": 15,
                    "sql": None
                },
                {
                    "source_table": "_Reference17",
                    "hash_column_indexes": [],
                    "n_columns": 10,
                    "sql": None
                },
                {
                    "source_table": "_Reference1723",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "_Reference1724",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "_Reference18",
                    "hash_column_indexes": [],
                    "n_columns": 19,
                    "sql": None
                },
                {
                    "source_table": "_Reference19",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "_Reference1944",
                    "hash_column_indexes": [],
                    "n_columns": 16,
                    "sql": None
                },
                {
                    "source_table": "_Reference1945",
                    "hash_column_indexes": [],
                    "n_columns": 12,
                    "sql": None
                },
                {
                    "source_table": "_Reference20",
                    "hash_column_indexes": [],
                    "n_columns": 35,
                    "sql": None
                },
                {
                    "source_table": "_Reference2073",
                    "hash_column_indexes": [],
                    "n_columns": 13,
                    "sql": None
                },
                {
                    "source_table": "_Reference21",
                    "hash_column_indexes": [],
                    "n_columns": 12,
                    "sql": None
                },
                {
                    "source_table": "_Reference22",
                    "hash_column_indexes": [],
                    "n_columns": 10,
                    "sql": None
                },
                {
                    "source_table": "_Reference2239",
                    "hash_column_indexes": [],
                    "n_columns": 17,
                    "sql": None
                },
                {
                    "source_table": "_Reference23",
                    "hash_column_indexes": [],
                    "n_columns": 12,
                    "sql": None
                },
                {
                    "source_table": "_Reference2364",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "_Reference24",
                    "hash_column_indexes": [],
                    "n_columns": 69,
                    "sql": None
                },
                {
                    "source_table": "_Reference2486",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "_Reference25",
                    "hash_column_indexes": [],
                    "n_columns": 18,
                    "sql": None
                },
                {
                    "source_table": "_Reference2519",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "_Reference25_VT154",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "_Reference26",
                    "hash_column_indexes": [],
                    "n_columns": 18,
                    "sql": None
                },
                {
                    "source_table": "_Reference2648",
                    "hash_column_indexes": [],
                    "n_columns": 11,
                    "sql": None
                },
                {
                    "source_table": "_Reference26_VT163",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "_Reference27",
                    "hash_column_indexes": [],
                    "n_columns": 25,
                    "sql": None
                },
                {
                    "source_table": "_Reference28",
                    "hash_column_indexes": [],
                    "n_columns": 19,
                    "sql": None
                },
                {
                    "source_table": "_Reference2802",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "_Reference2831",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "_Reference29",
                    "hash_column_indexes": [],
                    "n_columns": 19,
                    "sql": None
                },
                {
                    "source_table": "_Reference2924",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "_Reference2982",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "_Reference30",
                    "hash_column_indexes": [],
                    "n_columns": 12,
                    "sql": None
                },
                {
                    "source_table": "_Reference3004",
                    "hash_column_indexes": [],
                    "n_columns": 13,
                    "sql": None
                },
                {
                    "source_table": "_Reference3005",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "_Reference3094",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "_Reference31",
                    "hash_column_indexes": [],
                    "n_columns": 20,
                    "sql": None
                },
                {
                    "source_table": "_Reference3170",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "_Reference32",
                    "hash_column_indexes": [],
                    "n_columns": 11,
                    "sql": None
                },
                {
                    "source_table": "_Reference3233",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "_Reference3234",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "_Reference3235",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "_Reference3236",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "_Reference33",
                    "hash_column_indexes": [],
                    "n_columns": 11,
                    "sql": None
                },
                {
                    "source_table": "_Reference34",
                    "hash_column_indexes": [],
                    "n_columns": 20,
                    "sql": None
                },
                {
                    "source_table": "_Reference3423",
                    "hash_column_indexes": [],
                    "n_columns": 11,
                    "sql": None
                },
                {
                    "source_table": "_Reference3459",
                    "hash_column_indexes": [],
                    "n_columns": 10,
                    "sql": None
                },
                {
                    "source_table": "_Reference3460",
                    "hash_column_indexes": [],
                    "n_columns": 11,
                    "sql": None
                },
                {
                    "source_table": "_Reference35",
                    "hash_column_indexes": [],
                    "n_columns": 20,
                    "sql": None
                },
                {
                    "source_table": "_Reference3524",
                    "hash_column_indexes": [],
                    "n_columns": 20,
                    "sql": None
                },
                {
                    "source_table": "_Reference36",
                    "hash_column_indexes": [],
                    "n_columns": 11,
                    "sql": None
                },
                {
                    "source_table": "_Reference3669",
                    "hash_column_indexes": [],
                    "n_columns": 10,
                    "sql": None
                },
                {
                    "source_table": "_Reference3670",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "_Reference37",
                    "hash_column_indexes": [],
                    "n_columns": 17,
                    "sql": None
                },
                {
                    "source_table": "_Reference3735",
                    "hash_column_indexes": [],
                    "n_columns": 10,
                    "sql": None
                },
                {
                    "source_table": "_Reference38",
                    "hash_column_indexes": [],
                    "n_columns": 12,
                    "sql": None
                },
                {
                    "source_table": "_Reference3820",
                    "hash_column_indexes": [],
                    "n_columns": 12,
                    "sql": None
                },
                {
                    "source_table": "_Reference3821",
                    "hash_column_indexes": [],
                    "n_columns": 10,
                    "sql": None
                },
                {
                    "source_table": "_Reference3956",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "_Reference4023",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "_Reference4192",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "_Reference4884",
                    "hash_column_indexes": [],
                    "n_columns": 14,
                    "sql": None
                },
                {
                    "source_table": "_Reference4911",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "_Reference4912",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "_Reference5015",
                    "hash_column_indexes": [],
                    "n_columns": 16,
                    "sql": None
                },
                {
                    "source_table": "_Reference5295",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "_Reference5475",
                    "hash_column_indexes": [],
                    "n_columns": 12,
                    "sql": None
                },
                {
                    "source_table": "_Reference5484",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "_Reference7",
                    "hash_column_indexes": [],
                    "n_columns": 12,
                    "sql": None
                },
                {
                    "source_table": "_Reference8",
                    "hash_column_indexes": [],
                    "n_columns": 31,
                    "sql": None
                },
                {
                    "source_table": "_Reference853",
                    "hash_column_indexes": [],
                    "n_columns": 18,
                    "sql": None
                },
                {
                    "source_table": "_Reference9",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "_Reference970",
                    "hash_column_indexes": [],
                    "n_columns": 10,
                    "sql": None
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
                'target_table': table['source_table'].upper(),
                'hash_column_indexes': table['hash_column_indexes'],
                'n_columns': table['n_columns'],
                'sql': table['sql']
            }

            task = PythonOperator(
                task_id=f"{schema['target_schema']}.{table['source_table'].upper()}",
                python_callable=main,
                op_kwargs=params,
                trigger_rule='all_done'
            )

            tasks.append(task)

    chain(*tasks)