import datetime
import hashlib
from typing import Union, List, Optional, Tuple

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
        conn_secrets = Connection.get_connection_from_secrets(conn_id="MNE_BUSINESSREGISTER")
        return pymssql.connect(
                database=conn_secrets.schema,
                user=conn_secrets.loginn,
                password=conn_secrets.password,
                host=conn_secrets.host,
                port=conn_secrets.port)
    except Exception as ex:
        raise ex


def get_target_conn() -> any:
    """
    Geting connection creadentials from airflow secrets and performing connection with database

    Returns:
        connection with database
    """
    try:
        conn_secrets = Connection.get_connection_from_secrets(conn_id="local_postgres")
        return psycopg2.connect(
                dbname=conn_secrets.schema,
                user=conn_secrets.login,
                password=conn_secrets.password,
                host=conn_secrets.host,
                port=conn_secrets.port)
    except Exception as ex:
        raise ex


def hash(value: Union[str, int], hash_keyword: str) -> Union[str, None]:
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
    

def hash_columns(values: List[Union[str, int]], indexes: List[int], hash_keyword: str) -> Tuple[Union[str, None]]:
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
        sql = f"SELECT * FROM {source_schema}.{source_table}"

    conn_source = get_source_conn()
    conn_target = get_target_conn()

    try:
        with conn_source.cursor() as cursor_source, \
             conn_target.cursor() as cursor_target:

            cursor_source.execute(sql)

            truncate_table(target_schema, target_table)
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
        'dag_name': 'r_mne_rsp',
        'owner': 'Imangali',
        'start_date': datetime.datetime(2022, 12, 28),
        'schedule_interval': '0 0 * * *',
        'tags': ['mne', 'rsp', 'reglament'],
    }

    return configs[key]


with DAG(
    dag_id=get_dag_configs('dag_name'),
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
            'source_schema': 'dbo',
            'target_schema': 'MNE_RSP',
            'tables': [
                {   
                    'source_table': 'C_BUSINESSKIND',
                    'hash_column_indexes': [],
                    'n_columns': 7,
                    'sql': None,
                },
                {   
                    'source_table': 'C_ERRORTYPE',
                    'hash_column_indexes': [],
                    'n_columns': 7,
                    'sql': None,
                },
                {   
                    'source_table': 'C_LICENSEKIND',
                    'hash_column_indexes': [],
                    'n_columns': 7,
                    'sql': None,
                },
                {   
                    'source_table': 'C_LICENSESTATE',
                    'hash_column_indexes': [],
                    'n_columns': 7,
                    'sql': None,
                },
                {   
                    'source_table': 'C_MRP',
                    'hash_column_indexes': [],
                    'n_columns': 5,
                    'sql': None,
                },
                {   
                    'source_table': 'C_OKED',
                    'hash_column_indexes': [],
                    'n_columns': 6,
                    'sql': None,
                },
                {   
                    'source_table': 'C_OPF',
                    'hash_column_indexes': [],
                    'n_columns': 8,
                    'sql': None,
                },
                {   
                    'source_table': 'R_BUSINESSDATA',
                    'hash_column_indexes': [],
                    'n_columns': 17,
                    'sql': None,
                },
                {   
                    'source_table': 'R_IP',
                    'hash_column_indexes': [],
                    'n_columns': 11,
                    'sql': None,
                },
                {   
                    'source_table': 'R_LICENSE',
                    'hash_column_indexes': [],
                    'n_columns': 9,
                    'sql': None,
                },
                {   
                    'source_table': 'R_LICENSE_REQUEST',
                    'hash_column_indexes': [],
                    'n_columns': 5,
                    'sql': None,
                },
                {   
                    'source_table': 'R_PERSON',
                    'hash_column_indexes': [],
                    'n_columns': 7,
                    'sql': None,
                },
                {   
                    'source_table': 'R_UL',
                    'hash_column_indexes': [],
                    'n_columns': 12,
                    'sql': None,
                },
                {   
                    'source_table': 'R_ULOKED',
                    'hash_column_indexes': [],
                    'n_columns': 6,
                    'sql': None,
                }
            ]
        },
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
                op_kwargs=params
            )

            tasks.append(task)

    chain(*tasks)