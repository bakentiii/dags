import datetime
import hashlib
from typing import Union, List, Tuple, Optional

import psycopg2
import cx_Oracle
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
        conn_secrets = Connection.get_connection_from_secrets(conn_id="MTSZN_WMARKET")
        dsn_data = None

        if conn_secrets.extra['sid'] is not None:
            dsn_data = cx_Oracle.makedsn(host=conn_secrets.host, port=conn_secrets.port, sid=conn_secrets.extra['sid'])
        elif conn_secrets.extra['sevice_name'] is not None:
            dsn_data = cx_Oracle.makedsn(host=conn_secrets.host, port=conn_secrets.port, service_name=conn_secrets.extra['service_name'])
        
        return cx_Oracle.connect(conn_secrets.login, password=conn_secrets.password, dsn=dsn_data)
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
        'dag_id': "r_mtszn_irs",
        'owner': "Bakhtiyar",
        'start_date': datetime.datetime(2023, 7, 21),
        'schedule_interval': "0 0 * * *",
        'tags': ['mtszn', 'irs', 'reglament'],
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
            "source_schema": "WMARKET",
            "target_schema": "R_MTSZN_IRS",
            "tables": [
                {
                    "source_table": "ADDRESS",
                    "hash_column_indexes": [],
                    "n_columns": 25,
                    "sql": None
                },
                {
                    "source_table": "BANK_ATTR",
                    "hash_column_indexes": [
                        11,
                        12,
                        13
                    ],
                    "n_columns": 20,
                    "sql": None
                },
                {
                    "source_table": "D_AGE",
                    "hash_column_indexes": [],
                    "n_columns": 13,
                    "sql": None
                },
                {
                    "source_table": "D_AGENCY",
                    "hash_column_indexes": [],
                    "n_columns": 13,
                    "sql": None
                },
                {
                    "source_table": "D_BANK",
                    "hash_column_indexes": [],
                    "n_columns": 20,
                    "sql": None
                },
                {
                    "source_table": "D_CAT_SPEC",
                    "hash_column_indexes": [],
                    "n_columns": 11,
                    "sql": None
                },
                {
                    "source_table": "D_COUNTRY",
                    "hash_column_indexes": [],
                    "n_columns": 14,
                    "sql": None
                },
                {
                    "source_table": "D_CUSTOMER",
                    "hash_column_indexes": [],
                    "n_columns": 12,
                    "sql": None
                },
                {
                    "source_table": "D_DEP",
                    "hash_column_indexes": [],
                    "n_columns": 44,
                    "sql": None
                },
                {
                    "source_table": "D_DOC",
                    "hash_column_indexes": [],
                    "n_columns": 15,
                    "sql": None
                },
                {
                    "source_table": "D_EDUCATION",
                    "hash_column_indexes": [],
                    "n_columns": 11,
                    "sql": None
                },
                {
                    "source_table": "D_EDU_HIGH",
                    "hash_column_indexes": [],
                    "n_columns": 11,
                    "sql": None
                },
                {
                    "source_table": "D_EMPLOY_TYPE",
                    "hash_column_indexes": [],
                    "n_columns": 11,
                    "sql": None
                },
                {
                    "source_table": "D_EMP_ATTR",
                    "hash_column_indexes": [],
                    "n_columns": 11,
                    "sql": None
                },
                {
                    "source_table": "D_FOREIGN_CAT",
                    "hash_column_indexes": [],
                    "n_columns": 14,
                    "sql": None
                },
                {
                    "source_table": "D_FREE_SEARCH_IRS",
                    "hash_column_indexes": [],
                    "n_columns": 15,
                    "sql": None
                },
                {
                    "source_table": "D_FREE_SEARCH_IRS_COLS",
                    "hash_column_indexes": [],
                    "n_columns": 19,
                    "sql": None
                },
                {
                    "source_table": "D_F_INDEP_PROF",
                    "hash_column_indexes": [],
                    "n_columns": 13,
                    "sql": None
                },
                {
                    "source_table": "D_F_PERSON_TYPE",
                    "hash_column_indexes": [],
                    "n_columns": 11,
                    "sql": None
                },
                {
                    "source_table": "D_NATION",
                    "hash_column_indexes": [],
                    "n_columns": 13,
                    "sql": None
                },
                {
                    "source_table": "D_NATION_FL",
                    "hash_column_indexes": [],
                    "n_columns": 13,
                    "sql": None
                },
                {
                    "source_table": "D_OKED",
                    "hash_column_indexes": [],
                    "n_columns": 12,
                    "sql": None
                },
                {
                    "source_table": "D_PERIOD_TYPE",
                    "hash_column_indexes": [],
                    "n_columns": 13,
                    "sql": None
                },
                {
                    "source_table": "D_PERMIT_STAT",
                    "hash_column_indexes": [],
                    "n_columns": 11,
                    "sql": None
                },
                {
                    "source_table": "D_PERMIT_TYPE",
                    "hash_column_indexes": [],
                    "n_columns": 11,
                    "sql": None
                },
                {
                    "source_table": "D_POINT",
                    "hash_column_indexes": [],
                    "n_columns": 12,
                    "sql": None
                },
                {
                    "source_table": "D_POST_TYPE",
                    "hash_column_indexes": [],
                    "n_columns": 11,
                    "sql": None
                },
                {
                    "source_table": "D_POST_TYPE1",
                    "hash_column_indexes": [],
                    "n_columns": 11,
                    "sql": None
                },
                {
                    "source_table": "D_PROFESSION",
                    "hash_column_indexes": [],
                    "n_columns": 13,
                    "sql": None
                },
                {
                    "source_table": "D_PROF_ADD",
                    "hash_column_indexes": [],
                    "n_columns": 11,
                    "sql": None
                },
                {
                    "source_table": "D_PROF_CAT",
                    "hash_column_indexes": [],
                    "n_columns": 11,
                    "sql": None
                },
                {
                    "source_table": "D_PROF_TYPE",
                    "hash_column_indexes": [],
                    "n_columns": 11,
                    "sql": None
                },
                {
                    "source_table": "D_PROJECT",
                    "hash_column_indexes": [],
                    "n_columns": 11,
                    "sql": None
                },
                {
                    "source_table": "D_REGION",
                    "hash_column_indexes": [],
                    "n_columns": 18,
                    "sql": None
                },
                {
                    "source_table": "D_REG_STAT",
                    "hash_column_indexes": [],
                    "n_columns": 3,
                    "sql": None
                },
                {
                    "source_table": "D_REQ_AGENCY",
                    "hash_column_indexes": [],
                    "n_columns": 11,
                    "sql": None
                },
                {
                    "source_table": "D_REQ_DECISION",
                    "hash_column_indexes": [],
                    "n_columns": 10,
                    "sql": None
                },
                {
                    "source_table": "D_REQ_REFUSE",
                    "hash_column_indexes": [],
                    "n_columns": 11,
                    "sql": None
                },
                {
                    "source_table": "D_REQ_REISSUE",
                    "hash_column_indexes": [],
                    "n_columns": 11,
                    "sql": None
                },
                {
                    "source_table": "D_REQ_STATUS",
                    "hash_column_indexes": [],
                    "n_columns": 11,
                    "sql": None
                },
                {
                    "source_table": "D_SOC_STATUS",
                    "hash_column_indexes": [],
                    "n_columns": 11,
                    "sql": None
                },
                {
                    "source_table": "EMPLOYER",
                    "hash_column_indexes": [
                        0,
                        1
                    ],
                    "n_columns": 31,
                    "sql": None
                },
                {
                    "source_table": "EMP_ATTR",
                    "hash_column_indexes": [],
                    "n_columns": 21,
                    "sql": None
                },
                {
                    "source_table": "EMP_MANAGE",
                    "hash_column_indexes": [
                        7,
                        8,
                        10,
                        12
                    ],
                    "n_columns": 17,
                    "sql": None
                },
                {
                    "source_table": "F_CARD_AREA",
                    "hash_column_indexes": [],
                    "n_columns": 11,
                    "sql": None
                },
                {
                    "source_table": "F_CARD_REFUSE",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "F_CARD_STAT",
                    "hash_column_indexes": [],
                    "n_columns": 14,
                    "sql": None
                },
                {
                    "source_table": "F_CARD_XML",
                    "hash_column_indexes": [],
                    "n_columns": 24,
                    "sql": None
                },
                {
                    "source_table": "F_EU_LIST",
                    "hash_column_indexes": [],
                    "n_columns": 14,
                    "sql": None
                },
                {
                    "source_table": "F_OKED_CAT",
                    "hash_column_indexes": [],
                    "n_columns": 11,
                    "sql": None
                },
                {
                    "source_table": "F_PAY_TAX",
                    "hash_column_indexes": [
                        21
                    ],
                    "n_columns": 29,
                    "sql": None
                },
                {
                    "source_table": "F_QUOTA",
                    "hash_column_indexes": [],
                    "n_columns": 14,
                    "sql": None
                },
                {
                    "source_table": "F_QUOTA_ORDER",
                    "hash_column_indexes": [],
                    "n_columns": 13,
                    "sql": None
                },
                {
                    "source_table": "F_QUOTA_ORDER_LOG",
                    "hash_column_indexes": [],
                    "n_columns": 14,
                    "sql": None
                },
                {
                    "source_table": "F_REQ",
                    "hash_column_indexes": [],
                    "n_columns": 33,
                    "sql": None
                },
                {
                    "source_table": "F_REQ_AREA",
                    "hash_column_indexes": [],
                    "n_columns": 11,
                    "sql": None
                },
                {
                    "source_table": "F_REQ_CARD",
                    "hash_column_indexes": [],
                    "n_columns": 48,
                    "sql": None
                },
                {
                    "source_table": "F_REQ_CARD_LOG",
                    "hash_column_indexes": [],
                    "n_columns": 49,
                    "sql": None
                },
                {
                    "source_table": "F_REQ_EMP",
                    "hash_column_indexes": [],
                    "n_columns": 12,
                    "sql": None
                },
                {
                    "source_table": "F_REQ_LOG",
                    "hash_column_indexes": [],
                    "n_columns": 32,
                    "sql": None
                },
                {
                    "source_table": "F_REQ_MISSION",
                    "hash_column_indexes": [],
                    "n_columns": 12,
                    "sql": None
                },
                {
                    "source_table": "F_REQ_POINT",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "F_REQ_PROF",
                    "hash_column_indexes": [],
                    "n_columns": 13,
                    "sql": None
                },
                {
                    "source_table": "F_REQ_PROJ",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "F_REQ_REFUSE",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "F_SPEC_COND",
                    "hash_column_indexes": [],
                    "n_columns": 20,
                    "sql": None
                },
                {
                    "source_table": "F_SPLIT_CARD",
                    "hash_column_indexes": [],
                    "n_columns": 3,
                    "sql": None
                },
                {
                    "source_table": "GBDFL_PERS_KATO",
                    "hash_column_indexes": [
                        3
                    ],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "PA_CARD",
                    "hash_column_indexes": [
                        6,
                        25,
                        26,
                        30,
                        34,
                        36
                    ],
                    "n_columns": 44,
                    "sql": None
                },
                {
                    "source_table": "WS_F_CARD_LOG",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "WS_F_REQ_LOG",
                    "hash_column_indexes": [],
                    "n_columns": 20,
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