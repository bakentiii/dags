import datetime
import hashlib
from typing import Union, List, Tuple, Optional

import psycopg2
import mysql.connector
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models.connection import Connection
from airflow.models.baseoperator import chain
from airflow.models import Variable
from airflow.hooks.base_hook import BaseHook

def get_source_conn() -> any:
    """
    Geting connection creadentials from airflow secrets and performing connection with database

    Returns:
        connection with database
    """
    try:
        conn_AIS_secrets=Connection.get_connection_from_secrets(conn_id="MSH_VET")#скрытый коннекшн источника
        
        return mysql.connector.connect(database = conn_AIS_secrets.schema, user =conn_AIS_secrets.login, password =conn_AIS_secrets.password, host = conn_AIS_secrets.host, port = conn_AIS_secrets.port)
    except Exception as ex:
        raise ex


def TEST():
    sql = """SELECT code,
updated_at,
dscr,
title,
service_id,
created_by,
id,
module_id,
created_at,
is_active
 FROM msh_prod.roles"""

    conn_source = get_source_conn()


    try:
            with conn_source.cursor() as cursor_source:

                cursor_source.execute(sql)

                data = cursor_source.fetchmany(1000)

                print(data)

    except Exception as ex:
        raise ex
    finally:
        conn_source.close()

with DAG("test_mysql", description="test MySQL", start_date=datetime.datetime(2023, 8, 2), schedule_interval=None, catchup=False) as dag:

    TEST = PythonOperator(
        owner='Bakhtiyar',
        task_id='TEST',
        python_callable=TEST,
)