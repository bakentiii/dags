import requests
import json
from datetime import datetime, timedelta

import pandas as pd
import psycopg2
import hashlib

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models.connection import Connection
from airflow.models import Variable


def get_jwt_token():
    url = "https://sur.plem.kz/api/Auth/GetToken"

    payload = json.dumps({
    "login": "SUR",
    "password": "Sur2023!@zxc"
    })
    headers = {
    'Content-Type': 'application/json'
    }

    response = requests.request("POST", url, headers=headers, data=payload)

    print(response.text)

with DAG(
    "RSZH",
    start_date=datetime(2023, 9, 25),
    schedule_interval=None,
    tags=["msh_rszh"],
    catchup=False
) as dag:

    token = PythonOperator(
        owner='Bakhtiyar',
        task_id='GET_TOKEN',
        trigger_rule='all_done',
        python_callable=get_jwt_token,
    )