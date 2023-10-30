from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models.connection import Connection
from airflow.models.baseoperator import chain
import psycopg2# postgres conn
import datetime
import requests
import json


def token_generator(login,password):
    try:
        payload = json.dumps({
                "login": login,
                "password": password
            })
        headers = {
            'Content-Type': 'application/json'
        }
        response = requests.request("POST", url="https://sur.plem.kz/api/Auth/GetToken", headers=headers, data=payload, verify=False)
        return response.json()['Jwt']
    
    except Exception as ex:
        print(f'Error connection to RSZH server - {ex}')


def get_local_postgres_connection():
	try:
		conn_postgresSDU_secret = Connection.get_connection_from_secrets(conn_id="local_postgres_ch_backend")#скрытый коннекшн нашего postgres
		return psycopg2.connect(
			dbname=conn_postgresSDU_secret.schema,
			user=conn_postgresSDU_secret.login,
			password=conn_postgresSDU_secret.password,
			host=conn_postgresSDU_secret.host,
			port=conn_postgresSDU_secret.port)
	except Exception as ex:
		print(f'Error connection of SBD_POSTGRES - {ex}')


def insert_values(data, schema, table, n_columns):
	local_conn = get_local_postgres_connection()

	try:
		#sql = f"""INSERT INTO "{schema}".{table.lower()} VALUES({', '.join(['%s' for _ in range(n_columns)])});"""
		sql = f"""INSERT INTO "{schema}".{table.lower()} VALUES(""" + "%s" + ",%s" * (n_columns-1) + ");" 

		with local_conn.cursor() as cursor:
			cursor.executemany(sql,data)
			print(f'--> Inserted {len(data)} rows to {schema}.{table}')
		local_conn.commit()
	finally:
		local_conn.close()


def extract_and_load(login,pwd,link,target_schema,target_table, n_columns, column_name):
    SDU_LOAD_IN_DT = datetime.datetime.now() + datetime.timedelta(hours=6)

    token = token_generator(login,pwd)

    payload = {}
    headers = {
    'Authorization': f'Bearer {token}'
    }

    response = requests.request("GET", link, headers=headers, data=payload, verify = False)

    data = response.json()['Data'][[[column_name]]].values

    data = [[*list(x),SDU_LOAD_IN_DT] for x in data]

    insert_values(data,target_schema,target_table,n_columns)


def main(*args, **kwargs):
    extract_and_load(
        target_schema=kwargs['target_schema'],
        target_table=kwargs['target_table'],
        link=kwargs['link'],
        login=kwargs['login'],
        pwd=kwargs['password'],
        n_columns=kwargs['n_columns'],
        column_name=kwargs['column_names'],
        BATCH_SIZE=10000)



with DAG(
    dag_id='MSH_RSZH',
    default_args={'owner':'Bakhtiyar'},
    start_date=datetime.datetime(2023, 9, 26), 
    schedule_interval = '0 0 * * *',
    catchup = False,
    tags=['msh']
) as dag:

    URL = {
        '1' : {'table' : 'REGISTER_OF_SUBJECTS', 'n_columns' : 6, 'column_names' : "'Xin', 'Name', 'Code','Enterprisetype','Address'", 'url' : 'https://sur.plem.kz/api/data/getInfo?requestedInfoType=Subjects'}
        # 'https://sur.plem.kz/api/data/getInfo?requestedInfoType=Evaluations' : {'table' : 'BREEDING_CERTIFICATES', 'n_columns' : 17},
        # 'https://sur.plem.kz/api/data/getInfo?requestedInfoType=Certificates' : {'table' : 'ASSESSMENT_OF_BREEDING_ANIMALS', 'n_columns' : 11}
    }


    TARGET_SCHEMA = 'MSH_RSZH'

    conn_17_secrets = Connection.get_connection_from_secrets(conn_id="MSH_RSZH")##скрытй коннекшн источника

    LOGIN = conn_17_secrets.login
    PWD = conn_17_secrets.password

    tasks = []

    for key, value in URL.items():
        
        params = {
            'target_schema': TARGET_SCHEMA,
            'target_table': value['table'],
            'link': value['url'],
            'login' : LOGIN,
            'password' : PWD,
            'n_columns' : value['n_columns'],
            'column_names' : value['column_names']
        }

        task_ = PythonOperator(
            task_id=f"{TARGET_SCHEMA}.{key}",
            trigger_rule='all_done',
            python_callable=main,
            op_kwargs=params
        )

        tasks.append(task_)

    chain(*tasks)