import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models.connection import Connection
from airflow.models.baseoperator import chain
from airflow.models import Variable
import psycopg2
from clickhouse_driver import Client, connect

def get_click_5_conn():
	try:
		conn_5_secrets = Connection.get_connection_from_secrets(conn_id="Clickhouse-5")##скрытй коннекшн источника    
		return connect(host=conn_5_secrets.host, port=conn_5_secrets.port, password=conn_5_secrets.password, user=conn_5_secrets.login, connect_timeout=3600)
	except Exception as ex:
		print(f'Error connection of 5 server - {ex}')

def get_loacl_postgres_connection():
	try:
		conn_GRST_secrets = Connection.get_connection_from_secrets(conn_id="postgres_114")##скрытй коннекшн источника    
		return psycopg2.connect(
			dbname=conn_GRST_secrets.schema, 
			user=conn_GRST_secrets.login,
			password=conn_GRST_secrets.password, 
			host=conn_GRST_secrets.host, 
			port=conn_GRST_secrets.port)
	except Exception as ex:
		print(f'Error connection of MSH_GRST - {ex}')

def insert_values(data, schema, table, n_columns):
	local_conn = get_click_5_conn()

	try:
		sql = f"INSERT INTO {schema}.{table} VALUES" 

		with local_conn.cursor() as cursor:
			cursor.executemany(sql,data)
			print(f'--> Inserted {len(data)} rows to {schema}.{table}')
	finally:
		local_conn.close()

def truncate_table(schema, table):
	local_conn = get_click_5_conn()

	try:
		sql = f"""TRUNCATE TABLE {schema}.{table};"""
		with local_conn.cursor() as cursor:
			cursor.execute(sql)
		print(f'--> Truncated table {table} in {schema} schema')
	finally:
		local_conn.close()
		print('Truncate exiting')

def to_string(row, index):
	row[index] = json.dumps(row[index])
	return row

def extract_and_load_data(tagret_schema,target_table,n_columns):

	postgres_conn = get_loacl_postgres_connection()

	try:
		with postgres_conn.cursor() as cursor:
			
			data = cursor.execute(f"""SELECT * FROM "MSH_GRST"."{target_table.lower()}";""")


			truncate_table(tagret_schema, target_table)

			while True:
				print('fetching')
				data = cursor.fetchmany(50000)
				print('fetched')
				data = [[*list(x)] for x in data]
				
                print(f'--> {len(data)} rows fetched')
				

				if not len(data):
					print(f'--> break')
					break

				insert_values(data,tagret_schema,target_table,n_columns)
	finally:
		postgres_conn.close()

def main(*args, **kwargs):

	extract_and_load_data(
		tagret_schema = kwargs['target_schema'],
		target_table = kwargs['target_table'],
		n_columns = kwargs['n_columns']

	)
        
with DAG("MSH_GRST",default_args= {'owner': 'Bakhtiyar'}, description="Load table",start_date=datetime.datetime(2023, 8, 18),schedule_interval='0 6 * * *',catchup=False, tags = ['grst', 'msh']) as dag:

	TABLES = {    
		'GRST_AGENT' : {
			'columns' : 35			
		},
		'GRST_MACHINERY' : {
			'columns' : 48			
		},
		'GRST_ACCORDANCE_DOC' : {
			'columns' : 9			
		},
		'GRST_AGENT_TYPE' : {
			'columns' : 9			
		},
		'GRST_ARREST' : {
			'columns' : 13			
		},
		'GRST_CAUSE' : {
			'columns' : 6			
		},
		'GRST_COUNTRY' : {
			'columns' : 11			
		},
		'GRST_CURRENCY' : {
			'columns' : 3			
		},
		'GRST_DIVISION' : {
			'columns' : 8			
		},
		'GRST_DOCUMENT_ISSUER' : {
			'columns' : 3			
		},
		'GRST_DOCUMENT_KIND' : {
			'columns' : 5			
		},
		'GRST_ENTERPRISE_TYPE' : {
			'columns' : 5			
		},
		'GRST_EVENTS_SEP' : {
			'columns' : 72			
		},
		'GRST_FILE' : {
			'columns' : 4			
		},
		'GRST_GOVERNMENT_NUMBER_HISTORY' : {
			'columns' : 7			
		},
		'GRST_HISTORY' : {
			'columns' : 6			
		},
		'GRST_INFO' : {
			'columns' : 6			
		},
		'GRST_INSPECTION' : {
			'columns' : 8			
		},
		'GRST_KATO' : {
			'columns' : 10			
		},
		'GRST_MACHINERY_INFO_ADJUSTMENT_REQUEST' : {
			'columns' : 20			
		},
		'GRST_MARK' : {
			'columns' : 3			
		},
		'GRST_MESSAGE' : {
			'columns' : 5			
		},
		'GRST_MODEL' : {
			'columns' : 11			
		},
		'GRST_MOVE' : {
			'columns' : 3			
		},
		'GRST_OLD' : {
			'columns' : 3			
		},
		'GRST_OPF' : {
			'columns' : 6			
		},
		'GRST_PLANT' : {
			'columns' : 7			
		},
		'GRST_PLEDGE' : {
			'columns' : 40			
		},
		'GRST_POSITION' : {
			'columns' : 5			
		},
		'GRST_PRIVILEGE' : {
			'columns' : 6			
		},
		'GRST_REGION' : {
			'columns' : 6			
		},
		'GRST_REPLACEMENT_UNIT' : {
			'columns' : 25			
		},
		'GRST_REPORT_LINK' : {
			'columns' : 3			
		},
		'GRST_REPORT_STATUS' : {
			'columns' : 11			
		},
		'GRST_ROLE' : {
			'columns' : 7			
		},
		'GRST_ROLE_PRIVILEGE' : {
			'columns' : 4			
		},
		'GRST_SPECIALITY' : {
			'columns' : 5			
		},
		'GRST_STREET_TYPE' : {
			'columns' : 5			
		},
		'GRST_TEMPLATE' : {
			'columns' : 6			
		},
		'GRST_TMP_UUID_ARRAY' : {
			'columns' : 4			
		},
		'GRST_TRACTOR_DRIVER' : {
			'columns' : 20			
		},
		'GRST_TYPE' : {
			'columns' : 5			
		},
		'GRST_UNIT' : {
			'columns' : 5			
		},
		'GRST_USER' : {
			'columns' : 23			
		},
		'GRST_VIEW' : {
			'columns' : 5			
		},
		'KNEW' : {
			'columns' : 8			
		},
		'KOLD' : {
			'columns' : 2			
		},
		'KR' : {
			'columns' : 3			
		},
		'KR2' : {
			'columns' : 3			
		}
	}

	TARGET_SCHEMA = 'MSH_GRST'

	tasks = []

	for key, value in TABLES.items():

		params = {
			'target_schema'  : TARGET_SCHEMA,
			'target_table' : key,
			'N_COLUMNS' : value['columns']			
			'truncate' : True
		}

		task_ = PythonOperator(
            task_id=f"{TARGET_SCHEMA}.{key}",
            trigger_rule='all_done',
            python_callable=main,
            op_kwargs=params
        )

		tasks.append(task_)
	chain(*tasks)