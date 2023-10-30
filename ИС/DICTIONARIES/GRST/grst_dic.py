import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models.connection import Connection
from airflow.models.baseoperator import chain
from airflow.models import Variable
import hashlib
import psycopg2# postgres conn

def hash(data):##фунkция для хэша (hash_name объявляется внизу)
	hash_name = Variable.get("hash_password")
	if data == None:
		return None
	else:
		hash_data = f"{str(data).lower()}{hash_name}".encode("utf-8")
	res = hashlib.sha256(hash_data).hexdigest()
	return res.upper()

def hash_columns(data, indexes):
	tmp = list(data)
	for index in indexes:
		tmp[index] = hash(data[index])
	return tuple(tmp)

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

def get_grst_connection():
	try:
		conn_GRST_secrets = Connection.get_connection_from_secrets(conn_id="MSH_GRST")##скрытй коннекшн источника    
		return psycopg2.connect(
			dbname=conn_GRST_secrets.schema, 
			user=conn_GRST_secrets.login,
			password=conn_GRST_secrets.password, 
			host=conn_GRST_secrets.host, 
			port=conn_GRST_secrets.port)
	except Exception as ex:
		print(f'Error connection of MSH_GRST - {ex}')

def insert_values(data, n_columns):
	local_conn = get_local_postgres_connection()

	try:
		#sql = f"""INSERT INTO "{schema}".{table.lower()} VALUES({', '.join(['%s' for _ in range(n_columns)])});"""
		sql = f"""INSERT INTO "MSH_GRST".grst_dic VALUES(""" + "%s" + ",%s" * (n_columns-1) + ");" 

		with local_conn.cursor() as cursor:
			cursor.executemany(sql,data)
			#print(f'--> Inserted {len(data)} rows to {schema}.{table}')
		local_conn.commit()
	finally:
		local_conn.close()

def truncate_table():
	local_conn = get_local_postgres_connection()

	try:
		sql = f"""TRUNCATE TABLE "MSH_GRST".grst_dic;"""
		with local_conn.cursor() as cursor:
			cursor.execute(sql)
		local_conn.commit()
		#print(f'--> Truncated table {table} in {schema} schema')
	finally:
		local_conn.close()
		print('Truncate exiting')

def extract_and_load_data(load_date,n_columns, hash_indexes,sql):

	grst_conn = get_grst_connection()

	try:
		with grst_conn.cursor() as cursor:
			
			data = cursor.execute(sql)

			truncate_table()

			while True:
				print('fetching')
				data = cursor.fetchmany(50000)
				print('fetched')
				data = [[*list(x),load_date] for x in data]
				data = [*map(lambda x: hash_columns(x, indexes=hash_indexes), data)]
				print(f'--> {len(data)} rows fetched')

				if not len(data):
					print(f'--> break')
					break

				insert_values(data,n_columns)
	finally:
		grst_conn.close()

def main(*args, **kwargs):
	SDU_LOAD_IN_DT = datetime.datetime.now() + datetime.timedelta(hours=6)

	extract_and_load_data(
		load_date = SDU_LOAD_IN_DT,
		n_columns = kwargs['n_columns'],
		hash_indexes = kwargs['hash_indexes'],
        sql = kwargs['sql']

	)
        
with DAG("MSH_GRST_DIC",default_args= {'owner': 'Bakhtiyar'}, description="Load table",start_date=datetime.datetime(2023, 6, 20),schedule_interval=None,catchup=False, tags = ['grst', 'msh']) as dag:

	TABLES = {
		'GRST_AGENT' : {
            'sql' : '''select 
                ibin,
                a."name",
                location_te,
                go2.name_ru,
                gk.name_ru,
                concat(first_name,last_name,patronymic) as FIO,
                register_date
                from public.grst_agent a
                inner join public.grst_kato gk on a.location_te = gk.code
                left join public.grst_opf go2 on a.agent_to_opf = go2.id''',

			'columns' : 8,
			'hash_indexes' : [0,5]
		}
	}



	for key, value in TABLES.items():

		params = {
			'n_columns' : value['columns'],
			'hash_indexes' : value['hash_indexes'],
			'sql' : value['sql']
		}

		task_ = PythonOperator(
            task_id=f"GRST_DIC",
            trigger_rule='all_done',
            python_callable=main,
            op_kwargs=params
        )