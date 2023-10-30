from datetime import datetime
import time
from airflow import DAG
from clickhouse_driver import Client, connect
from airflow.operators.python import PythonOperator
from airflow.models.connection import Connection
import hashlib

def hash(data):
	hash_data = f"{str(data).lower()}".encode("utf-8")
	res = hashlib.sha1(hash_data).hexdigest()
	return res.upper()

def select_test_click():
	def make_list_of_table():
		cursor.execute(f"""SELECT distinct name FROM `system`.tables T WHERE database = '{schema_name_sdu}' AND name != 'CHECK_LOAD'""")
		tuple_of_tables = cursor.fetchall()
		list_of_tables = [table[0] for table in tuple_of_tables]
		return list_of_tables


	conn_sbd_sec = Connection.get_connection_from_secrets(conn_id="Clickhouse-5")
	conn_sbd = connect(host=conn_sbd_sec.host, port=conn_sbd_sec.port, password=conn_sbd_sec.password, user=conn_sbd_sec.login, connect_timeout=3600)
	cursor = conn_sbd.cursor()

	conn_shd_sec=Connection.get_connection_from_secrets(conn_id="Clickhouse-17")
	conn_shd=connect(host=conn_shd_sec.host, port=conn_shd_sec.port, password=conn_shd_sec.password, user=conn_shd_sec.login, connect_timeout=3600)
	cursor_shd = conn_shd.cursor()

	cursor.execute(f"""SELECT * FROM DICTIONARY.ISS""")
	tuple_of_schema = cursor.fetchall()
	list_of_schema = [table for table in tuple_of_schema]

	for shema_name in list_of_schema:
		author = 'Бахтияр'
		go_name_short = shema_name[1]
		is_name_short = shema_name[2]
		schema_name_sdu = shema_name[0]
		list_of_tables = make_list_of_table()
		type_of_etl = 'truncate'
		is_case_related = int(shema_name[3])
		remark = shema_name[4]
		list_to_load = []

		cursor_shd.execute(f"ALTER TABLE monitor.IS_UPDATE_DEMO DELETE where schema_name_sdu = '{schema_name_sdu}' and type_of_etl = 'truncate'")
		time.sleep(20)
		for table_name_sdu in list_of_tables:
			cursor.execute(f"SELECT max(SDU_LOAD_IN_DT) from {schema_name_sdu}.{table_name_sdu}")
			schema_table_name_sdu = f'{schema_name_sdu}.{table_name_sdu}'
			schema_table_name_hash = hash(f'{schema_name_sdu}.{table_name_sdu}')
			last_upload = cursor.fetchone()[0]
			list_to_load.append((author, go_name_short, is_name_short, schema_table_name_hash, schema_table_name_sdu, schema_name_sdu, table_name_sdu, last_upload, type_of_etl,is_case_related,remark))	
		cursor_shd.executemany("INSERT INTO monitor.IS_UPDATE_DEMO (author, go_name_short, is_name_short, schema_table_name_hash, schema_table_name_sdu, schema_name_sdu, table_name_sdu, last_upload, type_of_etl,is_case_related,remark) VALUES", list_to_load)

	conn_sbd.close()
	conn_shd.close()

with DAG("MNE_MONITOR", description="for common table", start_date=datetime(2022, 6, 3), schedule_interval=None, catchup=False) as dag:
		
	select_test_click = PythonOperator(
		owner='Bakhtiyar',
		task_id='select_test_click',
		python_callable=select_test_click,
	)