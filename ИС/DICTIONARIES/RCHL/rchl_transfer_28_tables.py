import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models.connection import Connection
from airflow.models.baseoperator import chain
from airflow.models import Variable
from clickhouse_driver import Client, connect


def get_17_connection():
	try:
		conn_17_secrets = Connection.get_connection_from_secrets(conn_id="Clickhouse-17")##скрытй коннекшн источника    
		return connect(host=conn_17_secrets.host, port=conn_17_secrets.port, password=conn_17_secrets.password, user=conn_17_secrets.login, connect_timeout=3600)
	except Exception as ex:
		print(f'Error connection of 17 server - {ex}')

def insert_values(table):
	local_conn = get_17_connection()

	try:
		#sql = f"""INSERT INTO "{schema}".{table.lower()} VALUES({', '.join(['%s' for _ in range(n_columns)])});"""
		sql = f"""INSERT INTO RCHL.{table} VALUES""" 

		with local_conn.cursor() as cursor:
			cursor.executemany(sql,data)
			print(f'--> Inserted {len(data)} rows to RCHL.{table}')
	finally:
		local_conn.close()

def truncate_table(table):
	local_conn = get_17_connection()

	try:
		sql = f"""TRUNCATE TABLE RCHL.{table};"""
		with local_conn.cursor() as cursor:
			cursor.execute(sql)
		local_conn.commit()
		print(f'--> Truncated table {table} in RCHL schema')
	finally:
		local_conn.close()
		print('Truncate exiting')

def extract_and_load_data(target_table):

	conn_17 = get_17_connection()

	try:
		with conn_17.cursor() as cursor:
			
			data = cursor.execute(f"""SELECT * FROM CKB_TEST.{source_table};""")

			truncate_table(target_table)

            print('fetching')
            data = cursor.fetchall()
            print('fetched')
            print(f'--> {len(data)} rows fetched')

            insert_values(target_table)
	finally:
		conn_17.close()

def main():
	
    TABLES = ['KNB_KVT_kontrol_ff',
'KREM_monopolii_ff',
'KREM_rynki_ff',
'KTRM_akkred_ff',
'KTRM_drag_ff',
'KTRM_metro_ff',
'KTRM_tehreg_ff',
'MCHS_biotoplivo_ff',
'MCHS_oborona_ff',
'MCHS_pozhar_ff',
'MCHS_prom_bezopasnost_ff',
'MIOR_smi_ff',
'MIOR_teleradio_ff',
'MKS_archive_ff',
'MKS_history_ff',
'MKS_igornyi_biznes_ff',
'MKS_lottery_ff',
'MKS_tourism_ff',
'MNE_reklama_ff',
'MP_obrazovanie_ff',
'MP_prava_rebenka_ff',
'MSH_ohrana_zemel_ff',
'MSH_rasteniya_ff',
'MSH_vet_ff',
'MSH_zerno_ff',
'MVD_ohrana_ff',
'MZ_farmkontrol_ff',
'MZ_medkontrol_ff']

    for table in TABLES:
        extract_and_load_data(table)
        
with DAG("Transfer_dictionaries",default_args= {'owner': 'Bakhtiyar'}, description="Load table",start_date=datetime.datetime(2023, 6, 19),schedule_interval=None,catchup=False, tags = ['rchl', 'ckb_test']) as dag:

	task_1 = PythonOperator(
        task_id = 'ALL_DATA',
        python_callable=main
    )
