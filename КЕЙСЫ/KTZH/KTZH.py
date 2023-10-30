from datetime import datetime
from airflow import DAG
from clickhouse_driver import Client, connect
from airflow.operators.python import PythonOperator
from airflow.models.connection import Connection
from airflow.models import Variable

def KTZH_CASE():
    conn_shd_sec=Connection.get_connection_from_secrets(conn_id="Clickhouse-17")
	conn_shd=connect(host=conn_shd_sec.host, port=conn_shd_sec.port, password=conn_shd_sec.password, user=conn_shd_sec.login, connect_timeout=3600)
	cursor_shd = conn_shd.cursor()

	conn_6_sec = Connection.get_connection_from_secrets(conn_id="Clickhouse_52.6")
	conn_6 = connect(host=conn_6_sec.host,port=conn_6_sec.port,user=conn_6_sec.login, password = conn_6_sec.password, connect_timeout = 3600)
	cursor_6 = conn_6.cursor()

	cursor_6.execute("TRUNCATE TABLE RF_CASE.KTZH_EXPORT_2021")
	cursor_6.execute("TRUNCATE TABLE RF_CASE.KTZH_EXPORT_2022")
	cursor_6.execute("TRUNCATE TABLE RF_CASE.KTZH_IMPORT_2021")
	cursor_6.execute("TRUNCATE TABLE RF_CASE.KTZH_IMPORT_2022")
	cursor_6.execute("TRUNCATE TABLE RF_CASE.KTZH_EXPORT_ALL")
	cursor_6.execute("TRUNCATE TABLE RF_CASE.KTZH_EXPORT_ALL")
	cursor_6.execute("TRUNCATE TABLE RF_CASE.KTZH_ALL")
	
	cursor_shd.execute("SELECT * from RF_CASE.KTZH_EXPORT_2021")
	flag = True
	while flag:
		KTZH = cursor_shd.fetchmany(10000)
		if len(KTZH) == 0:
			flag=False
		cursor_6.executemany("""INSERT INTO RF_CASE.KTZH_EXPORT_2021 (SENDING_NUM,ACCEPT_DATE,DISCRETING_DATE,SENDING_STATION_CODE,SENDING_STATION_NAME,DEST_STATION_CODE,
DEST_STATION_NAME,
SENDING_COUNTRY,
DEST_COUNTRY,
GNG_CODE,
GNG_NAME,
ETSNG_CODE,
ETSNG_NAME,
SENDER_CODE,
SENDER_NAME,
RECEIVER_CODE,
RECEIVER_NAME,
CONTAINER_NUM,
CARRIAGE_NUM,
WEIGHT,
CARGO_TYPE) VALUES""", KTZH)

	conn_6.close()
	conn_shd.close()

with DAG("KTZH_ALL", description="Case transfer from 17 to 2", start_date=datetime(2022, 10, 26), schedule_interval='30 0 * * 0', catchup=False) as dag:

	KTZH_CASE = PythonOperator(
		owner='Bakhtiyar',
 		task_id='KTZH_CASE',
		python_callable=KTZH_CASE,
	)