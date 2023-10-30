from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models.connection import Connection
from airflow.models import Variable
import psycopg2# postgres conn


def PERMISSION():
	cursor_POSTGRES_SBD.execute('TRUNCATE TABLE "MIIR_KAZREESTR"."PERMISSIONS/GET"')
	cnxn_postgres.commit()

	cursor_KZRSTR.execute('''select 
  t1.id as id, 
  t1.number as number, 
  t1.date as date, 
  t1.akimat_decree as akimat_decree, 
  t1.commissioning_date as commissioning_date, 
  t1_1.id as object_id, 
  t1_1.object_name as object_name, 
  t1_2.id as builder_id, 
  t1_2."name" as builder_name, 
  t1_3.id as auth_org_id, 
  t1_3."name" as auth_org_name, 
  t1_4.id as mio_id, 
  t1_4."name" as mio_name, 
  t1_5.name_ru as method_du,
  t1.time_modif as "update_date"
from rddu.permission t1 
    left join rddu.builder_objects t1_1 on t1.id=t1_1.permission_id 
    left join rddu.person t1_2 on t1_1.builder_org_id=t1_2.id 
    left join rddu.person t1_3 on t1_1.auth_org_id=t1_3.id 
    left join rddu.person t1_4 on t1_1.mio_org_id=t1_4.id 
    left join rddu.organization_method_du t1_5 on t1.organization_method_du_id=t1_5.id
order by t1.id;''')

	flag = True
	while flag:
		KZRSTR_DATA = cursor_KZRSTR.fetchmany(10000)
		if len(KZRSTR_DATA) == 0:
			flag=False
		kzrstr = []
		for i in range(len(KZRSTR_DATA)):
			kzrstr.append(KZRSTR_DATA[i]) 
		cursor_POSTGRES_SBD.executemany(f"""INSERT INTO "MIIR_KAZREESTR"."PERMISSIONS/GET" values ({', '.join(['%s' for _ in range(34)])})""",kzrstr)
		cnxn_postgres.commit()

        
with DAG("KAZREESTR",description="Load table",start_date=datetime(2023, 3, 9),schedule_interval=None,catchup=False) as dag:

	try:
		conn_KAZREESTR_secrets = Connection.get_connection_from_secrets(conn_id="miir_kazresstr_sdu_read")##скрытй коннекшн источника    
		cnxn = psycopg2.connect(
			dbname=conn_KAZREESTR_secrets.schema, 
			user=conn_KAZREESTR_secrets.login,
			password=conn_KAZREESTR_secrets.password, 
			host=conn_KAZREESTR_secrets.host, 
			port=conn_KAZREESTR_secrets.port)
	except Exception as ex:
		print(f'Error connection of KAZREESTR - {ex}')
    
	try:
		conn_postgresSDU_secret = Connection.get_connection_from_secrets(conn_id="local_postgres_ch_backend")#скрытый коннекшн нашего postgres
		cnxn_postgres = psycopg2.connect(
			dbname=conn_postgresSDU_secret.schema,
			user=conn_postgresSDU_secret.login,
			password=conn_postgresSDU_secret.password,
			host=conn_postgresSDU_secret.host,
			port=conn_postgresSDU_secret.port)
	except Exception as ex:
		print(f'Error connection of SBD_POSTGRES - {ex}')


	cursor_POSTGRES_SBD = cnxn_postgres.cursor()
	cursor_KZRSTR = cnxn.cursor()

	PERMISSION = PythonOperator(
		owner='Bakhtiyar',
 		task_id='PERMISSION',
		python_callable=PERMISSION,
	)