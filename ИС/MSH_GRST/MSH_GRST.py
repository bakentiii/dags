from datetime import datetime
from airflow import DAG
from clickhouse_driver import Client, connect
from airflow.operators.python import PythonOperator
from airflow.models.connection import Connection
from airflow.models import Variable
import hashlib
import psycopg2# postgres conn

def hash(data):##фунуция для хэша (hash_name объявляется внизу)
	if data == None:
		hash_data = f"{hash_name}".encode("utf-8")
	else:
		hash_data = f"{str(data).lower()}{hash_name}".encode("utf-8")
	res = hashlib.sha256(hash_data).hexdigest()
	return res.upper()

def hash_columns(data, indexes):
	tmp = list(data)
	for index in indexes:
		tmp[index] = hash(data[index])
	return tuple(tmp)



def GRST_AGENT():
	#cursor_POSTGRES_SBD.execute("TRUNCATE TABLE MNE_BUSINESSREGISTER.CHECK_LOAD")

	cursor_GRST.execute(f"""
    SELECT id, "name", ibin, first_name, last_name, patronymic, birth_date, register_date, certificate_number, is_issued_by_document, document_number, document_issue_date, businessman, okpo, checking_account, head, head_position, "time_stamp", location_te, region, address, street, house, apartment, work_phone, mobile_phone, email, agent_to_country, agent_to_agent_type, agent_to_street_type, agent_to_document_issuer, agent_to_document_kind, agent_to_enterprise_type, agent_to_opf
FROM public.grst_agent;
""")

	flag = True
	while flag:
		GRST_DATA = cursor_GRST.fetchmany(10000)
        GRST_DATA = [*map(lambda x: hash_columns(x, indexes=[2,3,4]), GRST_DATA)]
		if len(GRST_DATA) == 0:
			flag=False
		cursor_POSTGRES_SBD.executemany("""INSERT INTO "MSH_GRST".GRST_AGENT (
id,
name,
ibin,
first_name,
last_name,
patronymic,
birth_date,
register_date,
certificate_number,
is_issued_by_document,
document_number,
document_issue_date,
businessman,
okpo,
checking_account,
head,
head_position,
time_stamp,
location_te,
region,
address,
street,
house,
apartment,
work_phone,
mobile_phone,
email,
agent_to_country,
agent_to_agent_type,
agent_to_street_type,
agent_to_document_issuer,
agent_to_document_kind,
agent_to_enterprise_type,
agent_to_opf
        ) VALUES""", GRST_DATA)

def GRST_MACHINERY():
	#cursor_POSTGRES_SBD.execute("TRUNCATE TABLE MNE_BUSINESSREGISTER.CHECK_LOAD")

	cursor_GRST.execute(f"""
    SELECT id, machinery_to_agent, machinery_owner_agent, tp_series, register_date, location_te, factory_number, engine_number, graduation_year, pledge, plant_manufacturer, unreg, arrest, marks, "time_stamp", national_duty, num_receipt, date_of_payment, machinery_to_model, machinery_to_user, gn_national_duty, gn_num_receipt, gn_date_of_payment, gn_number, tp_number, tp_national_duty, tp_num_receipt, tp_date_of_payment, ur_date, ur_location_te, ur_comment, ur_time_stamp, machinery_to_view, machinery_to_speciality, machinery_to_type, machinery_to_cause, machinery_ur_cause, dealer_company, reg_primary
FROM public.grst_machinery;
""")

	flag = True
	while flag:
		GRST_DATA = cursor_GRST.fetchmany(10000)
		if len(GRST_DATA) == 0:
			flag=False
		cursor_POSTGRES_SBD.executemany("""INSERT INTO "MSH_GRST".GRST_MACHINERY (
id,
machinery_to_agent,
machinery_owner_agent,
tp_series,
register_date,
location_te,
factory_number,
engine_number,
graduation_year,
pledge,
plant_manufacturer,
unreg,
arrest,
marks,
time_stamp,
national_duty,
num_receipt,
date_of_payment,
machinery_to_model,
machinery_to_user,
gn_national_duty,
gn_num_receipt,
gn_date_of_payment,
gn_number,
tp_number,
tp_national_duty,
tp_num_receipt,
tp_date_of_payment,
ur_date,
ur_location_te,
ur_comment,
ur_time_stamp,
machinery_to_view,
machinery_to_speciality,
machinery_to_type,
machinery_to_cause,
machinery_ur_cause,
dealer_company,
reg_primary
        ) VALUES""", GRST_DATA)
        
with DAG("MSH_GRST",description="Load table",start_date=datetime(2022, 12, 22),schedule_interval=None,catchup=False) as dag:

    try:
	    conn_GRST_secrets = Connection.get_connection_from_secrets(conn_id="MSH_GRST")##скрытй коннекшн источника    
        cnxn = psycopg2.connect(
            dbname=conn_GRST_secrets.schema, 
            user=conn_GRST_secrets.login,
            password=conn_GRST_secrets.password, 
            host=conn_GRST_secrets.host, 
            port=conn_GRST_secrets.port)
    except Exception as except:
        print('Error connection of MSH_GRST')
    
    try:
        conn_postgresSDU_secret = Connection.get_connection_from_secrets(conn_id="local_postgres_ch_backend")#скрытый коннекшн нашего postgres
        cnxn_postgres = psycopg2.connect(
            dbname=conn_postgresSDU_secret.schema,
            user=conn_postgresSDU_secret.login,
            password=conn_postgresSDU_secret.password,
            host=conn_postgresSDU_secret.host,
            port=conn_postgresSDU_secret.port)
    except Exception as except:
        print('Error connection of SBD_POSTGRES')


	cursor_POSTGRES_SBD = cnxn_postgres.cursor()
	cursor_GRST = cnxn.cursor()
	hash_name = Variable.get("hash_password")##secret hash

	GRST_AGENT = PythonOperator(
		owner='Bakhtiyar',
 		task_id='GRST_AGENT',
		python_callable=GRST_AGENT,
	)
	GRST_MACHINERY = PythonOperator(
		owner='Bakhtiyar',
 		task_id='GRST_MACHINERY',
		python_callable=GRST_MACHINERY,
	)

	GRST_AGENT>>GRST_MACHINERY