from datetime import datetime
from airflow import DAG
from clickhouse_driver import Client, connect
from airflow.operators.python import PythonOperator
from airflow.models.connection import Connection
from airflow.models import Variable
import hashlib
import pymssql

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

def C_BUSINESSKIND():
	cursor_CH_SBD.execute("TRUNCATE TABLE MNE_BUSINESSREGISTER.CHECK_LOAD")
	cursor_CH_SBD.execute("TRUNCATE TABLE MNE_BUSINESSREGISTER.C_BUSINESSKIND")

	cursor_MNE.execute(f"""SELECT
 BUSINESSKIND_ID AS "BUSINESSKIND_ID"
, BUSINESSKIND_CODE AS "BUSINESSKIND_CODE"
, NAME_RU AS "NAME_RU"
, NAME_KZ AS "NAME_KZ"
, CORRECT AS "CORRECT"
FROM dbo."C_BUSINESSKIND"
""")

	flag = True
	while flag:
		req_MNE = cursor_MNE.fetchmany(10000)
		if len(req_MNE) == 0:
			flag=False
		cursor_CH_SBD.executemany("""INSERT INTO MNE_BUSINESSREGISTER.C_BUSINESSKIND (BUSINESSKIND_ID, BUSINESSKIND_CODE, NAME_RU, NAME_KZ, CORRECT) VALUES""", req_MNE)

	cursor_CH_SBD.execute(f"SELECT COUNT(*) FROM MNE_BUSINESSREGISTER.C_BUSINESSKIND")
	sbd_count_rows_value = cursor_CH_SBD.fetchone()
	cursor_MNE.execute(f"SELECT COUNT(*) FROM dbo.C_BUSINESSKIND")
	origin_count_rows_value = cursor_MNE.fetchone()

	a = 0
	if int(sbd_count_rows_value[0]) == int(origin_count_rows_value[0]):
		a = 1
	tuple_to_load = ("MNE_BUSINESSREGISTER", "C_BUSINESSKIND", "TRUNCATE",a, origin_count_rows_value[0], sbd_count_rows_value[0])
	list_to_load = [(tuple_to_load)]

	cursor_CH_SBD.executemany("INSERT INTO MNE_BUSINESSREGISTER.CHECK_LOAD (SCHEMA, TABLE, TYPE_OF_ETL, CORRECTLY_LOADED, TOTAL_COUNT_SOURCE, TOTAL_COUNT_SDU) VALUES", (list_to_load))

def C_ERRORTYPE():
	cursor_CH_SBD.execute("TRUNCATE TABLE MNE_BUSINESSREGISTER.C_ERRORTYPE")

	cursor_MNE.execute(f"""SELECT
 ERROR_TYPEID AS "ERROR_TYPEID"
, ERROR_CODE AS "ERROR_CODE"
, NAME_RU AS "NAME_RU"
, NAME_KZ AS "NAME_KZ"
, NAME_EN AS "NAME_EN"
FROM dbo."C_ERRORTYPE"
""")

	flag = True
	while flag:
		req_MNE = cursor_MNE.fetchmany(10000)
		if len(req_MNE) == 0:
			flag=False
		cursor_CH_SBD.executemany("""INSERT INTO MNE_BUSINESSREGISTER.C_ERRORTYPE (ERROR_TYPEID, ERROR_CODE, NAME_RU, NAME_KZ, NAME_EN) VALUES""", req_MNE)

	cursor_CH_SBD.execute(f"SELECT COUNT(*) FROM MNE_BUSINESSREGISTER.C_ERRORTYPE")
	sbd_count_rows_value = cursor_CH_SBD.fetchone()
	cursor_MNE.execute(f"SELECT COUNT(*) FROM dbo.C_ERRORTYPE")
	origin_count_rows_value = cursor_MNE.fetchone()

	a = 0
	if int(sbd_count_rows_value[0]) == int(origin_count_rows_value[0]):
		a = 1
	tuple_to_load = ("MNE_BUSINESSREGISTER", "C_ERRORTYPE", "TRUNCATE",a, origin_count_rows_value[0], sbd_count_rows_value[0])
	list_to_load = [(tuple_to_load)]

	cursor_CH_SBD.executemany("INSERT INTO MNE_BUSINESSREGISTER.CHECK_LOAD (SCHEMA, TABLE, TYPE_OF_ETL, CORRECTLY_LOADED, TOTAL_COUNT_SOURCE, TOTAL_COUNT_SDU) VALUES", (list_to_load))

def C_LICENSEKIND():
	cursor_CH_SBD.execute("TRUNCATE TABLE MNE_BUSINESSREGISTER.C_LICENSEKIND")

	cursor_MNE.execute(f"""SELECT
 LICENSEKIND_ID AS "LICENSEKIND_ID"
, LICENSEKIND_CODE AS "LICENSEKIND_CODE"
, LICENSEKIND_NAMERU AS "LICENSEKIND_NAMERU"
, LICENSEKIND_NAMEKZ AS "LICENSEKIND_NAMEKZ"
, LICENSEKIND_NAMEEN AS "LICENSEKIND_NAMEEN"
, LICENSEKIND_SPECIAL AS "LICENSEKIND_SPECIAL"
FROM dbo."C_LICENSEKIND"
""")

	flag = True
	while flag:
		req_MNE = cursor_MNE.fetchmany(10000)
		if len(req_MNE) == 0:
			flag=False
		cursor_CH_SBD.executemany("""INSERT INTO MNE_BUSINESSREGISTER.C_LICENSEKIND (LICENSEKIND_ID, LICENSEKIND_CODE, LICENSEKIND_NAMERU, LICENSEKIND_NAMEKZ, LICENSEKIND_NAMEEN, LICENSEKIND_SPECIAL) VALUES""", req_MNE)

	cursor_CH_SBD.execute(f"SELECT COUNT(*) FROM MNE_BUSINESSREGISTER.C_LICENSEKIND")
	sbd_count_rows_value = cursor_CH_SBD.fetchone()
	cursor_MNE.execute(f"SELECT COUNT(*) FROM dbo.C_LICENSEKIND")
	origin_count_rows_value = cursor_MNE.fetchone()

	a = 0
	if int(sbd_count_rows_value[0]) == int(origin_count_rows_value[0]):
		a = 1
	tuple_to_load = ("MNE_BUSINESSREGISTER", "C_LICENSEKIND", "TRUNCATE",a, origin_count_rows_value[0], sbd_count_rows_value[0])
	list_to_load = [(tuple_to_load)]

	cursor_CH_SBD.executemany("INSERT INTO MNE_BUSINESSREGISTER.CHECK_LOAD (SCHEMA, TABLE, TYPE_OF_ETL, CORRECTLY_LOADED, TOTAL_COUNT_SOURCE, TOTAL_COUNT_SDU) VALUES", (list_to_load))

def C_OKED():
	cursor_CH_SBD.execute("TRUNCATE TABLE MNE_BUSINESSREGISTER.C_OKED")

	cursor_MNE.execute(f"""SELECT
 OKED_ID AS "OKED_ID"
, OKED_CODE AS "OKED_CODE"
, NAME_RU AS "NAME_RU"
, NAME_KZ AS "NAME_KZ"
FROM dbo."C_OKED"
""")

	flag = True
	while flag:
		req_MNE = cursor_MNE.fetchmany(10000)
		if len(req_MNE) == 0:
			flag=False
		cursor_CH_SBD.executemany("""INSERT INTO MNE_BUSINESSREGISTER.C_OKED (OKED_ID, OKED_CODE, NAME_RU, NAME_KZ) VALUES""", req_MNE)

	cursor_CH_SBD.execute(f"SELECT COUNT(*) FROM MNE_BUSINESSREGISTER.C_OKED")
	sbd_count_rows_value = cursor_CH_SBD.fetchone()
	cursor_MNE.execute(f"SELECT COUNT(*) FROM dbo.C_OKED")
	origin_count_rows_value = cursor_MNE.fetchone()

	a = 0
	if int(sbd_count_rows_value[0]) == int(origin_count_rows_value[0]):
		a = 1
	tuple_to_load = ("MNE_BUSINESSREGISTER", "C_OKED", "TRUNCATE",a, origin_count_rows_value[0], sbd_count_rows_value[0])
	list_to_load = [(tuple_to_load)]

	cursor_CH_SBD.executemany("INSERT INTO MNE_BUSINESSREGISTER.CHECK_LOAD (SCHEMA, TABLE, TYPE_OF_ETL, CORRECTLY_LOADED, TOTAL_COUNT_SOURCE, TOTAL_COUNT_SDU) VALUES", (list_to_load))

def R_BUSINESSDATA():
	cursor_CH_SBD.execute("select max(BUSINESSDATA_ID) from MNE_BUSINESSREGISTER.R_BUSINESSDATA")
	max_id = cursor_CH_SBD.fetchone()[0]

	cursor_MNE.execute(f"""SELECT
 BUSINESSDATA_ID AS "BUSINESSDATA_ID"
, XIN AS "XIN"
, RNN AS "RNN"
, NAME AS "NAME"
, IS_SPECIAL_TAXPAYER AS "IS_SPECIAL_TAXPAYER"
, IS_BRANCH AS "IS_BRANCH"
, HEADER_BIN AS "HEADER_BIN"
, HEADER_RNN AS "HEADER_RNN"
, FNO_CODE AS "FNO_CODE"
, CELL_CODE AS "CELL_CODE"
, VALUE AS "VALUE"
, REPORT_YEAR AS "REPORT_YEAR"
, REPORT_PERIOD AS "REPORT_PERIOD"
, VERSION_NUMBER AS "VERSION_NUMBER"
, XIN_ISCORRECT AS "XIN_ISCORRECT"
, HASH AS "HASH"
, AUTHOR AS "AUTHOR"
FROM dbo."R_BUSINESSDATA" where "BUSINESSDATA_ID" > {max_id}
""")

	flag = True
	while flag:
		req_MNE = cursor_MNE.fetchmany(10000)
		req_MNE = [*map(lambda x: hash_columns(x, indexes=[1]), req_MNE)]
		if len(req_MNE) == 0:
			flag=False
		cursor_CH_SBD.executemany("""INSERT INTO MNE_BUSINESSREGISTER.R_BUSINESSDATA (BUSINESSDATA_ID, XIN, RNN, NAME, IS_SPECIAL_TAXPAYER, IS_BRANCH, HEADER_BIN, HEADER_RNN, FNO_CODE, CELL_CODE, VALUE, REPORT_YEAR, REPORT_PERIOD, VERSION_NUMBER, XIN_ISCORRECT, HASH, AUTHOR) VALUES""", req_MNE)

	cursor_CH_SBD.execute(f"SELECT COUNT(*) FROM MNE_BUSINESSREGISTER.R_BUSINESSDATA")
	sbd_count_rows_value = cursor_CH_SBD.fetchone()
	cursor_MNE.execute(f"SELECT COUNT(*) FROM dbo.R_BUSINESSDATA")
	origin_count_rows_value = cursor_MNE.fetchone()

	a = 0
	if int(sbd_count_rows_value[0]) == int(origin_count_rows_value[0]):
		a = 1
	tuple_to_load = ("MNE_BUSINESSREGISTER", "R_BUSINESSDATA", "DELTA",a, origin_count_rows_value[0], sbd_count_rows_value[0])
	list_to_load = [(tuple_to_load)]

	cursor_CH_SBD.executemany("INSERT INTO MNE_BUSINESSREGISTER.CHECK_LOAD (SCHEMA, TABLE, TYPE_OF_ETL, CORRECTLY_LOADED, TOTAL_COUNT_SOURCE, TOTAL_COUNT_SDU) VALUES", (list_to_load))

def R_IP():
	cursor_CH_SBD.execute("select max(IP_ID) from MNE_BUSINESSREGISTER.R_IP")
	max_id = cursor_CH_SBD.fetchone()[0]

	cursor_MNE.execute(f"""SELECT
 IP_ID AS "IP_ID"
, IP_IIN AS "IP_IIN"
, IP_NAME AS "IP_NAME"
, IP_SHORTNAME AS "IP_SHORTNAME"
, C_BUSINESSKIND_ID AS "C_BUSINESSKIND_ID"
, IP_JOINT AS "IP_JOINT"
, C_BUSINESSKIND_ID_S AS "C_BUSINESSKIND_ID_S"
, HASH AS "HASH"
, AUTHOR AS "AUTHOR"
FROM dbo."R_IP" where "IP_ID" > {max_id}
""")

	flag = True
	while flag:
		req_MNE = cursor_MNE.fetchmany(10000)
		req_MNE = [*map(lambda x: hash_columns(x, indexes=[1]), req_MNE)]
		if len(req_MNE) == 0:
			flag=False
		cursor_CH_SBD.executemany("""INSERT INTO MNE_BUSINESSREGISTER.R_IP (IP_ID, IP_IIN, IP_NAME, IP_SHORTNAME, C_BUSINESSKIND_ID, IP_JOINT, C_BUSINESSKIND_ID_S, HASH, AUTHOR) VALUES""", req_MNE)

	cursor_CH_SBD.execute(f"SELECT COUNT(*) FROM MNE_BUSINESSREGISTER.R_IP")
	sbd_count_rows_value = cursor_CH_SBD.fetchone()
	cursor_MNE.execute(f"SELECT COUNT(*) FROM dbo.R_IP")
	origin_count_rows_value = cursor_MNE.fetchone()

	a = 0
	if int(sbd_count_rows_value[0]) == int(origin_count_rows_value[0]):
		a = 1
	tuple_to_load = ("MNE_BUSINESSREGISTER", "R_IP", "TRUNCATE",a, origin_count_rows_value[0], sbd_count_rows_value[0])
	list_to_load = [(tuple_to_load)]

	cursor_CH_SBD.executemany("INSERT INTO MNE_BUSINESSREGISTER.CHECK_LOAD (SCHEMA, TABLE, TYPE_OF_ETL, CORRECTLY_LOADED, TOTAL_COUNT_SOURCE, TOTAL_COUNT_SDU) VALUES", (list_to_load))

def R_LICENSE():
	cursor_CH_SBD.execute("TRUNCATE TABLE MNE_BUSINESSREGISTER.R_LICENSE")

	cursor_MNE.execute(f"""SELECT
 LICENSE_ID AS "LICENSE_ID"
, R_UL_ID AS "R_UL_ID"
, R_IP_ID AS "R_IP_ID"
, C_LICENSEKIND_ID AS "C_LICENSEKIND_ID"
, C_LICENSESTATE_ID AS "C_LICENSESTATE_ID"
, HASH AS "HASH"
, AUTHOR AS "AUTHOR"
FROM dbo."R_LICENSE"
""")

	flag = True
	while flag:
		req_MNE = cursor_MNE.fetchmany(10000)
		if len(req_MNE) == 0:
			flag=False
		cursor_CH_SBD.executemany("""INSERT INTO MNE_BUSINESSREGISTER.R_LICENSE (LICENSE_ID, R_UL_ID, R_IP_ID, C_LICENSEKIND_ID, C_LICENSESTATE_ID, HASH, AUTHOR) VALUES""", req_MNE)

	cursor_CH_SBD.execute(f"SELECT COUNT(*) FROM MNE_BUSINESSREGISTER.R_LICENSE")
	sbd_count_rows_value = cursor_CH_SBD.fetchone()
	cursor_MNE.execute(f"SELECT COUNT(*) FROM dbo.R_LICENSE")
	origin_count_rows_value = cursor_MNE.fetchone()

	a = 0
	if int(sbd_count_rows_value[0]) == int(origin_count_rows_value[0]):
		a = 1
	tuple_to_load = ("MNE_BUSINESSREGISTER", "R_LICENSE", "TRUNCATE",a, origin_count_rows_value[0], sbd_count_rows_value[0])
	list_to_load = [(tuple_to_load)]

	cursor_CH_SBD.executemany("INSERT INTO MNE_BUSINESSREGISTER.CHECK_LOAD (SCHEMA, TABLE, TYPE_OF_ETL, CORRECTLY_LOADED, TOTAL_COUNT_SOURCE, TOTAL_COUNT_SDU) VALUES", (list_to_load))

def R_LICENSE_REQUEST():
	cursor_CH_SBD.execute("select max(LICENSE_REQUESTID) from MNE_BUSINESSREGISTER.R_LICENSE_REQUEST")
	max_id = cursor_CH_SBD.fetchone()[0]

	cursor_MNE.execute(f"""SELECT
 LICENSE_REQUESTID AS "LICENSE_REQUESTID"
, ANY_LICENSE AS "ANY_LICENSE"
, XIN AS "XIN"
FROM dbo."R_LICENSE_REQUEST" where "LICENSE_REQUESTID" > {max_id}
""")

	flag = True
	while flag:
		req_MNE = cursor_MNE.fetchmany(10000)
		req_MNE = [*map(lambda x: hash_columns(x, indexes=[2]), req_MNE)]
		if len(req_MNE) == 0:
			flag=False
		cursor_CH_SBD.executemany("""INSERT INTO MNE_BUSINESSREGISTER.R_LICENSE_REQUEST (LICENSE_REQUESTID, ANY_LICENSE, XIN) VALUES""", req_MNE)

	cursor_CH_SBD.execute(f"SELECT COUNT(*) FROM MNE_BUSINESSREGISTER.R_LICENSE_REQUEST")
	sbd_count_rows_value = cursor_CH_SBD.fetchone()
	cursor_MNE.execute(f"SELECT COUNT(*) FROM dbo.R_LICENSE_REQUEST")
	origin_count_rows_value = cursor_MNE.fetchone()

	a = 0
	if int(sbd_count_rows_value[0]) == int(origin_count_rows_value[0]):
		a = 1
	tuple_to_load = ("MNE_BUSINESSREGISTER", "R_LICENSE_REQUEST", "TRUNCATE",a, origin_count_rows_value[0], sbd_count_rows_value[0])
	list_to_load = [(tuple_to_load)]

	cursor_CH_SBD.executemany("INSERT INTO MNE_BUSINESSREGISTER.CHECK_LOAD (SCHEMA, TABLE, TYPE_OF_ETL, CORRECTLY_LOADED, TOTAL_COUNT_SOURCE, TOTAL_COUNT_SDU) VALUES", (list_to_load))

def R_PERSON():
	cursor_CH_SBD.execute("TRUNCATE TABLE MNE_BUSINESSREGISTER.R_PERSON")

	cursor_MNE.execute(f"""SELECT
 PERSON_ID AS "PERSON_ID"
, PERSON_LASTNAME AS "PERSON_LASTNAME"
, PERSON_FIRSTNAME AS "PERSON_FIRSTNAME"
, PERSON_MIDDLENAME AS "PERSON_MIDDLENAME"
, PERSON_IIN AS "PERSON_IIN"
FROM dbo."R_PERSON"
""")

	flag = True
	while flag:
		req_MNE = cursor_MNE.fetchmany(10000)
		req_MNE = [*map(lambda x: hash_columns(x, indexes=[1, 2, 3, 4]), req_MNE)]
		if len(req_MNE) == 0:
			flag=False
		cursor_CH_SBD.executemany("""INSERT INTO MNE_BUSINESSREGISTER.R_PERSON (PERSON_ID, PERSON_LASTNAME, PERSON_FIRSTNAME, PERSON_MIDDLENAME, PERSON_IIN) VALUES""", req_MNE)

	cursor_CH_SBD.execute(f"SELECT COUNT(*) FROM MNE_BUSINESSREGISTER.R_PERSON")
	sbd_count_rows_value = cursor_CH_SBD.fetchone()
	cursor_MNE.execute(f"SELECT COUNT(*) FROM dbo.R_PERSON")
	origin_count_rows_value = cursor_MNE.fetchone()

	a = 0
	if int(sbd_count_rows_value[0]) == int(origin_count_rows_value[0]):
		a = 1
	tuple_to_load = ("MNE_BUSINESSREGISTER", "R_PERSON", "TRUNCATE",a, origin_count_rows_value[0], sbd_count_rows_value[0])
	list_to_load = [(tuple_to_load)]

	cursor_CH_SBD.executemany("INSERT INTO MNE_BUSINESSREGISTER.CHECK_LOAD (SCHEMA, TABLE, TYPE_OF_ETL, CORRECTLY_LOADED, TOTAL_COUNT_SOURCE, TOTAL_COUNT_SDU) VALUES", (list_to_load))

def R_UL():
	cursor_CH_SBD.execute("TRUNCATE TABLE MNE_BUSINESSREGISTER.R_UL")

	cursor_MNE.execute(f"""SELECT
 UL_ID AS "UL_ID"
, UL_NAMERU AS "UL_NAMERU"
, UL_NAMEKZ AS "UL_NAMEKZ"
, UL_BIN AS "UL_BIN"
, C_BUSINESSKIND_ID AS "C_BUSINESSKIND_ID"
, C_OPF_ID AS "C_OPF_ID"
, C_BUSINESSKIND_ID_S AS "C_BUSINESSKIND_ID_S"
, C_ULSTATE_ID AS "C_ULSTATE_ID"
, HASH AS "HASH"
, AUTHOR AS "AUTHOR"
FROM dbo."R_UL"
""")

	flag = True
	while flag:
		req_MNE = cursor_MNE.fetchmany(10000)
		if len(req_MNE) == 0:
			flag=False
		cursor_CH_SBD.executemany("""INSERT INTO MNE_BUSINESSREGISTER.R_UL (UL_ID, UL_NAMERU, UL_NAMEKZ, UL_BIN, C_BUSINESSKIND_ID, C_OPF_ID, C_BUSINESSKIND_ID_S, C_ULSTATE_ID, HASH, AUTHOR) VALUES""", req_MNE)

	cursor_CH_SBD.execute(f"SELECT COUNT(*) FROM MNE_BUSINESSREGISTER.R_UL")
	sbd_count_rows_value = cursor_CH_SBD.fetchone()
	cursor_MNE.execute(f"SELECT COUNT(*) FROM dbo.R_UL")
	origin_count_rows_value = cursor_MNE.fetchone()

	a = 0
	if int(sbd_count_rows_value[0]) == int(origin_count_rows_value[0]):
		a = 1
	tuple_to_load = ("MNE_BUSINESSREGISTER", "R_UL", "TRUNCATE",a, origin_count_rows_value[0], sbd_count_rows_value[0])
	list_to_load = [(tuple_to_load)]

	cursor_CH_SBD.executemany("INSERT INTO MNE_BUSINESSREGISTER.CHECK_LOAD (SCHEMA, TABLE, TYPE_OF_ETL, CORRECTLY_LOADED, TOTAL_COUNT_SOURCE, TOTAL_COUNT_SDU) VALUES", (list_to_load))

def R_ULOKED():
	cursor_CH_SBD.execute("TRUNCATE TABLE MNE_BUSINESSREGISTER.R_ULOKED")

	cursor_MNE.execute(f"""SELECT
 ULOKED_ID AS "ULOKED_ID"
, R_UL_ID AS "R_UL_ID"
, C_OKED_ID AS "C_OKED_ID"
, MAIN AS "MAIN"
FROM dbo."R_ULOKED"
""")

	flag = True
	while flag:
		req_MNE = cursor_MNE.fetchmany(10000)
		if len(req_MNE) == 0:
			flag=False
		cursor_CH_SBD.executemany("""INSERT INTO MNE_BUSINESSREGISTER.R_ULOKED (ULOKED_ID, R_UL_ID, C_OKED_ID, MAIN) VALUES""", req_MNE)

	cursor_CH_SBD.execute(f"SELECT COUNT(*) FROM MNE_BUSINESSREGISTER.R_ULOKED")
	sbd_count_rows_value = cursor_CH_SBD.fetchone()
	cursor_MNE.execute(f"SELECT COUNT(*) FROM dbo.R_ULOKED")
	origin_count_rows_value = cursor_MNE.fetchone()

	a = 0
	if int(sbd_count_rows_value[0]) == int(origin_count_rows_value[0]):
		a = 1
	tuple_to_load = ("MNE_BUSINESSREGISTER", "R_ULOKED", "TRUNCATE",a, origin_count_rows_value[0], sbd_count_rows_value[0])
	list_to_load = [(tuple_to_load)]

	cursor_CH_SBD.executemany("INSERT INTO MNE_BUSINESSREGISTER.CHECK_LOAD (SCHEMA, TABLE, TYPE_OF_ETL, CORRECTLY_LOADED, TOTAL_COUNT_SOURCE, TOTAL_COUNT_SDU) VALUES", (list_to_load))

with DAG(
	"MNE_DELTA",description="Load_and_check",start_date=datetime(2022, 5, 12),schedule_interval='0 3 * * *',catchup=False) as dag:

	conn_MNE_secrets = Connection.get_connection_from_secrets(conn_id="MNE_BUSINESSREGISTER")##скрытй коннекшн источника
	cnxn = pymssql.connect(
		database = conn_MNE_secrets.schema,
		user = conn_MNE_secrets.login,
		password = conn_MNE_secrets.password,
		host = conn_MNE_secrets.host,
		port = conn_MNE_secrets.port)

	conn_CH_SBD_secrets = Connection.get_connection_from_secrets(conn_id="Clickhouse-5")##скрытй коннекшн 5го сервера
	click_SBD = connect(
		host=conn_CH_SBD_secrets.host,
		port=conn_CH_SBD_secrets.port, 
		password=conn_CH_SBD_secrets.password, 
		user=conn_CH_SBD_secrets.login, 
		connect_timeout=3600)

	cursor_CH_SBD = click_SBD.cursor()
	cursor_MNE = cnxn.cursor()
	hash_name = Variable.get("hash_password")##secret hash

	C_BUSINESSKIND = PythonOperator(
		owner='Bakhtiyar',
 		task_id='C_BUSINESSKIND',
		python_callable=C_BUSINESSKIND,
	)
	C_ERRORTYPE = PythonOperator(
		owner='Bakhtiyar',
 		task_id='C_ERRORTYPE',
		python_callable=C_ERRORTYPE,
	)
	C_LICENSEKIND = PythonOperator(
		owner='Bakhtiyar',
 		task_id='C_LICENSEKIND',
		python_callable=C_LICENSEKIND,
	)
	C_OKED = PythonOperator(
		owner='Bakhtiyar',
 		task_id='C_OKED',
		python_callable=C_OKED,
	)
	R_BUSINESSDATA = PythonOperator(
		owner='Bakhtiyar',
 		task_id='R_BUSINESSDATA',
		python_callable=R_BUSINESSDATA,
	)
	R_IP = PythonOperator(
		owner='Bakhtiyar',
 		task_id='R_IP',
		python_callable=R_IP,
	)
	R_LICENSE = PythonOperator(
		owner='Bakhtiyar',
 		task_id='R_LICENSE',
		python_callable=R_LICENSE,
	)
	R_LICENSE_REQUEST = PythonOperator(
		owner='Bakhtiyar',
 		task_id='R_LICENSE_REQUEST',
		python_callable=R_LICENSE_REQUEST,
	)
	R_PERSON = PythonOperator(
		owner='Bakhtiyar',
 		task_id='R_PERSON',
		python_callable=R_PERSON,
	)
	R_UL = PythonOperator(
		owner='Bakhtiyar',
 		task_id='R_UL',
		python_callable=R_UL,
	)
	R_ULOKED = PythonOperator(
		owner='Bakhtiyar',
 		task_id='R_ULOKED',
		python_callable=R_ULOKED,
	)
	
	C_BUSINESSKIND>>C_ERRORTYPE>>C_LICENSEKIND>>C_OKED>>R_BUSINESSDATA>>R_IP>>R_LICENSE>>R_LICENSE_REQUEST>>R_PERSON>>R_UL>>R_ULOKED