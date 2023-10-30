from datetime import datetime
from airflow import DAG
from clickhouse_driver import Client, connect
from airflow.operators.python import PythonOperator
from airflow.models.connection import Connection
from airflow.models import Variable
import hashlib
import cx_Oracle

def hash(data):
	hash_data = f"{str(data).lower()}{hash_name}".encode("utf-8")
	res = hashlib.sha256(hash_data).hexdigest()
	return res.upper()

def hash_columns(data, indexes):
	tmp = list(data)
	for index in indexes:
		tmp[index] = hash(data[index])
	return tuple(tmp)

def C_SDU_ACCIDENT_ELIM_WORK():
	conn_CH_SBD_secrets=Connection.get_connection_from_secrets(conn_id="Clickhouse-5")#скрытый коннекшн 5го сервера
	conn_CH_SBD=connect(host=conn_CH_SBD_secrets.host,port=conn_CH_SBD_secrets.port,user=conn_CH_SBD_secrets.login, password=conn_CH_SBD_secrets.password, connect_timeout=3600)
	cursor_CH_SBD = conn_CH_SBD.cursor()
	conn_MTSZN_secrets=Connection.get_connection_from_secrets(conn_id="MTSZN_WMARKET")#скрытый коннекшн источника
	dsn_tns_secrets=cx_Oracle.makedsn(host=conn_MTSZN_secrets.host,port=conn_MTSZN_secrets.port, service_name='SD')
	conn_MTSZN = cx_Oracle.connect(user =conn_MTSZN_secrets.login, password =conn_MTSZN_secrets.password, dsn=dsn_tns_secrets)
	cursor_MTSZN = conn_MTSZN.cursor()

	cursor_CH_SBD.execute("TRUNCATE TABLE MTSZN_LABORPROTECT.CHECK_LOAD")
	cursor_CH_SBD.execute("SELECT COALESCE(toString(max(SDU_DATE)),'000-00-00 00:00:00') as MAX_SDU_DATE from MTSZN_LABORPROTECT.ACCIDENT_ELIM_WORK")
	max_id = cursor_CH_SBD.fetchone()[0]

	cursor_MTSZN.execute(f"""SELECT
 ACCIDENT_ELIM_WORK_ID AS "ACCIDENT_ELIM_WORK_ID"
, N1_ID AS "N1_ID"
, ACCIDENT_INVEST_ID AS "ACCIDENT_INVEST_ID"
, WORK_NOTE AS "WORK_NOTE"
, EXECUTOR_FULLNAME AS "EXECUTOR_FULLNAME"
, NVL(TO_CHAR("BEGIN_DATE", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "BEGIN_DATE"
, NVL(TO_CHAR("END_DATE", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "END_DATE"
, D_WORK_ORDER_ID AS "D_WORK_ORDER_ID"
, NVL(TO_CHAR("DATE_ENTRY", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "DATE_ENTRY"
, USER_ID AS "USER_ID"
, WORK_KIND AS "WORK_KIND"
, SDU_DATE AS "SDU_DATE"
FROM LABORPROTECT."C_SDU_ACCIDENT_ELIM_WORK" where SDU_DATE > to_timestamp('{max_id}','YYYY-MM-DD HH24:MI:SS')
""")

	flag = True
	while flag:
		req_MTSZN = cursor_MTSZN.fetchmany(10000)
		if len(req_MTSZN) == 0:
			flag=False
		cursor_CH_SBD.executemany("""INSERT INTO MTSZN_LABORPROTECT.ACCIDENT_ELIM_WORK (ACCIDENT_ELIM_WORK_ID, N1_ID, ACCIDENT_INVEST_ID, WORK_NOTE, EXECUTOR_FULLNAME, BEGIN_DATE, END_DATE, D_WORK_ORDER_ID, DATE_ENTRY, USER_ID, WORK_KIND, SDU_DATE) VALUES""", req_MTSZN)

	cursor_CH_SBD.execute(f"SELECT COUNT(*) FROM MTSZN_LABORPROTECT.ACCIDENT_ELIM_WORK")
	sbd_count_rows_value = cursor_CH_SBD.fetchone()

	cursor_MTSZN.execute(f"SELECT COUNT(*) FROM LABORPROTECT.C_SDU_ACCIDENT_ELIM_WORK")
	origin_count_rows_value = cursor_MTSZN.fetchone()

	a = 0
	if int(sbd_count_rows_value[0]) == int(origin_count_rows_value[0]):
		a = 1
	tuple_to_load = ("LABORPROTECT", "ACCIDENT_ELIM_WORK", "TRUNC", a,  origin_count_rows_value[0],sbd_count_rows_value[0])
	list_to_load = [(tuple_to_load)]

	cursor_CH_SBD.executemany("INSERT INTO MTSZN_WMARKET.CHECK_LOAD (SCHEMA, TABLE, TYPE_OF_ETL, CORRECTLY_LOADED,TOTAL_COUNT_SOURCE,TOTAL_COUNT_SDU) VALUES", list_to_load)

	conn_MTSZN.close() 

def C_SDU_AFFAIR():
	conn_CH_SBD_secrets=Connection.get_connection_from_secrets(conn_id="Clickhouse-5")#скрытый коннекшн 5го сервера
	conn_CH_SBD=connect(host=conn_CH_SBD_secrets.host,port=conn_CH_SBD_secrets.port,user=conn_CH_SBD_secrets.login, password=conn_CH_SBD_secrets.password, connect_timeout=3600)
	cursor_CH_SBD = conn_CH_SBD.cursor()
	conn_MTSZN_secrets=Connection.get_connection_from_secrets(conn_id="MTSZN_WMARKET")#скрытый коннекшн источника
	dsn_tns_secrets=cx_Oracle.makedsn(host=conn_MTSZN_secrets.host,port=conn_MTSZN_secrets.port, service_name='SD')
	conn_MTSZN = cx_Oracle.connect(user =conn_MTSZN_secrets.login, password =conn_MTSZN_secrets.password, dsn=dsn_tns_secrets)
	cursor_MTSZN = conn_MTSZN.cursor()

	cursor_CH_SBD.execute("SELECT COALESCE(toString(max(SDU_DATE)),'000-00-00 00:00:00') as MAX_SDU_DATE from MTSZN_LABORPROTECT.AFFAIR")
	max_id = cursor_CH_SBD.fetchone()[0]

	cursor_MTSZN.execute(f"""SELECT
 D_LANG_ID AS "D_LANG_ID"
, PROTOCOL_INSPECTOR_NAME AS "PROTOCOL_INSPECTOR_NAME"
, RESOLUTION_INSPECTOR_NAME AS "RESOLUTION_INSPECTOR_NAME"
, RESOLUTION_SUMMA AS "RESOLUTION_SUMMA"
, PROTOCOL_NOTE AS "PROTOCOL_NOTE"
, EYE_WITNESS AS "EYE_WITNESS"
, RESOLUTION_MRP_SIZE AS "RESOLUTION_MRP_SIZE"
, ADM_ZAKON_RK_ID AS "ADM_ZAKON_RK_ID"
, LABOR_ZAKON_PUNKT AS "LABOR_ZAKON_PUNKT"
, ADM_ZAKON_PUNKT AS "ADM_ZAKON_PUNKT"
, PROTOCOL_NUM AS "PROTOCOL_NUM"
, IS_WARNING AS "IS_WARNING"
, RESOLUTION_NUM AS "RESOLUTION_NUM"
, USER_ID AS "USER_ID"
, METROLOG_DATA AS "METROLOG_DATA"
, LABOR_ZAKON_SUBPUNKT AS "LABOR_ZAKON_SUBPUNKT"
, ADM_ZAKON_SUBPUNKT AS "ADM_ZAKON_SUBPUNKT"
, EMP_CULPRIT AS "EMP_CULPRIT"
, NVL(TO_CHAR("INSPECT_DATETIME", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "INSPECT_DATETIME"
, PROTOCOL_NOTE_EMP AS "PROTOCOL_NOTE_EMP"
, METROLOG_NUM AS "METROLOG_NUM"
, SUD_NUM AS "SUD_NUM"
, DIR_BREACH_ID AS "DIR_BREACH_ID"
, LABOR_ZAKON_RK_ID AS "LABOR_ZAKON_RK_ID"
, METROLOG_NAME AS "METROLOG_NAME"
, RESOLUTION_MRP AS "RESOLUTION_MRP"
, INVEST_BASIS_ID AS "INVEST_BASIS_ID"
, NVL(TO_CHAR("DATE_ENTRY", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "DATE_ENTRY"
, NVL(TO_CHAR("PROTOCOL_DATE", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "PROTOCOL_DATE"
, NVL(TO_CHAR("RESOLUTION_DATE", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "RESOLUTION_DATE"
, NVL(TO_CHAR("SUD_DATE", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "SUD_DATE"
, NVL(TO_CHAR("METROLOG_DATE", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "METROLOG_DATE"
, RESOLUTION_ADDRESS AS "RESOLUTION_ADDRESS"
, ADDRESS AS "ADDRESS"
, AFFAIR_ID AS "AFFAIR_ID"
, SDU_DATE AS "SDU_DATE"
FROM LABORPROTECT."C_SDU_AFFAIR" where SDU_DATE > to_timestamp('{max_id}','YYYY-MM-DD HH24:MI:SS')
""")

	flag = True
	while flag:
		req_MTSZN = cursor_MTSZN.fetchmany(10000)
		if len(req_MTSZN) == 0:
			flag=False
		cursor_CH_SBD.executemany("""INSERT INTO MTSZN_LABORPROTECT.AFFAIR (D_LANG_ID, PROTOCOL_INSPECTOR_NAME, RESOLUTION_INSPECTOR_NAME, RESOLUTION_SUMMA, PROTOCOL_NOTE, EYE_WITNESS, RESOLUTION_MRP_SIZE, ADM_ZAKON_RK_ID, LABOR_ZAKON_PUNKT, ADM_ZAKON_PUNKT, PROTOCOL_NUM, IS_WARNING, RESOLUTION_NUM, USER_ID, METROLOG_DATA, LABOR_ZAKON_SUBPUNKT, ADM_ZAKON_SUBPUNKT, EMP_CULPRIT, INSPECT_DATETIME, PROTOCOL_NOTE_EMP, METROLOG_NUM, SUD_NUM, DIR_BREACH_ID, LABOR_ZAKON_RK_ID, METROLOG_NAME, RESOLUTION_MRP, INVEST_BASIS_ID, DATE_ENTRY, PROTOCOL_DATE, RESOLUTION_DATE, SUD_DATE, METROLOG_DATE, RESOLUTION_ADDRESS, ADDRESS, AFFAIR_ID, SDU_DATE) VALUES""", req_MTSZN)

	cursor_CH_SBD.execute(f"SELECT COUNT(*) FROM MTSZN_LABORPROTECT.AFFAIR")
	sbd_count_rows_value = cursor_CH_SBD.fetchone()

	cursor_MTSZN.execute(f"SELECT COUNT(*) FROM LABORPROTECT.C_SDU_AFFAIR")
	origin_count_rows_value = cursor_MTSZN.fetchone()

	a = 0
	if int(sbd_count_rows_value[0]) == int(origin_count_rows_value[0]):
		a = 1
	tuple_to_load = ("LABORPROTECT", "C_SDU_AFFAIR", "TRUNC", a,  origin_count_rows_value[0],sbd_count_rows_value[0])
	list_to_load = [(tuple_to_load)]

	cursor_CH_SBD.executemany("INSERT INTO MTSZN_WMARKET.CHECK_LOAD (SCHEMA, TABLE, TYPE_OF_ETL, CORRECTLY_LOADED,TOTAL_COUNT_SOURCE,TOTAL_COUNT_SDU) VALUES", list_to_load)

	conn_MTSZN.close() 

def C_SDU_AR_HOUSE():
	conn_CH_SBD_secrets=Connection.get_connection_from_secrets(conn_id="Clickhouse-5")#скрытый коннекшн 5го сервера
	conn_CH_SBD=connect(host=conn_CH_SBD_secrets.host,port=conn_CH_SBD_secrets.port,user=conn_CH_SBD_secrets.login, password=conn_CH_SBD_secrets.password, connect_timeout=3600)
	cursor_CH_SBD = conn_CH_SBD.cursor()
	conn_MTSZN_secrets=Connection.get_connection_from_secrets(conn_id="MTSZN_WMARKET")#скрытый коннекшн источника
	dsn_tns_secrets=cx_Oracle.makedsn(host=conn_MTSZN_secrets.host,port=conn_MTSZN_secrets.port, service_name='SD')
	conn_MTSZN = cx_Oracle.connect(user =conn_MTSZN_secrets.login, password =conn_MTSZN_secrets.password, dsn=dsn_tns_secrets)
	cursor_MTSZN = conn_MTSZN.cursor()

	cursor_CH_SBD.execute("SELECT COALESCE(toString(max(SDU_DATE)),'000-00-00 00:00:00') as MAX_SDU_DATE from MTSZN_LABORPROTECT.AR_HOUSE")
	max_id = cursor_CH_SBD.fetchone()[0]

	cursor_MTSZN.execute(f"""SELECT
  AR_HOUSE_ID AS " AR_HOUSE_ID"
,  ADDR_REG_ID AS " ADDR_REG_ID"
, CADAST_NUM AS "CADAST_NUM"
, ACCOUNT_NUM AS "ACCOUNT_NUM"
, PHONE_NUM AS "PHONE_NUM"
, NVL(TO_CHAR("DATE_REG", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "DATE_REG"
, NVL(TO_CHAR("DATE_CLOSE", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "DATE_CLOSE"
, NOTE AS "NOTE"
, NVL(TO_CHAR("DATE_ENTRY", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "DATE_ENTRY"
, USER_ID AS "USER_ID"
, PB_RCA AS "PB_RCA"
, BUILD_RCA AS "BUILD_RCA"
, DEP_ID AS "DEP_ID"
, REGION_ID AS "REGION_ID"
, GROUND_RCA AS "GROUND_RCA"
, TEXT_ADDRESS AS "TEXT_ADDRESS"
, SDU_DATE AS "SDU_DATE"
FROM LABORPROTECT."C_SDU_AR_HOUSE" where SDU_DATE > to_timestamp('{max_id}','YYYY-MM-DD HH24:MI:SS')
""")

	flag = True
	while flag:
		req_MTSZN = cursor_MTSZN.fetchmany(10000)
		if len(req_MTSZN) == 0:
			flag=False
		cursor_CH_SBD.executemany("""INSERT INTO MTSZN_LABORPROTECT.AR_HOUSE ( AR_HOUSE_ID,  ADDR_REG_ID, CADAST_NUM, ACCOUNT_NUM, PHONE_NUM, DATE_REG, DATE_CLOSE, NOTE, DATE_ENTRY, USER_ID, PB_RCA, BUILD_RCA, DEP_ID, REGION_ID, GROUND_RCA, TEXT_ADDRESS, SDU_DATE) VALUES""", req_MTSZN)

	cursor_CH_SBD.execute(f"SELECT COUNT(*) FROM MTSZN_LABORPROTECT.AR_HOUSE")
	sbd_count_rows_value = cursor_CH_SBD.fetchone()

	cursor_MTSZN.execute(f"SELECT COUNT(*) FROM LABORPROTECT.C_SDU_AR_HOUSE")
	origin_count_rows_value = cursor_MTSZN.fetchone()

	a = 0
	if int(sbd_count_rows_value[0]) == int(origin_count_rows_value[0]):
		a = 1
	tuple_to_load = ("LABORPROTECT", "C_SDU_AR_HOUSE", "TRUNC", a,  origin_count_rows_value[0],sbd_count_rows_value[0])
	list_to_load = [(tuple_to_load)]

	cursor_CH_SBD.executemany("INSERT INTO MTSZN_WMARKET.CHECK_LOAD (SCHEMA, TABLE, TYPE_OF_ETL, CORRECTLY_LOADED,TOTAL_COUNT_SOURCE,TOTAL_COUNT_SDU) VALUES", list_to_load)

	conn_MTSZN.close() 

def C_SDU_BREACH_RISK():
	conn_CH_SBD_secrets=Connection.get_connection_from_secrets(conn_id="Clickhouse-5")#скрытый коннекшн 5го сервера
	conn_CH_SBD=connect(host=conn_CH_SBD_secrets.host,port=conn_CH_SBD_secrets.port,user=conn_CH_SBD_secrets.login, password=conn_CH_SBD_secrets.password, connect_timeout=3600)
	cursor_CH_SBD = conn_CH_SBD.cursor()
	conn_MTSZN_secrets=Connection.get_connection_from_secrets(conn_id="MTSZN_WMARKET")#скрытый коннекшн источника
	dsn_tns_secrets=cx_Oracle.makedsn(host=conn_MTSZN_secrets.host,port=conn_MTSZN_secrets.port, service_name='SD')
	conn_MTSZN = cx_Oracle.connect(user =conn_MTSZN_secrets.login, password =conn_MTSZN_secrets.password, dsn=dsn_tns_secrets)
	cursor_MTSZN = conn_MTSZN.cursor()

	cursor_CH_SBD.execute("SELECT COALESCE(toString(max(SDU_DATE)),'000-00-00 00:00:00') as MAX_SDU_DATE from MTSZN_LABORPROTECT.BREACH_RISK")
	max_id = cursor_CH_SBD.fetchone()[0]

	cursor_MTSZN.execute(f"""SELECT
 BREACH_RISK_ID AS "BREACH_RISK_ID"
, D_BREACH_ID AS "D_BREACH_ID"
, D_RISK_DEGREE_ID AS "D_RISK_DEGREE_ID"
, NVL(TO_CHAR("DATE_FROM", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "DATE_FROM"
, NVL(TO_CHAR("DATE_TO", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "DATE_TO"
, POINT_FROM AS "POINT_FROM"
, POINT_TO AS "POINT_TO"
, STATUS AS "STATUS"
, USER_ID AS "USER_ID"
, NVL(TO_CHAR("DATE_ENTRY", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "DATE_ENTRY"
, SDU_DATE AS "SDU_DATE"
FROM LABORPROTECT."C_SDU_BREACH_RISK" where SDU_DATE > to_timestamp('{max_id}','YYYY-MM-DD HH24:MI:SS')
""")

	flag = True
	while flag:
		req_MTSZN = cursor_MTSZN.fetchmany(10000)
		if len(req_MTSZN) == 0:
			flag=False
		cursor_CH_SBD.executemany("""INSERT INTO MTSZN_LABORPROTECT.BREACH_RISK (BREACH_RISK_ID, D_BREACH_ID, D_RISK_DEGREE_ID, DATE_FROM, DATE_TO, POINT_FROM, POINT_TO, STATUS, USER_ID, DATE_ENTRY, SDU_DATE) VALUES""", req_MTSZN)

	cursor_CH_SBD.execute(f"SELECT COUNT(*) FROM MTSZN_LABORPROTECT.BREACH_RISK")
	sbd_count_rows_value = cursor_CH_SBD.fetchone()

	cursor_MTSZN.execute(f"SELECT COUNT(*) FROM LABORPROTECT.C_SDU_BREACH_RISK")
	origin_count_rows_value = cursor_MTSZN.fetchone()

	a = 0
	if int(sbd_count_rows_value[0]) == int(origin_count_rows_value[0]):
		a = 1
	tuple_to_load = ("LABORPROTECT", "C_SDU_BREACH_RISK", "TRUNC", a,  origin_count_rows_value[0],sbd_count_rows_value[0])
	list_to_load = [(tuple_to_load)]

	cursor_CH_SBD.executemany("INSERT INTO MTSZN_WMARKET.CHECK_LOAD (SCHEMA, TABLE, TYPE_OF_ETL, CORRECTLY_LOADED,TOTAL_COUNT_SOURCE,TOTAL_COUNT_SDU) VALUES", list_to_load)

	conn_MTSZN.close() 

def C_SDU_BREACH_ZAKON():
	conn_CH_SBD_secrets=Connection.get_connection_from_secrets(conn_id="Clickhouse-5")#скрытый коннекшн 5го сервера
	conn_CH_SBD=connect(host=conn_CH_SBD_secrets.host,port=conn_CH_SBD_secrets.port,user=conn_CH_SBD_secrets.login, password=conn_CH_SBD_secrets.password, connect_timeout=3600)
	cursor_CH_SBD = conn_CH_SBD.cursor()
	conn_MTSZN_secrets=Connection.get_connection_from_secrets(conn_id="MTSZN_WMARKET")#скрытый коннекшн источника
	dsn_tns_secrets=cx_Oracle.makedsn(host=conn_MTSZN_secrets.host,port=conn_MTSZN_secrets.port, service_name='SD')
	conn_MTSZN = cx_Oracle.connect(user =conn_MTSZN_secrets.login, password =conn_MTSZN_secrets.password, dsn=dsn_tns_secrets)
	cursor_MTSZN = conn_MTSZN.cursor()

	cursor_CH_SBD.execute("SELECT COALESCE(toString(max(SDU_DATE)),'000-00-00 00:00:00') as MAX_SDU_DATE from MTSZN_LABORPROTECT.BREACH_ZAKON")
	max_id = cursor_CH_SBD.fetchone()[0]

	cursor_MTSZN.execute(f"""SELECT
 BREACH_ZAKON_ID AS "BREACH_ZAKON_ID"
, DIR_BREACH_ID AS "DIR_BREACH_ID"
, D_ZAKON_RK_ID AS "D_ZAKON_RK_ID"
, USER_ID AS "USER_ID"
, NVL(TO_CHAR("DATE_ENTRY", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "DATE_ENTRY"
, SDU_DATE AS "SDU_DATE"
FROM LABORPROTECT."C_SDU_BREACH_ZAKON" where SDU_DATE > to_timestamp('{max_id}','YYYY-MM-DD HH24:MI:SS')
""")

	flag = True
	while flag:
		req_MTSZN = cursor_MTSZN.fetchmany(10000)
		if len(req_MTSZN) == 0:
			flag=False
		cursor_CH_SBD.executemany("""INSERT INTO MTSZN_LABORPROTECT.BREACH_ZAKON (BREACH_ZAKON_ID, DIR_BREACH_ID, D_ZAKON_RK_ID, USER_ID, DATE_ENTRY, SDU_DATE) VALUES""", req_MTSZN)

	cursor_CH_SBD.execute(f"SELECT COUNT(*) FROM MTSZN_LABORPROTECT.BREACH_ZAKON")
	sbd_count_rows_value = cursor_CH_SBD.fetchone()

	cursor_MTSZN.execute(f"SELECT COUNT(*) FROM LABORPROTECT.C_SDU_BREACH_ZAKON")
	origin_count_rows_value = cursor_MTSZN.fetchone()

	a = 0
	if int(sbd_count_rows_value[0]) == int(origin_count_rows_value[0]):
		a = 1
	tuple_to_load = ("LABORPROTECT", "C_SDU_BREACH_ZAKON", "TRUNC", a,  origin_count_rows_value[0],sbd_count_rows_value[0])
	list_to_load = [(tuple_to_load)]

	cursor_CH_SBD.executemany("INSERT INTO MTSZN_WMARKET.CHECK_LOAD (SCHEMA, TABLE, TYPE_OF_ETL, CORRECTLY_LOADED,TOTAL_COUNT_SOURCE,TOTAL_COUNT_SDU) VALUES", list_to_load)

	conn_MTSZN.close() 

def C_SDU_COLLECTIVE_CONTRACT():
	conn_CH_SBD_secrets=Connection.get_connection_from_secrets(conn_id="Clickhouse-5")#скрытый коннекшн 5го сервера
	conn_CH_SBD=connect(host=conn_CH_SBD_secrets.host,port=conn_CH_SBD_secrets.port,user=conn_CH_SBD_secrets.login, password=conn_CH_SBD_secrets.password, connect_timeout=3600)
	cursor_CH_SBD = conn_CH_SBD.cursor()
	conn_MTSZN_secrets=Connection.get_connection_from_secrets(conn_id="MTSZN_WMARKET")#скрытый коннекшн источника
	dsn_tns_secrets=cx_Oracle.makedsn(host=conn_MTSZN_secrets.host,port=conn_MTSZN_secrets.port, service_name='SD')
	conn_MTSZN = cx_Oracle.connect(user =conn_MTSZN_secrets.login, password =conn_MTSZN_secrets.password, dsn=dsn_tns_secrets)
	cursor_MTSZN = conn_MTSZN.cursor()

	cursor_CH_SBD.execute("SELECT COALESCE(toString(max(SDU_DATE)),'000-00-00 00:00:00') as MAX_SDU_DATE from MTSZN_LABORPROTECT.COLLECTIVE_CONTRACT")
	max_id = cursor_CH_SBD.fetchone()[0]

	cursor_MTSZN.execute(f"""SELECT
 COLLECTIVE_CONTRACT_ID AS "COLLECTIVE_CONTRACT_ID"
, EMPLOYER_ID AS "EMPLOYER_ID"
, CONTRACT_STATUS_ID AS "CONTRACT_STATUS_ID"
, NVL(TO_CHAR("CONTRACT_ENTRY_DATE", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "CONTRACT_ENTRY_DATE"
, REG_NUMBER AS "REG_NUMBER"
, AUTO_REG_NUMBER AS "AUTO_REG_NUMBER"
, NVL(TO_CHAR("CONTRACT_EXPIRE_DATE", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "CONTRACT_EXPIRE_DATE"
, NVL(TO_CHAR("CONTRACT_REVIEW_DATE", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "CONTRACT_REVIEW_DATE"
, CONTRACT_REVIEW_RESULT AS "CONTRACT_REVIEW_RESULT"
, NVL(TO_CHAR("DATE_ENTRY", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "DATE_ENTRY"
, USER_ID AS "USER_ID"
, SDU_DATE AS "SDU_DATE"
FROM LABORPROTECT."C_SDU_COLLECTIVE_CONTRACT" where SDU_DATE > to_timestamp('{max_id}','YYYY-MM-DD HH24:MI:SS')
""")

	flag = True
	while flag:
		req_MTSZN = cursor_MTSZN.fetchmany(10000)
		if len(req_MTSZN) == 0:
			flag=False
		cursor_CH_SBD.executemany("""INSERT INTO MTSZN_LABORPROTECT.COLLECTIVE_CONTRACT (COLLECTIVE_CONTRACT_ID, EMPLOYER_ID, CONTRACT_STATUS_ID, CONTRACT_ENTRY_DATE, REG_NUMBER, AUTO_REG_NUMBER, CONTRACT_EXPIRE_DATE, CONTRACT_REVIEW_DATE, CONTRACT_REVIEW_RESULT, DATE_ENTRY, USER_ID, SDU_DATE) VALUES""", req_MTSZN)

	cursor_CH_SBD.execute(f"SELECT COUNT(*) FROM MTSZN_LABORPROTECT.COLLECTIVE_CONTRACT")
	sbd_count_rows_value = cursor_CH_SBD.fetchone()

	cursor_MTSZN.execute(f"SELECT COUNT(*) FROM LABORPROTECT.C_SDU_COLLECTIVE_CONTRACT")
	origin_count_rows_value = cursor_MTSZN.fetchone()

	a = 0
	if int(sbd_count_rows_value[0]) == int(origin_count_rows_value[0]):
		a = 1
	tuple_to_load = ("LABORPROTECT", "C_SDU_COLLECTIVE_CONTRACT", "TRUNC", a,  origin_count_rows_value[0],sbd_count_rows_value[0])
	list_to_load = [(tuple_to_load)]

	cursor_CH_SBD.executemany("INSERT INTO MTSZN_WMARKET.CHECK_LOAD (SCHEMA, TABLE, TYPE_OF_ETL, CORRECTLY_LOADED,TOTAL_COUNT_SOURCE,TOTAL_COUNT_SDU) VALUES", list_to_load)

	conn_MTSZN.close() 

def C_SDU_EMPLOYER():
	conn_CH_SBD_secrets=Connection.get_connection_from_secrets(conn_id="Clickhouse-5")#скрытый коннекшн 5го сервера
	conn_CH_SBD=connect(host=conn_CH_SBD_secrets.host,port=conn_CH_SBD_secrets.port,user=conn_CH_SBD_secrets.login, password=conn_CH_SBD_secrets.password, connect_timeout=3600)
	cursor_CH_SBD = conn_CH_SBD.cursor()
	conn_MTSZN_secrets=Connection.get_connection_from_secrets(conn_id="MTSZN_WMARKET")#скрытый коннекшн источника
	dsn_tns_secrets=cx_Oracle.makedsn(host=conn_MTSZN_secrets.host,port=conn_MTSZN_secrets.port, service_name='SD')
	conn_MTSZN = cx_Oracle.connect(user =conn_MTSZN_secrets.login, password =conn_MTSZN_secrets.password, dsn=dsn_tns_secrets)
	cursor_MTSZN = conn_MTSZN.cursor()

	cursor_CH_SBD.execute("SELECT COALESCE(toString(max(SDU_DATE)),'000-00-00 00:00:00') as MAX_SDU_DATE from MTSZN_LABORPROTECT.EMPLOYER")
	max_id = cursor_CH_SBD.fetchone()[0]

	cursor_MTSZN.execute(f"""SELECT
 INACTIVE_STATUS AS "INACTIVE_STATUS"
, CERT_SERIES AS "CERT_SERIES"
, NOTE AS "NOTE"
, FULL_RNAME AS "FULL_RNAME"
, FULL_KNAME AS "FULL_KNAME"
, FULL_ENAME AS "FULL_ENAME"
, FOUNDER_COUNT AS "FOUNDER_COUNT"
, SHORT_RNAME AS "SHORT_RNAME"
, SHORT_KNAME AS "SHORT_KNAME"
, SHORT_ENAME AS "SHORT_ENAME"
, WPLACE_COUNT AS "WPLACE_COUNT"
, STAFF_COUNT AS "STAFF_COUNT"
, FOUNDER_UL_COUNT AS "FOUNDER_UL_COUNT"
, FOUNDER_FL_COUNT AS "FOUNDER_FL_COUNT"
, CODE_IIN AS "CODE_IIN"
, CODE_BIN AS "CODE_BIN"
, NVL(TO_CHAR("REG_DATE", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "REG_DATE"
, NVL(TO_CHAR("LAST_REG_DATE", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "LAST_REG_DATE"
, NVL(TO_CHAR("STAT_ACTUAL_DATE", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "STAT_ACTUAL_DATE"
, NVL(TO_CHAR("OPEN_DATE", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "OPEN_DATE"
, NVL(TO_CHAR("CLOSE_DATE", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "CLOSE_DATE"
, NVL(TO_CHAR("DATE_ENTRY", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "DATE_ENTRY"
, CLS_BIN_RECIEVER AS "CLS_BIN_RECIEVER"
, ADDRESS_TXT AS "ADDRESS_TXT"
, ADDRESS_TXT_KZ AS "ADDRESS_TXT_KZ"
, CERT_NUM AS "CERT_NUM"
, CLS_ORD_NUM AS "CLS_ORD_NUM"
, PA_CARD_ID AS "PA_CARD_ID"
, EMP_PAR_ID AS "EMP_PAR_ID"
, EMP_ID AS "EMP_ID"
, ONE_CITIZENSHIP AS "ONE_CITIZENSHIP"
, ENTERPRISE_SUBJECT AS "ENTERPRISE_SUBJECT"
, FOREIGN_INVEST AS "FOREIGN_INVEST"
, BRANCHES_EXISTENCE AS "BRANCHES_EXISTENCE"
, INTERNATIONAL AS "INTERNATIONAL"
, COMMERCE_ORG AS "COMMERCE_ORG"
, AFFILIATED AS "AFFILIATED"
, STANDART_CHARTER AS "STANDART_CHARTER"
, USER_ID AS "USER_ID"
, SDU_DATE AS "SDU_DATE"
FROM LABORPROTECT."C_SDU_EMPLOYER" where SDU_DATE > to_timestamp('{max_id}','YYYY-MM-DD HH24:MI:SS')
""")

	flag = True
	while flag:
		req_MTSZN = cursor_MTSZN.fetchmany(10000)
		if len(req_MTSZN) == 0:
			flag=False
		cursor_CH_SBD.executemany("""INSERT INTO MTSZN_LABORPROTECT.EMPLOYER (INACTIVE_STATUS, CERT_SERIES, NOTE, FULL_RNAME, FULL_KNAME, FULL_ENAME, FOUNDER_COUNT, SHORT_RNAME, SHORT_KNAME, SHORT_ENAME, WPLACE_COUNT, STAFF_COUNT, FOUNDER_UL_COUNT, FOUNDER_FL_COUNT, CODE_IIN, CODE_BIN, REG_DATE, LAST_REG_DATE, STAT_ACTUAL_DATE, OPEN_DATE, CLOSE_DATE, DATE_ENTRY, CLS_BIN_RECIEVER, ADDRESS_TXT, ADDRESS_TXT_KZ, CERT_NUM, CLS_ORD_NUM, PA_CARD_ID, EMP_PAR_ID, EMP_ID, ONE_CITIZENSHIP, ENTERPRISE_SUBJECT, FOREIGN_INVEST, BRANCHES_EXISTENCE, INTERNATIONAL, COMMERCE_ORG, AFFILIATED, STANDART_CHARTER, USER_ID, SDU_DATE) VALUES""", req_MTSZN)

	cursor_CH_SBD.execute(f"SELECT COUNT(*) FROM MTSZN_LABORPROTECT.EMPLOYER")
	sbd_count_rows_value = cursor_CH_SBD.fetchone()

	cursor_MTSZN.execute(f"SELECT COUNT(*) FROM LABORPROTECT.C_SDU_EMPLOYER")
	origin_count_rows_value = cursor_MTSZN.fetchone()

	a = 0
	if int(sbd_count_rows_value[0]) == int(origin_count_rows_value[0]):
		a = 1
	tuple_to_load = ("LABORPROTECT", "C_SDU_EMPLOYER", "TRUNC", a,  origin_count_rows_value[0],sbd_count_rows_value[0])
	list_to_load = [(tuple_to_load)]

	cursor_CH_SBD.executemany("INSERT INTO MTSZN_WMARKET.CHECK_LOAD (SCHEMA, TABLE, TYPE_OF_ETL, CORRECTLY_LOADED,TOTAL_COUNT_SOURCE,TOTAL_COUNT_SDU) VALUES", list_to_load)

	conn_MTSZN.close() 

def C_SDU_EMP_ATTR():
	conn_CH_SBD_secrets=Connection.get_connection_from_secrets(conn_id="Clickhouse-5")#скрытый коннекшн 5го сервера
	conn_CH_SBD=connect(host=conn_CH_SBD_secrets.host,port=conn_CH_SBD_secrets.port,user=conn_CH_SBD_secrets.login, password=conn_CH_SBD_secrets.password, connect_timeout=3600)
	cursor_CH_SBD = conn_CH_SBD.cursor()
	conn_MTSZN_secrets=Connection.get_connection_from_secrets(conn_id="MTSZN_WMARKET")#скрытый коннекшн источника
	dsn_tns_secrets=cx_Oracle.makedsn(host=conn_MTSZN_secrets.host,port=conn_MTSZN_secrets.port, service_name='SD')
	conn_MTSZN = cx_Oracle.connect(user =conn_MTSZN_secrets.login, password =conn_MTSZN_secrets.password, dsn=dsn_tns_secrets)
	cursor_MTSZN = conn_MTSZN.cursor()

	cursor_CH_SBD.execute("SELECT COALESCE(toString(max(SDU_DATE)),'000-00-00 00:00:00') as MAX_SDU_DATE from MTSZN_LABORPROTECT.EMP_ATTR")
	max_id = cursor_CH_SBD.fetchone()[0]

	cursor_MTSZN.execute(f"""SELECT
 PET_ID AS "PET_ID"
, FUND_TYPE_ID AS "FUND_TYPE_ID"
, GOV_COMP_TYPE_ID AS "GOV_COMP_TYPE_ID"
, STATUS_OO_ID AS "STATUS_OO_ID"
, SECTOR_ID AS "SECTOR_ID"
, EMP_STAT_ACTIVITY_ID AS "EMP_STAT_ACTIVITY_ID"
, STAT_OKED_ID AS "STAT_OKED_ID"
, OKPO_ID AS "OKPO_ID"
, OKED_ID AS "OKED_ID"
, CREATE_METHOD_ID AS "CREATE_METHOD_ID"
, KFS_ID AS "KFS_ID"
, KRP_ID AS "KRP_ID"
, KOPF_ID AS "KOPF_ID"
, WORK_TYPE_ID AS "WORK_TYPE_ID"
, EMP_TYPE_ID AS "EMP_TYPE_ID"
, EMP_REORG_TYPE_ID AS "EMP_REORG_TYPE_ID"
, VENDOR_ID AS "VENDOR_ID"
, WORK_STAT_ID AS "WORK_STAT_ID"
, CLS_STAT_ID AS "CLS_STAT_ID"
, CLS_TYPE_ID AS "CLS_TYPE_ID"
, EMP_DEBT_ID AS "EMP_DEBT_ID"
, EMP_ID AS "EMP_ID"
, INSTITUTE_ID AS "INSTITUTE_ID"
, EMP_ATTR_ID AS "EMP_ATTR_ID"
, NVL(TO_CHAR("DATE_FROM", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "DATE_FROM"
, NVL(TO_CHAR("DATE_TO", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "DATE_TO"
, NVL(TO_CHAR("DATE_ENTRY", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "DATE_ENTRY"
, USER_ID AS "USER_ID"
, SDU_DATE AS "SDU_DATE"
FROM LABORPROTECT."C_SDU_EMP_ATTR" where SDU_DATE > to_timestamp('{max_id}','YYYY-MM-DD HH24:MI:SS')
""")

	flag = True
	while flag:
		req_MTSZN = cursor_MTSZN.fetchmany(10000)
		if len(req_MTSZN) == 0:
			flag=False
		cursor_CH_SBD.executemany("""INSERT INTO MTSZN_LABORPROTECT.EMP_ATTR (PET_ID, FUND_TYPE_ID, GOV_COMP_TYPE_ID, STATUS_OO_ID, SECTOR_ID, EMP_STAT_ACTIVITY_ID, STAT_OKED_ID, OKPO_ID, OKED_ID, CREATE_METHOD_ID, KFS_ID, KRP_ID, KOPF_ID, WORK_TYPE_ID, EMP_TYPE_ID, EMP_REORG_TYPE_ID, VENDOR_ID, WORK_STAT_ID, CLS_STAT_ID, CLS_TYPE_ID, EMP_DEBT_ID, EMP_ID, INSTITUTE_ID, EMP_ATTR_ID, DATE_FROM, DATE_TO, DATE_ENTRY, USER_ID, SDU_DATE) VALUES""", req_MTSZN)

	cursor_CH_SBD.execute(f"SELECT COUNT(*) FROM MTSZN_LABORPROTECT.EMP_ATTR")
	sbd_count_rows_value = cursor_CH_SBD.fetchone()

	cursor_MTSZN.execute(f"SELECT COUNT(*) FROM LABORPROTECT.C_SDU_EMP_ATTR")
	origin_count_rows_value = cursor_MTSZN.fetchone()

	a = 0
	if int(sbd_count_rows_value[0]) == int(origin_count_rows_value[0]):
		a = 1
	tuple_to_load = ("LABORPROTECT", "C_SDU_EMP_ATTR", "TRUNC", a,  origin_count_rows_value[0],sbd_count_rows_value[0])
	list_to_load = [(tuple_to_load)]

	cursor_CH_SBD.executemany("INSERT INTO MTSZN_WMARKET.CHECK_LOAD (SCHEMA, TABLE, TYPE_OF_ETL, CORRECTLY_LOADED,TOTAL_COUNT_SOURCE,TOTAL_COUNT_SDU) VALUES", list_to_load)

	conn_MTSZN.close() 

def C_SDU_EMP_CONTACT():
	conn_CH_SBD_secrets=Connection.get_connection_from_secrets(conn_id="Clickhouse-5")#скрытый коннекшн 5го сервера
	conn_CH_SBD=connect(host=conn_CH_SBD_secrets.host,port=conn_CH_SBD_secrets.port,user=conn_CH_SBD_secrets.login, password=conn_CH_SBD_secrets.password, connect_timeout=3600)
	cursor_CH_SBD = conn_CH_SBD.cursor()
	conn_MTSZN_secrets=Connection.get_connection_from_secrets(conn_id="MTSZN_WMARKET")#скрытый коннекшн источника
	dsn_tns_secrets=cx_Oracle.makedsn(host=conn_MTSZN_secrets.host,port=conn_MTSZN_secrets.port, service_name='SD')
	conn_MTSZN = cx_Oracle.connect(user =conn_MTSZN_secrets.login, password =conn_MTSZN_secrets.password, dsn=dsn_tns_secrets)
	cursor_MTSZN = conn_MTSZN.cursor()

	cursor_CH_SBD.execute("SELECT COALESCE(toString(max(SDU_DATE)),'000-00-00 00:00:00') as MAX_SDU_DATE from MTSZN_LABORPROTECT.EMP_CONTACT")
	max_id = cursor_CH_SBD.fetchone()[0]

	cursor_MTSZN.execute(f"""SELECT
 EMAIL AS "EMAIL"
, LAST_NAME AS "LAST_NAME"
, PHONE_NUM AS "PHONE_NUM"
, PARENT_NAME AS "PARENT_NAME"
, MOB_PHONE_NUM AS "MOB_PHONE_NUM"
, FIRST_NAME AS "FIRST_NAME"
, ADD_PHONE_NUM AS "ADD_PHONE_NUM"
, EMP_ID AS "EMP_ID"
, POSITION_TYPE_ID AS "POSITION_TYPE_ID"
, CONTACT_ID AS "CONTACT_ID"
, NVL(TO_CHAR("DATE_FROM", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "DATE_FROM"
, NVL(TO_CHAR("DATE_TO", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "DATE_TO"
, NOTE AS "NOTE"
, USER_ID AS "USER_ID"
, NVL(TO_CHAR("DATE_ENTRY", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "DATE_ENTRY"
, SDU_DATE AS "SDU_DATE"
FROM LABORPROTECT."C_SDU_EMP_CONTACT" where SDU_DATE > to_timestamp('{max_id}','YYYY-MM-DD HH24:MI:SS')
""")

	flag = True
	while flag:
		req_MTSZN = cursor_MTSZN.fetchmany(10000)
		if len(req_MTSZN) == 0:
			flag=False
		cursor_CH_SBD.executemany("""INSERT INTO MTSZN_LABORPROTECT.EMP_CONTACT (EMAIL, LAST_NAME, PHONE_NUM, PARENT_NAME, MOB_PHONE_NUM, FIRST_NAME, ADD_PHONE_NUM, EMP_ID, POSITION_TYPE_ID, CONTACT_ID, DATE_FROM, DATE_TO, NOTE, USER_ID, DATE_ENTRY, SDU_DATE) VALUES""", req_MTSZN)

	cursor_CH_SBD.execute(f"SELECT COUNT(*) FROM MTSZN_LABORPROTECT.EMP_CONTACT")
	sbd_count_rows_value = cursor_CH_SBD.fetchone()

	cursor_MTSZN.execute(f"SELECT COUNT(*) FROM LABORPROTECT.C_SDU_EMP_CONTACT")
	origin_count_rows_value = cursor_MTSZN.fetchone()

	a = 0
	if int(sbd_count_rows_value[0]) == int(origin_count_rows_value[0]):
		a = 1
	tuple_to_load = ("LABORPROTECT", "C_SDU_EMP_CONTACT", "TRUNC", a,  origin_count_rows_value[0],sbd_count_rows_value[0])
	list_to_load = [(tuple_to_load)]

	cursor_CH_SBD.executemany("INSERT INTO MTSZN_WMARKET.CHECK_LOAD (SCHEMA, TABLE, TYPE_OF_ETL, CORRECTLY_LOADED,TOTAL_COUNT_SOURCE,TOTAL_COUNT_SDU) VALUES", list_to_load)

	conn_MTSZN.close() 

def C_SDU_EMP_FOUNDER():
	conn_CH_SBD_secrets=Connection.get_connection_from_secrets(conn_id="Clickhouse-5")#скрытый коннекшн 5го сервера
	conn_CH_SBD=connect(host=conn_CH_SBD_secrets.host,port=conn_CH_SBD_secrets.port,user=conn_CH_SBD_secrets.login, password=conn_CH_SBD_secrets.password, connect_timeout=3600)
	cursor_CH_SBD = conn_CH_SBD.cursor()
	conn_MTSZN_secrets=Connection.get_connection_from_secrets(conn_id="MTSZN_WMARKET")#скрытый коннекшн источника
	dsn_tns_secrets=cx_Oracle.makedsn(host=conn_MTSZN_secrets.host,port=conn_MTSZN_secrets.port, service_name='SD')
	conn_MTSZN = cx_Oracle.connect(user =conn_MTSZN_secrets.login, password =conn_MTSZN_secrets.password, dsn=dsn_tns_secrets)
	cursor_MTSZN = conn_MTSZN.cursor()

	cursor_CH_SBD.execute("SELECT COALESCE(toString(max(SDU_DATE)),'000-00-00 00:00:00') as MAX_SDU_DATE from MTSZN_LABORPROTECT.EMP_FOUNDER")
	max_id = cursor_CH_SBD.fetchone()[0]

	cursor_MTSZN.execute(f"""SELECT
 CHIEF_LAST_NAME AS "CHIEF_LAST_NAME"
, PHONE_NUM AS "PHONE_NUM"
, COUNTRY_ID AS "COUNTRY_ID"
, FULL_RNAME AS "FULL_RNAME"
, FULL_KNAME AS "FULL_KNAME"
, CHIEF_PARENT_NAME AS "CHIEF_PARENT_NAME"
, FULL_ENAME AS "FULL_ENAME"
, CODE_BIN AS "CODE_BIN"
, CHIEF_FIRST_NAME AS "CHIEF_FIRST_NAME"
, CODE_IIN AS "CODE_IIN"
, NVL(TO_CHAR("DATE_FROM", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "DATE_FROM"
, NVL(TO_CHAR("DATE_TO", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "DATE_TO"
, NVL(TO_CHAR("REG_DATE", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "REG_DATE"
, NVL(TO_CHAR("LAST_REG_DATE", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "LAST_REG_DATE"
, NVL(TO_CHAR("DATE_ENTRY", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "DATE_ENTRY"
, OKED_ID AS "OKED_ID"
, ADDRESS_TXT AS "ADDRESS_TXT"
, FND_EMP_ID AS "FND_EMP_ID"
, FND_PA_CARD_ID AS "FND_PA_CARD_ID"
, FOUNDER_ID AS "FOUNDER_ID"
, PROFESSION_ID AS "PROFESSION_ID"
, EMP_ID AS "EMP_ID"
, POSITION_TYPE_ID AS "POSITION_TYPE_ID"
, USER_ID AS "USER_ID"
, NOTE AS "NOTE"
, SDU_DATE AS "SDU_DATE"
FROM LABORPROTECT."C_SDU_EMP_FOUNDER" where SDU_DATE > to_timestamp('{max_id}','YYYY-MM-DD HH24:MI:SS')
""")

	flag = True
	while flag:
		req_MTSZN = cursor_MTSZN.fetchmany(10000)
		if len(req_MTSZN) == 0:
			flag=False
		cursor_CH_SBD.executemany("""INSERT INTO MTSZN_LABORPROTECT.EMP_FOUNDER (CHIEF_LAST_NAME, PHONE_NUM, COUNTRY_ID, FULL_RNAME, FULL_KNAME, CHIEF_PARENT_NAME, FULL_ENAME, CODE_BIN, CHIEF_FIRST_NAME, CODE_IIN, DATE_FROM, DATE_TO, REG_DATE, LAST_REG_DATE, DATE_ENTRY, OKED_ID, ADDRESS_TXT, FND_EMP_ID, FND_PA_CARD_ID, FOUNDER_ID, PROFESSION_ID, EMP_ID, POSITION_TYPE_ID, USER_ID, NOTE, SDU_DATE) VALUES""", req_MTSZN)

	cursor_CH_SBD.execute(f"SELECT COUNT(*) FROM MTSZN_LABORPROTECT.EMP_FOUNDER")
	sbd_count_rows_value = cursor_CH_SBD.fetchone()

	cursor_MTSZN.execute(f"SELECT COUNT(*) FROM LABORPROTECT.C_SDU_EMP_FOUNDER")
	origin_count_rows_value = cursor_MTSZN.fetchone()

	a = 0
	if int(sbd_count_rows_value[0]) == int(origin_count_rows_value[0]):
		a = 1
	tuple_to_load = ("LABORPROTECT", "C_SDU_EMP_FOUNDER", "TRUNC", a,  origin_count_rows_value[0],sbd_count_rows_value[0])
	list_to_load = [(tuple_to_load)]

	cursor_CH_SBD.executemany("INSERT INTO MTSZN_WMARKET.CHECK_LOAD (SCHEMA, TABLE, TYPE_OF_ETL, CORRECTLY_LOADED,TOTAL_COUNT_SOURCE,TOTAL_COUNT_SDU) VALUES", list_to_load)

	conn_MTSZN.close() 

def C_SDU_EMP_LEADER():
	conn_CH_SBD_secrets=Connection.get_connection_from_secrets(conn_id="Clickhouse-5")#скрытый коннекшн 5го сервера
	conn_CH_SBD=connect(host=conn_CH_SBD_secrets.host,port=conn_CH_SBD_secrets.port,user=conn_CH_SBD_secrets.login, password=conn_CH_SBD_secrets.password, connect_timeout=3600)
	cursor_CH_SBD = conn_CH_SBD.cursor()
	conn_MTSZN_secrets=Connection.get_connection_from_secrets(conn_id="MTSZN_WMARKET")#скрытый коннекшн источника
	dsn_tns_secrets=cx_Oracle.makedsn(host=conn_MTSZN_secrets.host,port=conn_MTSZN_secrets.port, service_name='SD')
	conn_MTSZN = cx_Oracle.connect(user =conn_MTSZN_secrets.login, password =conn_MTSZN_secrets.password, dsn=dsn_tns_secrets)
	cursor_MTSZN = conn_MTSZN.cursor()

	cursor_CH_SBD.execute("SELECT COALESCE(toString(max(SDU_DATE)),'000-00-00 00:00:00') as MAX_SDU_DATE from MTSZN_LABORPROTECT.EMP_LEADER")
	max_id = cursor_CH_SBD.fetchone()[0]

	cursor_MTSZN.execute(f"""SELECT
 LAST_NAME AS "LAST_NAME"
, PARENT_NAME AS "PARENT_NAME"
, FIRST_NAME AS "FIRST_NAME"
, CODE_IIN AS "CODE_IIN"
, NVL(TO_CHAR("ASSIGNMENT_DATE", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "ASSIGNMENT_DATE"
, NVL(TO_CHAR("DISMISSAL_DATE", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "DISMISSAL_DATE"
, LIVING_COUNTRY_ID AS "LIVING_COUNTRY_ID"
, EMP_ID AS "EMP_ID"
, NATIONALITY_ID AS "NATIONALITY_ID"
, POSITION_TYPE_ID AS "POSITION_TYPE_ID"
, CITIZENSHIP_COUNTRY_ID AS "CITIZENSHIP_COUNTRY_ID"
, EMP_LEADER_ID AS "EMP_LEADER_ID"
, NVL(TO_CHAR("DATE_ENTRY", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "DATE_ENTRY"
, NVL(TO_CHAR("DATE_FROM", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "DATE_FROM"
, NVL(TO_CHAR("DATE_TO", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "DATE_TO"
, USER_ID AS "USER_ID"
, SDU_DATE AS "SDU_DATE"
FROM LABORPROTECT."C_SDU_EMP_LEADER" where SDU_DATE > to_timestamp('{max_id}','YYYY-MM-DD HH24:MI:SS')
""")

	flag = True
	while flag:
		req_MTSZN = cursor_MTSZN.fetchmany(10000)
		if len(req_MTSZN) == 0:
			flag=False
		cursor_CH_SBD.executemany("""INSERT INTO MTSZN_LABORPROTECT.EMP_LEADER (LAST_NAME, PARENT_NAME, FIRST_NAME, CODE_IIN, ASSIGNMENT_DATE, DISMISSAL_DATE, LIVING_COUNTRY_ID, EMP_ID, NATIONALITY_ID, POSITION_TYPE_ID, CITIZENSHIP_COUNTRY_ID, EMP_LEADER_ID, DATE_ENTRY, DATE_FROM, DATE_TO, USER_ID, SDU_DATE) VALUES""", req_MTSZN)

	cursor_CH_SBD.execute(f"SELECT COUNT(*) FROM MTSZN_LABORPROTECT.EMP_LEADER")
	sbd_count_rows_value = cursor_CH_SBD.fetchone()

	cursor_MTSZN.execute(f"SELECT COUNT(*) FROM LABORPROTECT.C_SDU_EMP_LEADER")
	origin_count_rows_value = cursor_MTSZN.fetchone()

	a = 0
	if int(sbd_count_rows_value[0]) == int(origin_count_rows_value[0]):
		a = 1
	tuple_to_load = ("LABORPROTECT", "C_SDU_EMP_LEADER", "TRUNC", a,  origin_count_rows_value[0],sbd_count_rows_value[0])
	list_to_load = [(tuple_to_load)]

	cursor_CH_SBD.executemany("INSERT INTO MTSZN_WMARKET.CHECK_LOAD (SCHEMA, TABLE, TYPE_OF_ETL, CORRECTLY_LOADED,TOTAL_COUNT_SOURCE,TOTAL_COUNT_SDU) VALUES", list_to_load)

	conn_MTSZN.close() 

def C_SDU_INVEST_BASIS():
	conn_CH_SBD_secrets=Connection.get_connection_from_secrets(conn_id="Clickhouse-5")#скрытый коннекшн 5го сервера
	conn_CH_SBD=connect(host=conn_CH_SBD_secrets.host,port=conn_CH_SBD_secrets.port,user=conn_CH_SBD_secrets.login, password=conn_CH_SBD_secrets.password, connect_timeout=3600)
	cursor_CH_SBD = conn_CH_SBD.cursor()
	conn_MTSZN_secrets=Connection.get_connection_from_secrets(conn_id="MTSZN_WMARKET")#скрытый коннекшн источника
	dsn_tns_secrets=cx_Oracle.makedsn(host=conn_MTSZN_secrets.host,port=conn_MTSZN_secrets.port, service_name='SD')
	conn_MTSZN = cx_Oracle.connect(user =conn_MTSZN_secrets.login, password =conn_MTSZN_secrets.password, dsn=dsn_tns_secrets)
	cursor_MTSZN = conn_MTSZN.cursor()

	cursor_CH_SBD.execute("SELECT COALESCE(toString(max(SDU_DATE)),'000-00-00 00:00:00') as MAX_SDU_DATE from MTSZN_LABORPROTECT.INVEST_BASIS")
	max_id = cursor_CH_SBD.fetchone()[0]

	cursor_MTSZN.execute(f"""SELECT
NVL(TO_CHAR("AGREEMENT_BEGIN_DATE", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "AGREEMENT_BEGIN_DATE"
, NVL(TO_CHAR("AGREEMENT_DATE", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "AGREEMENT_DATE"
, AGREEMENT_EMP_TYPE AS "AGREEMENT_EMP_TYPE"
, NVL(TO_CHAR("AGREEMENT_END_DATE", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "AGREEMENT_END_DATE"
, AGREEMENT_NUM AS "AGREEMENT_NUM"
, COLLECTIVE_COMPLAINT_STATUS AS "COLLECTIVE_COMPLAINT_STATUS"
, D_INSPECTION_ID AS "D_INSPECTION_ID"
, D_INVEST_BASIS_ID AS "D_INVEST_BASIS_ID"
, NVL(TO_CHAR("DATE_ENTRY", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "DATE_ENTRY"
, NVL(TO_CHAR("DISCARD_DATE", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "DISCARD_DATE"
, NVL(TO_CHAR("DKSZ_REG_DATE", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "DKSZ_REG_DATE"
, DKSZ_REG_NUM AS "DKSZ_REG_NUM"
, NVL(TO_CHAR("DOC_FEED_DATE", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "DOC_FEED_DATE"
, DOC_NUM AS "DOC_NUM"
, EMP_ID AS "EMP_ID"
, GIT_ID AS "GIT_ID"
, INITIATOR AS "INITIATOR"
, NVL(TO_CHAR("INSPECT_PERIOD_BEGIN_DATE", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "INSPECT_PERIOD_BEGIN_DATE"
, NVL(TO_CHAR("INSPECT_PERIOD_END_DATE", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "INSPECT_PERIOD_END_DATE"
, NVL(TO_CHAR("INSPECTION_BEGIN_DATE", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "INSPECTION_BEGIN_DATE"
, NVL(TO_CHAR("INSPECTION_END_DATE", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "INSPECTION_END_DATE"
, INSPECTION_STATUS AS "INSPECTION_STATUS"
, INVEST_BASIS_ID AS "INVEST_BASIS_ID"
, INVEST_BASIS_NUM AS "INVEST_BASIS_NUM"
, NVL(TO_CHAR("MOIT_CHECK_DATE", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "MOIT_CHECK_DATE"
, NVL(TO_CHAR("MOIT_REG_DATE", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "MOIT_REG_DATE"
, MOIT_REG_NUM AS "MOIT_REG_NUM"
, NVL(TO_CHAR("NOTIFICATION_DATE", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "NOTIFICATION_DATE"
, NOTIFICATION_NUM AS "NOTIFICATION_NUM"
, NVL(TO_CHAR("ORDER_EXEC_DATE", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "ORDER_EXEC_DATE"
, ORGAN_NAME AS "ORGAN_NAME"
, PA_CARD_ID AS "PA_CARD_ID"
, POLICE_ADDRESS AS "POLICE_ADDRESS"
, NVL(TO_CHAR("POLICE_DIRECT_DATE", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "POLICE_DIRECT_DATE"
, POLICE_DOC_NUM AS "POLICE_DOC_NUM"
, NVL(TO_CHAR("POLICE_REG_DATE", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "POLICE_REG_DATE"
, NVL(TO_CHAR("PROCESS_DECISION_DATE", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "PROCESS_DECISION_DATE"
, PROSECUTOR_ADDRESS AS "PROSECUTOR_ADDRESS"
, NVL(TO_CHAR("PROSECUTOR_DIRECT_DATE", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "PROSECUTOR_DIRECT_DATE"
, PROSECUTOR_DOC_NUM AS "PROSECUTOR_DOC_NUM"
, NVL(TO_CHAR("PROSECUTOR_REG_DATE", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "PROSECUTOR_REG_DATE"
, REFUSE_STATUS AS "REFUSE_STATUS"
, REG_NUM AS "REG_NUM"
, NVL(TO_CHAR("REG_REFUSE_DATE", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "REG_REFUSE_DATE"
, REGION_ID AS "REGION_ID"
, NVL(TO_CHAR("UKPSSU_CHECK_DATE", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "UKPSSU_CHECK_DATE"
, NVL(TO_CHAR("UKPSSU_GIVE_DATE", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "UKPSSU_GIVE_DATE"
, NVL(TO_CHAR("UKPSSU_REG_DATE", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "UKPSSU_REG_DATE"
, UKPSSU_REG_NUM AS "UKPSSU_REG_NUM"
, USER_ID AS "USER_ID"
, SDU_DATE AS "SDU_DATE"
FROM LABORPROTECT."C_SDU_INVEST_BASIS" where SDU_DATE > to_timestamp('{max_id}','YYYY-MM-DD HH24:MI:SS')
""")

	flag = True
	while flag:
		req_MTSZN = cursor_MTSZN.fetchmany(10000)
		if len(req_MTSZN) == 0:
			flag=False
		cursor_CH_SBD.executemany("""INSERT INTO MTSZN_LABORPROTECT.INVEST_BASIS (AGREEMENT_BEGIN_DATE, AGREEMENT_DATE, AGREEMENT_EMP_TYPE, AGREEMENT_END_DATE, AGREEMENT_NUM, COLLECTIVE_COMPLAINT_STATUS, D_INSPECTION_ID, D_INVEST_BASIS_ID, DATE_ENTRY, DISCARD_DATE, DKSZ_REG_DATE, DKSZ_REG_NUM, DOC_FEED_DATE, DOC_NUM, EMP_ID, GIT_ID, INITIATOR, INSPECT_PERIOD_BEGIN_DATE, INSPECT_PERIOD_END_DATE, INSPECTION_BEGIN_DATE, INSPECTION_END_DATE, INSPECTION_STATUS, INVEST_BASIS_ID, INVEST_BASIS_NUM, MOIT_CHECK_DATE, MOIT_REG_DATE, MOIT_REG_NUM, NOTIFICATION_DATE, NOTIFICATION_NUM, ORDER_EXEC_DATE, ORGAN_NAME, PA_CARD_ID, POLICE_ADDRESS, POLICE_DIRECT_DATE, POLICE_DOC_NUM, POLICE_REG_DATE, PROCESS_DECISION_DATE, PROSECUTOR_ADDRESS, PROSECUTOR_DIRECT_DATE, PROSECUTOR_DOC_NUM, PROSECUTOR_REG_DATE, REFUSE_STATUS, REG_NUM, REG_REFUSE_DATE, REGION_ID, UKPSSU_CHECK_DATE, UKPSSU_GIVE_DATE, UKPSSU_REG_DATE, UKPSSU_REG_NUM, USER_ID, SDU_DATE) VALUES""", req_MTSZN)

	cursor_CH_SBD.execute(f"SELECT COUNT(*) FROM MTSZN_LABORPROTECT.INVEST_BASIS")
	sbd_count_rows_value = cursor_CH_SBD.fetchone()

	cursor_MTSZN.execute(f"SELECT COUNT(*) FROM LABORPROTECT.C_SDU_INVEST_BASIS")
	origin_count_rows_value = cursor_MTSZN.fetchone()

	a = 0
	if int(sbd_count_rows_value[0]) == int(origin_count_rows_value[0]):
		a = 1
	tuple_to_load = ("LABORPROTECT", "C_SDU_INVEST_BASIS", "TRUNC", a,  origin_count_rows_value[0],sbd_count_rows_value[0])
	list_to_load = [(tuple_to_load)]

	cursor_CH_SBD.executemany("INSERT INTO MTSZN_WMARKET.CHECK_LOAD (SCHEMA, TABLE, TYPE_OF_ETL, CORRECTLY_LOADED,TOTAL_COUNT_SOURCE,TOTAL_COUNT_SDU) VALUES", list_to_load)

	conn_MTSZN.close() 

def C_SDU_LABOR_DISPUTE():
	conn_CH_SBD_secrets=Connection.get_connection_from_secrets(conn_id="Clickhouse-5")#скрытый коннекшн 5го сервера
	conn_CH_SBD=connect(host=conn_CH_SBD_secrets.host,port=conn_CH_SBD_secrets.port,user=conn_CH_SBD_secrets.login, password=conn_CH_SBD_secrets.password, connect_timeout=3600)
	cursor_CH_SBD = conn_CH_SBD.cursor()
	conn_MTSZN_secrets=Connection.get_connection_from_secrets(conn_id="MTSZN_WMARKET")#скрытый коннекшн источника
	dsn_tns_secrets=cx_Oracle.makedsn(host=conn_MTSZN_secrets.host,port=conn_MTSZN_secrets.port, service_name='SD')
	conn_MTSZN = cx_Oracle.connect(user =conn_MTSZN_secrets.login, password =conn_MTSZN_secrets.password, dsn=dsn_tns_secrets)
	cursor_MTSZN = conn_MTSZN.cursor()

	cursor_CH_SBD.execute("SELECT COALESCE(toString(max(SDU_DATE)),'000-00-00 00:00:00') as MAX_SDU_DATE from MTSZN_LABORPROTECT.LABOR_DISPUTE")
	max_id = cursor_CH_SBD.fetchone()[0]

	cursor_MTSZN.execute(f"""SELECT
 ARBITR_ID AS "ARBITR_ID"
, ARBITR_REVIEW_RESULT AS "ARBITR_REVIEW_RESULT"
, NVL(TO_CHAR("COMMIS_CREATE_DATE", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "COMMIS_CREATE_DATE"
, COMMIS_DECISION AS "COMMIS_DECISION"
, COMMIS_ID AS "COMMIS_ID"
, COUNT_WORK AS "COUNT_WORK"
, NVL(TO_CHAR("DATE_ENTRY", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "DATE_ENTRY"
, EMP_ID AS "EMP_ID"
, NVL(TO_CHAR("LABOR_ARBITR_CREATE_DATE", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "LABOR_ARBITR_CREATE_DATE"
, LABOR_DISPUTE_ID AS "LABOR_DISPUTE_ID"
, MEDIATOR_ID AS "MEDIATOR_ID"
, NVL(TO_CHAR("MEDIATOR_REVIEW_DATE", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "MEDIATOR_REVIEW_DATE"
, MEDIATOR_REVIEW_RESULT AS "MEDIATOR_REVIEW_RESULT"
, NVL(TO_CHAR("NOTIFICATION_DATE", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "NOTIFICATION_DATE"
, NVL(TO_CHAR("REQ_REVIEW_DATE", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "REQ_REVIEW_DATE"
, REQ_REVIEW_RESULT AS "REQ_REVIEW_RESULT"
, REQUIREMENT AS "REQUIREMENT"
, REQUIREMENT_ID AS "REQUIREMENT_ID"
, NVL(TO_CHAR("STRIKE_DATE", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "STRIKE_DATE"
, STRIKE_ID AS "STRIKE_ID"
, STRIKE_RESULT AS "STRIKE_RESULT"
, USER_ID AS "USER_ID"
, SDU_DATE AS "SDU_DATE"
FROM LABORPROTECT."C_SDU_LABOR_DISPUTE" where SDU_DATE > to_timestamp('{max_id}','YYYY-MM-DD HH24:MI:SS')
""")

	flag = True
	while flag:
		req_MTSZN = cursor_MTSZN.fetchmany(10000)
		if len(req_MTSZN) == 0:
			flag=False
		cursor_CH_SBD.executemany("""INSERT INTO MTSZN_LABORPROTECT.LABOR_DISPUTE (ARBITR_ID, ARBITR_REVIEW_RESULT, COMMIS_CREATE_DATE, COMMIS_DECISION, COMMIS_ID, COUNT_WORK, DATE_ENTRY, EMP_ID, LABOR_ARBITR_CREATE_DATE, LABOR_DISPUTE_ID, MEDIATOR_ID, MEDIATOR_REVIEW_DATE, MEDIATOR_REVIEW_RESULT, NOTIFICATION_DATE, REQ_REVIEW_DATE, REQ_REVIEW_RESULT, REQUIREMENT, REQUIREMENT_ID, STRIKE_DATE, STRIKE_ID, STRIKE_RESULT, USER_ID, SDU_DATE) VALUES""", req_MTSZN)

	cursor_CH_SBD.execute(f"SELECT COUNT(*) FROM MTSZN_LABORPROTECT.LABOR_DISPUTE")
	sbd_count_rows_value = cursor_CH_SBD.fetchone()

	cursor_MTSZN.execute(f"SELECT COUNT(*) FROM LABORPROTECT.C_SDU_LABOR_DISPUTE")
	origin_count_rows_value = cursor_MTSZN.fetchone()

	a = 0
	if int(sbd_count_rows_value[0]) == int(origin_count_rows_value[0]):
		a = 1
	tuple_to_load = ("LABORPROTECT", "C_SDU_LABOR_DISPUTE", "TRUNC", a,  origin_count_rows_value[0],sbd_count_rows_value[0])
	list_to_load = [(tuple_to_load)]

	cursor_CH_SBD.executemany("INSERT INTO MTSZN_WMARKET.CHECK_LOAD (SCHEMA, TABLE, TYPE_OF_ETL, CORRECTLY_LOADED,TOTAL_COUNT_SOURCE,TOTAL_COUNT_SDU) VALUES", list_to_load)

	conn_MTSZN.close() 

def C_SDU_LABOR_RISK():
	conn_CH_SBD_secrets=Connection.get_connection_from_secrets(conn_id="Clickhouse-5")#скрытый коннекшн 5го сервера
	conn_CH_SBD=connect(host=conn_CH_SBD_secrets.host,port=conn_CH_SBD_secrets.port,user=conn_CH_SBD_secrets.login, password=conn_CH_SBD_secrets.password, connect_timeout=3600)
	cursor_CH_SBD = conn_CH_SBD.cursor()
	conn_MTSZN_secrets=Connection.get_connection_from_secrets(conn_id="MTSZN_WMARKET")#скрытый коннекшн источника
	dsn_tns_secrets=cx_Oracle.makedsn(host=conn_MTSZN_secrets.host,port=conn_MTSZN_secrets.port, service_name='SD')
	conn_MTSZN = cx_Oracle.connect(user =conn_MTSZN_secrets.login, password =conn_MTSZN_secrets.password, dsn=dsn_tns_secrets)
	cursor_MTSZN = conn_MTSZN.cursor()

	cursor_CH_SBD.execute("SELECT COALESCE(toString(max(SDU_DATE)),'000-00-00 00:00:00') as MAX_SDU_DATE from MTSZN_LABORPROTECT.LABOR_RISK")
	max_id = cursor_CH_SBD.fetchone()[0]

	cursor_MTSZN.execute(f"""SELECT
 ECON_BALL AS "ECON_BALL"
, ECON_CURRENCY AS "ECON_CURRENCY"
, ECON_DEBT_OPV AS "ECON_DEBT_OPV"
, ECON_DEBT_SALARY AS "ECON_DEBT_SALARY"
, ECON_EMP_HOLIDAY AS "ECON_EMP_HOLIDAY"
, ECON_EMP_MASSTHREAT AS "ECON_EMP_MASSTHREAT"
, ECON_EMP_MERGE AS "ECON_EMP_MERGE"
, ECON_EMP_PLAIN AS "ECON_EMP_PLAIN"
, ECON_EMP_REGMODE AS "ECON_EMP_REGMODE"
, ECON_EMP_WORKDAY AS "ECON_EMP_WORKDAY"
, ECON_EMP_WORKPLACE AS "ECON_EMP_WORKPLACE"
, ECON_IN_BALL AS "ECON_IN_BALL"
, ECON_OUT_BALL AS "ECON_OUT_BALL"
, ECON_PRICE_PRODUCT AS "ECON_PRICE_PRODUCT"
, REG_NUM AS "REG_NUM"
, SOC_CONT_NONCOM AS "SOC_CONT_NONCOM"
, SOC_CONTACT AS "SOC_CONTACT"
, SOC_DIS_SALARY AS "SOC_DIS_SALARY"
, SOC_DIS_WORK AS "SOC_DIS_WORK"
, SOC_LEVEL_CRIME AS "SOC_LEVEL_CRIME"
, SOC_LEVEL_UNEMP AS "SOC_LEVEL_UNEMP"
, SOC_LIVING_COST AS "SOC_LIVING_COST"
, SOC_NONCOMPLIANCE AS "SOC_NONCOMPLIANCE"
, SOC_NOTSTABILITY AS "SOC_NOTSTABILITY"
, SOC_STABILITY AS "SOC_STABILITY"
, WORK_ARBITR AS "WORK_ARBITR"
, WORK_COL AS "WORK_COL"
, WORK_COMMIS AS "WORK_COMMIS"
, WORK_CONF_COMPLAINT AS "WORK_CONF_COMPLAINT"
, WORK_FOUR AS "WORK_FOUR"
, WORK_IND_COMPLAINT AS "WORK_IND_COMPLAINT"
, WORK_N1 AS "WORK_N1"
, WORK_N1_GROUP AS "WORK_N1_GROUP"
, WORK_PARTICIPA AS "WORK_PARTICIPA"
, WORK_PROTEST AS "WORK_PROTEST"
, WORK_SEVEN AS "WORK_SEVEN"
, WORK_TEAM_COMPLAINT AS "WORK_TEAM_COMPLAINT"
, WORK_THREE AS "WORK_THREE"
, SDU_DATE AS "SDU_DATE"
FROM LABORPROTECT."C_SDU_LABOR_RISK" where SDU_DATE > to_timestamp('{max_id}','YYYY-MM-DD HH24:MI:SS')
""")

	flag = True
	while flag:
		req_MTSZN = cursor_MTSZN.fetchmany(10000)
		if len(req_MTSZN) == 0:
			flag=False
		cursor_CH_SBD.executemany("""INSERT INTO MTSZN_LABORPROTECT.LABOR_RISK (ECON_BALL, ECON_CURRENCY, ECON_DEBT_OPV, ECON_DEBT_SALARY, ECON_EMP_HOLIDAY, ECON_EMP_MASSTHREAT, ECON_EMP_MERGE, ECON_EMP_PLAIN, ECON_EMP_REGMODE, ECON_EMP_WORKDAY, ECON_EMP_WORKPLACE, ECON_IN_BALL, ECON_OUT_BALL, ECON_PRICE_PRODUCT, REG_NUM, SOC_CONT_NONCOM, SOC_CONTACT, SOC_DIS_SALARY, SOC_DIS_WORK, SOC_LEVEL_CRIME, SOC_LEVEL_UNEMP, SOC_LIVING_COST, SOC_NONCOMPLIANCE, SOC_NOTSTABILITY, SOC_STABILITY, WORK_ARBITR, WORK_COL, WORK_COMMIS, WORK_CONF_COMPLAINT, WORK_FOUR, WORK_IND_COMPLAINT, WORK_N1, WORK_N1_GROUP, WORK_PARTICIPA, WORK_PROTEST, WORK_SEVEN, WORK_TEAM_COMPLAINT, WORK_THREE, SDU_DATE) VALUES""", req_MTSZN)

	cursor_CH_SBD.execute(f"SELECT COUNT(*) FROM MTSZN_LABORPROTECT.LABOR_RISK")
	sbd_count_rows_value = cursor_CH_SBD.fetchone()

	cursor_MTSZN.execute(f"SELECT COUNT(*) FROM LABORPROTECT.C_SDU_LABOR_RISK")
	origin_count_rows_value = cursor_MTSZN.fetchone()

	a = 0
	if int(sbd_count_rows_value[0]) == int(origin_count_rows_value[0]):
		a = 1
	tuple_to_load = ("LABORPROTECT", "C_SDU_LABOR_RISK", "TRUNC", a,  origin_count_rows_value[0],sbd_count_rows_value[0])
	list_to_load = [(tuple_to_load)]

	cursor_CH_SBD.executemany("INSERT INTO MTSZN_WMARKET.CHECK_LOAD (SCHEMA, TABLE, TYPE_OF_ETL, CORRECTLY_LOADED,TOTAL_COUNT_SOURCE,TOTAL_COUNT_SDU) VALUES", list_to_load)

	conn_MTSZN.close() 

def C_SDU_N1():
	conn_CH_SBD_secrets=Connection.get_connection_from_secrets(conn_id="Clickhouse-5")#скрытый коннекшн 5го сервера
	conn_CH_SBD=connect(host=conn_CH_SBD_secrets.host,port=conn_CH_SBD_secrets.port,user=conn_CH_SBD_secrets.login, password=conn_CH_SBD_secrets.password, connect_timeout=3600)
	cursor_CH_SBD = conn_CH_SBD.cursor()
	conn_MTSZN_secrets=Connection.get_connection_from_secrets(conn_id="MTSZN_WMARKET")#скрытый коннекшн источника
	dsn_tns_secrets=cx_Oracle.makedsn(host=conn_MTSZN_secrets.host,port=conn_MTSZN_secrets.port, service_name='SD')
	conn_MTSZN = cx_Oracle.connect(user =conn_MTSZN_secrets.login, password =conn_MTSZN_secrets.password, dsn=dsn_tns_secrets)
	cursor_MTSZN = conn_MTSZN.cursor()

	cursor_CH_SBD.execute("SELECT COALESCE(toString(max(SDU_DATE)),'000-00-00 00:00:00') as MAX_SDU_DATE from MTSZN_LABORPROTECT.N1")
	max_id = cursor_CH_SBD.fetchone()[0]

	cursor_MTSZN.execute(f"""SELECT
NVL(TO_CHAR("ACCIDENT_DATE", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "ACCIDENT_DATE"
, ACCIDENT_PLACE AS "ACCIDENT_PLACE"
, D_INCIDENT_ID AS "D_INCIDENT_ID"
, D_INJURY_SEVERITY_ID AS "D_INJURY_SEVERITY_ID"
, D_PHYSIO_STATE_ID AS "D_PHYSIO_STATE_ID"
, D_PROFESSION_ID AS "D_PROFESSION_ID"
, NVL(TO_CHAR("DATE_ENTRY", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "DATE_ENTRY"
, EMPLOYER_GUILTY_RATE AS "EMPLOYER_GUILTY_RATE"
, EXPERIENCE_YEARS AS "EXPERIENCE_YEARS"
, NVL(TO_CHAR("INITIAL_GUIDANCE_DATE", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "INITIAL_GUIDANCE_DATE"
, NVL(TO_CHAR("INTRO_GUIDANCE_DATE", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "INTRO_GUIDANCE_DATE"
, INVEST_BASIS_ID AS "INVEST_BASIS_ID"
, NVL(TO_CHAR("KNOWLEDGE_VERIF_DATE", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "KNOWLEDGE_VERIF_DATE"
, NVL(TO_CHAR("N1_ACT_DATE", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "N1_ACT_DATE"
, N1_ID AS "N1_ID"
, N1_NUM AS "N1_NUM"
, PA_CARD_ID AS "PA_CARD_ID"
, NVL(TO_CHAR("PERIODIC_GUIDANCE_DATE", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "PERIODIC_GUIDANCE_DATE"
, NVL(TO_CHAR("PRE_GUIDANCE_DATE", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "PRE_GUIDANCE_DATE"
, REGION_ID AS "REGION_ID"
, USER_ID AS "USER_ID"
, WORKED_HOURS AS "WORKED_HOURS"
FROM LABORPROTECT."C_SDU_N1" where SDU_DATE > to_timestamp('{max_id}','YYYY-MM-DD HH24:MI:SS')
""")

	flag = True
	while flag:
		req_MTSZN = cursor_MTSZN.fetchmany(10000)
		if len(req_MTSZN) == 0:
			flag=False
		cursor_CH_SBD.executemany("""INSERT INTO MTSZN_LABORPROTECT.N1 (ACCIDENT_DATE, ACCIDENT_PLACE, D_INCIDENT_ID, D_INJURY_SEVERITY_ID, D_PHYSIO_STATE_ID, D_PROFESSION_ID, DATE_ENTRY,  EMPLOYER_GUILTY_RATE, EXPERIENCE_YEARS, INITIAL_GUIDANCE_DATE, INTRO_GUIDANCE_DATE, INVEST_BASIS_ID, KNOWLEDGE_VERIF_DATE, N1_ACT_DATE, N1_ID, N1_NUM, PA_CARD_ID, PERIODIC_GUIDANCE_DATE, PRE_GUIDANCE_DATE,REGION_ID, USER_ID, WORKED_HOURS) VALUES""", req_MTSZN)

	cursor_CH_SBD.execute(f"SELECT COUNT(*) FROM MTSZN_LABORPROTECT.N1")
	sbd_count_rows_value = cursor_CH_SBD.fetchone()

	cursor_MTSZN.execute(f"SELECT COUNT(*) FROM LABORPROTECT.C_SDU_N1")
	origin_count_rows_value = cursor_MTSZN.fetchone()

	a = 0
	if int(sbd_count_rows_value[0]) == int(origin_count_rows_value[0]):
		a = 1
	tuple_to_load = ("LABORPROTECT", "C_SDU_N1", "TRUNC", a,  origin_count_rows_value[0],sbd_count_rows_value[0])
	list_to_load = [(tuple_to_load)]

	cursor_CH_SBD.executemany("INSERT INTO MTSZN_WMARKET.CHECK_LOAD (SCHEMA, TABLE, TYPE_OF_ETL, CORRECTLY_LOADED,TOTAL_COUNT_SOURCE,TOTAL_COUNT_SDU) VALUES", list_to_load)

	conn_MTSZN.close() 


def C_SDU_PA_CARD():
	conn_CH_SBD_secrets=Connection.get_connection_from_secrets(conn_id="Clickhouse-5")#скрытый коннекшн 5го сервера
	conn_CH_SBD=connect(host=conn_CH_SBD_secrets.host,port=conn_CH_SBD_secrets.port,user=conn_CH_SBD_secrets.login, password=conn_CH_SBD_secrets.password, connect_timeout=3600)
	cursor_CH_SBD = conn_CH_SBD.cursor()
	conn_MTSZN_secrets=Connection.get_connection_from_secrets(conn_id="MTSZN_WMARKET")#скрытый коннекшн источника
	dsn_tns_secrets=cx_Oracle.makedsn(host=conn_MTSZN_secrets.host,port=conn_MTSZN_secrets.port, service_name='SD')
	conn_MTSZN = cx_Oracle.connect(user =conn_MTSZN_secrets.login, password =conn_MTSZN_secrets.password, dsn=dsn_tns_secrets)
	cursor_MTSZN = conn_MTSZN.cursor()

	cursor_CH_SBD.execute("SELECT COALESCE(toString(max(SDU_DATE)),'000-00-00 00:00:00') as MAX_SDU_DATE from MTSZN_LABORPROTECT.PA_CARD")
	max_id = cursor_CH_SBD.fetchone()[0]

	cursor_MTSZN.execute(f"""SELECT
 ADD_CONT_NAME AS "ADD_CONT_NAME"
, ADD_EMAIL AS "ADD_EMAIL"
, ADD_INFO AS "ADD_INFO"
, ADD_PHONE_NUM AS "ADD_PHONE_NUM"
, ALONE_TYPE AS "ALONE_TYPE"
, BIR_COUNTRY_ID AS "BIR_COUNTRY_ID"
, BIR_REGION_ID AS "BIR_REGION_ID"
, BIR_TEXT_ADDR AS "BIR_TEXT_ADDR"
, CLOSE_TYPE AS "CLOSE_TYPE"
, CODE_IIN AS "CODE_IIN"
, COMP_SKILL_INFO AS "COMP_SKILL_INFO"
, COUNTRY_ID AS "COUNTRY_ID"
, DATE_BIRTH AS "DATE_BIRTH"
, NVL(TO_CHAR("DATE_CLOSE", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "DATE_CLOSE"
, NVL(TO_CHAR("DATE_CREATE", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "DATE_CREATE"
, NVL(TO_CHAR("DATE_ENTRY", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "DATE_ENTRY"
, DATE_ISSUE AS "DATE_ISSUE"
, NVL(TO_CHAR("DOC_DATE_EXPIRE", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "DOC_DATE_EXPIRE"
, DOC_ID AS "DOC_ID"
, DOC_NUM AS "DOC_NUM"
, DOC_SERIES AS "DOC_SERIES"
, EDUCATION_ID AS "EDUCATION_ID"
, EMAIL AS "EMAIL"
, FIRST_NAME AS "FIRST_NAME"
, ISSUE_AGENCY_ID AS "ISSUE_AGENCY_ID"
, ISSUE_AGENCY_TXT AS "ISSUE_AGENCY_TXT"
, LAST_NAME AS "LAST_NAME"
, MARITAL_ID AS "MARITAL_ID"
, MOB_PHONE_NUM AS "MOB_PHONE_NUM"
, MSGR_CONTACT AS "MSGR_CONTACT"
, NATION_ID AS "NATION_ID"
, PA_CARD_ID AS "PA_CARD_ID"
, PARENT_NAME AS "PARENT_NAME"
, PHONE_NUM AS "PHONE_NUM"
, PROF_SKILL_INFO AS "PROF_SKILL_INFO"
, QUEUE_ID AS "QUEUE_ID"
, SEX_ID AS "SEX_ID"
, USER_ID AS "USER_ID"
, SDU_DATE AS "SDU_DATE"
FROM LABORPROTECT."C_SDU_PA_CARD" where SDU_DATE > to_timestamp('{max_id}','YYYY-MM-DD HH24:MI:SS')
""")

	flag = True
	while flag:
		req_MTSZN = cursor_MTSZN.fetchmany(10000)
		req_MTSZN = [*map(lambda x: hash_columns(x, indexes=[9,19,23,26,32]), req_MTSZN)]
		if len(req_MTSZN) == 0:
			flag=False
		cursor_CH_SBD.executemany("""INSERT INTO MTSZN_LABORPROTECT.PA_CARD (ADD_CONT_NAME, ADD_EMAIL, ADD_INFO, ADD_PHONE_NUM, ALONE_TYPE, BIR_COUNTRY_ID, BIR_REGION_ID, BIR_TEXT_ADDR, CLOSE_TYPE, CODE_IIN, COMP_SKILL_INFO, COUNTRY_ID, DATE_BIRTH, DATE_CLOSE, DATE_CREATE, DATE_ENTRY, DATE_ISSUE, DOC_DATE_EXPIRE, DOC_ID, DOC_NUM, DOC_SERIES, EDUCATION_ID, EMAIL, FIRST_NAME, ISSUE_AGENCY_ID, ISSUE_AGENCY_TXT, LAST_NAME, MARITAL_ID, MOB_PHONE_NUM, MSGR_CONTACT, NATION_ID, PA_CARD_ID, PARENT_NAME, PHONE_NUM, PROF_SKILL_INFO, QUEUE_ID, SEX_ID, USER_ID, SDU_DATE) VALUES""", req_MTSZN)

	cursor_CH_SBD.execute(f"SELECT COUNT(*) FROM MTSZN_LABORPROTECT.PA_CARD")
	sbd_count_rows_value = cursor_CH_SBD.fetchone()

	cursor_MTSZN.execute(f"SELECT COUNT(*) FROM LABORPROTECT.C_SDU_PA_CARD")
	origin_count_rows_value = cursor_MTSZN.fetchone()

	a = 0
	if int(sbd_count_rows_value[0]) == int(origin_count_rows_value[0]):
		a = 1
	tuple_to_load = ("LABORPROTECT", "C_SDU_PA_CARD", "TRUNC", a,  origin_count_rows_value[0],sbd_count_rows_value[0])
	list_to_load = [(tuple_to_load)]

	cursor_CH_SBD.executemany("INSERT INTO MTSZN_WMARKET.CHECK_LOAD (SCHEMA, TABLE, TYPE_OF_ETL, CORRECTLY_LOADED,TOTAL_COUNT_SOURCE,TOTAL_COUNT_SDU) VALUES", list_to_load)

	conn_MTSZN.close() 

def PRECEPT_EMP():
	conn_CH_SBD_secrets=Connection.get_connection_from_secrets(conn_id="Clickhouse-5")#скрытый коннекшн 5го сервера
	conn_CH_SBD=connect(host=conn_CH_SBD_secrets.host,port=conn_CH_SBD_secrets.port,user=conn_CH_SBD_secrets.login, password=conn_CH_SBD_secrets.password, connect_timeout=3600)
	cursor_CH_SBD = conn_CH_SBD.cursor()
	conn_MTSZN_secrets=Connection.get_connection_from_secrets(conn_id="MTSZN_WMARKET")#скрытый коннекшн источника
	dsn_tns_secrets=cx_Oracle.makedsn(host=conn_MTSZN_secrets.host,port=conn_MTSZN_secrets.port, service_name='SD')
	conn_MTSZN = cx_Oracle.connect(user =conn_MTSZN_secrets.login, password =conn_MTSZN_secrets.password, dsn=dsn_tns_secrets)
	cursor_MTSZN = conn_MTSZN.cursor()

	cursor_CH_SBD.execute("SELECT COALESCE(toString(max(SDU_DATE)),'000-00-00 00:00:00') as MAX_SDU_DATE from MTSZN_LABORPROTECT.PRECEPT_EMP")
	max_id = cursor_CH_SBD.fetchone()[0]

	cursor_MTSZN.execute(f"""SELECT
NVL(TO_CHAR("DATE_ENTRY", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "DATE_ENTRY"
, EMP_ID AS "EMP_ID"
, GIT_ID AS "GIT_ID"
, INSPECTOR_NAME AS "INSPECTOR_NAME"
, INVEST_BASIS_ID AS "INVEST_BASIS_ID"
, PRECEPT_ADDRESS AS "PRECEPT_ADDRESS"
, NVL(TO_CHAR("PRECEPT_DATE", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "PRECEPT_DATE"
, PRECEPT_EMP_ID AS "PRECEPT_EMP_ID"
, PRECEPT_NUM AS "PRECEPT_NUM"
, REGION_ID AS "REGION_ID"
, USER_ID AS "USER_ID"
, SDU_DATE AS "SDU_DATE" 
FROM LABORPROTECT."C_SDU_PRECEPT_EMP" where SDU_DATE > to_timestamp('{max_id}','YYYY-MM-DD HH24:MI:SS')
""")

	flag = True
	while flag:
		req_MTSZN = cursor_MTSZN.fetchmany(10000)
		if len(req_MTSZN) == 0:
			flag=False
		cursor_CH_SBD.executemany("""INSERT INTO MTSZN_LABORPROTECT.PRECEPT_EMP (DATE_ENTRY, EMP_ID, GIT_ID, INSPECTOR_NAME, INVEST_BASIS_ID, PRECEPT_ADDRESS, PRECEPT_DATE, PRECEPT_EMP_ID, PRECEPT_NUM, REGION_ID, USER_ID,SDU_DATE) VALUES""", req_MTSZN)

	cursor_CH_SBD.execute(f"SELECT COUNT(*) FROM MTSZN_LABORPROTECT.PRECEPT_EMP")
	sbd_count_rows_value = cursor_CH_SBD.fetchone()

	cursor_MTSZN.execute(f"SELECT COUNT(*) FROM LABORPROTECT.C_SDU_PRECEPT_EMP")
	origin_count_rows_value = cursor_MTSZN.fetchone()

	a = 0
	if int(sbd_count_rows_value[0]) == int(origin_count_rows_value[0]):
		a = 1
	tuple_to_load = ("LABORPROTECT", "PRECEPT_EMP", "TRUNC", a,  origin_count_rows_value[0],sbd_count_rows_value[0])
	list_to_load = [(tuple_to_load)]

	cursor_CH_SBD.executemany("INSERT INTO MTSZN_WMARKET.CHECK_LOAD (SCHEMA, TABLE, TYPE_OF_ETL, CORRECTLY_LOADED,TOTAL_COUNT_SOURCE,TOTAL_COUNT_SDU) VALUES", list_to_load)

	conn_MTSZN.close() 

with DAG("MTSZN_LABORPROTECT", description="Trunc and delta", start_date=datetime(2022, 10, 7), schedule_interval='30 0 * * 0', catchup=False) as dag:
	hash_name = Variable.get("hash_password")

	C_SDU_ACCIDENT_ELIM_WORK = PythonOperator(
		owner='Bakhtiyar',
 		task_id='C_SDU_ACCIDENT_ELIM_WORK',
		python_callable=C_SDU_ACCIDENT_ELIM_WORK,
	)
	C_SDU_AFFAIR = PythonOperator(
		owner='Bakhtiyar',
 		task_id='C_SDU_AFFAIR',
		python_callable=C_SDU_AFFAIR,
	)
	C_SDU_AR_HOUSE = PythonOperator(
		owner='Bakhtiyar',
 		task_id='C_SDU_AR_HOUSE',
		python_callable=C_SDU_AR_HOUSE,
	)
	C_SDU_BREACH_RISK = PythonOperator(
		owner='Bakhtiyar',
 		task_id='C_SDU_BREACH_RISK',
		python_callable=C_SDU_BREACH_RISK,
	)
	C_SDU_BREACH_ZAKON = PythonOperator(
		owner='Bakhtiyar',
 		task_id='C_SDU_BREACH_ZAKON',
		python_callable=C_SDU_BREACH_ZAKON,
	)
	C_SDU_COLLECTIVE_CONTRACT = PythonOperator(
		owner='Bakhtiyar',
 		task_id='C_SDU_COLLECTIVE_CONTRACT',
		python_callable=C_SDU_COLLECTIVE_CONTRACT,
	)
	C_SDU_EMPLOYER = PythonOperator(
		owner='Bakhtiyar',
 		task_id='C_SDU_EMPLOYER',
		python_callable=C_SDU_EMPLOYER,
	)
	C_SDU_EMP_ATTR = PythonOperator(
		owner='Bakhtiyar',
 		task_id='C_SDU_EMP_ATTR',
		python_callable=C_SDU_EMP_ATTR,
	)
	C_SDU_EMP_CONTACT = PythonOperator(
		owner='Bakhtiyar',
 		task_id='C_SDU_EMP_CONTACT',
		python_callable=C_SDU_EMP_CONTACT,
	)
	C_SDU_EMP_FOUNDER = PythonOperator(
		owner='Bakhtiyar',
 		task_id='C_SDU_EMP_FOUNDER',
		python_callable=C_SDU_EMP_FOUNDER,
	)
	C_SDU_EMP_LEADER = PythonOperator(
		owner='Bakhtiyar',
 		task_id='C_SDU_EMP_LEADER',
		python_callable=C_SDU_EMP_LEADER,
	)
	C_SDU_INVEST_BASIS = PythonOperator(
		owner='Bakhtiyar',
 		task_id='C_SDU_INVEST_BASIS',
		python_callable=C_SDU_INVEST_BASIS,
	)
	C_SDU_LABOR_DISPUTE = PythonOperator(
		owner='Bakhtiyar',
 		task_id='C_SDU_LABOR_DISPUTE',
		python_callable=C_SDU_LABOR_DISPUTE,
	)
	C_SDU_LABOR_RISK = PythonOperator(
		owner='Bakhtiyar',
 		task_id='C_SDU_LABOR_RISK',
		python_callable=C_SDU_LABOR_RISK,
	)
	C_SDU_N1 = PythonOperator(
		owner='Bakhtiyar',
 		task_id='C_SDU_N1',
		python_callable=C_SDU_N1,
	)
	C_SDU_PA_CARD = PythonOperator(
		owner='Bakhtiyar',
 		task_id='C_SDU_PA_CARD',
		python_callable=C_SDU_PA_CARD,
	)
	PRECEPT_EMP = PythonOperator(
		owner='Bakhtiyar',
 		task_id='PRECEPT_EMP',
		python_callable=PRECEPT_EMP,
	)
	
	C_SDU_ACCIDENT_ELIM_WORK>>C_SDU_AFFAIR>>C_SDU_AR_HOUSE>>C_SDU_BREACH_RISK>>C_SDU_BREACH_ZAKON>>C_SDU_COLLECTIVE_CONTRACT>>C_SDU_EMPLOYER>>C_SDU_EMP_ATTR>>C_SDU_EMP_CONTACT>>C_SDU_EMP_FOUNDER>>C_SDU_EMP_LEADER>>C_SDU_INVEST_BASIS>>C_SDU_LABOR_DISPUTE>>C_SDU_LABOR_RISK>>C_SDU_N1>>C_SDU_PA_CARD>>PRECEPT_EMP