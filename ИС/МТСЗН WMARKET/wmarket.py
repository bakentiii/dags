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

def D_AGE():
	conn_CH_SBD_secrets=Connection.get_connection_from_secrets(conn_id="Clickhouse-5")#скрытый коннекшн 5го сервера
	conn_CH_SBD=connect(host=conn_CH_SBD_secrets.host,port=conn_CH_SBD_secrets.port,user=conn_CH_SBD_secrets.login, password=conn_CH_SBD_secrets.password, connect_timeout=3600)
	cursor_CH_SBD = conn_CH_SBD.cursor()
	conn_MTSZN_secrets=Connection.get_connection_from_secrets(conn_id="MTSZN_WMARKET")#скрытый коннекшн источника
	dsn_tns_secrets=cx_Oracle.makedsn(host=conn_MTSZN_secrets.host,port=conn_MTSZN_secrets.port, service_name='SD')
	conn_MTSZN = cx_Oracle.connect(user =conn_MTSZN_secrets.login, password =conn_MTSZN_secrets.password, dsn=dsn_tns_secrets)
	cursor_MTSZN = conn_MTSZN.cursor()

	cursor_CH_SBD.execute("TRUNCATE TABLE MTSZN_WMARKET.D_AGE")
	cursor_CH_SBD.execute("TRUNCATE TABLE MTSZN_WMARKET.CHECK_LOAD")

	cursor_MTSZN.execute(f"""SELECT
 AGE_FROM AS "AGE_FROM"
, AGE_TO AS "AGE_TO"
, CODE AS "CODE"
, NVL(TO_CHAR("DATE_ENTRY", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "DATE_ENTRY"
, NVL(TO_CHAR("DATE_FROM", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "DATE_FROM"
, NVL(TO_CHAR("DATE_TO", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "DATE_TO"
, ENAME AS "ENAME"
, ID AS "ID"
, KNAME AS "KNAME"
, PAR_ID AS "PAR_ID"
, RNAME AS "RNAME"
, USER_ID AS "USER_ID"
FROM WMARKET."D_AGE"
""")

	flag = True
	while flag:
		req_MTSZN = cursor_MTSZN.fetchmany(10000)
		if len(req_MTSZN) == 0:
			flag=False
		cursor_CH_SBD.executemany("""INSERT INTO MTSZN_WMARKET.D_AGE (AGE_FROM, AGE_TO, CODE, DATE_ENTRY, DATE_FROM, DATE_TO, ENAME, ID, KNAME, PAR_ID, RNAME, USER_ID) VALUES""", req_MTSZN)

	cursor_CH_SBD.execute(f"SELECT COUNT(*) FROM MTSZN_WMARKET.D_AGE")
	sbd_count_rows_value = cursor_CH_SBD.fetchone()

	cursor_MTSZN.execute(f"SELECT COUNT(*) FROM WMARKET.D_AGE")
	origin_count_rows_value = cursor_MTSZN.fetchone()

	a = 0
	if int(sbd_count_rows_value[0]) == int(origin_count_rows_value[0]):
		a = 1
	tuple_to_load = ("SELECT COUNT(*) FROM MTSZN_WMARKET.D_AGE", "D_AGE", "TRUNC", a,  origin_count_rows_value[0],sbd_count_rows_value[0])
	list_to_load = [(tuple_to_load)]

	cursor_CH_SBD.executemany("INSERT INTO MTSZN_WMARKET.CHECK_LOAD (SCHEMA, TABLE, TYPE_OF_ETL, CORRECTLY_LOADED,TOTAL_COUNT_SOURCE,TOTAL_COUNT_SDU) VALUES", list_to_load)

	conn_MTSZN.close() 

def D_AGENCY():
	conn_CH_SBD_secrets=Connection.get_connection_from_secrets(conn_id="Clickhouse-5")#скрытый коннекшн 5го сервера
	conn_CH_SBD=connect(host=conn_CH_SBD_secrets.host,port=conn_CH_SBD_secrets.port,user=conn_CH_SBD_secrets.login, password=conn_CH_SBD_secrets.password, connect_timeout=3600)
	cursor_CH_SBD = conn_CH_SBD.cursor()
	conn_MTSZN_secrets=Connection.get_connection_from_secrets(conn_id="MTSZN_WMARKET")#скрытый коннекшн источника
	dsn_tns_secrets=cx_Oracle.makedsn(host=conn_MTSZN_secrets.host,port=conn_MTSZN_secrets.port, service_name='SD')
	conn_MTSZN = cx_Oracle.connect(user =conn_MTSZN_secrets.login, password =conn_MTSZN_secrets.password, dsn=dsn_tns_secrets)
	cursor_MTSZN = conn_MTSZN.cursor()

	cursor_CH_SBD.execute("TRUNCATE TABLE MTSZN_WMARKET.D_AGENCY")

	cursor_MTSZN.execute(f"""SELECT
 CODE AS "CODE"
, NVL(TO_CHAR("DATE_ENTRY", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "DATE_ENTRY"
, NVL(TO_CHAR("DATE_FROM", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "DATE_FROM"
, NVL(TO_CHAR("DATE_TO", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "DATE_TO"
, ENAME AS "ENAME"
, GBDFL_CODE AS "GBDFL_CODE"
, GBDUL_CODE AS "GBDUL_CODE"
, ID AS "ID"
, KNAME AS "KNAME"
, PAR_ID AS "PAR_ID"
, RNAME AS "RNAME"
, USER_ID AS "USER_ID"
FROM WMARKET."D_AGENCY"
""")

	flag = True
	while flag:
		req_MTSZN = cursor_MTSZN.fetchmany(10000)
		if len(req_MTSZN) == 0:
			flag=False
		cursor_CH_SBD.executemany("""INSERT INTO MTSZN_WMARKET.D_AGENCY (CODE, DATE_ENTRY, DATE_FROM, DATE_TO, ENAME, GBDFL_CODE, GBDUL_CODE, ID, KNAME, PAR_ID, RNAME, USER_ID) VALUES""", req_MTSZN)

	cursor_CH_SBD.execute(f"SELECT COUNT(*) FROM MTSZN_WMARKET.D_AGENCY")
	sbd_count_rows_value = cursor_CH_SBD.fetchone()

	cursor_MTSZN.execute(f"SELECT COUNT(*) FROM WMARKET.D_AGENCY")
	origin_count_rows_value = cursor_MTSZN.fetchone()

	a = 0
	if int(sbd_count_rows_value[0]) == int(origin_count_rows_value[0]):
		a = 1
	tuple_to_load = ("SELECT COUNT(*) FROM MTSZN_WMARKET.D_AGENCY", "D_AGENCY", "TRUNC", a,  origin_count_rows_value[0],sbd_count_rows_value[0])
	list_to_load = [(tuple_to_load)]

	cursor_CH_SBD.executemany("INSERT INTO MTSZN_WMARKET.CHECK_LOAD (SCHEMA, TABLE, TYPE_OF_ETL, CORRECTLY_LOADED,TOTAL_COUNT_SOURCE,TOTAL_COUNT_SDU) VALUES", list_to_load)

	conn_MTSZN.close() 

def D_BANK():
	conn_CH_SBD_secrets=Connection.get_connection_from_secrets(conn_id="Clickhouse-5")#скрытый коннекшн 5го сервера
	conn_CH_SBD=connect(host=conn_CH_SBD_secrets.host,port=conn_CH_SBD_secrets.port,user=conn_CH_SBD_secrets.login, password=conn_CH_SBD_secrets.password, connect_timeout=3600)
	cursor_CH_SBD = conn_CH_SBD.cursor()
	conn_MTSZN_secrets=Connection.get_connection_from_secrets(conn_id="MTSZN_WMARKET")#скрытый коннекшн источника
	dsn_tns_secrets=cx_Oracle.makedsn(host=conn_MTSZN_secrets.host,port=conn_MTSZN_secrets.port, service_name='SD')
	conn_MTSZN = cx_Oracle.connect(user =conn_MTSZN_secrets.login, password =conn_MTSZN_secrets.password, dsn=dsn_tns_secrets)
	cursor_MTSZN = conn_MTSZN.cursor()

	cursor_CH_SBD.execute("TRUNCATE TABLE MTSZN_WMARKET.D_BANK")

	cursor_MTSZN.execute(f"""SELECT
 BCODE AS "BCODE"
, BIK AS "BIK"
, CBD_BIK AS "CBD_BIK"
, CODE AS "CODE"
, NVL(TO_CHAR("DATE_ENTRY", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "DATE_ENTRY"
, NVL(TO_CHAR("DATE_FROM", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "DATE_FROM"
, NVL(TO_CHAR("DATE_TO", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "DATE_TO"
, ENAME AS "ENAME"
, GBD_CODE AS "GBD_CODE"
, ID AS "ID"
, IIK AS "IIK"
, IS_RESIDENT AS "IS_RESIDENT"
, KBE_ID AS "KBE_ID"
, KNAME AS "KNAME"
, PAR_ID AS "PAR_ID"
, RFRC_ID AS "RFRC_ID"
, RNAME AS "RNAME"
, SECTOR_ID AS "SECTOR_ID"
, USER_ID AS "USER_ID"
FROM WMARKET."D_BANK"
""")

	flag = True
	while flag:
		req_MTSZN = cursor_MTSZN.fetchmany(10000)
		if len(req_MTSZN) == 0:
			flag=False
		cursor_CH_SBD.executemany("""INSERT INTO MTSZN_WMARKET.D_BANK (BCODE, BIK, CBD_BIK, CODE, DATE_ENTRY, DATE_FROM, DATE_TO, ENAME, GBD_CODE, ID, IIK, IS_RESIDENT, KBE_ID, KNAME, PAR_ID, RFRC_ID, RNAME, SECTOR_ID, USER_ID) VALUES""", req_MTSZN)

	cursor_CH_SBD.execute(f"SELECT COUNT(*) FROM MTSZN_WMARKET.D_BANK")
	sbd_count_rows_value = cursor_CH_SBD.fetchone()

	cursor_MTSZN.execute(f"SELECT COUNT(*) FROM WMARKET.D_BANK")
	origin_count_rows_value = cursor_MTSZN.fetchone()

	a = 0
	if int(sbd_count_rows_value[0]) == int(origin_count_rows_value[0]):
		a = 1
	tuple_to_load = ("SELECT COUNT(*) FROM MTSZN_WMARKET.D_BANK", "D_BANK", "TRUNC", a,  origin_count_rows_value[0],sbd_count_rows_value[0])
	list_to_load = [(tuple_to_load)]

	cursor_CH_SBD.executemany("INSERT INTO MTSZN_WMARKET.CHECK_LOAD (SCHEMA, TABLE, TYPE_OF_ETL, CORRECTLY_LOADED,TOTAL_COUNT_SOURCE,TOTAL_COUNT_SDU) VALUES", list_to_load)

	conn_MTSZN.close() 

def D_CAT_SPEC():
	conn_CH_SBD_secrets=Connection.get_connection_from_secrets(conn_id="Clickhouse-5")#скрытый коннекшн 5го сервера
	conn_CH_SBD=connect(host=conn_CH_SBD_secrets.host,port=conn_CH_SBD_secrets.port,user=conn_CH_SBD_secrets.login, password=conn_CH_SBD_secrets.password, connect_timeout=3600)
	cursor_CH_SBD = conn_CH_SBD.cursor()
	conn_MTSZN_secrets=Connection.get_connection_from_secrets(conn_id="MTSZN_WMARKET")#скрытый коннекшн источника
	dsn_tns_secrets=cx_Oracle.makedsn(host=conn_MTSZN_secrets.host,port=conn_MTSZN_secrets.port, service_name='SD')
	conn_MTSZN = cx_Oracle.connect(user =conn_MTSZN_secrets.login, password =conn_MTSZN_secrets.password, dsn=dsn_tns_secrets)
	cursor_MTSZN = conn_MTSZN.cursor()

	cursor_CH_SBD.execute("TRUNCATE TABLE MTSZN_WMARKET.D_CAT_SPEC")

	cursor_MTSZN.execute(f"""SELECT
 CODE AS "CODE"
, NVL(TO_CHAR("DATE_ENTRY", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "DATE_ENTRY"
, NVL(TO_CHAR("DATE_FROM", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "DATE_FROM"
, NVL(TO_CHAR("DATE_TO", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "DATE_TO"
, ENAME AS "ENAME"
, ID AS "ID"
, KNAME AS "KNAME"
, PAR_ID AS "PAR_ID"
, RNAME AS "RNAME"
, USER_ID AS "USER_ID"
FROM WMARKET."D_CAT_SPEC"
""")

	flag = True
	while flag:
		req_MTSZN = cursor_MTSZN.fetchmany(10000)
		if len(req_MTSZN) == 0:
			flag=False
		cursor_CH_SBD.executemany("""INSERT INTO MTSZN_WMARKET.D_CAT_SPEC (CODE, DATE_ENTRY, DATE_FROM, DATE_TO, ENAME, ID, KNAME, PAR_ID, RNAME, USER_ID) VALUES""", req_MTSZN)

	cursor_CH_SBD.execute(f"SELECT COUNT(*) FROM MTSZN_WMARKET.D_CAT_SPEC")
	sbd_count_rows_value = cursor_CH_SBD.fetchone()

	cursor_MTSZN.execute(f"SELECT COUNT(*) FROM WMARKET.D_CAT_SPEC")
	origin_count_rows_value = cursor_MTSZN.fetchone()

	a = 0
	if int(sbd_count_rows_value[0]) == int(origin_count_rows_value[0]):
		a = 1
	tuple_to_load = ("SELECT COUNT(*) FROM MTSZN_WMARKET.D_CAT_SPEC", "D_CAT_SPEC", "TRUNC", a,  origin_count_rows_value[0],sbd_count_rows_value[0])
	list_to_load = [(tuple_to_load)]

	cursor_CH_SBD.executemany("INSERT INTO MTSZN_WMARKET.CHECK_LOAD (SCHEMA, TABLE, TYPE_OF_ETL, CORRECTLY_LOADED,TOTAL_COUNT_SOURCE,TOTAL_COUNT_SDU) VALUES", list_to_load)

	conn_MTSZN.close() 

def D_COUNTRY():
	conn_CH_SBD_secrets=Connection.get_connection_from_secrets(conn_id="Clickhouse-5")#скрытый коннекшн 5го сервера
	conn_CH_SBD=connect(host=conn_CH_SBD_secrets.host,port=conn_CH_SBD_secrets.port,user=conn_CH_SBD_secrets.login, password=conn_CH_SBD_secrets.password, connect_timeout=3600)
	cursor_CH_SBD = conn_CH_SBD.cursor()
	conn_MTSZN_secrets=Connection.get_connection_from_secrets(conn_id="MTSZN_WMARKET")#скрытый коннекшн источника
	dsn_tns_secrets=cx_Oracle.makedsn(host=conn_MTSZN_secrets.host,port=conn_MTSZN_secrets.port, service_name='SD')
	conn_MTSZN = cx_Oracle.connect(user =conn_MTSZN_secrets.login, password =conn_MTSZN_secrets.password, dsn=dsn_tns_secrets)
	cursor_MTSZN = conn_MTSZN.cursor()

	cursor_CH_SBD.execute("TRUNCATE TABLE MTSZN_WMARKET.D_COUNTRY")

	cursor_MTSZN.execute(f"""SELECT
 CODE AS "CODE"
, NVL(TO_CHAR("DATE_ENTRY", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "DATE_ENTRY"
, NVL(TO_CHAR("DATE_FROM", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "DATE_FROM"
, NVL(TO_CHAR("DATE_TO", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "DATE_TO"
, ENAME AS "ENAME"
, GBDFL_CODE AS "GBDFL_CODE"
, GBDUL_CODE AS "GBDUL_CODE"
, ID AS "ID"
, IS_CIS AS "IS_CIS"
, KNAME AS "KNAME"
, PAR_ID AS "PAR_ID"
, RNAME AS "RNAME"
, USER_ID AS "USER_ID"
FROM WMARKET."D_COUNTRY"
""")

	flag = True
	while flag:
		req_MTSZN = cursor_MTSZN.fetchmany(10000)
		if len(req_MTSZN) == 0:
			flag=False
		cursor_CH_SBD.executemany("""INSERT INTO MTSZN_WMARKET.D_COUNTRY (CODE, DATE_ENTRY, DATE_FROM, DATE_TO, ENAME, GBDFL_CODE, GBDUL_CODE, ID, IS_CIS, KNAME, PAR_ID, RNAME, USER_ID) VALUES""", req_MTSZN)

	cursor_CH_SBD.execute(f"SELECT COUNT(*) FROM MTSZN_WMARKET.D_COUNTRY")
	sbd_count_rows_value = cursor_CH_SBD.fetchone()

	cursor_MTSZN.execute(f"SELECT COUNT(*) FROM WMARKET.D_COUNTRY")
	origin_count_rows_value = cursor_MTSZN.fetchone()

	a = 0
	if int(sbd_count_rows_value[0]) == int(origin_count_rows_value[0]):
		a = 1
	tuple_to_load = ("SELECT COUNT(*) FROM MTSZN_WMARKET.D_COUNTRY", "D_COUNTRY", "TRUNC", a,  origin_count_rows_value[0],sbd_count_rows_value[0])
	list_to_load = [(tuple_to_load)]

	cursor_CH_SBD.executemany("INSERT INTO MTSZN_WMARKET.CHECK_LOAD (SCHEMA, TABLE, TYPE_OF_ETL, CORRECTLY_LOADED,TOTAL_COUNT_SOURCE,TOTAL_COUNT_SDU) VALUES", list_to_load)

	conn_MTSZN.close() 

def D_CUSTOMER():
	conn_CH_SBD_secrets=Connection.get_connection_from_secrets(conn_id="Clickhouse-5")#скрытый коннекшн 5го сервера
	conn_CH_SBD=connect(host=conn_CH_SBD_secrets.host,port=conn_CH_SBD_secrets.port,user=conn_CH_SBD_secrets.login, password=conn_CH_SBD_secrets.password, connect_timeout=3600)
	cursor_CH_SBD = conn_CH_SBD.cursor()
	conn_MTSZN_secrets=Connection.get_connection_from_secrets(conn_id="MTSZN_WMARKET")#скрытый коннекшн источника
	dsn_tns_secrets=cx_Oracle.makedsn(host=conn_MTSZN_secrets.host,port=conn_MTSZN_secrets.port, service_name='SD')
	conn_MTSZN = cx_Oracle.connect(user =conn_MTSZN_secrets.login, password =conn_MTSZN_secrets.password, dsn=dsn_tns_secrets)
	cursor_MTSZN = conn_MTSZN.cursor()

	cursor_CH_SBD.execute("TRUNCATE TABLE MTSZN_WMARKET.D_CUSTOMER")

	cursor_MTSZN.execute(f"""SELECT
 CODE AS "CODE"
, NVL(TO_CHAR("DATE_ENTRY", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "DATE_ENTRY"
, NVL(TO_CHAR("DATE_FROM", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "DATE_FROM"
, NVL(TO_CHAR("DATE_TO", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "DATE_TO"
, ENAME AS "ENAME"
, ID AS "ID"
, KNAME AS "KNAME"
, PAR_ID AS "PAR_ID"
, RNAME AS "RNAME"
, STATUS AS "STATUS"
, USER_ID AS "USER_ID"
FROM WMARKET."D_CUSTOMER"
""")

	flag = True
	while flag:
		req_MTSZN = cursor_MTSZN.fetchmany(10000)
		if len(req_MTSZN) == 0:
			flag=False
		cursor_CH_SBD.executemany("""INSERT INTO MTSZN_WMARKET.D_CUSTOMER (CODE, DATE_ENTRY, DATE_FROM, DATE_TO, ENAME, ID, KNAME, PAR_ID, RNAME, STATUS, USER_ID) VALUES""", req_MTSZN)

	cursor_CH_SBD.execute(f"SELECT COUNT(*) FROM MTSZN_WMARKET.D_CUSTOMER")
	sbd_count_rows_value = cursor_CH_SBD.fetchone()

	cursor_MTSZN.execute(f"SELECT COUNT(*) FROM WMARKET.D_CUSTOMER")
	origin_count_rows_value = cursor_MTSZN.fetchone()

	a = 0
	if int(sbd_count_rows_value[0]) == int(origin_count_rows_value[0]):
		a = 1
	tuple_to_load = ("SELECT COUNT(*) FROM MTSZN_WMARKET.D_CUSTOMER", "D_CUSTOMER", "TRUNC", a,  origin_count_rows_value[0],sbd_count_rows_value[0])
	list_to_load = [(tuple_to_load)]

	cursor_CH_SBD.executemany("INSERT INTO MTSZN_WMARKET.CHECK_LOAD (SCHEMA, TABLE, TYPE_OF_ETL, CORRECTLY_LOADED,TOTAL_COUNT_SOURCE,TOTAL_COUNT_SDU) VALUES", list_to_load)

	conn_MTSZN.close() 

def D_DEP():
	conn_CH_SBD_secrets=Connection.get_connection_from_secrets(conn_id="Clickhouse-5")#скрытый коннекшн 5го сервера
	conn_CH_SBD=connect(host=conn_CH_SBD_secrets.host,port=conn_CH_SBD_secrets.port,user=conn_CH_SBD_secrets.login, password=conn_CH_SBD_secrets.password, connect_timeout=3600)
	cursor_CH_SBD = conn_CH_SBD.cursor()
	conn_MTSZN_secrets=Connection.get_connection_from_secrets(conn_id="MTSZN_WMARKET")#скрытый коннекшн источника
	dsn_tns_secrets=cx_Oracle.makedsn(host=conn_MTSZN_secrets.host,port=conn_MTSZN_secrets.port, service_name='SD')
	conn_MTSZN = cx_Oracle.connect(user =conn_MTSZN_secrets.login, password =conn_MTSZN_secrets.password, dsn=dsn_tns_secrets)
	cursor_MTSZN = conn_MTSZN.cursor()

	cursor_CH_SBD.execute("TRUNCATE TABLE MTSZN_WMARKET.D_DEP")

	cursor_MTSZN.execute(f"""SELECT
 ADDR_KNAME AS "ADDR_KNAME"
, ADDR_RNAME AS "ADDR_RNAME"
, CHIEF_ACNT_NAME AS "CHIEF_ACNT_NAME"
, CHIEF_NAME AS "CHIEF_NAME"
, CODE AS "CODE"
, CODE_BIK AS "CODE_BIK"
, CODE_BIN AS "CODE_BIN"
, CODE_IIK AS "CODE_IIK"
, CODE_IIK_S50 AS "CODE_IIK_S50"
, CODE_IIK_S59 AS "CODE_IIK_S59"
, CODE_MFO_BIK_S52 AS "CODE_MFO_BIK_S52"
, CODE_MFO_BIK_S57 AS "CODE_MFO_BIK_S57"
, CODE_REGION AS "CODE_REGION"
, CODE_RNN AS "CODE_RNN"
, NVL(TO_CHAR("DATE_ENTRY", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "DATE_ENTRY"
, NVL(TO_CHAR("DATE_FROM", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "DATE_FROM"
, NVL(TO_CHAR("DATE_TO", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "DATE_TO"
, NVL(TO_CHAR("DEL_DATE", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "DEL_DATE"
, DEL_USER_ID AS "DEL_USER_ID"
, DEP_CODE AS "DEP_CODE"
, EL_CODE AS "EL_CODE"
, EMAIL AS "EMAIL"
, ENAME AS "ENAME"
, FEE_SUM AS "FEE_SUM"
, FULL_KNAME AS "FULL_KNAME"
, FULL_RNAME AS "FULL_RNAME"
, ID AS "ID"
, IISCON_CODE AS "IISCON_CODE"
, IS_FWORK AS "IS_FWORK"
, IS_LMARKET AS "IS_LMARKET"
, IS_LPROTECT AS "IS_LPROTECT"
, IS_SOC_HELP AS "IS_SOC_HELP"
, KNAME AS "KNAME"
, NATIVE_ZONE_ID AS "NATIVE_ZONE_ID"
, PAR_ID AS "PAR_ID"
, PHONE_NUM AS "PHONE_NUM"
, POST_IDX AS "POST_IDX"
, REGION_ID AS "REGION_ID"
, RFBN_ID AS "RFBN_ID"
, RNAME AS "RNAME"
, SECTOR_ID AS "SECTOR_ID"
, STATUS AS "STATUS"
, USER_ID AS "USER_ID"
FROM WMARKET."D_DEP"
""")

	flag = True
	while flag:
		req_MTSZN = cursor_MTSZN.fetchmany(10000)
		if len(req_MTSZN) == 0:
			flag=False
		cursor_CH_SBD.executemany("""INSERT INTO MTSZN_WMARKET.D_DEP (ADDR_KNAME, ADDR_RNAME, CHIEF_ACNT_NAME, CHIEF_NAME, CODE, CODE_BIK, CODE_BIN, CODE_IIK, CODE_IIK_S50, CODE_IIK_S59, CODE_MFO_BIK_S52, CODE_MFO_BIK_S57, CODE_REGION, CODE_RNN, DATE_ENTRY, DATE_FROM, DATE_TO, DEL_DATE, DEL_USER_ID, DEP_CODE, EL_CODE, EMAIL, ENAME, FEE_SUM, FULL_KNAME, FULL_RNAME, ID, IISCON_CODE, IS_FWORK, IS_LMARKET, IS_LPROTECT, IS_SOC_HELP, KNAME, NATIVE_ZONE_ID, PAR_ID, PHONE_NUM, POST_IDX, REGION_ID, RFBN_ID, RNAME, SECTOR_ID, STATUS, USER_ID) VALUES""", req_MTSZN)

	cursor_CH_SBD.execute(f"SELECT COUNT(*) FROM MTSZN_WMARKET.D_DEP")
	sbd_count_rows_value = cursor_CH_SBD.fetchone()

	cursor_MTSZN.execute(f"SELECT COUNT(*) FROM WMARKET.D_DEP")
	origin_count_rows_value = cursor_MTSZN.fetchone()

	a = 0
	if int(sbd_count_rows_value[0]) == int(origin_count_rows_value[0]):
		a = 1
	tuple_to_load = ("SELECT COUNT(*) FROM MTSZN_WMARKET.D_DEP", "D_DEP", "TRUNC", a,  origin_count_rows_value[0],sbd_count_rows_value[0])
	list_to_load = [(tuple_to_load)]

	cursor_CH_SBD.executemany("INSERT INTO MTSZN_WMARKET.CHECK_LOAD (SCHEMA, TABLE, TYPE_OF_ETL, CORRECTLY_LOADED,TOTAL_COUNT_SOURCE,TOTAL_COUNT_SDU) VALUES", list_to_load)

	conn_MTSZN.close() 

def D_DOC():
	conn_CH_SBD_secrets=Connection.get_connection_from_secrets(conn_id="Clickhouse-5")#скрытый коннекшн 5го сервера
	conn_CH_SBD=connect(host=conn_CH_SBD_secrets.host,port=conn_CH_SBD_secrets.port,user=conn_CH_SBD_secrets.login, password=conn_CH_SBD_secrets.password, connect_timeout=3600)
	cursor_CH_SBD = conn_CH_SBD.cursor()
	conn_MTSZN_secrets=Connection.get_connection_from_secrets(conn_id="MTSZN_WMARKET")#скрытый коннекшн источника
	dsn_tns_secrets=cx_Oracle.makedsn(host=conn_MTSZN_secrets.host,port=conn_MTSZN_secrets.port, service_name='SD')
	conn_MTSZN = cx_Oracle.connect(user =conn_MTSZN_secrets.login, password =conn_MTSZN_secrets.password, dsn=dsn_tns_secrets)
	cursor_MTSZN = conn_MTSZN.cursor()

	cursor_CH_SBD.execute("TRUNCATE TABLE MTSZN_WMARKET.D_DOC")

	cursor_MTSZN.execute(f"""SELECT
 CODE AS "CODE"
, NVL(TO_CHAR("DATE_ENTRY", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "DATE_ENTRY"
, NVL(TO_CHAR("DATE_FROM", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "DATE_FROM"
, NVL(TO_CHAR("DATE_TO", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "DATE_TO"
, ENAME AS "ENAME"
, GBDFL_CODE AS "GBDFL_CODE"
, GBDUL_CODE AS "GBDUL_CODE"
, ID AS "ID"
, IS_COMMON AS "IS_COMMON"
, KNAME AS "KNAME"
, MAIN_REF_CODE AS "MAIN_REF_CODE"
, PAR_ID AS "PAR_ID"
, RNAME AS "RNAME"
, USER_ID AS "USER_ID"
FROM WMARKET."D_DOC"
""")

	flag = True
	while flag:
		req_MTSZN = cursor_MTSZN.fetchmany(10000)
		if len(req_MTSZN) == 0:
			flag=False
		cursor_CH_SBD.executemany("""INSERT INTO MTSZN_WMARKET.D_DOC (CODE, DATE_ENTRY, DATE_FROM, DATE_TO, ENAME, GBDFL_CODE, GBDUL_CODE, ID, IS_COMMON, KNAME, MAIN_REF_CODE, PAR_ID, RNAME, USER_ID) VALUES""", req_MTSZN)

	cursor_CH_SBD.execute(f"SELECT COUNT(*) FROM MTSZN_WMARKET.D_DOC")
	sbd_count_rows_value = cursor_CH_SBD.fetchone()

	cursor_MTSZN.execute(f"SELECT COUNT(*) FROM WMARKET.D_DOC")
	origin_count_rows_value = cursor_MTSZN.fetchone()

	a = 0
	if int(sbd_count_rows_value[0]) == int(origin_count_rows_value[0]):
		a = 1
	tuple_to_load = ("SELECT COUNT(*) FROM MTSZN_WMARKET.D_DOC", "D_DOC", "TRUNC", a,  origin_count_rows_value[0],sbd_count_rows_value[0])
	list_to_load = [(tuple_to_load)]

	cursor_CH_SBD.executemany("INSERT INTO MTSZN_WMARKET.CHECK_LOAD (SCHEMA, TABLE, TYPE_OF_ETL, CORRECTLY_LOADED,TOTAL_COUNT_SOURCE,TOTAL_COUNT_SDU) VALUES", list_to_load)

	conn_MTSZN.close() 

def D_EDUCATION():
	conn_CH_SBD_secrets=Connection.get_connection_from_secrets(conn_id="Clickhouse-5")#скрытый коннекшн 5го сервера
	conn_CH_SBD=connect(host=conn_CH_SBD_secrets.host,port=conn_CH_SBD_secrets.port,user=conn_CH_SBD_secrets.login, password=conn_CH_SBD_secrets.password, connect_timeout=3600)
	cursor_CH_SBD = conn_CH_SBD.cursor()
	conn_MTSZN_secrets=Connection.get_connection_from_secrets(conn_id="MTSZN_WMARKET")#скрытый коннекшн источника
	dsn_tns_secrets=cx_Oracle.makedsn(host=conn_MTSZN_secrets.host,port=conn_MTSZN_secrets.port, service_name='SD')
	conn_MTSZN = cx_Oracle.connect(user =conn_MTSZN_secrets.login, password =conn_MTSZN_secrets.password, dsn=dsn_tns_secrets)
	cursor_MTSZN = conn_MTSZN.cursor()

	cursor_CH_SBD.execute("TRUNCATE TABLE MTSZN_WMARKET.D_EDUCATION")

	cursor_MTSZN.execute(f"""SELECT
 CODE AS "CODE"
, NVL(TO_CHAR("DATE_ENTRY", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "DATE_ENTRY"
, NVL(TO_CHAR("DATE_FROM", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "DATE_FROM"
, NVL(TO_CHAR("DATE_TO", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "DATE_TO"
, ENAME AS "ENAME"
, ID AS "ID"
, KNAME AS "KNAME"
, PAR_ID AS "PAR_ID"
, RNAME AS "RNAME"
, USER_ID AS "USER_ID"
FROM WMARKET."D_EDUCATION"
""")

	flag = True
	while flag:
		req_MTSZN = cursor_MTSZN.fetchmany(10000)
		if len(req_MTSZN) == 0:
			flag=False
		cursor_CH_SBD.executemany("""INSERT INTO MTSZN_WMARKET.D_EDUCATION (CODE, DATE_ENTRY, DATE_FROM, DATE_TO, ENAME, ID, KNAME, PAR_ID, RNAME, USER_ID) VALUES""", req_MTSZN)

	cursor_CH_SBD.execute(f"SELECT COUNT(*) FROM MTSZN_WMARKET.D_EDUCATION")
	sbd_count_rows_value = cursor_CH_SBD.fetchone()

	cursor_MTSZN.execute(f"SELECT COUNT(*) FROM WMARKET.D_EDUCATION")
	origin_count_rows_value = cursor_MTSZN.fetchone()

	a = 0
	if int(sbd_count_rows_value[0]) == int(origin_count_rows_value[0]):
		a = 1
	tuple_to_load = ("SELECT COUNT(*) FROM MTSZN_WMARKET.D_EDUCATION", "D_EDUCATION", "TRUNC", a,  origin_count_rows_value[0],sbd_count_rows_value[0])
	list_to_load = [(tuple_to_load)]

	cursor_CH_SBD.executemany("INSERT INTO MTSZN_WMARKET.CHECK_LOAD (SCHEMA, TABLE, TYPE_OF_ETL, CORRECTLY_LOADED,TOTAL_COUNT_SOURCE,TOTAL_COUNT_SDU) VALUES", list_to_load)

	conn_MTSZN.close() 

def D_EDU_HIGH():
	conn_CH_SBD_secrets=Connection.get_connection_from_secrets(conn_id="Clickhouse-5")#скрытый коннекшн 5го сервера
	conn_CH_SBD=connect(host=conn_CH_SBD_secrets.host,port=conn_CH_SBD_secrets.port,user=conn_CH_SBD_secrets.login, password=conn_CH_SBD_secrets.password, connect_timeout=3600)
	cursor_CH_SBD = conn_CH_SBD.cursor()
	conn_MTSZN_secrets=Connection.get_connection_from_secrets(conn_id="MTSZN_WMARKET")#скрытый коннекшн источника
	dsn_tns_secrets=cx_Oracle.makedsn(host=conn_MTSZN_secrets.host,port=conn_MTSZN_secrets.port, service_name='SD')
	conn_MTSZN = cx_Oracle.connect(user =conn_MTSZN_secrets.login, password =conn_MTSZN_secrets.password, dsn=dsn_tns_secrets)
	cursor_MTSZN = conn_MTSZN.cursor()

	cursor_CH_SBD.execute("TRUNCATE TABLE MTSZN_WMARKET.D_EDU_HIGH")

	cursor_MTSZN.execute(f"""SELECT
 CODE AS "CODE"
, NVL(TO_CHAR("DATE_ENTRY", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "DATE_ENTRY"
, NVL(TO_CHAR("DATE_FROM", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "DATE_FROM"
, NVL(TO_CHAR("DATE_TO", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "DATE_TO"
, ENAME AS "ENAME"
, ID AS "ID"
, KNAME AS "KNAME"
, PAR_ID AS "PAR_ID"
, RNAME AS "RNAME"
, USER_ID AS "USER_ID"
FROM WMARKET."D_EDU_HIGH"
""")

	flag = True
	while flag:
		req_MTSZN = cursor_MTSZN.fetchmany(10000)
		if len(req_MTSZN) == 0:
			flag=False
		cursor_CH_SBD.executemany("""INSERT INTO MTSZN_WMARKET.D_EDU_HIGH (CODE, DATE_ENTRY, DATE_FROM, DATE_TO, ENAME, ID, KNAME, PAR_ID, RNAME, USER_ID) VALUES""", req_MTSZN)

	cursor_CH_SBD.execute(f"SELECT COUNT(*) FROM MTSZN_WMARKET.D_EDU_HIGH")
	sbd_count_rows_value = cursor_CH_SBD.fetchone()

	cursor_MTSZN.execute(f"SELECT COUNT(*) FROM WMARKET.D_EDU_HIGH")
	origin_count_rows_value = cursor_MTSZN.fetchone()

	a = 0
	if int(sbd_count_rows_value[0]) == int(origin_count_rows_value[0]):
		a = 1
	tuple_to_load = ("SELECT COUNT(*) FROM MTSZN_WMARKET.D_EDU_HIGH", "D_EDU_HIGH", "TRUNC", a,  origin_count_rows_value[0],sbd_count_rows_value[0])
	list_to_load = [(tuple_to_load)]

	cursor_CH_SBD.executemany("INSERT INTO MTSZN_WMARKET.CHECK_LOAD (SCHEMA, TABLE, TYPE_OF_ETL, CORRECTLY_LOADED,TOTAL_COUNT_SOURCE,TOTAL_COUNT_SDU) VALUES", list_to_load)

	conn_MTSZN.close() 

def D_EMPLOY_TYPE():
	conn_CH_SBD_secrets=Connection.get_connection_from_secrets(conn_id="Clickhouse-5")#скрытый коннекшн 5го сервера
	conn_CH_SBD=connect(host=conn_CH_SBD_secrets.host,port=conn_CH_SBD_secrets.port,user=conn_CH_SBD_secrets.login, password=conn_CH_SBD_secrets.password, connect_timeout=3600)
	cursor_CH_SBD = conn_CH_SBD.cursor()
	conn_MTSZN_secrets=Connection.get_connection_from_secrets(conn_id="MTSZN_WMARKET")#скрытый коннекшн источника
	dsn_tns_secrets=cx_Oracle.makedsn(host=conn_MTSZN_secrets.host,port=conn_MTSZN_secrets.port, service_name='SD')
	conn_MTSZN = cx_Oracle.connect(user =conn_MTSZN_secrets.login, password =conn_MTSZN_secrets.password, dsn=dsn_tns_secrets)
	cursor_MTSZN = conn_MTSZN.cursor()

	cursor_CH_SBD.execute("TRUNCATE TABLE MTSZN_WMARKET.D_EMPLOY_TYPE")

	cursor_MTSZN.execute(f"""SELECT
 CODE AS "CODE"
, NVL(TO_CHAR("DATE_ENTRY", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "DATE_ENTRY"
, NVL(TO_CHAR("DATE_FROM", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "DATE_FROM"
, NVL(TO_CHAR("DATE_TO", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "DATE_TO"
, ENAME AS "ENAME"
, ID AS "ID"
, KNAME AS "KNAME"
, PAR_ID AS "PAR_ID"
, RNAME AS "RNAME"
, USER_ID AS "USER_ID"
FROM WMARKET."D_EMPLOY_TYPE"
""")

	flag = True
	while flag:
		req_MTSZN = cursor_MTSZN.fetchmany(10000)
		if len(req_MTSZN) == 0:
			flag=False
		cursor_CH_SBD.executemany("""INSERT INTO MTSZN_WMARKET.D_EMPLOY_TYPE (CODE, DATE_ENTRY, DATE_FROM, DATE_TO, ENAME, ID, KNAME, PAR_ID, RNAME, USER_ID) VALUES""", req_MTSZN)

	cursor_CH_SBD.execute(f"SELECT COUNT(*) FROM MTSZN_WMARKET.D_EMPLOY_TYPE")
	sbd_count_rows_value = cursor_CH_SBD.fetchone()

	cursor_MTSZN.execute(f"SELECT COUNT(*) FROM WMARKET.D_EMPLOY_TYPE")
	origin_count_rows_value = cursor_MTSZN.fetchone()

	a = 0
	if int(sbd_count_rows_value[0]) == int(origin_count_rows_value[0]):
		a = 1
	tuple_to_load = ("SELECT COUNT(*) FROM MTSZN_WMARKET.D_EMPLOY_TYPE", "D_EMPLOY_TYPE", "TRUNC", a,  origin_count_rows_value[0],sbd_count_rows_value[0])
	list_to_load = [(tuple_to_load)]

	cursor_CH_SBD.executemany("INSERT INTO MTSZN_WMARKET.CHECK_LOAD (SCHEMA, TABLE, TYPE_OF_ETL, CORRECTLY_LOADED,TOTAL_COUNT_SOURCE,TOTAL_COUNT_SDU) VALUES", list_to_load)

	conn_MTSZN.close() 

def D_EMP_ATTR():
	conn_CH_SBD_secrets=Connection.get_connection_from_secrets(conn_id="Clickhouse-5")#скрытый коннекшн 5го сервера
	conn_CH_SBD=connect(host=conn_CH_SBD_secrets.host,port=conn_CH_SBD_secrets.port,user=conn_CH_SBD_secrets.login, password=conn_CH_SBD_secrets.password, connect_timeout=3600)
	cursor_CH_SBD = conn_CH_SBD.cursor()
	conn_MTSZN_secrets=Connection.get_connection_from_secrets(conn_id="MTSZN_WMARKET")#скрытый коннекшн источника
	dsn_tns_secrets=cx_Oracle.makedsn(host=conn_MTSZN_secrets.host,port=conn_MTSZN_secrets.port, service_name='SD')
	conn_MTSZN = cx_Oracle.connect(user =conn_MTSZN_secrets.login, password =conn_MTSZN_secrets.password, dsn=dsn_tns_secrets)
	cursor_MTSZN = conn_MTSZN.cursor()

	cursor_CH_SBD.execute("TRUNCATE TABLE MTSZN_WMARKET.D_EMP_ATTR")

	cursor_MTSZN.execute(f"""SELECT
 CODE AS "CODE"
, NVL(TO_CHAR("DATE_ENTRY", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "DATE_ENTRY"
, NVL(TO_CHAR("DATE_FROM", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "DATE_FROM"
, NVL(TO_CHAR("DATE_TO", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "DATE_TO"
, ENAME AS "ENAME"
, ID AS "ID"
, KNAME AS "KNAME"
, PAR_ID AS "PAR_ID"
, RNAME AS "RNAME"
, USER_ID AS "USER_ID"
FROM WMARKET."D_EMP_ATTR"
""")

	flag = True
	while flag:
		req_MTSZN = cursor_MTSZN.fetchmany(10000)
		if len(req_MTSZN) == 0:
			flag=False
		cursor_CH_SBD.executemany("""INSERT INTO MTSZN_WMARKET.D_EMP_ATTR (CODE, DATE_ENTRY, DATE_FROM, DATE_TO, ENAME, ID, KNAME, PAR_ID, RNAME, USER_ID) VALUES""", req_MTSZN)

	cursor_CH_SBD.execute(f"SELECT COUNT(*) FROM MTSZN_WMARKET.D_EMP_ATTR")
	sbd_count_rows_value = cursor_CH_SBD.fetchone()

	cursor_MTSZN.execute(f"SELECT COUNT(*) FROM WMARKET.D_EMP_ATTR")
	origin_count_rows_value = cursor_MTSZN.fetchone()

	a = 0
	if int(sbd_count_rows_value[0]) == int(origin_count_rows_value[0]):
		a = 1
	tuple_to_load = ("SELECT COUNT(*) FROM MTSZN_WMARKET.D_EMP_ATTR", "D_EMP_ATTR", "TRUNC", a,  origin_count_rows_value[0],sbd_count_rows_value[0])
	list_to_load = [(tuple_to_load)]

	cursor_CH_SBD.executemany("INSERT INTO MTSZN_WMARKET.CHECK_LOAD (SCHEMA, TABLE, TYPE_OF_ETL, CORRECTLY_LOADED,TOTAL_COUNT_SOURCE,TOTAL_COUNT_SDU) VALUES", list_to_load)

	conn_MTSZN.close() 

def D_FOREIGN_CAT():
	conn_CH_SBD_secrets=Connection.get_connection_from_secrets(conn_id="Clickhouse-5")#скрытый коннекшн 5го сервера
	conn_CH_SBD=connect(host=conn_CH_SBD_secrets.host,port=conn_CH_SBD_secrets.port,user=conn_CH_SBD_secrets.login, password=conn_CH_SBD_secrets.password, connect_timeout=3600)
	cursor_CH_SBD = conn_CH_SBD.cursor()
	conn_MTSZN_secrets=Connection.get_connection_from_secrets(conn_id="MTSZN_WMARKET")#скрытый коннекшн источника
	dsn_tns_secrets=cx_Oracle.makedsn(host=conn_MTSZN_secrets.host,port=conn_MTSZN_secrets.port, service_name='SD')
	conn_MTSZN = cx_Oracle.connect(user =conn_MTSZN_secrets.login, password =conn_MTSZN_secrets.password, dsn=dsn_tns_secrets)
	cursor_MTSZN = conn_MTSZN.cursor()

	cursor_CH_SBD.execute("TRUNCATE TABLE MTSZN_WMARKET.D_FOREIGN_CAT")

	cursor_MTSZN.execute(f"""SELECT
 CODE AS "CODE"
, NVL(TO_CHAR("DATE_ENTRY", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "DATE_ENTRY"
, NVL(TO_CHAR("DATE_FROM", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "DATE_FROM"
, NVL(TO_CHAR("DATE_TO", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "DATE_TO"
, ENAME AS "ENAME"
, FULL_ENAME AS "FULL_ENAME"
, FULL_KNAME AS "FULL_KNAME"
, FULL_RNAME AS "FULL_RNAME"
, ID AS "ID"
, KNAME AS "KNAME"
, PAR_ID AS "PAR_ID"
, RNAME AS "RNAME"
, USER_ID AS "USER_ID"
FROM WMARKET."D_FOREIGN_CAT"
""")

	flag = True
	while flag:
		req_MTSZN = cursor_MTSZN.fetchmany(10000)
		if len(req_MTSZN) == 0:
			flag=False
		cursor_CH_SBD.executemany("""INSERT INTO MTSZN_WMARKET.D_FOREIGN_CAT (CODE, DATE_ENTRY, DATE_FROM, DATE_TO, ENAME, FULL_ENAME, FULL_KNAME, FULL_RNAME, ID, KNAME, PAR_ID, RNAME, USER_ID) VALUES""", req_MTSZN)

	cursor_CH_SBD.execute(f"SELECT COUNT(*) FROM MTSZN_WMARKET.D_FOREIGN_CAT")
	sbd_count_rows_value = cursor_CH_SBD.fetchone()

	cursor_MTSZN.execute(f"SELECT COUNT(*) FROM WMARKET.D_FOREIGN_CAT")
	origin_count_rows_value = cursor_MTSZN.fetchone()

	a = 0
	if int(sbd_count_rows_value[0]) == int(origin_count_rows_value[0]):
		a = 1
	tuple_to_load = ("SELECT COUNT(*) FROM MTSZN_WMARKET.D_FOREIGN_CAT", "D_FOREIGN_CAT", "TRUNC", a,  origin_count_rows_value[0],sbd_count_rows_value[0])
	list_to_load = [(tuple_to_load)]

	cursor_CH_SBD.executemany("INSERT INTO MTSZN_WMARKET.CHECK_LOAD (SCHEMA, TABLE, TYPE_OF_ETL, CORRECTLY_LOADED,TOTAL_COUNT_SOURCE,TOTAL_COUNT_SDU) VALUES", list_to_load)

	conn_MTSZN.close() 

def D_FREE_SEARCH_IRS():
	conn_CH_SBD_secrets=Connection.get_connection_from_secrets(conn_id="Clickhouse-5")#скрытый коннекшн 5го сервера
	conn_CH_SBD=connect(host=conn_CH_SBD_secrets.host,port=conn_CH_SBD_secrets.port,user=conn_CH_SBD_secrets.login, password=conn_CH_SBD_secrets.password, connect_timeout=3600)
	cursor_CH_SBD = conn_CH_SBD.cursor()
	conn_MTSZN_secrets=Connection.get_connection_from_secrets(conn_id="MTSZN_WMARKET")#скрытый коннекшн источника
	dsn_tns_secrets=cx_Oracle.makedsn(host=conn_MTSZN_secrets.host,port=conn_MTSZN_secrets.port, service_name='SD')
	conn_MTSZN = cx_Oracle.connect(user =conn_MTSZN_secrets.login, password =conn_MTSZN_secrets.password, dsn=dsn_tns_secrets)
	cursor_MTSZN = conn_MTSZN.cursor()

	cursor_CH_SBD.execute("TRUNCATE TABLE MTSZN_WMARKET.D_FREE_SEARCH_IRS")

	cursor_MTSZN.execute(f"""SELECT
 CODE AS "CODE"
, NVL(TO_CHAR("DATE_ENTRY", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "DATE_ENTRY"
, NVL(TO_CHAR("DATE_FROM", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "DATE_FROM"
, NVL(TO_CHAR("DATE_TO", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "DATE_TO"
, DIR_NAME AS "DIR_NAME"
, ENAME AS "ENAME"
, ID AS "ID"
, KNAME AS "KNAME"
, LENGHT_FLD AS "LENGHT_FLD"
, PARAM_NAME AS "PARAM_NAME"
, PAR_ID AS "PAR_ID"
, RNAME AS "RNAME"
, TYPE_FLD AS "TYPE_FLD"
, USER_ID AS "USER_ID"
FROM WMARKET."D_FREE_SEARCH_IRS"
""")

	flag = True
	while flag:
		req_MTSZN = cursor_MTSZN.fetchmany(10000)
		if len(req_MTSZN) == 0:
			flag=False
		cursor_CH_SBD.executemany("""INSERT INTO MTSZN_WMARKET.D_FREE_SEARCH_IRS (CODE, DATE_ENTRY, DATE_FROM, DATE_TO, DIR_NAME, ENAME, ID, KNAME, LENGHT_FLD, PARAM_NAME, PAR_ID, RNAME, TYPE_FLD, USER_ID) VALUES""", req_MTSZN)

	cursor_CH_SBD.execute(f"SELECT COUNT(*) FROM MTSZN_WMARKET.D_FREE_SEARCH_IRS")
	sbd_count_rows_value = cursor_CH_SBD.fetchone()

	cursor_MTSZN.execute(f"SELECT COUNT(*) FROM WMARKET.D_FREE_SEARCH_IRS")
	origin_count_rows_value = cursor_MTSZN.fetchone()

	a = 0
	if int(sbd_count_rows_value[0]) == int(origin_count_rows_value[0]):
		a = 1
	tuple_to_load = ("SELECT COUNT(*) FROM MTSZN_WMARKET.D_FREE_SEARCH_IRS", "D_FREE_SEARCH_IRS", "TRUNC", a,  origin_count_rows_value[0],sbd_count_rows_value[0])
	list_to_load = [(tuple_to_load)]

	cursor_CH_SBD.executemany("INSERT INTO MTSZN_WMARKET.CHECK_LOAD (SCHEMA, TABLE, TYPE_OF_ETL, CORRECTLY_LOADED,TOTAL_COUNT_SOURCE,TOTAL_COUNT_SDU) VALUES", list_to_load)

	conn_MTSZN.close() 

def D_FREE_SEARCH_IRS_COLS():
	conn_CH_SBD_secrets=Connection.get_connection_from_secrets(conn_id="Clickhouse-5")#скрытый коннекшн 5го сервера
	conn_CH_SBD=connect(host=conn_CH_SBD_secrets.host,port=conn_CH_SBD_secrets.port,user=conn_CH_SBD_secrets.login, password=conn_CH_SBD_secrets.password, connect_timeout=3600)
	cursor_CH_SBD = conn_CH_SBD.cursor()
	conn_MTSZN_secrets=Connection.get_connection_from_secrets(conn_id="MTSZN_WMARKET")#скрытый коннекшн источника
	dsn_tns_secrets=cx_Oracle.makedsn(host=conn_MTSZN_secrets.host,port=conn_MTSZN_secrets.port, service_name='SD')
	conn_MTSZN = cx_Oracle.connect(user =conn_MTSZN_secrets.login, password =conn_MTSZN_secrets.password, dsn=dsn_tns_secrets)
	cursor_MTSZN = conn_MTSZN.cursor()

	cursor_CH_SBD.execute("TRUNCATE TABLE MTSZN_WMARKET.D_FREE_SEARCH_IRS_COLS")

	cursor_MTSZN.execute(f"""SELECT
 CODE AS "CODE"
, NVL(TO_CHAR("DATE_ENTRY", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "DATE_ENTRY"
, NVL(TO_CHAR("DATE_FROM", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "DATE_FROM"
, NVL(TO_CHAR("DATE_TO", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "DATE_TO"
, ENAME AS "ENAME"
, FIELD_NAME AS "FIELD_NAME"
, FROM_PARAM AS "FROM_PARAM"
, ID AS "ID"
, KNAME AS "KNAME"
, MESSAGE_PARAM AS "MESSAGE_PARAM"
, PAR_ID AS "PAR_ID"
, PAR_PARAM AS "PAR_PARAM"
, RNAME AS "RNAME"
, TABLE_ALIAS AS "TABLE_ALIAS"
, TABLE_NAME AS "TABLE_NAME"
, USER_ID AS "USER_ID"
, WHERE_PARAM AS "WHERE_PARAM"
, WHERE_TABLE AS "WHERE_TABLE"
FROM WMARKET."D_FREE_SEARCH_IRS_COLS"
""")

	flag = True
	while flag:
		req_MTSZN = cursor_MTSZN.fetchmany(10000)
		if len(req_MTSZN) == 0:
			flag=False
		cursor_CH_SBD.executemany("""INSERT INTO MTSZN_WMARKET.D_FREE_SEARCH_IRS_COLS (CODE, DATE_ENTRY, DATE_FROM, DATE_TO, ENAME, FIELD_NAME, FROM_PARAM, ID, KNAME, MESSAGE_PARAM, PAR_ID, PAR_PARAM, RNAME, TABLE_ALIAS, TABLE_NAME, USER_ID, WHERE_PARAM, WHERE_TABLE) VALUES""", req_MTSZN)

	cursor_CH_SBD.execute(f"SELECT COUNT(*) FROM MTSZN_WMARKET.D_FREE_SEARCH_IRS_COLS")
	sbd_count_rows_value = cursor_CH_SBD.fetchone()

	cursor_MTSZN.execute(f"SELECT COUNT(*) FROM WMARKET.D_FREE_SEARCH_IRS_COLS")
	origin_count_rows_value = cursor_MTSZN.fetchone()

	a = 0
	if int(sbd_count_rows_value[0]) == int(origin_count_rows_value[0]):
		a = 1
	tuple_to_load = ("SELECT COUNT(*) FROM MTSZN_WMARKET.D_FREE_SEARCH_IRS_COLS", "D_FREE_SEARCH_IRS_COLS", "TRUNC", a,  origin_count_rows_value[0],sbd_count_rows_value[0])
	list_to_load = [(tuple_to_load)]

	cursor_CH_SBD.executemany("INSERT INTO MTSZN_WMARKET.CHECK_LOAD (SCHEMA, TABLE, TYPE_OF_ETL, CORRECTLY_LOADED,TOTAL_COUNT_SOURCE,TOTAL_COUNT_SDU) VALUES", list_to_load)

	conn_MTSZN.close() 

def D_F_INDEP_PROF():
	conn_CH_SBD_secrets=Connection.get_connection_from_secrets(conn_id="Clickhouse-5")#скрытый коннекшн 5го сервера
	conn_CH_SBD=connect(host=conn_CH_SBD_secrets.host,port=conn_CH_SBD_secrets.port,user=conn_CH_SBD_secrets.login, password=conn_CH_SBD_secrets.password, connect_timeout=3600)
	cursor_CH_SBD = conn_CH_SBD.cursor()
	conn_MTSZN_secrets=Connection.get_connection_from_secrets(conn_id="MTSZN_WMARKET")#скрытый коннекшн источника
	dsn_tns_secrets=cx_Oracle.makedsn(host=conn_MTSZN_secrets.host,port=conn_MTSZN_secrets.port, service_name='SD')
	conn_MTSZN = cx_Oracle.connect(user =conn_MTSZN_secrets.login, password =conn_MTSZN_secrets.password, dsn=dsn_tns_secrets)
	cursor_MTSZN = conn_MTSZN.cursor()

	cursor_CH_SBD.execute("TRUNCATE TABLE MTSZN_WMARKET.D_F_INDEP_PROF")

	cursor_MTSZN.execute(f"""SELECT
 CODE AS "CODE"
, NVL(TO_CHAR("DATE_ENTRY", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "DATE_ENTRY"
, NVL(TO_CHAR("DATE_FROM", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "DATE_FROM"
, NVL(TO_CHAR("DATE_TO", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "DATE_TO"
, ENAME AS "ENAME"
, ID AS "ID"
, KNAME AS "KNAME"
, PAR_ID AS "PAR_ID"
, PROF_CAT AS "PROF_CAT"
, PROF_CAT_ID AS "PROF_CAT_ID"
, RNAME AS "RNAME"
, USER_ID AS "USER_ID"
FROM WMARKET."D_F_INDEP_PROF"
""")

	flag = True
	while flag:
		req_MTSZN = cursor_MTSZN.fetchmany(10000)
		if len(req_MTSZN) == 0:
			flag=False
		cursor_CH_SBD.executemany("""INSERT INTO MTSZN_WMARKET.D_F_INDEP_PROF (CODE, DATE_ENTRY, DATE_FROM, DATE_TO, ENAME, ID, KNAME, PAR_ID, PROF_CAT, PROF_CAT_ID, RNAME, USER_ID) VALUES""", req_MTSZN)

	cursor_CH_SBD.execute(f"SELECT COUNT(*) FROM MTSZN_WMARKET.D_F_INDEP_PROF")
	sbd_count_rows_value = cursor_CH_SBD.fetchone()

	cursor_MTSZN.execute(f"SELECT COUNT(*) FROM WMARKET.D_F_INDEP_PROF")
	origin_count_rows_value = cursor_MTSZN.fetchone()

	a = 0
	if int(sbd_count_rows_value[0]) == int(origin_count_rows_value[0]):
		a = 1
	tuple_to_load = ("SELECT COUNT(*) FROM MTSZN_WMARKET.D_F_INDEP_PROF", "D_F_INDEP_PROF", "TRUNC", a,  origin_count_rows_value[0],sbd_count_rows_value[0])
	list_to_load = [(tuple_to_load)]

	cursor_CH_SBD.executemany("INSERT INTO MTSZN_WMARKET.CHECK_LOAD (SCHEMA, TABLE, TYPE_OF_ETL, CORRECTLY_LOADED,TOTAL_COUNT_SOURCE,TOTAL_COUNT_SDU) VALUES", list_to_load)

	conn_MTSZN.close() 

def D_F_PERSON_TYPE():
	conn_CH_SBD_secrets=Connection.get_connection_from_secrets(conn_id="Clickhouse-5")#скрытый коннекшн 5го сервера
	conn_CH_SBD=connect(host=conn_CH_SBD_secrets.host,port=conn_CH_SBD_secrets.port,user=conn_CH_SBD_secrets.login, password=conn_CH_SBD_secrets.password, connect_timeout=3600)
	cursor_CH_SBD = conn_CH_SBD.cursor()
	conn_MTSZN_secrets=Connection.get_connection_from_secrets(conn_id="MTSZN_WMARKET")#скрытый коннекшн источника
	dsn_tns_secrets=cx_Oracle.makedsn(host=conn_MTSZN_secrets.host,port=conn_MTSZN_secrets.port, service_name='SD')
	conn_MTSZN = cx_Oracle.connect(user =conn_MTSZN_secrets.login, password =conn_MTSZN_secrets.password, dsn=dsn_tns_secrets)
	cursor_MTSZN = conn_MTSZN.cursor()

	cursor_CH_SBD.execute("TRUNCATE TABLE MTSZN_WMARKET.D_F_PERSON_TYPE")

	cursor_MTSZN.execute(f"""SELECT
 CODE AS "CODE"
, NVL(TO_CHAR("DATE_ENTRY", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "DATE_ENTRY"
, NVL(TO_CHAR("DATE_FROM", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "DATE_FROM"
, NVL(TO_CHAR("DATE_TO", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "DATE_TO"
, ENAME AS "ENAME"
, ID AS "ID"
, KNAME AS "KNAME"
, PAR_ID AS "PAR_ID"
, RNAME AS "RNAME"
, USER_ID AS "USER_ID"
FROM WMARKET."D_F_PERSON_TYPE"
""")

	flag = True
	while flag:
		req_MTSZN = cursor_MTSZN.fetchmany(10000)
		if len(req_MTSZN) == 0:
			flag=False
		cursor_CH_SBD.executemany("""INSERT INTO MTSZN_WMARKET.D_F_PERSON_TYPE (CODE, DATE_ENTRY, DATE_FROM, DATE_TO, ENAME, ID, KNAME, PAR_ID, RNAME, USER_ID) VALUES""", req_MTSZN)

	cursor_CH_SBD.execute(f"SELECT COUNT(*) FROM MTSZN_WMARKET.D_F_PERSON_TYPE")
	sbd_count_rows_value = cursor_CH_SBD.fetchone()

	cursor_MTSZN.execute(f"SELECT COUNT(*) FROM WMARKET.D_F_PERSON_TYPE")
	origin_count_rows_value = cursor_MTSZN.fetchone()

	a = 0
	if int(sbd_count_rows_value[0]) == int(origin_count_rows_value[0]):
		a = 1
	tuple_to_load = ("SELECT COUNT(*) FROM MTSZN_WMARKET.D_F_PERSON_TYPE", "D_F_PERSON_TYPE", "TRUNC", a,  origin_count_rows_value[0],sbd_count_rows_value[0])
	list_to_load = [(tuple_to_load)]

	cursor_CH_SBD.executemany("INSERT INTO MTSZN_WMARKET.CHECK_LOAD (SCHEMA, TABLE, TYPE_OF_ETL, CORRECTLY_LOADED,TOTAL_COUNT_SOURCE,TOTAL_COUNT_SDU) VALUES", list_to_load)

	conn_MTSZN.close() 

def D_NATION():
	conn_CH_SBD_secrets=Connection.get_connection_from_secrets(conn_id="Clickhouse-5")#скрытый коннекшн 5го сервера
	conn_CH_SBD=connect(host=conn_CH_SBD_secrets.host,port=conn_CH_SBD_secrets.port,user=conn_CH_SBD_secrets.login, password=conn_CH_SBD_secrets.password, connect_timeout=3600)
	cursor_CH_SBD = conn_CH_SBD.cursor()
	conn_MTSZN_secrets=Connection.get_connection_from_secrets(conn_id="MTSZN_WMARKET")#скрытый коннекшн источника
	dsn_tns_secrets=cx_Oracle.makedsn(host=conn_MTSZN_secrets.host,port=conn_MTSZN_secrets.port, service_name='SD')
	conn_MTSZN = cx_Oracle.connect(user =conn_MTSZN_secrets.login, password =conn_MTSZN_secrets.password, dsn=dsn_tns_secrets)
	cursor_MTSZN = conn_MTSZN.cursor()

	cursor_CH_SBD.execute("TRUNCATE TABLE MTSZN_WMARKET.D_NATION")

	cursor_MTSZN.execute(f"""SELECT
 CODE AS "CODE"
, NVL(TO_CHAR("DATE_ENTRY", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "DATE_ENTRY"
, NVL(TO_CHAR("DATE_FROM", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "DATE_FROM"
, NVL(TO_CHAR("DATE_TO", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "DATE_TO"
, ENAME AS "ENAME"
, GBDFL_CODE AS "GBDFL_CODE"
, GBDUL_CODE AS "GBDUL_CODE"
, ID AS "ID"
, KNAME AS "KNAME"
, PAR_ID AS "PAR_ID"
, RNAME AS "RNAME"
, USER_ID AS "USER_ID"
FROM WMARKET."D_NATION"
""")

	flag = True
	while flag:
		req_MTSZN = cursor_MTSZN.fetchmany(10000)
		if len(req_MTSZN) == 0:
			flag=False
		cursor_CH_SBD.executemany("""INSERT INTO MTSZN_WMARKET.D_NATION (CODE, DATE_ENTRY, DATE_FROM, DATE_TO, ENAME, GBDFL_CODE, GBDUL_CODE, ID, KNAME, PAR_ID, RNAME, USER_ID) VALUES""", req_MTSZN)

	cursor_CH_SBD.execute(f"SELECT COUNT(*) FROM MTSZN_WMARKET.D_NATION")
	sbd_count_rows_value = cursor_CH_SBD.fetchone()

	cursor_MTSZN.execute(f"SELECT COUNT(*) FROM WMARKET.D_NATION")
	origin_count_rows_value = cursor_MTSZN.fetchone()

	a = 0
	if int(sbd_count_rows_value[0]) == int(origin_count_rows_value[0]):
		a = 1
	tuple_to_load = ("SELECT COUNT(*) FROM MTSZN_WMARKET.D_NATION", "D_NATION", "TRUNC", a,  origin_count_rows_value[0],sbd_count_rows_value[0])
	list_to_load = [(tuple_to_load)]

	cursor_CH_SBD.executemany("INSERT INTO MTSZN_WMARKET.CHECK_LOAD (SCHEMA, TABLE, TYPE_OF_ETL, CORRECTLY_LOADED,TOTAL_COUNT_SOURCE,TOTAL_COUNT_SDU) VALUES", list_to_load)

	conn_MTSZN.close() 

def D_NATION_FL():
	conn_CH_SBD_secrets=Connection.get_connection_from_secrets(conn_id="Clickhouse-5")#скрытый коннекшн 5го сервера
	conn_CH_SBD=connect(host=conn_CH_SBD_secrets.host,port=conn_CH_SBD_secrets.port,user=conn_CH_SBD_secrets.login, password=conn_CH_SBD_secrets.password, connect_timeout=3600)
	cursor_CH_SBD = conn_CH_SBD.cursor()
	conn_MTSZN_secrets=Connection.get_connection_from_secrets(conn_id="MTSZN_WMARKET")#скрытый коннекшн источника
	dsn_tns_secrets=cx_Oracle.makedsn(host=conn_MTSZN_secrets.host,port=conn_MTSZN_secrets.port, service_name='SD')
	conn_MTSZN = cx_Oracle.connect(user =conn_MTSZN_secrets.login, password =conn_MTSZN_secrets.password, dsn=dsn_tns_secrets)
	cursor_MTSZN = conn_MTSZN.cursor()

	cursor_CH_SBD.execute("TRUNCATE TABLE MTSZN_WMARKET.D_NATION_FL")

	cursor_MTSZN.execute(f"""SELECT
 CODE AS "CODE"
, NVL(TO_CHAR("DATE_ENTRY", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "DATE_ENTRY"
, NVL(TO_CHAR("DATE_FROM", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "DATE_FROM"
, NVL(TO_CHAR("DATE_TO", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "DATE_TO"
, ENAME AS "ENAME"
, GBDFL_CODE AS "GBDFL_CODE"
, GBDUL_CODE AS "GBDUL_CODE"
, ID AS "ID"
, KNAME AS "KNAME"
, PAR_ID AS "PAR_ID"
, RNAME AS "RNAME"
, USER_ID AS "USER_ID"
FROM WMARKET."D_NATION_FL"
""")

	flag = True
	while flag:
		req_MTSZN = cursor_MTSZN.fetchmany(10000)
		if len(req_MTSZN) == 0:
			flag=False
		cursor_CH_SBD.executemany("""INSERT INTO MTSZN_WMARKET.D_NATION_FL (CODE, DATE_ENTRY, DATE_FROM, DATE_TO, ENAME, GBDFL_CODE, GBDUL_CODE, ID, KNAME, PAR_ID, RNAME, USER_ID) VALUES""", req_MTSZN)

	cursor_CH_SBD.execute(f"SELECT COUNT(*) FROM MTSZN_WMARKET.D_NATION_FL")
	sbd_count_rows_value = cursor_CH_SBD.fetchone()

	cursor_MTSZN.execute(f"SELECT COUNT(*) FROM WMARKET.D_NATION_FL")
	origin_count_rows_value = cursor_MTSZN.fetchone()

	a = 0
	if int(sbd_count_rows_value[0]) == int(origin_count_rows_value[0]):
		a = 1
	tuple_to_load = ("SELECT COUNT(*) FROM MTSZN_WMARKET.D_NATION_FL", "D_NATION_FL", "TRUNC", a,  origin_count_rows_value[0],sbd_count_rows_value[0])
	list_to_load = [(tuple_to_load)]

	cursor_CH_SBD.executemany("INSERT INTO MTSZN_WMARKET.CHECK_LOAD (SCHEMA, TABLE, TYPE_OF_ETL, CORRECTLY_LOADED,TOTAL_COUNT_SOURCE,TOTAL_COUNT_SDU) VALUES", list_to_load)

	conn_MTSZN.close() 

def D_OKED():
	conn_CH_SBD_secrets=Connection.get_connection_from_secrets(conn_id="Clickhouse-5")#скрытый коннекшн 5го сервера
	conn_CH_SBD=connect(host=conn_CH_SBD_secrets.host,port=conn_CH_SBD_secrets.port,user=conn_CH_SBD_secrets.login, password=conn_CH_SBD_secrets.password, connect_timeout=3600)
	cursor_CH_SBD = conn_CH_SBD.cursor()
	conn_MTSZN_secrets=Connection.get_connection_from_secrets(conn_id="MTSZN_WMARKET")#скрытый коннекшн источника
	dsn_tns_secrets=cx_Oracle.makedsn(host=conn_MTSZN_secrets.host,port=conn_MTSZN_secrets.port, service_name='SD')
	conn_MTSZN = cx_Oracle.connect(user =conn_MTSZN_secrets.login, password =conn_MTSZN_secrets.password, dsn=dsn_tns_secrets)
	cursor_MTSZN = conn_MTSZN.cursor()

	cursor_CH_SBD.execute("TRUNCATE TABLE MTSZN_WMARKET.D_OKED")

	cursor_MTSZN.execute(f"""SELECT
 CODE AS "CODE"
, NVL(TO_CHAR("DATE_ENTRY", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "DATE_ENTRY"
, NVL(TO_CHAR("DATE_FROM", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "DATE_FROM"
, NVL(TO_CHAR("DATE_TO", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "DATE_TO"
, ENAME AS "ENAME"
, ID AS "ID"
, KNAME AS "KNAME"
, MAIN_PAR_ID AS "MAIN_PAR_ID"
, PAR_ID AS "PAR_ID"
, RNAME AS "RNAME"
, USER_ID AS "USER_ID"
FROM WMARKET."D_OKED"
""")

	flag = True
	while flag:
		req_MTSZN = cursor_MTSZN.fetchmany(10000)
		if len(req_MTSZN) == 0:
			flag=False
		cursor_CH_SBD.executemany("""INSERT INTO MTSZN_WMARKET.D_OKED (CODE, DATE_ENTRY, DATE_FROM, DATE_TO, ENAME, ID, KNAME, MAIN_PAR_ID, PAR_ID, RNAME, USER_ID) VALUES""", req_MTSZN)

	cursor_CH_SBD.execute(f"SELECT COUNT(*) FROM MTSZN_WMARKET.D_OKED")
	sbd_count_rows_value = cursor_CH_SBD.fetchone()

	cursor_MTSZN.execute(f"SELECT COUNT(*) FROM WMARKET.D_OKED")
	origin_count_rows_value = cursor_MTSZN.fetchone()

	a = 0
	if int(sbd_count_rows_value[0]) == int(origin_count_rows_value[0]):
		a = 1
	tuple_to_load = ("SELECT COUNT(*) FROM MTSZN_WMARKET.D_OKED", "D_OKED", "TRUNC", a,  origin_count_rows_value[0],sbd_count_rows_value[0])
	list_to_load = [(tuple_to_load)]

	cursor_CH_SBD.executemany("INSERT INTO MTSZN_WMARKET.CHECK_LOAD (SCHEMA, TABLE, TYPE_OF_ETL, CORRECTLY_LOADED,TOTAL_COUNT_SOURCE,TOTAL_COUNT_SDU) VALUES", list_to_load)

	conn_MTSZN.close() 

def D_PERIOD_TYPE():
	conn_CH_SBD_secrets=Connection.get_connection_from_secrets(conn_id="Clickhouse-5")#скрытый коннекшн 5го сервера
	conn_CH_SBD=connect(host=conn_CH_SBD_secrets.host,port=conn_CH_SBD_secrets.port,user=conn_CH_SBD_secrets.login, password=conn_CH_SBD_secrets.password, connect_timeout=3600)
	cursor_CH_SBD = conn_CH_SBD.cursor()
	conn_MTSZN_secrets=Connection.get_connection_from_secrets(conn_id="MTSZN_WMARKET")#скрытый коннекшн источника
	dsn_tns_secrets=cx_Oracle.makedsn(host=conn_MTSZN_secrets.host,port=conn_MTSZN_secrets.port, service_name='SD')
	conn_MTSZN = cx_Oracle.connect(user =conn_MTSZN_secrets.login, password =conn_MTSZN_secrets.password, dsn=dsn_tns_secrets)
	cursor_MTSZN = conn_MTSZN.cursor()

	cursor_CH_SBD.execute("TRUNCATE TABLE MTSZN_WMARKET.D_PERIOD_TYPE")

	cursor_MTSZN.execute(f"""SELECT
 CODE AS "CODE"
, NVL(TO_CHAR("DATE_ENTRY", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "DATE_ENTRY"
, NVL(TO_CHAR("DATE_FROM", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "DATE_FROM"
, NVL(TO_CHAR("DATE_TO", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "DATE_TO"
, ENAME AS "ENAME"
, ID AS "ID"
, KNAME AS "KNAME"
, PAR_ID AS "PAR_ID"
, PERIOD_AMOUNT AS "PERIOD_AMOUNT"
, PERIOD_CODE AS "PERIOD_CODE"
, RNAME AS "RNAME"
, USER_ID AS "USER_ID"
FROM WMARKET."D_PERIOD_TYPE"
""")

	flag = True
	while flag:
		req_MTSZN = cursor_MTSZN.fetchmany(10000)
		if len(req_MTSZN) == 0:
			flag=False
		cursor_CH_SBD.executemany("""INSERT INTO MTSZN_WMARKET.D_PERIOD_TYPE (CODE, DATE_ENTRY, DATE_FROM, DATE_TO, ENAME, ID, KNAME, PAR_ID, PERIOD_AMOUNT, PERIOD_CODE, RNAME, USER_ID) VALUES""", req_MTSZN)

	cursor_CH_SBD.execute(f"SELECT COUNT(*) FROM MTSZN_WMARKET.D_PERIOD_TYPE")
	sbd_count_rows_value = cursor_CH_SBD.fetchone()

	cursor_MTSZN.execute(f"SELECT COUNT(*) FROM WMARKET.D_PERIOD_TYPE")
	origin_count_rows_value = cursor_MTSZN.fetchone()

	a = 0
	if int(sbd_count_rows_value[0]) == int(origin_count_rows_value[0]):
		a = 1
	tuple_to_load = ("SELECT COUNT(*) FROM MTSZN_WMARKET.D_PERIOD_TYPE", "D_PERIOD_TYPE", "TRUNC", a,  origin_count_rows_value[0],sbd_count_rows_value[0])
	list_to_load = [(tuple_to_load)]

	cursor_CH_SBD.executemany("INSERT INTO MTSZN_WMARKET.CHECK_LOAD (SCHEMA, TABLE, TYPE_OF_ETL, CORRECTLY_LOADED,TOTAL_COUNT_SOURCE,TOTAL_COUNT_SDU) VALUES", list_to_load)

	conn_MTSZN.close() 

def D_PERMIT_STAT():
	conn_CH_SBD_secrets=Connection.get_connection_from_secrets(conn_id="Clickhouse-5")#скрытый коннекшн 5го сервера
	conn_CH_SBD=connect(host=conn_CH_SBD_secrets.host,port=conn_CH_SBD_secrets.port,user=conn_CH_SBD_secrets.login, password=conn_CH_SBD_secrets.password, connect_timeout=3600)
	cursor_CH_SBD = conn_CH_SBD.cursor()
	conn_MTSZN_secrets=Connection.get_connection_from_secrets(conn_id="MTSZN_WMARKET")#скрытый коннекшн источника
	dsn_tns_secrets=cx_Oracle.makedsn(host=conn_MTSZN_secrets.host,port=conn_MTSZN_secrets.port, service_name='SD')
	conn_MTSZN = cx_Oracle.connect(user =conn_MTSZN_secrets.login, password =conn_MTSZN_secrets.password, dsn=dsn_tns_secrets)
	cursor_MTSZN = conn_MTSZN.cursor()

	cursor_CH_SBD.execute("TRUNCATE TABLE MTSZN_WMARKET.D_PERMIT_STAT")

	cursor_MTSZN.execute(f"""SELECT
 CODE AS "CODE"
, NVL(TO_CHAR("DATE_ENTRY", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "DATE_ENTRY"
, NVL(TO_CHAR("DATE_FROM", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "DATE_FROM"
, NVL(TO_CHAR("DATE_TO", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "DATE_TO"
, ENAME AS "ENAME"
, ID AS "ID"
, KNAME AS "KNAME"
, PAR_ID AS "PAR_ID"
, RNAME AS "RNAME"
, USER_ID AS "USER_ID"
FROM WMARKET."D_PERMIT_STAT"
""")

	flag = True
	while flag:
		req_MTSZN = cursor_MTSZN.fetchmany(10000)
		if len(req_MTSZN) == 0:
			flag=False
		cursor_CH_SBD.executemany("""INSERT INTO MTSZN_WMARKET.D_PERMIT_STAT (CODE, DATE_ENTRY, DATE_FROM, DATE_TO, ENAME, ID, KNAME, PAR_ID, RNAME, USER_ID) VALUES""", req_MTSZN)

	cursor_CH_SBD.execute(f"SELECT COUNT(*) FROM MTSZN_WMARKET.D_PERMIT_STAT")
	sbd_count_rows_value = cursor_CH_SBD.fetchone()

	cursor_MTSZN.execute(f"SELECT COUNT(*) FROM WMARKET.D_PERMIT_STAT")
	origin_count_rows_value = cursor_MTSZN.fetchone()

	a = 0
	if int(sbd_count_rows_value[0]) == int(origin_count_rows_value[0]):
		a = 1
	tuple_to_load = ("SELECT COUNT(*) FROM MTSZN_WMARKET.D_PERMIT_STAT", "D_PERMIT_STAT", "TRUNC", a,  origin_count_rows_value[0],sbd_count_rows_value[0])
	list_to_load = [(tuple_to_load)]

	cursor_CH_SBD.executemany("INSERT INTO MTSZN_WMARKET.CHECK_LOAD (SCHEMA, TABLE, TYPE_OF_ETL, CORRECTLY_LOADED,TOTAL_COUNT_SOURCE,TOTAL_COUNT_SDU) VALUES", list_to_load)

	conn_MTSZN.close() 

def D_PERMIT_TYPE():
	conn_CH_SBD_secrets=Connection.get_connection_from_secrets(conn_id="Clickhouse-5")#скрытый коннекшн 5го сервера
	conn_CH_SBD=connect(host=conn_CH_SBD_secrets.host,port=conn_CH_SBD_secrets.port,user=conn_CH_SBD_secrets.login, password=conn_CH_SBD_secrets.password, connect_timeout=3600)
	cursor_CH_SBD = conn_CH_SBD.cursor()
	conn_MTSZN_secrets=Connection.get_connection_from_secrets(conn_id="MTSZN_WMARKET")#скрытый коннекшн источника
	dsn_tns_secrets=cx_Oracle.makedsn(host=conn_MTSZN_secrets.host,port=conn_MTSZN_secrets.port, service_name='SD')
	conn_MTSZN = cx_Oracle.connect(user =conn_MTSZN_secrets.login, password =conn_MTSZN_secrets.password, dsn=dsn_tns_secrets)
	cursor_MTSZN = conn_MTSZN.cursor()

	cursor_CH_SBD.execute("TRUNCATE TABLE MTSZN_WMARKET.D_PERMIT_TYPE")

	cursor_MTSZN.execute(f"""SELECT
 CODE AS "CODE"
, NVL(TO_CHAR("DATE_ENTRY", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "DATE_ENTRY"
, NVL(TO_CHAR("DATE_FROM", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "DATE_FROM"
, NVL(TO_CHAR("DATE_TO", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "DATE_TO"
, ENAME AS "ENAME"
, ID AS "ID"
, KNAME AS "KNAME"
, PAR_ID AS "PAR_ID"
, RNAME AS "RNAME"
, USER_ID AS "USER_ID"
FROM WMARKET."D_PERMIT_TYPE"
""")

	flag = True
	while flag:
		req_MTSZN = cursor_MTSZN.fetchmany(10000)
		if len(req_MTSZN) == 0:
			flag=False
		cursor_CH_SBD.executemany("""INSERT INTO MTSZN_WMARKET.D_PERMIT_TYPE (CODE, DATE_ENTRY, DATE_FROM, DATE_TO, ENAME, ID, KNAME, PAR_ID, RNAME, USER_ID) VALUES""", req_MTSZN)

	cursor_CH_SBD.execute(f"SELECT COUNT(*) FROM MTSZN_WMARKET.D_PERMIT_TYPE")
	sbd_count_rows_value = cursor_CH_SBD.fetchone()

	cursor_MTSZN.execute(f"SELECT COUNT(*) FROM WMARKET.D_PERMIT_TYPE")
	origin_count_rows_value = cursor_MTSZN.fetchone()

	a = 0
	if int(sbd_count_rows_value[0]) == int(origin_count_rows_value[0]):
		a = 1
	tuple_to_load = ("SELECT COUNT(*) FROM MTSZN_WMARKET.D_PERMIT_TYPE", "D_PERMIT_TYPE", "TRUNC", a,  origin_count_rows_value[0],sbd_count_rows_value[0])
	list_to_load = [(tuple_to_load)]

	cursor_CH_SBD.executemany("INSERT INTO MTSZN_WMARKET.CHECK_LOAD (SCHEMA, TABLE, TYPE_OF_ETL, CORRECTLY_LOADED,TOTAL_COUNT_SOURCE,TOTAL_COUNT_SDU) VALUES", list_to_load)

	conn_MTSZN.close() 

def D_POINT():
	conn_CH_SBD_secrets=Connection.get_connection_from_secrets(conn_id="Clickhouse-5")#скрытый коннекшн 5го сервера
	conn_CH_SBD=connect(host=conn_CH_SBD_secrets.host,port=conn_CH_SBD_secrets.port,user=conn_CH_SBD_secrets.login, password=conn_CH_SBD_secrets.password, connect_timeout=3600)
	cursor_CH_SBD = conn_CH_SBD.cursor()
	conn_MTSZN_secrets=Connection.get_connection_from_secrets(conn_id="MTSZN_WMARKET")#скрытый коннекшн источника
	dsn_tns_secrets=cx_Oracle.makedsn(host=conn_MTSZN_secrets.host,port=conn_MTSZN_secrets.port, service_name='SD')
	conn_MTSZN = cx_Oracle.connect(user =conn_MTSZN_secrets.login, password =conn_MTSZN_secrets.password, dsn=dsn_tns_secrets)
	cursor_MTSZN = conn_MTSZN.cursor()

	cursor_CH_SBD.execute("TRUNCATE TABLE MTSZN_WMARKET.D_POINT")

	cursor_MTSZN.execute(f"""SELECT
 CODE AS "CODE"
, NVL(TO_CHAR("DATE_ENTRY", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "DATE_ENTRY"
, NVL(TO_CHAR("DATE_FROM", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "DATE_FROM"
, NVL(TO_CHAR("DATE_TO", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "DATE_TO"
, ENAME AS "ENAME"
, ID AS "ID"
, KNAME AS "KNAME"
, PAR_ID AS "PAR_ID"
, POINT_NUM AS "POINT_NUM"
, RNAME AS "RNAME"
, USER_ID AS "USER_ID"
FROM WMARKET."D_POINT"
""")

	flag = True
	while flag:
		req_MTSZN = cursor_MTSZN.fetchmany(10000)
		if len(req_MTSZN) == 0:
			flag=False
		cursor_CH_SBD.executemany("""INSERT INTO MTSZN_WMARKET.D_POINT (CODE, DATE_ENTRY, DATE_FROM, DATE_TO, ENAME, ID, KNAME, PAR_ID, POINT_NUM, RNAME, USER_ID) VALUES""", req_MTSZN)

	cursor_CH_SBD.execute(f"SELECT COUNT(*) FROM MTSZN_WMARKET.D_POINT")
	sbd_count_rows_value = cursor_CH_SBD.fetchone()

	cursor_MTSZN.execute(f"SELECT COUNT(*) FROM WMARKET.D_POINT")
	origin_count_rows_value = cursor_MTSZN.fetchone()

	a = 0
	if int(sbd_count_rows_value[0]) == int(origin_count_rows_value[0]):
		a = 1
	tuple_to_load = ("SELECT COUNT(*) FROM MTSZN_WMARKET.D_POINT", "D_POINT", "TRUNC", a,  origin_count_rows_value[0],sbd_count_rows_value[0])
	list_to_load = [(tuple_to_load)]

	cursor_CH_SBD.executemany("INSERT INTO MTSZN_WMARKET.CHECK_LOAD (SCHEMA, TABLE, TYPE_OF_ETL, CORRECTLY_LOADED,TOTAL_COUNT_SOURCE,TOTAL_COUNT_SDU) VALUES", list_to_load)

	conn_MTSZN.close() 

def D_POST_TYPE():
	conn_CH_SBD_secrets=Connection.get_connection_from_secrets(conn_id="Clickhouse-5")#скрытый коннекшн 5го сервера
	conn_CH_SBD=connect(host=conn_CH_SBD_secrets.host,port=conn_CH_SBD_secrets.port,user=conn_CH_SBD_secrets.login, password=conn_CH_SBD_secrets.password, connect_timeout=3600)
	cursor_CH_SBD = conn_CH_SBD.cursor()
	conn_MTSZN_secrets=Connection.get_connection_from_secrets(conn_id="MTSZN_WMARKET")#скрытый коннекшн источника
	dsn_tns_secrets=cx_Oracle.makedsn(host=conn_MTSZN_secrets.host,port=conn_MTSZN_secrets.port, service_name='SD')
	conn_MTSZN = cx_Oracle.connect(user =conn_MTSZN_secrets.login, password =conn_MTSZN_secrets.password, dsn=dsn_tns_secrets)
	cursor_MTSZN = conn_MTSZN.cursor()

	cursor_CH_SBD.execute("TRUNCATE TABLE MTSZN_WMARKET.D_POST_TYPE")

	cursor_MTSZN.execute(f"""SELECT
 CODE AS "CODE"
, NVL(TO_CHAR("DATE_ENTRY", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "DATE_ENTRY"
, NVL(TO_CHAR("DATE_FROM", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "DATE_FROM"
, NVL(TO_CHAR("DATE_TO", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "DATE_TO"
, ENAME AS "ENAME"
, ID AS "ID"
, KNAME AS "KNAME"
, PAR_ID AS "PAR_ID"
, RNAME AS "RNAME"
, USER_ID AS "USER_ID"
FROM WMARKET."D_POST_TYPE"
""")

	flag = True
	while flag:
		req_MTSZN = cursor_MTSZN.fetchmany(10000)
		if len(req_MTSZN) == 0:
			flag=False
		cursor_CH_SBD.executemany("""INSERT INTO MTSZN_WMARKET.D_POST_TYPE (CODE, DATE_ENTRY, DATE_FROM, DATE_TO, ENAME, ID, KNAME, PAR_ID, RNAME, USER_ID) VALUES""", req_MTSZN)

	cursor_CH_SBD.execute(f"SELECT COUNT(*) FROM MTSZN_WMARKET.D_POST_TYPE")
	sbd_count_rows_value = cursor_CH_SBD.fetchone()

	cursor_MTSZN.execute(f"SELECT COUNT(*) FROM WMARKET.D_POST_TYPE")
	origin_count_rows_value = cursor_MTSZN.fetchone()

	a = 0
	if int(sbd_count_rows_value[0]) == int(origin_count_rows_value[0]):
		a = 1
	tuple_to_load = ("SELECT COUNT(*) FROM MTSZN_WMARKET.D_POST_TYPE", "D_POST_TYPE", "TRUNC", a,  origin_count_rows_value[0],sbd_count_rows_value[0])
	list_to_load = [(tuple_to_load)]

	cursor_CH_SBD.executemany("INSERT INTO MTSZN_WMARKET.CHECK_LOAD (SCHEMA, TABLE, TYPE_OF_ETL, CORRECTLY_LOADED,TOTAL_COUNT_SOURCE,TOTAL_COUNT_SDU) VALUES", list_to_load)

	conn_MTSZN.close() 

def D_POST_TYPE1():
	conn_CH_SBD_secrets=Connection.get_connection_from_secrets(conn_id="Clickhouse-5")#скрытый коннекшн 5го сервера
	conn_CH_SBD=connect(host=conn_CH_SBD_secrets.host,port=conn_CH_SBD_secrets.port,user=conn_CH_SBD_secrets.login, password=conn_CH_SBD_secrets.password, connect_timeout=3600)
	cursor_CH_SBD = conn_CH_SBD.cursor()
	conn_MTSZN_secrets=Connection.get_connection_from_secrets(conn_id="MTSZN_WMARKET")#скрытый коннекшн источника
	dsn_tns_secrets=cx_Oracle.makedsn(host=conn_MTSZN_secrets.host,port=conn_MTSZN_secrets.port, service_name='SD')
	conn_MTSZN = cx_Oracle.connect(user =conn_MTSZN_secrets.login, password =conn_MTSZN_secrets.password, dsn=dsn_tns_secrets)
	cursor_MTSZN = conn_MTSZN.cursor()

	cursor_CH_SBD.execute("TRUNCATE TABLE MTSZN_WMARKET.D_POST_TYPE1")

	cursor_MTSZN.execute(f"""SELECT
 CODE AS "CODE"
, NVL(TO_CHAR("DATE_ENTRY", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "DATE_ENTRY"
, NVL(TO_CHAR("DATE_FROM", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "DATE_FROM"
, NVL(TO_CHAR("DATE_TO", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "DATE_TO"
, ENAME AS "ENAME"
, ID AS "ID"
, KNAME AS "KNAME"
, PAR_ID AS "PAR_ID"
, RNAME AS "RNAME"
, USER_ID AS "USER_ID"
FROM WMARKET."D_POST_TYPE1"
""")

	flag = True
	while flag:
		req_MTSZN = cursor_MTSZN.fetchmany(10000)
		if len(req_MTSZN) == 0:
			flag=False
		cursor_CH_SBD.executemany("""INSERT INTO MTSZN_WMARKET.D_POST_TYPE1 (CODE, DATE_ENTRY, DATE_FROM, DATE_TO, ENAME, ID, KNAME, PAR_ID, RNAME, USER_ID) VALUES""", req_MTSZN)

	cursor_CH_SBD.execute(f"SELECT COUNT(*) FROM MTSZN_WMARKET.D_POST_TYPE1")
	sbd_count_rows_value = cursor_CH_SBD.fetchone()

	cursor_MTSZN.execute(f"SELECT COUNT(*) FROM WMARKET.D_POST_TYPE1")
	origin_count_rows_value = cursor_MTSZN.fetchone()

	a = 0
	if int(sbd_count_rows_value[0]) == int(origin_count_rows_value[0]):
		a = 1
	tuple_to_load = ("SELECT COUNT(*) FROM MTSZN_WMARKET.D_POST_TYPE1", "D_POST_TYPE1", "TRUNC", a,  origin_count_rows_value[0],sbd_count_rows_value[0])
	list_to_load = [(tuple_to_load)]

	cursor_CH_SBD.executemany("INSERT INTO MTSZN_WMARKET.CHECK_LOAD (SCHEMA, TABLE, TYPE_OF_ETL, CORRECTLY_LOADED,TOTAL_COUNT_SOURCE,TOTAL_COUNT_SDU) VALUES", list_to_load)

	conn_MTSZN.close() 

def D_PROFESSION():
	conn_CH_SBD_secrets=Connection.get_connection_from_secrets(conn_id="Clickhouse-5")#скрытый коннекшн 5го сервера
	conn_CH_SBD=connect(host=conn_CH_SBD_secrets.host,port=conn_CH_SBD_secrets.port,user=conn_CH_SBD_secrets.login, password=conn_CH_SBD_secrets.password, connect_timeout=3600)
	cursor_CH_SBD = conn_CH_SBD.cursor()
	conn_MTSZN_secrets=Connection.get_connection_from_secrets(conn_id="MTSZN_WMARKET")#скрытый коннекшн источника
	dsn_tns_secrets=cx_Oracle.makedsn(host=conn_MTSZN_secrets.host,port=conn_MTSZN_secrets.port, service_name='SD')
	conn_MTSZN = cx_Oracle.connect(user =conn_MTSZN_secrets.login, password =conn_MTSZN_secrets.password, dsn=dsn_tns_secrets)
	cursor_MTSZN = conn_MTSZN.cursor()

	cursor_CH_SBD.execute("TRUNCATE TABLE MTSZN_WMARKET.D_PROFESSION")

	cursor_MTSZN.execute(f"""SELECT
 CODE AS "CODE"
, NVL(TO_CHAR("DATE_ENTRY", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "DATE_ENTRY"
, NVL(TO_CHAR("DATE_FROM", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "DATE_FROM"
, NVL(TO_CHAR("DATE_TO", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "DATE_TO"
, ENAME AS "ENAME"
, ID AS "ID"
, KNAME AS "KNAME"
, PAR_ID AS "PAR_ID"
, PROF_CAT AS "PROF_CAT"
, PROF_CAT_ID AS "PROF_CAT_ID"
, RNAME AS "RNAME"
, USER_ID AS "USER_ID"
FROM WMARKET."D_PROFESSION"
""")

	flag = True
	while flag:
		req_MTSZN = cursor_MTSZN.fetchmany(10000)
		if len(req_MTSZN) == 0:
			flag=False
		cursor_CH_SBD.executemany("""INSERT INTO MTSZN_WMARKET.D_PROFESSION (CODE, DATE_ENTRY, DATE_FROM, DATE_TO, ENAME, ID, KNAME, PAR_ID, PROF_CAT, PROF_CAT_ID, RNAME, USER_ID) VALUES""", req_MTSZN)

	cursor_CH_SBD.execute(f"SELECT COUNT(*) FROM MTSZN_WMARKET.D_PROFESSION")
	sbd_count_rows_value = cursor_CH_SBD.fetchone()

	cursor_MTSZN.execute(f"SELECT COUNT(*) FROM WMARKET.D_PROFESSION")
	origin_count_rows_value = cursor_MTSZN.fetchone()

	a = 0
	if int(sbd_count_rows_value[0]) == int(origin_count_rows_value[0]):
		a = 1
	tuple_to_load = ("SELECT COUNT(*) FROM MTSZN_WMARKET.D_PROFESSION", "D_PROFESSION", "TRUNC", a,  origin_count_rows_value[0],sbd_count_rows_value[0])
	list_to_load = [(tuple_to_load)]

	cursor_CH_SBD.executemany("INSERT INTO MTSZN_WMARKET.CHECK_LOAD (SCHEMA, TABLE, TYPE_OF_ETL, CORRECTLY_LOADED,TOTAL_COUNT_SOURCE,TOTAL_COUNT_SDU) VALUES", list_to_load)

	conn_MTSZN.close() 

def D_PROF_ADD():
	conn_CH_SBD_secrets=Connection.get_connection_from_secrets(conn_id="Clickhouse-5")#скрытый коннекшн 5го сервера
	conn_CH_SBD=connect(host=conn_CH_SBD_secrets.host,port=conn_CH_SBD_secrets.port,user=conn_CH_SBD_secrets.login, password=conn_CH_SBD_secrets.password, connect_timeout=3600)
	cursor_CH_SBD = conn_CH_SBD.cursor()
	conn_MTSZN_secrets=Connection.get_connection_from_secrets(conn_id="MTSZN_WMARKET")#скрытый коннекшн источника
	dsn_tns_secrets=cx_Oracle.makedsn(host=conn_MTSZN_secrets.host,port=conn_MTSZN_secrets.port, service_name='SD')
	conn_MTSZN = cx_Oracle.connect(user =conn_MTSZN_secrets.login, password =conn_MTSZN_secrets.password, dsn=dsn_tns_secrets)
	cursor_MTSZN = conn_MTSZN.cursor()

	cursor_CH_SBD.execute("TRUNCATE TABLE MTSZN_WMARKET.D_PROF_ADD")

	cursor_MTSZN.execute(f"""SELECT
 CODE AS "CODE"
, NVL(TO_CHAR("DATE_ENTRY", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "DATE_ENTRY"
, NVL(TO_CHAR("DATE_FROM", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "DATE_FROM"
, NVL(TO_CHAR("DATE_TO", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "DATE_TO"
, ENAME AS "ENAME"
, ID AS "ID"
, KNAME AS "KNAME"
, PAR_ID AS "PAR_ID"
, RNAME AS "RNAME"
, USER_ID AS "USER_ID"
FROM WMARKET."D_PROF_ADD"
""")

	flag = True
	while flag:
		req_MTSZN = cursor_MTSZN.fetchmany(10000)
		if len(req_MTSZN) == 0:
			flag=False
		cursor_CH_SBD.executemany("""INSERT INTO MTSZN_WMARKET.D_PROF_ADD (CODE, DATE_ENTRY, DATE_FROM, DATE_TO, ENAME, ID, KNAME, PAR_ID, RNAME, USER_ID) VALUES""", req_MTSZN)

	cursor_CH_SBD.execute(f"SELECT COUNT(*) FROM MTSZN_WMARKET.D_PROF_ADD")
	sbd_count_rows_value = cursor_CH_SBD.fetchone()

	cursor_MTSZN.execute(f"SELECT COUNT(*) FROM WMARKET.D_PROF_ADD")
	origin_count_rows_value = cursor_MTSZN.fetchone()

	a = 0
	if int(sbd_count_rows_value[0]) == int(origin_count_rows_value[0]):
		a = 1
	tuple_to_load = ("SELECT COUNT(*) FROM MTSZN_WMARKET.D_PROF_ADD", "D_PROF_ADD", "TRUNC", a,  origin_count_rows_value[0],sbd_count_rows_value[0])
	list_to_load = [(tuple_to_load)]

	cursor_CH_SBD.executemany("INSERT INTO MTSZN_WMARKET.CHECK_LOAD (SCHEMA, TABLE, TYPE_OF_ETL, CORRECTLY_LOADED,TOTAL_COUNT_SOURCE,TOTAL_COUNT_SDU) VALUES", list_to_load)

	conn_MTSZN.close() 

def D_PROF_CAT():
	conn_CH_SBD_secrets=Connection.get_connection_from_secrets(conn_id="Clickhouse-5")#скрытый коннекшн 5го сервера
	conn_CH_SBD=connect(host=conn_CH_SBD_secrets.host,port=conn_CH_SBD_secrets.port,user=conn_CH_SBD_secrets.login, password=conn_CH_SBD_secrets.password, connect_timeout=3600)
	cursor_CH_SBD = conn_CH_SBD.cursor()
	conn_MTSZN_secrets=Connection.get_connection_from_secrets(conn_id="MTSZN_WMARKET")#скрытый коннекшн источника
	dsn_tns_secrets=cx_Oracle.makedsn(host=conn_MTSZN_secrets.host,port=conn_MTSZN_secrets.port, service_name='SD')
	conn_MTSZN = cx_Oracle.connect(user =conn_MTSZN_secrets.login, password =conn_MTSZN_secrets.password, dsn=dsn_tns_secrets)
	cursor_MTSZN = conn_MTSZN.cursor()

	cursor_CH_SBD.execute("TRUNCATE TABLE MTSZN_WMARKET.D_PROF_CAT")

	cursor_MTSZN.execute(f"""SELECT
 CODE AS "CODE"
, NVL(TO_CHAR("DATE_ENTRY", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "DATE_ENTRY"
, NVL(TO_CHAR("DATE_FROM", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "DATE_FROM"
, NVL(TO_CHAR("DATE_TO", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "DATE_TO"
, ENAME AS "ENAME"
, ID AS "ID"
, KNAME AS "KNAME"
, PAR_ID AS "PAR_ID"
, RNAME AS "RNAME"
, USER_ID AS "USER_ID"
FROM WMARKET."D_PROF_CAT"
""")

	flag = True
	while flag:
		req_MTSZN = cursor_MTSZN.fetchmany(10000)
		if len(req_MTSZN) == 0:
			flag=False
		cursor_CH_SBD.executemany("""INSERT INTO MTSZN_WMARKET.D_PROF_CAT (CODE, DATE_ENTRY, DATE_FROM, DATE_TO, ENAME, ID, KNAME, PAR_ID, RNAME, USER_ID) VALUES""", req_MTSZN)

	cursor_CH_SBD.execute(f"SELECT COUNT(*) FROM MTSZN_WMARKET.D_PROF_CAT")
	sbd_count_rows_value = cursor_CH_SBD.fetchone()

	cursor_MTSZN.execute(f"SELECT COUNT(*) FROM WMARKET.D_PROF_CAT")
	origin_count_rows_value = cursor_MTSZN.fetchone()

	a = 0
	if int(sbd_count_rows_value[0]) == int(origin_count_rows_value[0]):
		a = 1
	tuple_to_load = ("SELECT COUNT(*) FROM MTSZN_WMARKET.D_PROF_CAT", "D_PROF_CAT", "TRUNC", a,  origin_count_rows_value[0],sbd_count_rows_value[0])
	list_to_load = [(tuple_to_load)]

	cursor_CH_SBD.executemany("INSERT INTO MTSZN_WMARKET.CHECK_LOAD (SCHEMA, TABLE, TYPE_OF_ETL, CORRECTLY_LOADED,TOTAL_COUNT_SOURCE,TOTAL_COUNT_SDU) VALUES", list_to_load)

	conn_MTSZN.close() 

def D_PROF_TYPE():
	conn_CH_SBD_secrets=Connection.get_connection_from_secrets(conn_id="Clickhouse-5")#скрытый коннекшн 5го сервера
	conn_CH_SBD=connect(host=conn_CH_SBD_secrets.host,port=conn_CH_SBD_secrets.port,user=conn_CH_SBD_secrets.login, password=conn_CH_SBD_secrets.password, connect_timeout=3600)
	cursor_CH_SBD = conn_CH_SBD.cursor()
	conn_MTSZN_secrets=Connection.get_connection_from_secrets(conn_id="MTSZN_WMARKET")#скрытый коннекшн источника
	dsn_tns_secrets=cx_Oracle.makedsn(host=conn_MTSZN_secrets.host,port=conn_MTSZN_secrets.port, service_name='SD')
	conn_MTSZN = cx_Oracle.connect(user =conn_MTSZN_secrets.login, password =conn_MTSZN_secrets.password, dsn=dsn_tns_secrets)
	cursor_MTSZN = conn_MTSZN.cursor()

	cursor_CH_SBD.execute("TRUNCATE TABLE MTSZN_WMARKET.D_PROF_TYPE")

	cursor_MTSZN.execute(f"""SELECT
 CODE AS "CODE"
, NVL(TO_CHAR("DATE_ENTRY", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "DATE_ENTRY"
, NVL(TO_CHAR("DATE_FROM", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "DATE_FROM"
, NVL(TO_CHAR("DATE_TO", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "DATE_TO"
, ENAME AS "ENAME"
, ID AS "ID"
, KNAME AS "KNAME"
, PAR_ID AS "PAR_ID"
, RNAME AS "RNAME"
, USER_ID AS "USER_ID"
FROM WMARKET."D_PROF_TYPE"
""")

	flag = True
	while flag:
		req_MTSZN = cursor_MTSZN.fetchmany(10000)
		if len(req_MTSZN) == 0:
			flag=False
		cursor_CH_SBD.executemany("""INSERT INTO MTSZN_WMARKET.D_PROF_TYPE (CODE, DATE_ENTRY, DATE_FROM, DATE_TO, ENAME, ID, KNAME, PAR_ID, RNAME, USER_ID) VALUES""", req_MTSZN)

	cursor_CH_SBD.execute(f"SELECT COUNT(*) FROM MTSZN_WMARKET.D_PROF_TYPE")
	sbd_count_rows_value = cursor_CH_SBD.fetchone()

	cursor_MTSZN.execute(f"SELECT COUNT(*) FROM WMARKET.D_PROF_TYPE")
	origin_count_rows_value = cursor_MTSZN.fetchone()

	a = 0
	if int(sbd_count_rows_value[0]) == int(origin_count_rows_value[0]):
		a = 1
	tuple_to_load = ("SELECT COUNT(*) FROM MTSZN_WMARKET.D_PROF_TYPE", "D_PROF_TYPE", "TRUNC", a,  origin_count_rows_value[0],sbd_count_rows_value[0])
	list_to_load = [(tuple_to_load)]

	cursor_CH_SBD.executemany("INSERT INTO MTSZN_WMARKET.CHECK_LOAD (SCHEMA, TABLE, TYPE_OF_ETL, CORRECTLY_LOADED,TOTAL_COUNT_SOURCE,TOTAL_COUNT_SDU) VALUES", list_to_load)

	conn_MTSZN.close() 

def D_PROJECT():
	conn_CH_SBD_secrets=Connection.get_connection_from_secrets(conn_id="Clickhouse-5")#скрытый коннекшн 5го сервера
	conn_CH_SBD=connect(host=conn_CH_SBD_secrets.host,port=conn_CH_SBD_secrets.port,user=conn_CH_SBD_secrets.login, password=conn_CH_SBD_secrets.password, connect_timeout=3600)
	cursor_CH_SBD = conn_CH_SBD.cursor()
	conn_MTSZN_secrets=Connection.get_connection_from_secrets(conn_id="MTSZN_WMARKET")#скрытый коннекшн источника
	dsn_tns_secrets=cx_Oracle.makedsn(host=conn_MTSZN_secrets.host,port=conn_MTSZN_secrets.port, service_name='SD')
	conn_MTSZN = cx_Oracle.connect(user =conn_MTSZN_secrets.login, password =conn_MTSZN_secrets.password, dsn=dsn_tns_secrets)
	cursor_MTSZN = conn_MTSZN.cursor()

	cursor_CH_SBD.execute("TRUNCATE TABLE MTSZN_WMARKET.D_PROJECT")

	cursor_MTSZN.execute(f"""SELECT
 CODE AS "CODE"
, NVL(TO_CHAR("DATE_ENTRY", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "DATE_ENTRY"
, NVL(TO_CHAR("DATE_FROM", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "DATE_FROM"
, NVL(TO_CHAR("DATE_TO", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "DATE_TO"
, ENAME AS "ENAME"
, ID AS "ID"
, KNAME AS "KNAME"
, PAR_ID AS "PAR_ID"
, RNAME AS "RNAME"
, USER_ID AS "USER_ID"
FROM WMARKET."D_PROJECT"
""")

	flag = True
	while flag:
		req_MTSZN = cursor_MTSZN.fetchmany(10000)
		if len(req_MTSZN) == 0:
			flag=False
		cursor_CH_SBD.executemany("""INSERT INTO MTSZN_WMARKET.D_PROJECT (CODE, DATE_ENTRY, DATE_FROM, DATE_TO, ENAME, ID, KNAME, PAR_ID, RNAME, USER_ID) VALUES""", req_MTSZN)

	cursor_CH_SBD.execute(f"SELECT COUNT(*) FROM MTSZN_WMARKET.D_PROJECT")
	sbd_count_rows_value = cursor_CH_SBD.fetchone()

	cursor_MTSZN.execute(f"SELECT COUNT(*) FROM WMARKET.D_PROJECT")
	origin_count_rows_value = cursor_MTSZN.fetchone()

	a = 0
	if int(sbd_count_rows_value[0]) == int(origin_count_rows_value[0]):
		a = 1
	tuple_to_load = ("SELECT COUNT(*) FROM MTSZN_WMARKET.D_PROJECT", "D_PROJECT", "TRUNC", a,  origin_count_rows_value[0],sbd_count_rows_value[0])
	list_to_load = [(tuple_to_load)]

	cursor_CH_SBD.executemany("INSERT INTO MTSZN_WMARKET.CHECK_LOAD (SCHEMA, TABLE, TYPE_OF_ETL, CORRECTLY_LOADED,TOTAL_COUNT_SOURCE,TOTAL_COUNT_SDU) VALUES", list_to_load)

	conn_MTSZN.close() 

def D_REGION():
	conn_CH_SBD_secrets=Connection.get_connection_from_secrets(conn_id="Clickhouse-5")#скрытый коннекшн 5го сервера
	conn_CH_SBD=connect(host=conn_CH_SBD_secrets.host,port=conn_CH_SBD_secrets.port,user=conn_CH_SBD_secrets.login, password=conn_CH_SBD_secrets.password, connect_timeout=3600)
	cursor_CH_SBD = conn_CH_SBD.cursor()
	conn_MTSZN_secrets=Connection.get_connection_from_secrets(conn_id="MTSZN_WMARKET")#скрытый коннекшн источника
	dsn_tns_secrets=cx_Oracle.makedsn(host=conn_MTSZN_secrets.host,port=conn_MTSZN_secrets.port, service_name='SD')
	conn_MTSZN = cx_Oracle.connect(user =conn_MTSZN_secrets.login, password =conn_MTSZN_secrets.password, dsn=dsn_tns_secrets)
	cursor_MTSZN = conn_MTSZN.cursor()

	cursor_CH_SBD.execute("TRUNCATE TABLE MTSZN_WMARKET.D_REGION")

	cursor_MTSZN.execute(f"""SELECT
 AREA_ID AS "AREA_ID"
, AREA_TYPE AS "AREA_TYPE"
, CODE AS "CODE"
, NVL(TO_CHAR("DATE_ENTRY", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "DATE_ENTRY"
, NVL(TO_CHAR("DATE_FROM", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "DATE_FROM"
, NVL(TO_CHAR("DATE_TO", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "DATE_TO"
, ENAME AS "ENAME"
, GBDFL_CODE AS "GBDFL_CODE"
, GBDUL_CODE AS "GBDUL_CODE"
, ID AS "ID"
, IS_EXCLUDE AS "IS_EXCLUDE"
, KNAME AS "KNAME"
, LEVEL_NUM AS "LEVEL_NUM"
, ORGUNIT_CODE AS "ORGUNIT_CODE"
, PAR_ID AS "PAR_ID"
, RNAME AS "RNAME"
, USER_ID AS "USER_ID"
FROM WMARKET."D_REGION"
""")

	flag = True
	while flag:
		req_MTSZN = cursor_MTSZN.fetchmany(10000)
		if len(req_MTSZN) == 0:
			flag=False
		cursor_CH_SBD.executemany("""INSERT INTO MTSZN_WMARKET.D_REGION (AREA_ID, AREA_TYPE, CODE, DATE_ENTRY, DATE_FROM, DATE_TO, ENAME, GBDFL_CODE, GBDUL_CODE, ID, IS_EXCLUDE, KNAME, LEVEL_NUM, ORGUNIT_CODE, PAR_ID, RNAME, USER_ID) VALUES""", req_MTSZN)

	cursor_CH_SBD.execute(f"SELECT COUNT(*) FROM MTSZN_WMARKET.D_REGION")
	sbd_count_rows_value = cursor_CH_SBD.fetchone()

	cursor_MTSZN.execute(f"SELECT COUNT(*) FROM WMARKET.D_REGION")
	origin_count_rows_value = cursor_MTSZN.fetchone()

	a = 0
	if int(sbd_count_rows_value[0]) == int(origin_count_rows_value[0]):
		a = 1
	tuple_to_load = ("SELECT COUNT(*) FROM MTSZN_WMARKET.D_REGION", "D_REGION", "TRUNC", a,  origin_count_rows_value[0],sbd_count_rows_value[0])
	list_to_load = [(tuple_to_load)]

	cursor_CH_SBD.executemany("INSERT INTO MTSZN_WMARKET.CHECK_LOAD (SCHEMA, TABLE, TYPE_OF_ETL, CORRECTLY_LOADED,TOTAL_COUNT_SOURCE,TOTAL_COUNT_SDU) VALUES", list_to_load)

	conn_MTSZN.close() 

def D_REG_STAT():
	conn_CH_SBD_secrets=Connection.get_connection_from_secrets(conn_id="Clickhouse-5")#скрытый коннекшн 5го сервера
	conn_CH_SBD=connect(host=conn_CH_SBD_secrets.host,port=conn_CH_SBD_secrets.port,user=conn_CH_SBD_secrets.login, password=conn_CH_SBD_secrets.password, connect_timeout=3600)
	cursor_CH_SBD = conn_CH_SBD.cursor()
	conn_MTSZN_secrets=Connection.get_connection_from_secrets(conn_id="MTSZN_WMARKET")#скрытый коннекшн источника
	dsn_tns_secrets=cx_Oracle.makedsn(host=conn_MTSZN_secrets.host,port=conn_MTSZN_secrets.port, service_name='SD')
	conn_MTSZN = cx_Oracle.connect(user =conn_MTSZN_secrets.login, password =conn_MTSZN_secrets.password, dsn=dsn_tns_secrets)
	cursor_MTSZN = conn_MTSZN.cursor()

	cursor_CH_SBD.execute("TRUNCATE TABLE MTSZN_WMARKET.D_REG_STAT")

	cursor_MTSZN.execute(f"""SELECT
 REG_TYPE AS "REG_TYPE"
, STAT_ID AS "STAT_ID"
FROM WMARKET."D_REG_STAT"
""")

	flag = True
	while flag:
		req_MTSZN = cursor_MTSZN.fetchmany(10000)
		if len(req_MTSZN) == 0:
			flag=False
		cursor_CH_SBD.executemany("""INSERT INTO MTSZN_WMARKET.D_REG_STAT (REG_TYPE, STAT_ID) VALUES""", req_MTSZN)

	cursor_CH_SBD.execute(f"SELECT COUNT(*) FROM MTSZN_WMARKET.D_REG_STAT")
	sbd_count_rows_value = cursor_CH_SBD.fetchone()

	cursor_MTSZN.execute(f"SELECT COUNT(*) FROM WMARKET.D_REG_STAT")
	origin_count_rows_value = cursor_MTSZN.fetchone()

	a = 0
	if int(sbd_count_rows_value[0]) == int(origin_count_rows_value[0]):
		a = 1
	tuple_to_load = ("SELECT COUNT(*) FROM MTSZN_WMARKET.D_REG_STAT", "D_REG_STAT", "TRUNC", a,  origin_count_rows_value[0],sbd_count_rows_value[0])
	list_to_load = [(tuple_to_load)]

	cursor_CH_SBD.executemany("INSERT INTO MTSZN_WMARKET.CHECK_LOAD (SCHEMA, TABLE, TYPE_OF_ETL, CORRECTLY_LOADED,TOTAL_COUNT_SOURCE,TOTAL_COUNT_SDU) VALUES", list_to_load)

	conn_MTSZN.close() 

def D_REQ_AGENCY():
	conn_CH_SBD_secrets=Connection.get_connection_from_secrets(conn_id="Clickhouse-5")#скрытый коннекшн 5го сервера
	conn_CH_SBD=connect(host=conn_CH_SBD_secrets.host,port=conn_CH_SBD_secrets.port,user=conn_CH_SBD_secrets.login, password=conn_CH_SBD_secrets.password, connect_timeout=3600)
	cursor_CH_SBD = conn_CH_SBD.cursor()
	conn_MTSZN_secrets=Connection.get_connection_from_secrets(conn_id="MTSZN_WMARKET")#скрытый коннекшн источника
	dsn_tns_secrets=cx_Oracle.makedsn(host=conn_MTSZN_secrets.host,port=conn_MTSZN_secrets.port, service_name='SD')
	conn_MTSZN = cx_Oracle.connect(user =conn_MTSZN_secrets.login, password =conn_MTSZN_secrets.password, dsn=dsn_tns_secrets)
	cursor_MTSZN = conn_MTSZN.cursor()

	cursor_CH_SBD.execute("TRUNCATE TABLE MTSZN_WMARKET.D_REQ_AGENCY")

	cursor_MTSZN.execute(f"""SELECT
 CODE AS "CODE"
, NVL(TO_CHAR("DATE_ENTRY", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "DATE_ENTRY"
, NVL(TO_CHAR("DATE_FROM", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "DATE_FROM"
, NVL(TO_CHAR("DATE_TO", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "DATE_TO"
, ENAME AS "ENAME"
, ID AS "ID"
, KNAME AS "KNAME"
, PAR_ID AS "PAR_ID"
, RNAME AS "RNAME"
, USER_ID AS "USER_ID"
FROM WMARKET."D_REQ_AGENCY"
""")

	flag = True
	while flag:
		req_MTSZN = cursor_MTSZN.fetchmany(10000)
		if len(req_MTSZN) == 0:
			flag=False
		cursor_CH_SBD.executemany("""INSERT INTO MTSZN_WMARKET.D_REQ_AGENCY (CODE, DATE_ENTRY, DATE_FROM, DATE_TO, ENAME, ID, KNAME, PAR_ID, RNAME, USER_ID) VALUES""", req_MTSZN)

	cursor_CH_SBD.execute(f"SELECT COUNT(*) FROM MTSZN_WMARKET.D_REQ_AGENCY")
	sbd_count_rows_value = cursor_CH_SBD.fetchone()

	cursor_MTSZN.execute(f"SELECT COUNT(*) FROM WMARKET.D_REQ_AGENCY")
	origin_count_rows_value = cursor_MTSZN.fetchone()

	a = 0
	if int(sbd_count_rows_value[0]) == int(origin_count_rows_value[0]):
		a = 1
	tuple_to_load = ("SELECT COUNT(*) FROM MTSZN_WMARKET.D_REQ_AGENCY", "D_REQ_AGENCY", "TRUNC", a,  origin_count_rows_value[0],sbd_count_rows_value[0])
	list_to_load = [(tuple_to_load)]

	cursor_CH_SBD.executemany("INSERT INTO MTSZN_WMARKET.CHECK_LOAD (SCHEMA, TABLE, TYPE_OF_ETL, CORRECTLY_LOADED,TOTAL_COUNT_SOURCE,TOTAL_COUNT_SDU) VALUES", list_to_load)

	conn_MTSZN.close() 

def D_REQ_DECISION():
	conn_CH_SBD_secrets=Connection.get_connection_from_secrets(conn_id="Clickhouse-5")#скрытый коннекшн 5го сервера
	conn_CH_SBD=connect(host=conn_CH_SBD_secrets.host,port=conn_CH_SBD_secrets.port,user=conn_CH_SBD_secrets.login, password=conn_CH_SBD_secrets.password, connect_timeout=3600)
	cursor_CH_SBD = conn_CH_SBD.cursor()
	conn_MTSZN_secrets=Connection.get_connection_from_secrets(conn_id="MTSZN_WMARKET")#скрытый коннекшн источника
	dsn_tns_secrets=cx_Oracle.makedsn(host=conn_MTSZN_secrets.host,port=conn_MTSZN_secrets.port, service_name='SD')
	conn_MTSZN = cx_Oracle.connect(user =conn_MTSZN_secrets.login, password =conn_MTSZN_secrets.password, dsn=dsn_tns_secrets)
	cursor_MTSZN = conn_MTSZN.cursor()

	cursor_CH_SBD.execute("TRUNCATE TABLE MTSZN_WMARKET.D_REQ_DECISION")

	cursor_MTSZN.execute(f"""SELECT
 CODE AS "CODE"
, NVL(TO_CHAR("DATE_ENTRY", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "DATE_ENTRY"
, NVL(TO_CHAR("DATE_FROM", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "DATE_FROM"
, NVL(TO_CHAR("DATE_TO", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "DATE_TO"
, ENAME AS "ENAME"
, ID AS "ID"
, KNAME AS "KNAME"
, PAR_ID AS "PAR_ID"
, RNAME AS "RNAME"
FROM WMARKET."D_REQ_DECISION"
""")

	flag = True
	while flag:
		req_MTSZN = cursor_MTSZN.fetchmany(10000)
		if len(req_MTSZN) == 0:
			flag=False
		cursor_CH_SBD.executemany("""INSERT INTO MTSZN_WMARKET.D_REQ_DECISION (CODE, DATE_ENTRY, DATE_FROM, DATE_TO, ENAME, ID, KNAME, PAR_ID, RNAME) VALUES""", req_MTSZN)

	cursor_CH_SBD.execute(f"SELECT COUNT(*) FROM MTSZN_WMARKET.D_REQ_DECISION")
	sbd_count_rows_value = cursor_CH_SBD.fetchone()

	cursor_MTSZN.execute(f"SELECT COUNT(*) FROM WMARKET.D_REQ_DECISION")
	origin_count_rows_value = cursor_MTSZN.fetchone()

	a = 0
	if int(sbd_count_rows_value[0]) == int(origin_count_rows_value[0]):
		a = 1
	tuple_to_load = ("SELECT COUNT(*) FROM MTSZN_WMARKET.D_REQ_DECISION", "D_REQ_DECISION", "TRUNC", a,  origin_count_rows_value[0],sbd_count_rows_value[0])
	list_to_load = [(tuple_to_load)]

	cursor_CH_SBD.executemany("INSERT INTO MTSZN_WMARKET.CHECK_LOAD (SCHEMA, TABLE, TYPE_OF_ETL, CORRECTLY_LOADED,TOTAL_COUNT_SOURCE,TOTAL_COUNT_SDU) VALUES", list_to_load)

	conn_MTSZN.close() 

def D_REQ_REFUSE():
	conn_CH_SBD_secrets=Connection.get_connection_from_secrets(conn_id="Clickhouse-5")#скрытый коннекшн 5го сервера
	conn_CH_SBD=connect(host=conn_CH_SBD_secrets.host,port=conn_CH_SBD_secrets.port,user=conn_CH_SBD_secrets.login, password=conn_CH_SBD_secrets.password, connect_timeout=3600)
	cursor_CH_SBD = conn_CH_SBD.cursor()
	conn_MTSZN_secrets=Connection.get_connection_from_secrets(conn_id="MTSZN_WMARKET")#скрытый коннекшн источника
	dsn_tns_secrets=cx_Oracle.makedsn(host=conn_MTSZN_secrets.host,port=conn_MTSZN_secrets.port, service_name='SD')
	conn_MTSZN = cx_Oracle.connect(user =conn_MTSZN_secrets.login, password =conn_MTSZN_secrets.password, dsn=dsn_tns_secrets)
	cursor_MTSZN = conn_MTSZN.cursor()

	cursor_CH_SBD.execute("TRUNCATE TABLE MTSZN_WMARKET.D_REQ_REFUSE")

	cursor_MTSZN.execute(f"""SELECT
 CODE AS "CODE"
, NVL(TO_CHAR("DATE_ENTRY", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "DATE_ENTRY"
, NVL(TO_CHAR("DATE_FROM", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "DATE_FROM"
, NVL(TO_CHAR("DATE_TO", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "DATE_TO"
, ENAME AS "ENAME"
, ID AS "ID"
, KNAME AS "KNAME"
, PAR_ID AS "PAR_ID"
, RNAME AS "RNAME"
, USER_ID AS "USER_ID"
FROM WMARKET."D_REQ_REFUSE"
""")

	flag = True
	while flag:
		req_MTSZN = cursor_MTSZN.fetchmany(10000)
		if len(req_MTSZN) == 0:
			flag=False
		cursor_CH_SBD.executemany("""INSERT INTO MTSZN_WMARKET.D_REQ_REFUSE (CODE, DATE_ENTRY, DATE_FROM, DATE_TO, ENAME, ID, KNAME, PAR_ID, RNAME, USER_ID) VALUES""", req_MTSZN)

	cursor_CH_SBD.execute(f"SELECT COUNT(*) FROM MTSZN_WMARKET.D_REQ_REFUSE")
	sbd_count_rows_value = cursor_CH_SBD.fetchone()

	cursor_MTSZN.execute(f"SELECT COUNT(*) FROM WMARKET.D_REQ_REFUSE")
	origin_count_rows_value = cursor_MTSZN.fetchone()

	a = 0
	if int(sbd_count_rows_value[0]) == int(origin_count_rows_value[0]):
		a = 1
	tuple_to_load = ("SELECT COUNT(*) FROM MTSZN_WMARKET.D_REQ_REFUSE", "D_REQ_REFUSE", "TRUNC", a,  origin_count_rows_value[0],sbd_count_rows_value[0])
	list_to_load = [(tuple_to_load)]

	cursor_CH_SBD.executemany("INSERT INTO MTSZN_WMARKET.CHECK_LOAD (SCHEMA, TABLE, TYPE_OF_ETL, CORRECTLY_LOADED,TOTAL_COUNT_SOURCE,TOTAL_COUNT_SDU) VALUES", list_to_load)

	conn_MTSZN.close() 

def D_REQ_REISSUE():
	conn_CH_SBD_secrets=Connection.get_connection_from_secrets(conn_id="Clickhouse-5")#скрытый коннекшн 5го сервера
	conn_CH_SBD=connect(host=conn_CH_SBD_secrets.host,port=conn_CH_SBD_secrets.port,user=conn_CH_SBD_secrets.login, password=conn_CH_SBD_secrets.password, connect_timeout=3600)
	cursor_CH_SBD = conn_CH_SBD.cursor()
	conn_MTSZN_secrets=Connection.get_connection_from_secrets(conn_id="MTSZN_WMARKET")#скрытый коннекшн источника
	dsn_tns_secrets=cx_Oracle.makedsn(host=conn_MTSZN_secrets.host,port=conn_MTSZN_secrets.port, service_name='SD')
	conn_MTSZN = cx_Oracle.connect(user =conn_MTSZN_secrets.login, password =conn_MTSZN_secrets.password, dsn=dsn_tns_secrets)
	cursor_MTSZN = conn_MTSZN.cursor()

	cursor_CH_SBD.execute("TRUNCATE TABLE MTSZN_WMARKET.D_REQ_REISSUE")

	cursor_MTSZN.execute(f"""SELECT
 CODE AS "CODE"
, NVL(TO_CHAR("DATE_ENTRY", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "DATE_ENTRY"
, NVL(TO_CHAR("DATE_FROM", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "DATE_FROM"
, NVL(TO_CHAR("DATE_TO", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "DATE_TO"
, ENAME AS "ENAME"
, ID AS "ID"
, KNAME AS "KNAME"
, PAR_ID AS "PAR_ID"
, RNAME AS "RNAME"
, USER_ID AS "USER_ID"
FROM WMARKET."D_REQ_REISSUE"
""")

	flag = True
	while flag:
		req_MTSZN = cursor_MTSZN.fetchmany(10000)
		if len(req_MTSZN) == 0:
			flag=False
		cursor_CH_SBD.executemany("""INSERT INTO MTSZN_WMARKET.D_REQ_REISSUE (CODE, DATE_ENTRY, DATE_FROM, DATE_TO, ENAME, ID, KNAME, PAR_ID, RNAME, USER_ID) VALUES""", req_MTSZN)

	cursor_CH_SBD.execute(f"SELECT COUNT(*) FROM MTSZN_WMARKET.D_REQ_REISSUE")
	sbd_count_rows_value = cursor_CH_SBD.fetchone()

	cursor_MTSZN.execute(f"SELECT COUNT(*) FROM WMARKET.D_REQ_REISSUE")
	origin_count_rows_value = cursor_MTSZN.fetchone()

	a = 0
	if int(sbd_count_rows_value[0]) == int(origin_count_rows_value[0]):
		a = 1
	tuple_to_load = ("SELECT COUNT(*) FROM MTSZN_WMARKET.D_REQ_REISSUE", "D_REQ_REISSUE", "TRUNC", a,  origin_count_rows_value[0],sbd_count_rows_value[0])
	list_to_load = [(tuple_to_load)]

	cursor_CH_SBD.executemany("INSERT INTO MTSZN_WMARKET.CHECK_LOAD (SCHEMA, TABLE, TYPE_OF_ETL, CORRECTLY_LOADED,TOTAL_COUNT_SOURCE,TOTAL_COUNT_SDU) VALUES", list_to_load)

	conn_MTSZN.close() 

def D_REQ_STATUS():
	conn_CH_SBD_secrets=Connection.get_connection_from_secrets(conn_id="Clickhouse-5")#скрытый коннекшн 5го сервера
	conn_CH_SBD=connect(host=conn_CH_SBD_secrets.host,port=conn_CH_SBD_secrets.port,user=conn_CH_SBD_secrets.login, password=conn_CH_SBD_secrets.password, connect_timeout=3600)
	cursor_CH_SBD = conn_CH_SBD.cursor()
	conn_MTSZN_secrets=Connection.get_connection_from_secrets(conn_id="MTSZN_WMARKET")#скрытый коннекшн источника
	dsn_tns_secrets=cx_Oracle.makedsn(host=conn_MTSZN_secrets.host,port=conn_MTSZN_secrets.port, service_name='SD')
	conn_MTSZN = cx_Oracle.connect(user =conn_MTSZN_secrets.login, password =conn_MTSZN_secrets.password, dsn=dsn_tns_secrets)
	cursor_MTSZN = conn_MTSZN.cursor()

	cursor_CH_SBD.execute("TRUNCATE TABLE MTSZN_WMARKET.D_REQ_STATUS")

	cursor_MTSZN.execute(f"""SELECT
 CODE AS "CODE"
, NVL(TO_CHAR("DATE_ENTRY", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "DATE_ENTRY"
, NVL(TO_CHAR("DATE_FROM", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "DATE_FROM"
, NVL(TO_CHAR("DATE_TO", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "DATE_TO"
, ENAME AS "ENAME"
, ID AS "ID"
, KNAME AS "KNAME"
, PAR_ID AS "PAR_ID"
, RNAME AS "RNAME"
, USER_ID AS "USER_ID"
FROM WMARKET."D_REQ_STATUS"
""")

	flag = True
	while flag:
		req_MTSZN = cursor_MTSZN.fetchmany(10000)
		if len(req_MTSZN) == 0:
			flag=False
		cursor_CH_SBD.executemany("""INSERT INTO MTSZN_WMARKET.D_REQ_STATUS (CODE, DATE_ENTRY, DATE_FROM, DATE_TO, ENAME, ID, KNAME, PAR_ID, RNAME, USER_ID) VALUES""", req_MTSZN)

	cursor_CH_SBD.execute(f"SELECT COUNT(*) FROM MTSZN_WMARKET.D_REQ_STATUS")
	sbd_count_rows_value = cursor_CH_SBD.fetchone()

	cursor_MTSZN.execute(f"SELECT COUNT(*) FROM WMARKET.D_REQ_STATUS")
	origin_count_rows_value = cursor_MTSZN.fetchone()

	a = 0
	if int(sbd_count_rows_value[0]) == int(origin_count_rows_value[0]):
		a = 1
	tuple_to_load = ("SELECT COUNT(*) FROM MTSZN_WMARKET.D_REQ_STATUS", "D_REQ_STATUS", "TRUNC", a,  origin_count_rows_value[0],sbd_count_rows_value[0])
	list_to_load = [(tuple_to_load)]

	cursor_CH_SBD.executemany("INSERT INTO MTSZN_WMARKET.CHECK_LOAD (SCHEMA, TABLE, TYPE_OF_ETL, CORRECTLY_LOADED,TOTAL_COUNT_SOURCE,TOTAL_COUNT_SDU) VALUES", list_to_load)

	conn_MTSZN.close() 

def D_SOC_STATUS():
	conn_CH_SBD_secrets=Connection.get_connection_from_secrets(conn_id="Clickhouse-5")#скрытый коннекшн 5го сервера
	conn_CH_SBD=connect(host=conn_CH_SBD_secrets.host,port=conn_CH_SBD_secrets.port,user=conn_CH_SBD_secrets.login, password=conn_CH_SBD_secrets.password, connect_timeout=3600)
	cursor_CH_SBD = conn_CH_SBD.cursor()
	conn_MTSZN_secrets=Connection.get_connection_from_secrets(conn_id="MTSZN_WMARKET")#скрытый коннекшн источника
	dsn_tns_secrets=cx_Oracle.makedsn(host=conn_MTSZN_secrets.host,port=conn_MTSZN_secrets.port, service_name='SD')
	conn_MTSZN = cx_Oracle.connect(user =conn_MTSZN_secrets.login, password =conn_MTSZN_secrets.password, dsn=dsn_tns_secrets)
	cursor_MTSZN = conn_MTSZN.cursor()

	cursor_CH_SBD.execute("TRUNCATE TABLE MTSZN_WMARKET.D_SOC_STATUS")

	cursor_MTSZN.execute(f"""SELECT
 CODE AS "CODE"
, NVL(TO_CHAR("DATE_ENTRY", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "DATE_ENTRY"
, NVL(TO_CHAR("DATE_FROM", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "DATE_FROM"
, NVL(TO_CHAR("DATE_TO", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "DATE_TO"
, ENAME AS "ENAME"
, ID AS "ID"
, KNAME AS "KNAME"
, PAR_ID AS "PAR_ID"
, RNAME AS "RNAME"
, USER_ID AS "USER_ID"
FROM WMARKET."D_SOC_STATUS"
""")

	flag = True
	while flag:
		req_MTSZN = cursor_MTSZN.fetchmany(10000)
		if len(req_MTSZN) == 0:
			flag=False
		cursor_CH_SBD.executemany("""INSERT INTO MTSZN_WMARKET.D_SOC_STATUS (CODE, DATE_ENTRY, DATE_FROM, DATE_TO, ENAME, ID, KNAME, PAR_ID, RNAME, USER_ID) VALUES""", req_MTSZN)

	cursor_CH_SBD.execute(f"SELECT COUNT(*) FROM MTSZN_WMARKET.D_SOC_STATUS")
	sbd_count_rows_value = cursor_CH_SBD.fetchone()

	cursor_MTSZN.execute(f"SELECT COUNT(*) FROM WMARKET.D_SOC_STATUS")
	origin_count_rows_value = cursor_MTSZN.fetchone()

	a = 0
	if int(sbd_count_rows_value[0]) == int(origin_count_rows_value[0]):
		a = 1
	tuple_to_load = ("SELECT COUNT(*) FROM MTSZN_WMARKET.D_SOC_STATUS", "D_SOC_STATUS", "TRUNC", a,  origin_count_rows_value[0],sbd_count_rows_value[0])
	list_to_load = [(tuple_to_load)]

	cursor_CH_SBD.executemany("INSERT INTO MTSZN_WMARKET.CHECK_LOAD (SCHEMA, TABLE, TYPE_OF_ETL, CORRECTLY_LOADED,TOTAL_COUNT_SOURCE,TOTAL_COUNT_SDU) VALUES", list_to_load)

	conn_MTSZN.close() 

def F_OKED_CAT():
	conn_CH_SBD_secrets=Connection.get_connection_from_secrets(conn_id="Clickhouse-5")#скрытый коннекшн 5го сервера
	conn_CH_SBD=connect(host=conn_CH_SBD_secrets.host,port=conn_CH_SBD_secrets.port,user=conn_CH_SBD_secrets.login, password=conn_CH_SBD_secrets.password, connect_timeout=3600)
	cursor_CH_SBD = conn_CH_SBD.cursor()
	conn_MTSZN_secrets=Connection.get_connection_from_secrets(conn_id="MTSZN_WMARKET")#скрытый коннекшн источника
	dsn_tns_secrets=cx_Oracle.makedsn(host=conn_MTSZN_secrets.host,port=conn_MTSZN_secrets.port, service_name='SD')
	conn_MTSZN = cx_Oracle.connect(user =conn_MTSZN_secrets.login, password =conn_MTSZN_secrets.password, dsn=dsn_tns_secrets)
	cursor_MTSZN = conn_MTSZN.cursor()

	cursor_CH_SBD.execute("TRUNCATE TABLE MTSZN_WMARKET.F_OKED_CAT")

	cursor_MTSZN.execute(f"""SELECT
NVL(TO_CHAR("DATE_FROM", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "DATE_FROM"
, NVL(TO_CHAR("DATE_TO", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "DATE_TO"
, FOREIGN_CAT_ID AS "FOREIGN_CAT_ID"
, MRP_CNT AS "MRP_CNT"
, OKED_ID AS "OKED_ID"
FROM WMARKET."F_OKED_CAT"
""")

	flag = True
	while flag:
		req_MTSZN = cursor_MTSZN.fetchmany(10000)
		if len(req_MTSZN) == 0:
			flag=False
		cursor_CH_SBD.executemany("""INSERT INTO MTSZN_WMARKET.F_OKED_CAT (DATE_FROM, DATE_TO, FOREIGN_CAT_ID, MRP_CNT, OKED_ID) VALUES""", req_MTSZN)

	cursor_CH_SBD.execute(f"SELECT COUNT(*) FROM MTSZN_WMARKET.F_OKED_CAT")
	sbd_count_rows_value = cursor_CH_SBD.fetchone()

	cursor_MTSZN.execute(f"SELECT COUNT(*) FROM WMARKET.F_OKED_CAT")
	origin_count_rows_value = cursor_MTSZN.fetchone()

	a = 0
	if int(sbd_count_rows_value[0]) == int(origin_count_rows_value[0]):
		a = 1
	tuple_to_load = ("SELECT COUNT(*) FROM MTSZN_WMARKET.F_OKED_CAT", "F_OKED_CAT", "TRUNC", a,  origin_count_rows_value[0],sbd_count_rows_value[0])
	list_to_load = [(tuple_to_load)]

	cursor_CH_SBD.executemany("INSERT INTO MTSZN_WMARKET.CHECK_LOAD (SCHEMA, TABLE, TYPE_OF_ETL, CORRECTLY_LOADED,TOTAL_COUNT_SOURCE,TOTAL_COUNT_SDU) VALUES", list_to_load)

	conn_MTSZN.close() 

def F_QUOTA():
	conn_CH_SBD_secrets=Connection.get_connection_from_secrets(conn_id="Clickhouse-5")#скрытый коннекшн 5го сервера
	conn_CH_SBD=connect(host=conn_CH_SBD_secrets.host,port=conn_CH_SBD_secrets.port,user=conn_CH_SBD_secrets.login, password=conn_CH_SBD_secrets.password, connect_timeout=3600)
	cursor_CH_SBD = conn_CH_SBD.cursor()
	conn_MTSZN_secrets=Connection.get_connection_from_secrets(conn_id="MTSZN_WMARKET")#скрытый коннекшн источника
	dsn_tns_secrets=cx_Oracle.makedsn(host=conn_MTSZN_secrets.host,port=conn_MTSZN_secrets.port, service_name='SD')
	conn_MTSZN = cx_Oracle.connect(user =conn_MTSZN_secrets.login, password =conn_MTSZN_secrets.password, dsn=dsn_tns_secrets)
	cursor_MTSZN = conn_MTSZN.cursor()

	cursor_CH_SBD.execute("TRUNCATE TABLE MTSZN_WMARKET.F_QUOTA")

	cursor_MTSZN.execute(f"""SELECT
NVL(TO_CHAR("DATE_ENTRY", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "DATE_ENTRY"
, NVL(TO_CHAR("DEL_DATE", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "DEL_DATE"
, DEL_USER_ID AS "DEL_USER_ID"
, FOREIGN_CAT_ID AS "FOREIGN_CAT_ID"
, F_QUOTA_ID AS "F_QUOTA_ID"
, F_QUOTA_ORDER_ID AS "F_QUOTA_ORDER_ID"
, NOTE AS "NOTE"
, PAR_ID AS "PAR_ID"
, QUOTA_NUM AS "QUOTA_NUM"
, QUOTA_PERC AS "QUOTA_PERC"
, REGION_ID AS "REGION_ID"
, STATUS AS "STATUS"
, USER_ID AS "USER_ID"
FROM WMARKET."F_QUOTA"
""")

	flag = True
	while flag:
		req_MTSZN = cursor_MTSZN.fetchmany(10000)
		if len(req_MTSZN) == 0:
			flag=False
		cursor_CH_SBD.executemany("""INSERT INTO MTSZN_WMARKET.F_QUOTA (DATE_ENTRY, DEL_DATE, DEL_USER_ID, FOREIGN_CAT_ID, F_QUOTA_ID, F_QUOTA_ORDER_ID, NOTE, PAR_ID, QUOTA_NUM, QUOTA_PERC, REGION_ID, STATUS, USER_ID) VALUES""", req_MTSZN)

	cursor_CH_SBD.execute(f"SELECT COUNT(*) FROM MTSZN_WMARKET.F_QUOTA")
	sbd_count_rows_value = cursor_CH_SBD.fetchone()

	cursor_MTSZN.execute(f"SELECT COUNT(*) FROM WMARKET.F_QUOTA")
	origin_count_rows_value = cursor_MTSZN.fetchone()

	a = 0
	if int(sbd_count_rows_value[0]) == int(origin_count_rows_value[0]):
		a = 1
	tuple_to_load = ("SELECT COUNT(*) FROM MTSZN_WMARKET.F_QUOTA", "F_QUOTA", "TRUNC", a,  origin_count_rows_value[0],sbd_count_rows_value[0])
	list_to_load = [(tuple_to_load)]

	cursor_CH_SBD.executemany("INSERT INTO MTSZN_WMARKET.CHECK_LOAD (SCHEMA, TABLE, TYPE_OF_ETL, CORRECTLY_LOADED,TOTAL_COUNT_SOURCE,TOTAL_COUNT_SDU) VALUES", list_to_load)

	conn_MTSZN.close() 

def F_QUOTA_ORDER():
	conn_CH_SBD_secrets=Connection.get_connection_from_secrets(conn_id="Clickhouse-5")#скрытый коннекшн 5го сервера
	conn_CH_SBD=connect(host=conn_CH_SBD_secrets.host,port=conn_CH_SBD_secrets.port,user=conn_CH_SBD_secrets.login, password=conn_CH_SBD_secrets.password, connect_timeout=3600)
	cursor_CH_SBD = conn_CH_SBD.cursor()
	conn_MTSZN_secrets=Connection.get_connection_from_secrets(conn_id="MTSZN_WMARKET")#скрытый коннекшн источника
	dsn_tns_secrets=cx_Oracle.makedsn(host=conn_MTSZN_secrets.host,port=conn_MTSZN_secrets.port, service_name='SD')
	conn_MTSZN = cx_Oracle.connect(user =conn_MTSZN_secrets.login, password =conn_MTSZN_secrets.password, dsn=dsn_tns_secrets)
	cursor_MTSZN = conn_MTSZN.cursor()

	cursor_CH_SBD.execute("TRUNCATE TABLE MTSZN_WMARKET.F_QUOTA_ORDER")

	cursor_MTSZN.execute(f"""SELECT
NVL(TO_CHAR("DATE_ENTRY", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "DATE_ENTRY"
, NVL(TO_CHAR("DEL_DATE", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "DEL_DATE"
, DEL_USER_ID AS "DEL_USER_ID"
, F_QUOTA_ORDER_ID AS "F_QUOTA_ORDER_ID"
, MAIN_ORDER_ID AS "MAIN_ORDER_ID"
, NOTE AS "NOTE"
, NVL(TO_CHAR("ORDER_DATE", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "ORDER_DATE"
, ORDER_NUM AS "ORDER_NUM"
, NVL(TO_CHAR("QUOTA_FROM", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "QUOTA_FROM"
, NVL(TO_CHAR("QUOTA_TO", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "QUOTA_TO" 
, STATUS AS "STATUS"
, USER_ID AS "USER_ID"
FROM WMARKET."F_QUOTA_ORDER"
""")

	flag = True
	while flag:
		req_MTSZN = cursor_MTSZN.fetchmany(10000)
		if len(req_MTSZN) == 0:
			flag=False
		cursor_CH_SBD.executemany("""INSERT INTO MTSZN_WMARKET.F_QUOTA_ORDER (DATE_ENTRY, DEL_DATE, DEL_USER_ID, F_QUOTA_ORDER_ID, MAIN_ORDER_ID, NOTE, ORDER_DATE, ORDER_NUM, QUOTA_FROM, QUOTA_TO, STATUS, USER_ID) VALUES""", req_MTSZN)

	cursor_CH_SBD.execute(f"SELECT COUNT(*) FROM MTSZN_WMARKET.F_QUOTA_ORDER")
	sbd_count_rows_value = cursor_CH_SBD.fetchone()

	cursor_MTSZN.execute(f"SELECT COUNT(*) FROM WMARKET.F_QUOTA_ORDER")
	origin_count_rows_value = cursor_MTSZN.fetchone()

	a = 0
	if int(sbd_count_rows_value[0]) == int(origin_count_rows_value[0]):
		a = 1
	tuple_to_load = ("SELECT COUNT(*) FROM MTSZN_WMARKET.F_QUOTA_ORDER", "F_QUOTA_ORDER", "TRUNC", a,  origin_count_rows_value[0],sbd_count_rows_value[0])
	list_to_load = [(tuple_to_load)]

	cursor_CH_SBD.executemany("INSERT INTO MTSZN_WMARKET.CHECK_LOAD (SCHEMA, TABLE, TYPE_OF_ETL, CORRECTLY_LOADED,TOTAL_COUNT_SOURCE,TOTAL_COUNT_SDU) VALUES", list_to_load)

	conn_MTSZN.close() 

def F_QUOTA_ORDER_LOG():
	conn_CH_SBD_secrets=Connection.get_connection_from_secrets(conn_id="Clickhouse-5")#скрытый коннекшн 5го сервера
	conn_CH_SBD=connect(host=conn_CH_SBD_secrets.host,port=conn_CH_SBD_secrets.port,user=conn_CH_SBD_secrets.login, password=conn_CH_SBD_secrets.password, connect_timeout=3600)
	cursor_CH_SBD = conn_CH_SBD.cursor()
	conn_MTSZN_secrets=Connection.get_connection_from_secrets(conn_id="MTSZN_WMARKET")#скрытый коннекшн источника
	dsn_tns_secrets=cx_Oracle.makedsn(host=conn_MTSZN_secrets.host,port=conn_MTSZN_secrets.port, service_name='SD')
	conn_MTSZN = cx_Oracle.connect(user =conn_MTSZN_secrets.login, password =conn_MTSZN_secrets.password, dsn=dsn_tns_secrets)
	cursor_MTSZN = conn_MTSZN.cursor()

	cursor_CH_SBD.execute("TRUNCATE TABLE MTSZN_WMARKET.F_QUOTA_ORDER_LOG")

	cursor_MTSZN.execute(f"""SELECT
NVL(TO_CHAR("DATE_ENTRY", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "DATE_ENTRY"
, NVL(TO_CHAR("DEL_DATE", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "DEL_DATE"
, DEL_USER_ID AS "DEL_USER_ID"
, F_QORDER_LOG_ID AS "F_QORDER_LOG_ID"
, F_QUOTA_ORDER_ID AS "F_QUOTA_ORDER_ID"
, MAIN_ORDER_ID AS "MAIN_ORDER_ID"
, NOTE AS "NOTE"
, NVL(TO_CHAR("ORDER_DATE", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "ORDER_DATE"
, ORDER_NUM AS "ORDER_NUM"
, NVL(TO_CHAR("QUOTA_FROM", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "QUOTA_FROM"
, NVL(TO_CHAR("QUOTA_TO", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "QUOTA_TO" 
, STATUS AS "STATUS"
, USER_ID AS "USER_ID"
FROM WMARKET."F_QUOTA_ORDER_LOG"
""")

	flag = True
	while flag:
		req_MTSZN = cursor_MTSZN.fetchmany(10000)
		if len(req_MTSZN) == 0:
			flag=False
		cursor_CH_SBD.executemany("""INSERT INTO MTSZN_WMARKET.F_QUOTA_ORDER_LOG (DATE_ENTRY, DEL_DATE, DEL_USER_ID, F_QORDER_LOG_ID, F_QUOTA_ORDER_ID, MAIN_ORDER_ID, NOTE, ORDER_DATE, ORDER_NUM, QUOTA_FROM, QUOTA_TO, STATUS, USER_ID) VALUES""", req_MTSZN)

	cursor_CH_SBD.execute(f"SELECT COUNT(*) FROM MTSZN_WMARKET.F_QUOTA_ORDER_LOG")
	sbd_count_rows_value = cursor_CH_SBD.fetchone()

	cursor_MTSZN.execute(f"SELECT COUNT(*) FROM WMARKET.F_QUOTA_ORDER_LOG")
	origin_count_rows_value = cursor_MTSZN.fetchone()

	a = 0
	if int(sbd_count_rows_value[0]) == int(origin_count_rows_value[0]):
		a = 1
	tuple_to_load = ("SELECT COUNT(*) FROM MTSZN_WMARKET.F_QUOTA_ORDER_LOG", "F_QUOTA_ORDER_LOG", "TRUNC", a,  origin_count_rows_value[0],sbd_count_rows_value[0])
	list_to_load = [(tuple_to_load)]

	cursor_CH_SBD.executemany("INSERT INTO MTSZN_WMARKET.CHECK_LOAD (SCHEMA, TABLE, TYPE_OF_ETL, CORRECTLY_LOADED,TOTAL_COUNT_SOURCE,TOTAL_COUNT_SDU) VALUES", list_to_load)

	conn_MTSZN.close() 

def F_REQ_EMP():
	conn_CH_SBD_secrets=Connection.get_connection_from_secrets(conn_id="Clickhouse-5")#скрытый коннекшн 5го сервера
	conn_CH_SBD=connect(host=conn_CH_SBD_secrets.host,port=conn_CH_SBD_secrets.port,user=conn_CH_SBD_secrets.login, password=conn_CH_SBD_secrets.password, connect_timeout=3600)
	cursor_CH_SBD = conn_CH_SBD.cursor()
	conn_MTSZN_secrets=Connection.get_connection_from_secrets(conn_id="MTSZN_WMARKET")#скрытый коннекшн источника
	dsn_tns_secrets=cx_Oracle.makedsn(host=conn_MTSZN_secrets.host,port=conn_MTSZN_secrets.port, service_name='SD')
	conn_MTSZN = cx_Oracle.connect(user =conn_MTSZN_secrets.login, password =conn_MTSZN_secrets.password, dsn=dsn_tns_secrets)
	cursor_MTSZN = conn_MTSZN.cursor()

	cursor_CH_SBD.execute("TRUNCATE TABLE MTSZN_WMARKET.F_REQ_EMP")

	cursor_MTSZN.execute(f"""SELECT
NVL(TO_CHAR("DATE_ENTRY", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "DATE_ENTRY"
, NVL(TO_CHAR("DATE_FROM", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "DATE_FROM"
, NVL(TO_CHAR("DATE_TO", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "DATE_TO"
, NVL(TO_CHAR("DEL_DATE", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "DEL_DATE"
, DEL_USER_ID AS "DEL_USER_ID"
, EMP_ID AS "EMP_ID"
, F_REQ_EMP_ID AS "F_REQ_EMP_ID"
, F_REQ_ID AS "F_REQ_ID"
, JOB_AGR_NUM AS "JOB_AGR_NUM"
, STATUS AS "STATUS"
, USER_ID AS "USER_ID"
FROM WMARKET."F_REQ_EMP"
""")

	flag = True
	while flag:
		req_MTSZN = cursor_MTSZN.fetchmany(10000)
		if len(req_MTSZN) == 0:
			flag=False
		cursor_CH_SBD.executemany("""INSERT INTO MTSZN_WMARKET.F_REQ_EMP (DATE_ENTRY, DATE_FROM, DATE_TO, DEL_DATE, DEL_USER_ID, EMP_ID, F_REQ_EMP_ID, F_REQ_ID, JOB_AGR_NUM, STATUS, USER_ID) VALUES""", req_MTSZN)

	cursor_CH_SBD.execute(f"SELECT COUNT(*) FROM MTSZN_WMARKET.F_REQ_EMP")
	sbd_count_rows_value = cursor_CH_SBD.fetchone()

	cursor_MTSZN.execute(f"SELECT COUNT(*) FROM WMARKET.F_REQ_EMP")
	origin_count_rows_value = cursor_MTSZN.fetchone()

	a = 0
	if int(sbd_count_rows_value[0]) == int(origin_count_rows_value[0]):
		a = 1
	tuple_to_load = ("SELECT COUNT(*) FROM MTSZN_WMARKET.F_REQ_EMP", "F_REQ_EMP", "TRUNC", a,  origin_count_rows_value[0],sbd_count_rows_value[0])
	list_to_load = [(tuple_to_load)]

	cursor_CH_SBD.executemany("INSERT INTO MTSZN_WMARKET.CHECK_LOAD (SCHEMA, TABLE, TYPE_OF_ETL, CORRECTLY_LOADED,TOTAL_COUNT_SOURCE,TOTAL_COUNT_SDU) VALUES", list_to_load)

	conn_MTSZN.close() 

def F_REQ_MISSION():
	conn_CH_SBD_secrets=Connection.get_connection_from_secrets(conn_id="Clickhouse-5")#скрытый коннекшн 5го сервера
	conn_CH_SBD=connect(host=conn_CH_SBD_secrets.host,port=conn_CH_SBD_secrets.port,user=conn_CH_SBD_secrets.login, password=conn_CH_SBD_secrets.password, connect_timeout=3600)
	cursor_CH_SBD = conn_CH_SBD.cursor()
	conn_MTSZN_secrets=Connection.get_connection_from_secrets(conn_id="MTSZN_WMARKET")#скрытый коннекшн источника
	dsn_tns_secrets=cx_Oracle.makedsn(host=conn_MTSZN_secrets.host,port=conn_MTSZN_secrets.port, service_name='SD')
	conn_MTSZN = cx_Oracle.connect(user =conn_MTSZN_secrets.login, password =conn_MTSZN_secrets.password, dsn=dsn_tns_secrets)
	cursor_MTSZN = conn_MTSZN.cursor()

	cursor_CH_SBD.execute("TRUNCATE TABLE MTSZN_WMARKET.F_REQ_MISSION")

	cursor_MTSZN.execute(f"""SELECT
NVL(TO_CHAR("DATE_ENTRY", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "DATE_ENTRY"
, NVL(TO_CHAR("DATE_FROM", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "DATE_FROM"
, NVL(TO_CHAR("DATE_TO", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "DATE_TO"
, NVL(TO_CHAR("DEL_DATE", 'YYYY-MM-DD HH24:MI:SS'),'0000-00-00 00:00:00') AS "DEL_DATE"
, DEL_USER_ID AS "DEL_USER_ID"
, F_REQ_CARD_ID AS "F_REQ_CARD_ID"
, F_REQ_MISSION_ID AS "F_REQ_MISSION_ID"
, PAR_ID AS "PAR_ID"
, REGION_ID AS "REGION_ID"
, STATUS AS "STATUS"
, USER_ID AS "USER_ID"
FROM WMARKET."F_REQ_MISSION"
""")

	flag = True
	while flag:
		req_MTSZN = cursor_MTSZN.fetchmany(10000)
		if len(req_MTSZN) == 0:
			flag=False
		cursor_CH_SBD.executemany("""INSERT INTO MTSZN_WMARKET.F_REQ_MISSION (DATE_ENTRY, DATE_FROM, DATE_TO, DEL_DATE, DEL_USER_ID, F_REQ_CARD_ID, F_REQ_MISSION_ID, PAR_ID, REGION_ID, STATUS, USER_ID) VALUES""", req_MTSZN)

	cursor_CH_SBD.execute(f"SELECT COUNT(*) FROM MTSZN_WMARKET.F_REQ_MISSION")
	sbd_count_rows_value = cursor_CH_SBD.fetchone()

	cursor_MTSZN.execute(f"SELECT COUNT(*) FROM WMARKET.F_REQ_MISSION")
	origin_count_rows_value = cursor_MTSZN.fetchone()

	a = 0
	if int(sbd_count_rows_value[0]) == int(origin_count_rows_value[0]):
		a = 1
	tuple_to_load = ("SELECT COUNT(*) FROM MTSZN_WMARKET.F_REQ_MISSION", "F_REQ_MISSION", "TRUNC", a,  origin_count_rows_value[0],sbd_count_rows_value[0])
	list_to_load = [(tuple_to_load)]

	cursor_CH_SBD.executemany("INSERT INTO MTSZN_WMARKET.CHECK_LOAD (SCHEMA, TABLE, TYPE_OF_ETL, CORRECTLY_LOADED,TOTAL_COUNT_SOURCE,TOTAL_COUNT_SDU) VALUES", list_to_load)

	conn_MTSZN.close() 

with DAG("MTSZN_WMARKET", description="Trunc and delta", start_date=datetime(2022, 10, 7), schedule_interval='0 0 * * 0', catchup=False) as dag:
	hash_name = Variable.get("hash_password")

	D_AGE = PythonOperator(
		owner='Bakhtiyar',
 		task_id='D_AGE',
		python_callable=D_AGE,
	)
	D_AGENCY = PythonOperator(
		owner='Bakhtiyar',
 		task_id='D_AGENCY',
		python_callable=D_AGENCY,
	)
	D_BANK = PythonOperator(
		owner='Bakhtiyar',
 		task_id='D_BANK',
		python_callable=D_BANK,
	)
	D_CAT_SPEC = PythonOperator(
		owner='Bakhtiyar',
 		task_id='D_CAT_SPEC',
		python_callable=D_CAT_SPEC,
	)
	D_COUNTRY = PythonOperator(
		owner='Bakhtiyar',
 		task_id='D_COUNTRY',
		python_callable=D_COUNTRY,
	)
	D_CUSTOMER = PythonOperator(
		owner='Bakhtiyar',
 		task_id='D_CUSTOMER',
		python_callable=D_CUSTOMER,
	)
	D_DEP = PythonOperator(
		owner='Bakhtiyar',
 		task_id='D_DEP',
		python_callable=D_DEP,
	)
	D_DOC = PythonOperator(
		owner='Bakhtiyar',
 		task_id='D_DOC',
		python_callable=D_DOC,
	)
	D_EDUCATION = PythonOperator(
		owner='Bakhtiyar',
 		task_id='D_EDUCATION',
		python_callable=D_EDUCATION,
	)
	D_EDU_HIGH = PythonOperator(
		owner='Bakhtiyar',
 		task_id='D_EDU_HIGH',
		python_callable=D_EDU_HIGH,
	)
	D_EMPLOY_TYPE = PythonOperator(
		owner='Bakhtiyar',
 		task_id='D_EMPLOY_TYPE',
		python_callable=D_EMPLOY_TYPE,
	)
	D_EMP_ATTR = PythonOperator(
		owner='Bakhtiyar',
 		task_id='D_EMP_ATTR',
		python_callable=D_EMP_ATTR,
	)
	D_FOREIGN_CAT = PythonOperator(
		owner='Bakhtiyar',
 		task_id='D_FOREIGN_CAT',
		python_callable=D_FOREIGN_CAT,
	)
	D_FREE_SEARCH_IRS = PythonOperator(
		owner='Bakhtiyar',
 		task_id='D_FREE_SEARCH_IRS',
		python_callable=D_FREE_SEARCH_IRS,
	)
	D_FREE_SEARCH_IRS_COLS = PythonOperator(
		owner='Bakhtiyar',
 		task_id='D_FREE_SEARCH_IRS_COLS',
		python_callable=D_FREE_SEARCH_IRS_COLS,
	)
	D_F_INDEP_PROF = PythonOperator(
		owner='Bakhtiyar',
 		task_id='D_F_INDEP_PROF',
		python_callable=D_F_INDEP_PROF,
	)
	D_F_PERSON_TYPE = PythonOperator(
		owner='Bakhtiyar',
 		task_id='D_F_PERSON_TYPE',
		python_callable=D_F_PERSON_TYPE,
	)
	D_NATION = PythonOperator(
		owner='Bakhtiyar',
 		task_id='D_NATION',
		python_callable=D_NATION,
	)
	D_NATION_FL = PythonOperator(
		owner='Bakhtiyar',
 		task_id='D_NATION_FL',
		python_callable=D_NATION_FL,
	)
	D_OKED = PythonOperator(
		owner='Bakhtiyar',
 		task_id='D_OKED',
		python_callable=D_OKED,
	)
	D_PERIOD_TYPE = PythonOperator(
		owner='Bakhtiyar',
 		task_id='D_PERIOD_TYPE',
		python_callable=D_PERIOD_TYPE,
	)
	D_PERMIT_STAT = PythonOperator(
		owner='Bakhtiyar',
 		task_id='D_PERMIT_STAT',
		python_callable=D_PERMIT_STAT,
	)
	D_PERMIT_TYPE = PythonOperator(
		owner='Bakhtiyar',
 		task_id='D_PERMIT_TYPE',
		python_callable=D_PERMIT_TYPE,
	)
	D_POINT = PythonOperator(
		owner='Bakhtiyar',
 		task_id='D_POINT',
		python_callable=D_POINT,
	)
	D_POST_TYPE = PythonOperator(
		owner='Bakhtiyar',
 		task_id='D_POST_TYPE',
		python_callable=D_POST_TYPE,
	)
	D_POST_TYPE1 = PythonOperator(
		owner='Bakhtiyar',
 		task_id='D_POST_TYPE1',
		python_callable=D_POST_TYPE1,
	)
	D_PROFESSION = PythonOperator(
		owner='Bakhtiyar',
 		task_id='D_PROFESSION',
		python_callable=D_PROFESSION,
	)
	D_PROF_ADD = PythonOperator(
		owner='Bakhtiyar',
 		task_id='D_PROF_ADD',
		python_callable=D_PROF_ADD,
	)
	D_PROF_CAT = PythonOperator(
		owner='Bakhtiyar',
 		task_id='D_PROF_CAT',
		python_callable=D_PROF_CAT,
	)
	D_PROF_TYPE = PythonOperator(
		owner='Bakhtiyar',
 		task_id='D_PROF_TYPE',
		python_callable=D_PROF_TYPE,
	)
	D_PROJECT = PythonOperator(
		owner='Bakhtiyar',
 		task_id='D_PROJECT',
		python_callable=D_PROJECT,
	)
	D_REGION = PythonOperator(
		owner='Bakhtiyar',
 		task_id='D_REGION',
		python_callable=D_REGION,
	)
	D_REG_STAT = PythonOperator(
		owner='Bakhtiyar',
 		task_id='D_REG_STAT',
		python_callable=D_REG_STAT,
	)
	D_REQ_AGENCY = PythonOperator(
		owner='Bakhtiyar',
 		task_id='D_REQ_AGENCY',
		python_callable=D_REQ_AGENCY,
	)
	D_REQ_DECISION = PythonOperator(
		owner='Bakhtiyar',
 		task_id='D_REQ_DECISION',
		python_callable=D_REQ_DECISION,
	)
	D_REQ_REFUSE = PythonOperator(
		owner='Bakhtiyar',
 		task_id='D_REQ_REFUSE',
		python_callable=D_REQ_REFUSE,
	)
	D_REQ_REISSUE = PythonOperator(
		owner='Bakhtiyar',
 		task_id='D_REQ_REISSUE',
		python_callable=D_REQ_REISSUE,
	)
	D_REQ_STATUS = PythonOperator(
		owner='Bakhtiyar',
 		task_id='D_REQ_STATUS',
		python_callable=D_REQ_STATUS,
	)
	D_SOC_STATUS = PythonOperator(
		owner='Bakhtiyar',
 		task_id='D_SOC_STATUS',
		python_callable=D_SOC_STATUS,
	)
	F_OKED_CAT = PythonOperator(
		owner='Bakhtiyar',
 		task_id='F_OKED_CAT',
		python_callable=F_OKED_CAT,
	)
	F_QUOTA = PythonOperator(
		owner='Bakhtiyar',
 		task_id='F_QUOTA',
		python_callable=F_QUOTA,
	)
	F_QUOTA_ORDER = PythonOperator(
		owner='Bakhtiyar',
 		task_id='F_QUOTA_ORDER',
		python_callable=F_QUOTA_ORDER,
	)
	F_QUOTA_ORDER_LOG = PythonOperator(
		owner='Bakhtiyar',
 		task_id='F_QUOTA_ORDER_LOG',
		python_callable=F_QUOTA_ORDER_LOG,
	)
	F_REQ_EMP = PythonOperator(
		owner='Bakhtiyar',
 		task_id='F_REQ_EMP',
		python_callable=F_REQ_EMP,
	)
	F_REQ_MISSION = PythonOperator(
		owner='Bakhtiyar',
 		task_id='F_REQ_MISSION',
		python_callable=F_REQ_MISSION,
	)
	
	D_AGE>>D_AGENCY>>D_BANK>>D_CAT_SPEC>>D_COUNTRY>>D_CUSTOMER>>D_DEP>>D_DOC>>D_EDUCATION>>D_EDU_HIGH>>D_EMPLOY_TYPE>>D_EMP_ATTR>>D_FOREIGN_CAT>>D_FREE_SEARCH_IRS>>D_FREE_SEARCH_IRS_COLS>>D_F_INDEP_PROF>>D_F_PERSON_TYPE>>D_NATION>>D_NATION_FL>>D_OKED>>D_PERIOD_TYPE>>D_PERMIT_STAT>>D_PERMIT_TYPE>>D_POINT>>D_POST_TYPE>>D_POST_TYPE1>>D_PROFESSION>>D_PROF_ADD>>D_PROF_CAT>>D_PROF_TYPE>>D_PROJECT>>D_REGION>>D_REG_STAT>>D_REQ_AGENCY>>D_REQ_DECISION>>D_REQ_REFUSE>>D_REQ_REISSUE>>D_REQ_STATUS>>D_SOC_STATUS>>F_OKED_CAT>>F_QUOTA>>F_QUOTA_ORDER>>F_QUOTA_ORDER_LOG>>F_REQ_EMP>>F_REQ_MISSION