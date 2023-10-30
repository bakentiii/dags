from datetime import datetime
from airflow import DAG
from clickhouse_driver import Client, connect
from airflow.operators.python import PythonOperator
from airflow.models.connection import Connection
from airflow.models import Variable
import hashlib
import mysql.connector

def hash(data):
	hash_data = f"{str(data).lower()}{hash_name}".encode("utf-8")
	res = hashlib.sha256(hash_data).hexdigest()
	return res.upper()

def hash_columns(data, indexes):
	tmp = list(data)
	for index in indexes:
		tmp[index] = hash(data[index])
	return tuple(tmp)

def ADAM():
	conn_CH_SBD_secrets=Connection.get_connection_from_secrets(conn_id="Clickhouse-5")#скрытый коннекшн 5го сервера
	conn_CH_SBD=connect(host=conn_CH_SBD_secrets.host,port=conn_CH_SBD_secrets.port,user=conn_CH_SBD_secrets.login, password=conn_CH_SBD_secrets.password, connect_timeout=3600)
	cursor_CH_SBD = conn_CH_SBD.cursor()
	conn_AIS_secrets=Connection.get_connection_from_secrets(conn_id="AIS_NASELENIE")#скрытый коннекшн источника
	conn_AIS = mysql.connector.connect(database = conn_AIS_secrets.schema, user =conn_AIS_secrets.login, password =conn_AIS_secrets.password, host = conn_AIS_secrets.host, port = conn_AIS_secrets.port)
	cursor_AIS = conn_AIS.cursor()

	cursor_CH_SBD.execute("SELECT MAX(TIMESTAMP) AS MAX_TIMESTAMP FROM AKIMAT_ALMATY_PASPORT1.ADAM")
	max_date = cursor_CH_SBD.fetchone()[0]

	cursor_AIS.execute(f"""SELECT
 ID_PEOPLE_UNIQUE AS "ID_PEOPLE_UNIQUE"
, ID_POINT AS `ID_POINT`
, ID_PEOPLE AS `ID_PEOPLE`
, ID_NATIONALITY AS `ID_NATIONALITY`
, ID_COUNTRY_BORN AS `ID_COUNTRY_BORN`
, ID_COUNTRY_FOREIGNER AS `ID_COUNTRY_FOREIGNER`
, ID_TYPE_FOREIGNER AS `ID_TYPE_FOREIGNER`
, DATE_FORMAT(`DATE_BORN`, '%d %m %Y %T %f') AS `DATE_BORN`
, NAME_FAMILY AS `NAME_FAMILY`
, NAME_FIRSTNAME AS `NAME_FIRSTNAME`
, NAME_LASTNAME AS `NAME_LASTNAME`
, STATE_BORN AS `STATE_BORN`
, REGION_BORN AS `REGION_BORN`
, PLACE_BORN AS `PLACE_BORN`
, SEX AS `SEX`
, SIGN_CONVICTION AS `SIGN_CONVICTION`
, SIGN_CITIZENSHIP AS `SIGN_CITIZENSHIP`
, SIGN_MAJORITY AS `SIGN_MAJORITY`
, PERS_NR AS `PERS_NR`
, SIGN_ACTUAL AS `SIGN_ACTUAL`
, SIGN_DELETE AS `SIGN_DELETE`
, DATE_FORMAT(`TIMESTAMP`, '%d %m %Y %T %f') AS `TIMESTAMP`
FROM PASPORT1.`ADAM` where `TIMESTAMP` > '{max_date}'
""")

	flag = True
	while flag:
		req_AIS = cursor_AIS.fetchmany(50000)
		if len(req_AIS) == 0:
			flag=False
		cursor_CH_SBD.executemany("""INSERT INTO AKIMAT_ALMATY_PASPORT1.ADAM (ID_PEOPLE_UNIQUE, ID_POINT, ID_PEOPLE, ID_NATIONALITY, ID_COUNTRY_BORN, ID_COUNTRY_FOREIGNER, ID_TYPE_FOREIGNER, DATE_BORN, NAME_FAMILY, NAME_FIRSTNAME, NAME_LASTNAME, STATE_BORN, REGION_BORN, PLACE_BORN, SEX, SIGN_CONVICTION, SIGN_CITIZENSHIP, SIGN_MAJORITY, PERS_NR, SIGN_ACTUAL, SIGN_DELETE, TIMESTAMP) VALUES""", req_AIS)

	cursor_CH_SBD.execute(f"SELECT COUNT(*) FROM AKIMAT_ALMATY_PASPORT1.ADAM")
	sbd_count_rows_value = cursor_CH_SBD.fetchone()#считаем кол-во данных в СДУ

	cursor_AIS.execute(f"SELECT COUNT(*) FROM PASPORT1.ADAM")
	origin_count_rows_value = cursor_AIS.fetchone()#считаем кол-во данных на источнике

	a = 0#если не совпадают
	if int(sbd_count_rows_value[0]) == int(origin_count_rows_value[0]):
		a = 1#если совпадают
	tuple_to_load = ("SELECT COUNT(*) FROM AKIMAT_ALMATY_PASPORT1.ADAM", "ADAM", "TRUNC", a,  origin_count_rows_value[0],sbd_count_rows_value[0])
	list_to_load = [(tuple_to_load)]

	cursor_CH_SBD.executemany("INSERT INTO AKIMAT_ALMATY_PASPORT1.CHECK_LOAD (SCHEMA, TABLE, TYPE_OF_ETL, CORRECTLY_LOADED,TOTAL_COUNT_SOURCE,TOTAL_COUNT_SDU) VALUES", list_to_load)#загружаем в таблицу для мониторинга

	conn_AIS.close()#закрываем коннекшны
	conn_CH_SBD.close()#закрываем коннекшны 

def CW_BUNCH():
	conn_CH_SBD_secrets=Connection.get_connection_from_secrets(conn_id="Clickhouse-5")#скрытый коннекшн 5го сервера
	conn_CH_SBD=connect(host=conn_CH_SBD_secrets.host,port=conn_CH_SBD_secrets.port,user=conn_CH_SBD_secrets.login, password=conn_CH_SBD_secrets.password, connect_timeout=3600)
	cursor_CH_SBD = conn_CH_SBD.cursor()
	conn_AIS_secrets=Connection.get_connection_from_secrets(conn_id="AIS_NASELENIE")#скрытый коннекшн источника
	conn_AIS = mysql.connector.connect(database = conn_AIS_secrets.schema, user =conn_AIS_secrets.login, password =conn_AIS_secrets.password, host = conn_AIS_secrets.host, port = conn_AIS_secrets.port)
	cursor_AIS = conn_AIS.cursor()

	cursor_CH_SBD.execute("SELECT MAX(TIMESTAMP) AS MAX_TIMESTAMP FROM AKIMAT_ALMATY_PASPORT1.CW_BUNCH")
	max_date = cursor_CH_SBD.fetchone()[0]

	cursor_AIS.execute(f"""SELECT
 ID_BUNCH AS "ID_BUNCH"
, ID_POINT AS `ID_POINT`
, ID_PEOPLE_UNIQUE AS `ID_PEOPLE_UNIQUE`
, ID_DECLARATION AS `ID_DECLARATION`
, ID_APARTMENT AS `ID_APARTMENT`
, ID_OPER AS `ID_OPER`
, SIGN_LANDLORD AS `SIGN_LANDLORD`
, SIGN_LODGER AS `SIGN_LODGER`
, SIGN_GET_IN AS `SIGN_GET_IN`
, SIGN_IN_ORDER AS `SIGN_IN_ORDER`
, SIGN_BABIES_OWNER AS `SIGN_BABIES_OWNER`
, SIGN_ACTUAL AS `SIGN_ACTUAL`
, SIGN_DELETE AS `SIGN_DELETE`
, DATE_FORMAT(`TIMESTAMP`, '%d %m %Y %T %f') AS `TIMESTAMP`
FROM PASPORT1.`CW_BUNCH` where `TIMESTAMP` > '{max_date}'
""")

	flag = True
	while flag:
		req_AIS = cursor_AIS.fetchmany(50000)
		if len(req_AIS) == 0:
			flag=False
		cursor_CH_SBD.executemany("""INSERT INTO AKIMAT_ALMATY_PASPORT1.CW_BUNCH (ID_BUNCH, ID_POINT, ID_PEOPLE_UNIQUE, ID_DECLARATION, ID_APARTMENT, ID_OPER, SIGN_LANDLORD, SIGN_LODGER, SIGN_GET_IN, SIGN_IN_ORDER, SIGN_BABIES_OWNER, SIGN_ACTUAL, SIGN_DELETE, TIMESTAMP) VALUES""", req_AIS)

	cursor_CH_SBD.execute(f"SELECT COUNT(*) FROM AKIMAT_ALMATY_PASPORT1.CW_BUNCH")
	sbd_count_rows_value = cursor_CH_SBD.fetchone()#считаем кол-во данных в СДУ

	cursor_AIS.execute(f"SELECT COUNT(*) FROM PASPORT1.CW_BUNCH")
	origin_count_rows_value = cursor_AIS.fetchone()#считаем кол-во данных на источнике

	a = 0#если не совпадают
	if int(sbd_count_rows_value[0]) == int(origin_count_rows_value[0]):
		a = 1#если совпадают
	tuple_to_load = ("SELECT COUNT(*) FROM AKIMAT_ALMATY_PASPORT1.CW_BUNCH", "CW_BUNCH", "TRUNC", a,  origin_count_rows_value[0],sbd_count_rows_value[0])
	list_to_load = [(tuple_to_load)]

	cursor_CH_SBD.executemany("INSERT INTO AKIMAT_ALMATY_PASPORT1.CHECK_LOAD (SCHEMA, TABLE, TYPE_OF_ETL, CORRECTLY_LOADED,TOTAL_COUNT_SOURCE,TOTAL_COUNT_SDU) VALUES", list_to_load)#загружаем в таблицу для мониторинга

	conn_AIS.close()#закрываем коннекшны
	conn_CH_SBD.close()#закрываем коннекшны 

def DOMA():
	conn_CH_SBD_secrets=Connection.get_connection_from_secrets(conn_id="Clickhouse-5")#скрытый коннекшн 5го сервера
	conn_CH_SBD=connect(host=conn_CH_SBD_secrets.host,port=conn_CH_SBD_secrets.port,user=conn_CH_SBD_secrets.login, password=conn_CH_SBD_secrets.password, connect_timeout=3600)
	cursor_CH_SBD = conn_CH_SBD.cursor()
	conn_AIS_secrets=Connection.get_connection_from_secrets(conn_id="AIS_NASELENIE")#скрытый коннекшн источника
	conn_AIS = mysql.connector.connect(database = conn_AIS_secrets.schema, user =conn_AIS_secrets.login, password =conn_AIS_secrets.password, host = conn_AIS_secrets.host, port = conn_AIS_secrets.port)
	cursor_AIS = conn_AIS.cursor()

	cursor_CH_SBD.execute("SELECT MAX(TIMESTAMP) AS MAX_TIMESTAMP FROM AKIMAT_ALMATY_PASPORT1.DOMA")
	max_date = cursor_CH_SBD.fetchone()[0]

	cursor_AIS.execute(f"""SELECT
 ID_HOUSE AS "ID_HOUSE"
, ID_PLACE_UNIQUE AS `ID_PLACE_UNIQUE`
, ID_POINT AS `ID_POINT`
, ID_STREET_UNIQUE AS `ID_STREET_UNIQUE`
, ID_REGION_UNIQUE AS `ID_REGION_UNIQUE`
, ID_STREET_UNIQUE1 AS `ID_STREET_UNIQUE1`
, HOUSE AS `HOUSE`
, HOUSE1 AS `HOUSE1`
, BLOCK AS `BLOCK`
, SIGN_ACTUAL AS `SIGN_ACTUAL`
, SIGN_DELETE AS `SIGN_DELETE`
, DATE_FORMAT(`TIMESTAMP`, '%d %m %Y %T %f') AS `TIMESTAMP`
FROM PASPORT1.`DOMA` where `TIMESTAMP` > '{max_date}'
""")

	flag = True
	while flag:
		req_AIS = cursor_AIS.fetchmany(50000)
		if len(req_AIS) == 0:
			flag=False
		cursor_CH_SBD.executemany("""INSERT INTO AKIMAT_ALMATY_PASPORT1.DOMA (ID_HOUSE, ID_PLACE_UNIQUE, ID_POINT, ID_STREET_UNIQUE, ID_REGION_UNIQUE, ID_STREET_UNIQUE1, HOUSE, HOUSE1, BLOCK, SIGN_ACTUAL, SIGN_DELETE, TIMESTAMP) VALUES""", req_AIS)

	cursor_CH_SBD.execute(f"SELECT COUNT(*) FROM AKIMAT_ALMATY_PASPORT1.DOMA")
	sbd_count_rows_value = cursor_CH_SBD.fetchone()#считаем кол-во данных в СДУ

	cursor_AIS.execute(f"SELECT COUNT(*) FROM PASPORT1.DOMA")
	origin_count_rows_value = cursor_AIS.fetchone()#считаем кол-во данных на источнике

	a = 0#если не совпадают
	if int(sbd_count_rows_value[0]) == int(origin_count_rows_value[0]):
		a = 1#если совпадают
	tuple_to_load = ("SELECT COUNT(*) FROM AKIMAT_ALMATY_PASPORT1.DOMA", "DOMA", "TRUNC", a,  origin_count_rows_value[0],sbd_count_rows_value[0])
	list_to_load = [(tuple_to_load)]

	cursor_CH_SBD.executemany("INSERT INTO AKIMAT_ALMATY_PASPORT1.CHECK_LOAD (SCHEMA, TABLE, TYPE_OF_ETL, CORRECTLY_LOADED,TOTAL_COUNT_SOURCE,TOTAL_COUNT_SDU) VALUES", list_to_load)#загружаем в таблицу для мониторинга

	conn_AIS.close()#закрываем коннекшны
	conn_CH_SBD.close()#закрываем коннекшны 

def PATER():
	conn_CH_SBD_secrets=Connection.get_connection_from_secrets(conn_id="Clickhouse-5")#скрытый коннекшн 5го сервера
	conn_CH_SBD=connect(host=conn_CH_SBD_secrets.host,port=conn_CH_SBD_secrets.port,user=conn_CH_SBD_secrets.login, password=conn_CH_SBD_secrets.password, connect_timeout=3600)
	cursor_CH_SBD = conn_CH_SBD.cursor()
	conn_AIS_secrets=Connection.get_connection_from_secrets(conn_id="AIS_NASELENIE")#скрытый коннекшн источника
	conn_AIS = mysql.connector.connect(database = conn_AIS_secrets.schema, user =conn_AIS_secrets.login, password =conn_AIS_secrets.password, host = conn_AIS_secrets.host, port = conn_AIS_secrets.port)
	cursor_AIS = conn_AIS.cursor()

	cursor_CH_SBD.execute("SELECT MAX(TIMESTAMP) AS MAX_TIMESTAMP FROM AKIMAT_ALMATY_PASPORT1.PATER")
	max_date = cursor_CH_SBD.fetchone()[0]

	cursor_AIS.execute(f"""SELECT
 ID_APARTMENT AS "ID_APARTMENT"
, ID_POINT AS `ID_POINT`
, ID_HOUSE AS `ID_HOUSE`
, ID_TYPE_APARTMENT AS `ID_TYPE_APARTMENT`
, ID_HOUSE_MANAGEMENT AS `ID_HOUSE_MANAGEMENT`
, FLAT AS `FLAT`
, PART AS `PART`
, TELEPHONE AS `TELEPHONE`
, COMMENTS AS `COMMENTS`
, SIGN_ESTATE AS `SIGN_ESTATE`
, S_ALL AS `S_ALL`
, S_LIVE AS `S_LIVE`
, ROOM_COUNT AS `ROOM_COUNT`
, SIGN_ACTUAL AS `SIGN_ACTUAL`
, SIGN_DELETE AS `SIGN_DELETE`
, DATE_FORMAT(`TIMESTAMP`, '%d %m %Y %T %f') AS `TIMESTAMP`
FROM PASPORT1.`PATER` where `TIMESTAMP` > '{max_date}'
""")

	flag = True
	while flag:
		req_AIS = cursor_AIS.fetchmany(50000)
		if len(req_AIS) == 0:
			flag=False
		cursor_CH_SBD.executemany("""INSERT INTO AKIMAT_ALMATY_PASPORT1.PATER (ID_APARTMENT, ID_POINT, ID_HOUSE, ID_TYPE_APARTMENT, ID_HOUSE_MANAGEMENT, FLAT, PART, TELEPHONE, COMMENTS, SIGN_ESTATE, S_ALL, S_LIVE, ROOM_COUNT, SIGN_ACTUAL, SIGN_DELETE, TIMESTAMP) VALUES""", req_AIS)

	cursor_CH_SBD.execute(f"SELECT COUNT(*) FROM AKIMAT_ALMATY_PASPORT1.PATER")
	sbd_count_rows_value = cursor_CH_SBD.fetchone()#считаем кол-во данных в СДУ

	cursor_AIS.execute(f"SELECT COUNT(*) FROM PASPORT1.PATER")
	origin_count_rows_value = cursor_AIS.fetchone()#считаем кол-во данных на источнике

	a = 0#если не совпадают
	if int(sbd_count_rows_value[0]) == int(origin_count_rows_value[0]):
		a = 1#если совпадают
	tuple_to_load = ("SELECT COUNT(*) FROM AKIMAT_ALMATY_PASPORT1.PATER", "PATER", "TRUNC", a,  origin_count_rows_value[0],sbd_count_rows_value[0])
	list_to_load = [(tuple_to_load)]

	cursor_CH_SBD.executemany("INSERT INTO AKIMAT_ALMATY_PASPORT1.CHECK_LOAD (SCHEMA, TABLE, TYPE_OF_ETL, CORRECTLY_LOADED,TOTAL_COUNT_SOURCE,TOTAL_COUNT_SDU) VALUES", list_to_load)#загружаем в таблицу для мониторинга

	conn_AIS.close()#закрываем коннекшны
	conn_CH_SBD.close()#закрываем коннекшны 

def PRIBYL():
	conn_CH_SBD_secrets=Connection.get_connection_from_secrets(conn_id="Clickhouse-5")#скрытый коннекшн 5го сервера
	conn_CH_SBD=connect(host=conn_CH_SBD_secrets.host,port=conn_CH_SBD_secrets.port,user=conn_CH_SBD_secrets.login, password=conn_CH_SBD_secrets.password, connect_timeout=3600)
	cursor_CH_SBD = conn_CH_SBD.cursor()
	conn_AIS_secrets=Connection.get_connection_from_secrets(conn_id="AIS_NASELENIE")#скрытый коннекшн источника
	conn_AIS = mysql.connector.connect(database = conn_AIS_secrets.schema, user =conn_AIS_secrets.login, password =conn_AIS_secrets.password, host = conn_AIS_secrets.host, port = conn_AIS_secrets.port)
	cursor_AIS = conn_AIS.cursor()

	cursor_CH_SBD.execute("SELECT MAX(TIMESTAMP) AS MAX_TIMESTAMP FROM AKIMAT_ALMATY_PASPORT1.PRIBYL")
	max_date = cursor_CH_SBD.fetchone()[0]

	cursor_AIS.execute(f"""SELECT
 ID_GET_IN AS "ID_GET_IN"
, ID_POINT AS `ID_POINT`
, ID_OPER AS `ID_OPER`
, ID_BUNCH AS `ID_BUNCH`
, ID_REASON_GET_IN AS `ID_REASON_GET_IN`
, ID_PURPOSE_GET_IN AS `ID_PURPOSE_GET_IN`
, ID_APARTMENT_FROM AS `ID_APARTMENT_FROM`
, ID_COUNTRY_FROM AS `ID_COUNTRY_FROM`
, ID_REGION_FROM AS `ID_REGION_FROM`
, ID_PLACE_FROM AS `ID_PLACE_FROM`
, ID_BLOOD_TIES AS `ID_BLOOD_TIES`
, ID_DOCUMENT AS `ID_DOCUMENT`
, DATE_FORMAT(`DATE_INPUT`, '%d %m %Y %T %f') AS `DATE_INPUT`
, DATE_FORMAT(`DATE_GIVING`, '%d %m %Y %T %f') AS `DATE_GIVING`
, DATE_FORMAT(`DATE_END_REG`, '%d %m %Y %T %f') AS `DATE_END_REG`
, DATE_FORMAT(`DATE_REGISTRATION`, '%d %m %Y %T %f') AS `DATE_REGISTRATION`
, BOSS AS `BOSS`
, SIGN_STAT AS `SIGN_STAT`
, COMMENTS AS `COMMENTS`
, SIGN_LODGER AS `SIGN_LODGER`
, SIGN_ACTUAL AS `SIGN_ACTUAL`
, SIGN_DELETE AS `SIGN_DELETE`
, DATE_FORMAT(`TIMESTAMP`, '%d %m %Y %T %f') AS `TIMESTAMP`
FROM PASPORT1.`PRIBYL` where `TIMESTAMP` > '{max_date}'
""")

	flag = True
	while flag:
		req_AIS = cursor_AIS.fetchmany(50000)
		if len(req_AIS) == 0:
			flag=False
		cursor_CH_SBD.executemany("""INSERT INTO AKIMAT_ALMATY_PASPORT1.PRIBYL (ID_GET_IN, ID_POINT, ID_OPER, ID_BUNCH, ID_REASON_GET_IN, ID_PURPOSE_GET_IN, ID_APARTMENT_FROM, ID_COUNTRY_FROM, ID_REGION_FROM, ID_PLACE_FROM, ID_BLOOD_TIES, ID_DOCUMENT, DATE_INPUT, DATE_GIVING, DATE_END_REG, DATE_REGISTRATION, BOSS, SIGN_STAT, COMMENTS, SIGN_LODGER, SIGN_ACTUAL, SIGN_DELETE, TIMESTAMP) VALUES""", req_AIS)

	cursor_CH_SBD.execute(f"SELECT COUNT(*) FROM AKIMAT_ALMATY_PASPORT1.PRIBYL")
	sbd_count_rows_value = cursor_CH_SBD.fetchone()#считаем кол-во данных в СДУ

	cursor_AIS.execute(f"SELECT COUNT(*) FROM PASPORT1.PRIBYL")
	origin_count_rows_value = cursor_AIS.fetchone()#считаем кол-во данных на источнике

	a = 0#если не совпадают
	if int(sbd_count_rows_value[0]) == int(origin_count_rows_value[0]):
		a = 1#если совпадают
	tuple_to_load = ("SELECT COUNT(*) FROM AKIMAT_ALMATY_PASPORT1.PRIBYL", "PRIBYL", "TRUNC", a,  origin_count_rows_value[0],sbd_count_rows_value[0])
	list_to_load = [(tuple_to_load)]

	cursor_CH_SBD.executemany("INSERT INTO AKIMAT_ALMATY_PASPORT1.CHECK_LOAD (SCHEMA, TABLE, TYPE_OF_ETL, CORRECTLY_LOADED,TOTAL_COUNT_SOURCE,TOTAL_COUNT_SDU) VALUES", list_to_load)#загружаем в таблицу для мониторинга

	conn_AIS.close()#закрываем коннекшны
	conn_CH_SBD.close()#закрываем коннекшны 

def S_COMPANY():
	conn_CH_SBD_secrets=Connection.get_connection_from_secrets(conn_id="Clickhouse-5")#скрытый коннекшн 5го сервера
	conn_CH_SBD=connect(host=conn_CH_SBD_secrets.host,port=conn_CH_SBD_secrets.port,user=conn_CH_SBD_secrets.login, password=conn_CH_SBD_secrets.password, connect_timeout=3600)
	cursor_CH_SBD = conn_CH_SBD.cursor()
	conn_AIS_secrets=Connection.get_connection_from_secrets(conn_id="AIS_NASELENIE")#скрытый коннекшн источника
	conn_AIS = mysql.connector.connect(database = conn_AIS_secrets.schema, user =conn_AIS_secrets.login, password =conn_AIS_secrets.password, host = conn_AIS_secrets.host, port = conn_AIS_secrets.port)
	cursor_AIS = conn_AIS.cursor()

	cursor_CH_SBD.execute("TRUNCATE TABLE AKIMAT_ALMATY_PASPORT1.S_COMPANY")

	cursor_AIS.execute(f"""SELECT
 ID_COMPANY AS "ID_COMPANY"
, NAME_COMPANY AS `NAME_COMPANY`
, DATE_FORMAT(`DATE_COMPANY`, '%d %m %Y %T %f') AS `DATE_COMPANY`
, RNN_COMPANY AS `RNN_COMPANY`
, SIGN_ACTUAL AS `SIGN_ACTUAL`
, SIGN_DELETE AS `SIGN_DELETE`
FROM PASPORT1.`S_COMPANY`
""")

	flag = True
	while flag:
		req_AIS = cursor_AIS.fetchmany(50000)
		if len(req_AIS) == 0:
			flag=False
		cursor_CH_SBD.executemany("""INSERT INTO AKIMAT_ALMATY_PASPORT1.S_COMPANY (ID_COMPANY, NAME_COMPANY, DATE_COMPANY, RNN_COMPANY, SIGN_ACTUAL, SIGN_DELETE) VALUES""", req_AIS)

	cursor_CH_SBD.execute(f"SELECT COUNT(*) FROM AKIMAT_ALMATY_PASPORT1.S_COMPANY")
	sbd_count_rows_value = cursor_CH_SBD.fetchone()#считаем кол-во данных в СДУ

	cursor_AIS.execute(f"SELECT COUNT(*) FROM PASPORT1.S_COMPANY")
	origin_count_rows_value = cursor_AIS.fetchone()#считаем кол-во данных на источнике

	a = 0#если не совпадают
	if int(sbd_count_rows_value[0]) == int(origin_count_rows_value[0]):
		a = 1#если совпадают
	tuple_to_load = ("SELECT COUNT(*) FROM AKIMAT_ALMATY_PASPORT1.S_COMPANY", "S_COMPANY", "TRUNC", a,  origin_count_rows_value[0],sbd_count_rows_value[0])
	list_to_load = [(tuple_to_load)]

	cursor_CH_SBD.executemany("INSERT INTO AKIMAT_ALMATY_PASPORT1.CHECK_LOAD (SCHEMA, TABLE, TYPE_OF_ETL, CORRECTLY_LOADED,TOTAL_COUNT_SOURCE,TOTAL_COUNT_SDU) VALUES", list_to_load)#загружаем в таблицу для мониторинга

	conn_AIS.close()#закрываем коннекшны
	conn_CH_SBD.close()#закрываем коннекшны 

def S_COUNTRIES():
	conn_CH_SBD_secrets=Connection.get_connection_from_secrets(conn_id="Clickhouse-5")#скрытый коннекшн 5го сервера
	conn_CH_SBD=connect(host=conn_CH_SBD_secrets.host,port=conn_CH_SBD_secrets.port,user=conn_CH_SBD_secrets.login, password=conn_CH_SBD_secrets.password, connect_timeout=3600)
	cursor_CH_SBD = conn_CH_SBD.cursor()
	conn_AIS_secrets=Connection.get_connection_from_secrets(conn_id="AIS_NASELENIE")#скрытый коннекшн источника
	conn_AIS = mysql.connector.connect(database = conn_AIS_secrets.schema, user =conn_AIS_secrets.login, password =conn_AIS_secrets.password, host = conn_AIS_secrets.host, port = conn_AIS_secrets.port)
	cursor_AIS = conn_AIS.cursor()

	cursor_CH_SBD.execute("SELECT MAX(TIMESTAMP) AS MAX_TIMESTAMP FROM AKIMAT_ALMATY_PASPORT1.S_COUNTRIES")
	max_date = cursor_CH_SBD.fetchone()[0]

	cursor_AIS.execute(f"""SELECT
 ID_COUNTRY AS "ID_COUNTRY"
, ID_COUNTRY_DOTS AS `ID_COUNTRY_DOTS`
, NAME_COUNTRY AS `NAME_COUNTRY`
, NAME_COUNTRY_ AS `NAME_COUNTRY_`
, SIGN_COUNTRY AS `SIGN_COUNTRY`
, SIGN_ACTUAL AS `SIGN_ACTUAL`
, SIGN_DELETE AS `SIGN_DELETE`
, DATE_FORMAT(`TIMESTAMP`, '%d %m %Y %T %f') AS `TIMESTAMP`
FROM PASPORT1.`S_COUNTRIES` where `TIMESTAMP` > '{max_date}'
""")

	flag = True
	while flag:
		req_AIS = cursor_AIS.fetchmany(50000)
		if len(req_AIS) == 0:
			flag=False
		cursor_CH_SBD.executemany("""INSERT INTO AKIMAT_ALMATY_PASPORT1.S_COUNTRIES (ID_COUNTRY, ID_COUNTRY_DOTS, NAME_COUNTRY, NAME_COUNTRY_, SIGN_COUNTRY, SIGN_ACTUAL, SIGN_DELETE, TIMESTAMP) VALUES""", req_AIS)

	cursor_CH_SBD.execute(f"SELECT COUNT(*) FROM AKIMAT_ALMATY_PASPORT1.S_COUNTRIES")
	sbd_count_rows_value = cursor_CH_SBD.fetchone()#считаем кол-во данных в СДУ

	cursor_AIS.execute(f"SELECT COUNT(*) FROM PASPORT1.S_COUNTRIES")
	origin_count_rows_value = cursor_AIS.fetchone()#считаем кол-во данных на источнике

	a = 0#если не совпадают
	if int(sbd_count_rows_value[0]) == int(origin_count_rows_value[0]):
		a = 1#если совпадают
	tuple_to_load = ("SELECT COUNT(*) FROM AKIMAT_ALMATY_PASPORT1.S_COUNTRIES", "S_COUNTRIES", "TRUNC", a,  origin_count_rows_value[0],sbd_count_rows_value[0])
	list_to_load = [(tuple_to_load)]

	cursor_CH_SBD.executemany("INSERT INTO AKIMAT_ALMATY_PASPORT1.CHECK_LOAD (SCHEMA, TABLE, TYPE_OF_ETL, CORRECTLY_LOADED,TOTAL_COUNT_SOURCE,TOTAL_COUNT_SDU) VALUES", list_to_load)#загружаем в таблицу для мониторинга

	conn_AIS.close()#закрываем коннекшны
	conn_CH_SBD.close()#закрываем коннекшны 

def S_DOC():
	conn_CH_SBD_secrets=Connection.get_connection_from_secrets(conn_id="Clickhouse-5")#скрытый коннекшн 5го сервера
	conn_CH_SBD=connect(host=conn_CH_SBD_secrets.host,port=conn_CH_SBD_secrets.port,user=conn_CH_SBD_secrets.login, password=conn_CH_SBD_secrets.password, connect_timeout=3600)
	cursor_CH_SBD = conn_CH_SBD.cursor()
	conn_AIS_secrets=Connection.get_connection_from_secrets(conn_id="AIS_NASELENIE")#скрытый коннекшн источника
	conn_AIS = mysql.connector.connect(database = conn_AIS_secrets.schema, user =conn_AIS_secrets.login, password =conn_AIS_secrets.password, host = conn_AIS_secrets.host, port = conn_AIS_secrets.port)
	cursor_AIS = conn_AIS.cursor()

	cursor_CH_SBD.execute("SELECT MAX(TIMESTAMP) AS MAX_TIMESTAMP FROM AKIMAT_ALMATY_PASPORT1.S_DOC")
	max_date = cursor_CH_SBD.fetchone()[0]

	cursor_AIS.execute(f"""SELECT
 ID_TYPE_DOC AS "ID_TYPE_DOC"
, ID_POINT AS `ID_POINT`
, NAME_TYPE_DOC AS `NAME_TYPE_DOC`
, NAME_TYPE_DOC_ AS `NAME_TYPE_DOC_`
, SIGN_ACTUAL AS `SIGN_ACTUAL`
, SIGN_DELETE AS `SIGN_DELETE`
, DATE_FORMAT(`TIMESTAMP`, '%d %m %Y %T %f') AS `TIMESTAMP`
FROM PASPORT1.`S_DOC` where `TIMESTAMP` > '{max_date}'
""")

	flag = True
	while flag:
		req_AIS = cursor_AIS.fetchmany(50000)
		if len(req_AIS) == 0:
			flag=False
		cursor_CH_SBD.executemany("""INSERT INTO AKIMAT_ALMATY_PASPORT1.S_DOC (ID_TYPE_DOC, ID_POINT, NAME_TYPE_DOC, NAME_TYPE_DOC_, SIGN_ACTUAL, SIGN_DELETE, TIMESTAMP) VALUES""", req_AIS)

	cursor_CH_SBD.execute(f"SELECT COUNT(*) FROM AKIMAT_ALMATY_PASPORT1.S_DOC")
	sbd_count_rows_value = cursor_CH_SBD.fetchone()#считаем кол-во данных в СДУ

	cursor_AIS.execute(f"SELECT COUNT(*) FROM PASPORT1.S_DOC")
	origin_count_rows_value = cursor_AIS.fetchone()#считаем кол-во данных на источнике

	a = 0#если не совпадают
	if int(sbd_count_rows_value[0]) == int(origin_count_rows_value[0]):
		a = 1#если совпадают
	tuple_to_load = ("SELECT COUNT(*) FROM AKIMAT_ALMATY_PASPORT1.S_DOC", "S_DOC", "TRUNC", a,  origin_count_rows_value[0],sbd_count_rows_value[0])
	list_to_load = [(tuple_to_load)]

	cursor_CH_SBD.executemany("INSERT INTO AKIMAT_ALMATY_PASPORT1.CHECK_LOAD (SCHEMA, TABLE, TYPE_OF_ETL, CORRECTLY_LOADED,TOTAL_COUNT_SOURCE,TOTAL_COUNT_SDU) VALUES", list_to_load)#загружаем в таблицу для мониторинга

	conn_AIS.close()#закрываем коннекшны
	conn_CH_SBD.close()#закрываем коннекшны 

def S_NAC():
	conn_CH_SBD_secrets=Connection.get_connection_from_secrets(conn_id="Clickhouse-5")#скрытый коннекшн 5го сервера
	conn_CH_SBD=connect(host=conn_CH_SBD_secrets.host,port=conn_CH_SBD_secrets.port,user=conn_CH_SBD_secrets.login, password=conn_CH_SBD_secrets.password, connect_timeout=3600)
	cursor_CH_SBD = conn_CH_SBD.cursor()
	conn_AIS_secrets=Connection.get_connection_from_secrets(conn_id="AIS_NASELENIE")#скрытый коннекшн источника
	conn_AIS = mysql.connector.connect(database = conn_AIS_secrets.schema, user =conn_AIS_secrets.login, password =conn_AIS_secrets.password, host = conn_AIS_secrets.host, port = conn_AIS_secrets.port)
	cursor_AIS = conn_AIS.cursor()

	cursor_CH_SBD.execute("SELECT MAX(TIMESTAMP) AS MAX_TIMESTAMP FROM AKIMAT_ALMATY_PASPORT1.S_NAC")
	max_date = cursor_CH_SBD.fetchone()[0]

	cursor_AIS.execute(f"""SELECT
 ID_NATIONALITY AS "ID_NATIONALITY"
, NAME_NAT_MALE AS `NAME_NAT_MALE`
, NAME_NAT_FEMALE AS `NAME_NAT_FEMALE`
, NAME_NAT_MALE_ AS `NAME_NAT_MALE_`
, NAME_NAT_FEMALE_ AS `NAME_NAT_FEMALE_`
, SIGN_ACTUAL AS `SIGN_ACTUAL`
, SIGN_DELETE AS `SIGN_DELETE`
, DATE_FORMAT(`TIMESTAMP`, '%d %m %Y %T %f') AS `TIMESTAMP`
FROM PASPORT1.`S_NAC` where `TIMESTAMP` > '{max_date}'
""")

	flag = True
	while flag:
		req_AIS = cursor_AIS.fetchmany(50000)
		if len(req_AIS) == 0:
			flag=False
		cursor_CH_SBD.executemany("""INSERT INTO AKIMAT_ALMATY_PASPORT1.S_NAC (ID_NATIONALITY, NAME_NAT_MALE, NAME_NAT_FEMALE, NAME_NAT_MALE_, NAME_NAT_FEMALE_, SIGN_ACTUAL, SIGN_DELETE, TIMESTAMP) VALUES""", req_AIS)

	cursor_CH_SBD.execute(f"SELECT COUNT(*) FROM AKIMAT_ALMATY_PASPORT1.S_NAC")
	sbd_count_rows_value = cursor_CH_SBD.fetchone()#считаем кол-во данных в СДУ

	cursor_AIS.execute(f"SELECT COUNT(*) FROM PASPORT1.S_NAC")
	origin_count_rows_value = cursor_AIS.fetchone()#считаем кол-во данных на источнике

	a = 0#если не совпадают
	if int(sbd_count_rows_value[0]) == int(origin_count_rows_value[0]):
		a = 1#если совпадают
	tuple_to_load = ("SELECT COUNT(*) FROM AKIMAT_ALMATY_PASPORT1.S_NAC", "S_NAC", "TRUNC", a,  origin_count_rows_value[0],sbd_count_rows_value[0])
	list_to_load = [(tuple_to_load)]

	cursor_CH_SBD.executemany("INSERT INTO AKIMAT_ALMATY_PASPORT1.CHECK_LOAD (SCHEMA, TABLE, TYPE_OF_ETL, CORRECTLY_LOADED,TOTAL_COUNT_SOURCE,TOTAL_COUNT_SDU) VALUES", list_to_load)#загружаем в таблицу для мониторинга

	conn_AIS.close()#закрываем коннекшны
	conn_CH_SBD.close()#закрываем коннекшны 

def S_NSPNKT():
	conn_CH_SBD_secrets=Connection.get_connection_from_secrets(conn_id="Clickhouse-5")#скрытый коннекшн 5го сервера
	conn_CH_SBD=connect(host=conn_CH_SBD_secrets.host,port=conn_CH_SBD_secrets.port,user=conn_CH_SBD_secrets.login, password=conn_CH_SBD_secrets.password, connect_timeout=3600)
	cursor_CH_SBD = conn_CH_SBD.cursor()
	conn_AIS_secrets=Connection.get_connection_from_secrets(conn_id="AIS_NASELENIE")#скрытый коннекшн источника
	conn_AIS = mysql.connector.connect(database = conn_AIS_secrets.schema, user =conn_AIS_secrets.login, password =conn_AIS_secrets.password, host = conn_AIS_secrets.host, port = conn_AIS_secrets.port)
	cursor_AIS = conn_AIS.cursor()

	cursor_CH_SBD.execute("SELECT MAX(TIMESTAMP) AS MAX_TIMESTAMP FROM AKIMAT_ALMATY_PASPORT1.S_NSPNKT")
	max_date = cursor_CH_SBD.fetchone()[0]

	cursor_AIS.execute(f"""SELECT
 ID_PLACE_UNIQUE AS "ID_PLACE_UNIQUE"
, ID_PLACE AS `ID_PLACE`
, NAME_PLACE AS `NAME_PLACE`
, NAME_PLACE_ AS `NAME_PLACE_`
, SIGN_ACTUAL AS `SIGN_ACTUAL`
, SIGN_DELETE AS `SIGN_DELETE`
, DATE_FORMAT(`TIMESTAMP`, '%d %m %Y %T %f') AS `TIMESTAMP`
FROM PASPORT1.`S_NSPNKT` where `TIMESTAMP` > '{max_date}'
""")

	flag = True
	while flag:
		req_AIS = cursor_AIS.fetchmany(50000)
		if len(req_AIS) == 0:
			flag=False
		cursor_CH_SBD.executemany("""INSERT INTO AKIMAT_ALMATY_PASPORT1.S_NSPNKT (ID_PLACE_UNIQUE, ID_PLACE, NAME_PLACE, NAME_PLACE_, SIGN_ACTUAL, SIGN_DELETE, TIMESTAMP) VALUES""", req_AIS)

	cursor_CH_SBD.execute(f"SELECT COUNT(*) FROM AKIMAT_ALMATY_PASPORT1.S_NSPNKT")
	sbd_count_rows_value = cursor_CH_SBD.fetchone()#считаем кол-во данных в СДУ

	cursor_AIS.execute(f"SELECT COUNT(*) FROM PASPORT1.S_NSPNKT")
	origin_count_rows_value = cursor_AIS.fetchone()#считаем кол-во данных на источнике

	a = 0#если не совпадают
	if int(sbd_count_rows_value[0]) == int(origin_count_rows_value[0]):
		a = 1#если совпадают
	tuple_to_load = ("SELECT COUNT(*) FROM AKIMAT_ALMATY_PASPORT1.S_NSPNKT", "S_NSPNKT", "TRUNC", a,  origin_count_rows_value[0],sbd_count_rows_value[0])
	list_to_load = [(tuple_to_load)]

	cursor_CH_SBD.executemany("INSERT INTO AKIMAT_ALMATY_PASPORT1.CHECK_LOAD (SCHEMA, TABLE, TYPE_OF_ETL, CORRECTLY_LOADED,TOTAL_COUNT_SOURCE,TOTAL_COUNT_SDU) VALUES", list_to_load)#загружаем в таблицу для мониторинга

	conn_AIS.close()#закрываем коннекшны
	conn_CH_SBD.close()#закрываем коннекшны 

def S_OPERAC():
	conn_CH_SBD_secrets=Connection.get_connection_from_secrets(conn_id="Clickhouse-5")#скрытый коннекшн 5го сервера
	conn_CH_SBD=connect(host=conn_CH_SBD_secrets.host,port=conn_CH_SBD_secrets.port,user=conn_CH_SBD_secrets.login, password=conn_CH_SBD_secrets.password, connect_timeout=3600)
	cursor_CH_SBD = conn_CH_SBD.cursor()
	conn_AIS_secrets=Connection.get_connection_from_secrets(conn_id="AIS_NASELENIE")#скрытый коннекшн источника
	conn_AIS = mysql.connector.connect(database = conn_AIS_secrets.schema, user =conn_AIS_secrets.login, password =conn_AIS_secrets.password, host = conn_AIS_secrets.host, port = conn_AIS_secrets.port)
	cursor_AIS = conn_AIS.cursor()

	cursor_CH_SBD.execute("SELECT MAX(TIMESTAMP) AS MAX_TIMESTAMP FROM AKIMAT_ALMATY_PASPORT1.S_OPERAC")
	max_date = cursor_CH_SBD.fetchone()[0]

	cursor_AIS.execute(f"""SELECT
 ID_REASON_GET AS "ID_REASON_GET"
, NAME_REASON_GET AS `NAME_REASON_GET`
, NAME_REASON_GET_ AS `NAME_REASON_GET_`
, SIGN_WHERE AS `SIGN_WHERE`
, SIGN_ACTUAL AS `SIGN_ACTUAL`
, SIGN_DELETE AS `SIGN_DELETE`
, SIGN_PEREG AS `SIGN_PEREG`
, DATE_FORMAT(`TIMESTAMP`, '%d %m %Y %T %f') AS `TIMESTAMP`
FROM PASPORT1.`S_OPERAC` where `TIMESTAMP` > '{max_date}'
""")

	flag = True
	while flag:
		req_AIS = cursor_AIS.fetchmany(50000)
		if len(req_AIS) == 0:
			flag=False
		cursor_CH_SBD.executemany("""INSERT INTO AKIMAT_ALMATY_PASPORT1.S_OPERAC (ID_REASON_GET, NAME_REASON_GET, NAME_REASON_GET_, SIGN_WHERE, SIGN_ACTUAL, SIGN_DELETE, SIGN_PEREG, TIMESTAMP) VALUES""", req_AIS)

	cursor_CH_SBD.execute(f"SELECT COUNT(*) FROM AKIMAT_ALMATY_PASPORT1.S_OPERAC")
	sbd_count_rows_value = cursor_CH_SBD.fetchone()#считаем кол-во данных в СДУ

	cursor_AIS.execute(f"SELECT COUNT(*) FROM PASPORT1.S_OPERAC")
	origin_count_rows_value = cursor_AIS.fetchone()#считаем кол-во данных на источнике

	a = 0#если не совпадают
	if int(sbd_count_rows_value[0]) == int(origin_count_rows_value[0]):
		a = 1#если совпадают
	tuple_to_load = ("SELECT COUNT(*) FROM AKIMAT_ALMATY_PASPORT1.S_OPERAC", "S_OPERAC", "TRUNC", a,  origin_count_rows_value[0],sbd_count_rows_value[0])
	list_to_load = [(tuple_to_load)]

	cursor_CH_SBD.executemany("INSERT INTO AKIMAT_ALMATY_PASPORT1.CHECK_LOAD (SCHEMA, TABLE, TYPE_OF_ETL, CORRECTLY_LOADED,TOTAL_COUNT_SOURCE,TOTAL_COUNT_SDU) VALUES", list_to_load)#загружаем в таблицу для мониторинга

	conn_AIS.close()#закрываем коннекшны
	conn_CH_SBD.close()#закрываем коннекшны 

def S_RAY():
	conn_CH_SBD_secrets=Connection.get_connection_from_secrets(conn_id="Clickhouse-5")#скрытый коннекшн 5го сервера
	conn_CH_SBD=connect(host=conn_CH_SBD_secrets.host,port=conn_CH_SBD_secrets.port,user=conn_CH_SBD_secrets.login, password=conn_CH_SBD_secrets.password, connect_timeout=3600)
	cursor_CH_SBD = conn_CH_SBD.cursor()
	conn_AIS_secrets=Connection.get_connection_from_secrets(conn_id="AIS_NASELENIE")#скрытый коннекшн источника
	conn_AIS = mysql.connector.connect(database = conn_AIS_secrets.schema, user =conn_AIS_secrets.login, password =conn_AIS_secrets.password, host = conn_AIS_secrets.host, port = conn_AIS_secrets.port)
	cursor_AIS = conn_AIS.cursor()

	cursor_CH_SBD.execute("SELECT MAX(TIMESTAMP) AS MAX_TIMESTAMP FROM AKIMAT_ALMATY_PASPORT1.S_RAY")
	max_date = cursor_CH_SBD.fetchone()[0]

	cursor_AIS.execute(f"""SELECT
 ID_REGION_UNIQUE AS "ID_REGION_UNIQUE"
, ID_STATE_UNIQUE AS `ID_STATE_UNIQUE`
, ID_REGION AS `ID_REGION`
, ID_REGION_DOTS AS `ID_REGION_DOTS`
, NAME_REGION AS `NAME_REGION`
, NAME_REGION_ AS `NAME_REGION_`
, SIGN_ACTUAL AS `SIGN_ACTUAL`
, SIGN_DELETE AS `SIGN_DELETE`
, DATE_FORMAT(`TIMESTAMP`, '%d %m %Y %T %f') AS `TIMESTAMP`
FROM PASPORT1.`S_RAY` where `TIMESTAMP` > '{max_date}'
""")

	flag = True
	while flag:
		req_AIS = cursor_AIS.fetchmany(50000)
		if len(req_AIS) == 0:
			flag=False
		cursor_CH_SBD.executemany("""INSERT INTO AKIMAT_ALMATY_PASPORT1.S_RAY (ID_REGION_UNIQUE, ID_STATE_UNIQUE, ID_REGION, ID_REGION_DOTS, NAME_REGION, NAME_REGION_, SIGN_ACTUAL, SIGN_DELETE, TIMESTAMP) VALUES""", req_AIS)

	cursor_CH_SBD.execute(f"SELECT COUNT(*) FROM AKIMAT_ALMATY_PASPORT1.S_RAY")
	sbd_count_rows_value = cursor_CH_SBD.fetchone()#считаем кол-во данных в СДУ

	cursor_AIS.execute(f"SELECT COUNT(*) FROM PASPORT1.S_RAY")
	origin_count_rows_value = cursor_AIS.fetchone()#считаем кол-во данных на источнике

	a = 0#если не совпадают
	if int(sbd_count_rows_value[0]) == int(origin_count_rows_value[0]):
		a = 1#если совпадают
	tuple_to_load = ("SELECT COUNT(*) FROM AKIMAT_ALMATY_PASPORT1.S_RAY", "S_RAY", "TRUNC", a,  origin_count_rows_value[0],sbd_count_rows_value[0])
	list_to_load = [(tuple_to_load)]

	cursor_CH_SBD.executemany("INSERT INTO AKIMAT_ALMATY_PASPORT1.CHECK_LOAD (SCHEMA, TABLE, TYPE_OF_ETL, CORRECTLY_LOADED,TOTAL_COUNT_SOURCE,TOTAL_COUNT_SDU) VALUES", list_to_load)#загружаем в таблицу для мониторинга

	conn_AIS.close()#закрываем коннекшны
	conn_CH_SBD.close()#закрываем коннекшны 

def S_SEX():
	conn_CH_SBD_secrets=Connection.get_connection_from_secrets(conn_id="Clickhouse-5")#скрытый коннекшн 5го сервера
	conn_CH_SBD=connect(host=conn_CH_SBD_secrets.host,port=conn_CH_SBD_secrets.port,user=conn_CH_SBD_secrets.login, password=conn_CH_SBD_secrets.password, connect_timeout=3600)
	cursor_CH_SBD = conn_CH_SBD.cursor()
	conn_AIS_secrets=Connection.get_connection_from_secrets(conn_id="AIS_NASELENIE")#скрытый коннекшн источника
	conn_AIS = mysql.connector.connect(database = conn_AIS_secrets.schema, user =conn_AIS_secrets.login, password =conn_AIS_secrets.password, host = conn_AIS_secrets.host, port = conn_AIS_secrets.port)
	cursor_AIS = conn_AIS.cursor()

	cursor_CH_SBD.execute("TRUNCATE TABLE AKIMAT_ALMATY_PASPORT1.S_SEX")

	cursor_AIS.execute(f"""SELECT
 SEX AS "SEX"
, NAME_SEX AS `NAME_SEX`
, NAME_SEX_ AS `NAME_SEX_`
, SIGN_ACTUAL AS `SIGN_ACTUAL`
, SIGN_DELETE AS `SIGN_DELETE`
FROM PASPORT1.`S_SEX`
""")

	flag = True
	while flag:
		req_AIS = cursor_AIS.fetchmany(50000)
		if len(req_AIS) == 0:
			flag=False
		cursor_CH_SBD.executemany("""INSERT INTO AKIMAT_ALMATY_PASPORT1.S_SEX (SEX, NAME_SEX, NAME_SEX_, SIGN_ACTUAL, SIGN_DELETE) VALUES""", req_AIS)

	cursor_CH_SBD.execute(f"SELECT COUNT(*) FROM AKIMAT_ALMATY_PASPORT1.S_SEX")
	sbd_count_rows_value = cursor_CH_SBD.fetchone()#считаем кол-во данных в СДУ

	cursor_AIS.execute(f"SELECT COUNT(*) FROM PASPORT1.S_SEX")
	origin_count_rows_value = cursor_AIS.fetchone()#считаем кол-во данных на источнике

	a = 0#если не совпадают
	if int(sbd_count_rows_value[0]) == int(origin_count_rows_value[0]):
		a = 1#если совпадают
	tuple_to_load = ("SELECT COUNT(*) FROM AKIMAT_ALMATY_PASPORT1.S_SEX", "S_SEX", "TRUNC", a,  origin_count_rows_value[0],sbd_count_rows_value[0])
	list_to_load = [(tuple_to_load)]

	cursor_CH_SBD.executemany("INSERT INTO AKIMAT_ALMATY_PASPORT1.CHECK_LOAD (SCHEMA, TABLE, TYPE_OF_ETL, CORRECTLY_LOADED,TOTAL_COUNT_SOURCE,TOTAL_COUNT_SDU) VALUES", list_to_load)#загружаем в таблицу для мониторинга

	conn_AIS.close()#закрываем коннекшны
	conn_CH_SBD.close()#закрываем коннекшны 

def S_STATES():
	conn_CH_SBD_secrets=Connection.get_connection_from_secrets(conn_id="Clickhouse-5")#скрытый коннекшн 5го сервера
	conn_CH_SBD=connect(host=conn_CH_SBD_secrets.host,port=conn_CH_SBD_secrets.port,user=conn_CH_SBD_secrets.login, password=conn_CH_SBD_secrets.password, connect_timeout=3600)
	cursor_CH_SBD = conn_CH_SBD.cursor()
	conn_AIS_secrets=Connection.get_connection_from_secrets(conn_id="AIS_NASELENIE")#скрытый коннекшн источника
	conn_AIS = mysql.connector.connect(database = conn_AIS_secrets.schema, user =conn_AIS_secrets.login, password =conn_AIS_secrets.password, host = conn_AIS_secrets.host, port = conn_AIS_secrets.port)
	cursor_AIS = conn_AIS.cursor()

	cursor_CH_SBD.execute("SELECT MAX(TIMESTAMP) AS MAX_TIMESTAMP FROM AKIMAT_ALMATY_PASPORT1.S_STATES")
	max_date = cursor_CH_SBD.fetchone()[0]

	cursor_AIS.execute(f"""SELECT
 ID_STATE_UNIQUE AS "ID_STATE_UNIQUE"
, ID_STATE AS `ID_STATE`
, ID_COUNTRY AS `ID_COUNTRY`
, NAME_STATE AS `NAME_STATE`
, NAME_STATE_ AS `NAME_STATE_`
, SIGN_ACTUAL AS `SIGN_ACTUAL`
, SIGN_DELETE AS `SIGN_DELETE`
, DATE_FORMAT(`TIMESTAMP`, '%d %m %Y %T %f') AS `TIMESTAMP`
FROM PASPORT1.`S_STATES` where `TIMESTAMP` > '{max_date}'
""")

	flag = True
	while flag:
		req_AIS = cursor_AIS.fetchmany(50000)
		if len(req_AIS) == 0:
			flag=False
		cursor_CH_SBD.executemany("""INSERT INTO AKIMAT_ALMATY_PASPORT1.S_STATES (ID_STATE_UNIQUE, ID_STATE, ID_COUNTRY, NAME_STATE, NAME_STATE_, SIGN_ACTUAL, SIGN_DELETE, TIMESTAMP) VALUES""", req_AIS)

	cursor_CH_SBD.execute(f"SELECT COUNT(*) FROM AKIMAT_ALMATY_PASPORT1.S_STATES")
	sbd_count_rows_value = cursor_CH_SBD.fetchone()#считаем кол-во данных в СДУ

	cursor_AIS.execute(f"SELECT COUNT(*) FROM PASPORT1.S_STATES")
	origin_count_rows_value = cursor_AIS.fetchone()#считаем кол-во данных на источнике

	a = 0#если не совпадают
	if int(sbd_count_rows_value[0]) == int(origin_count_rows_value[0]):
		a = 1#если совпадают
	tuple_to_load = ("SELECT COUNT(*) FROM AKIMAT_ALMATY_PASPORT1.S_STATES", "S_STATES", "TRUNC", a,  origin_count_rows_value[0],sbd_count_rows_value[0])
	list_to_load = [(tuple_to_load)]

	cursor_CH_SBD.executemany("INSERT INTO AKIMAT_ALMATY_PASPORT1.CHECK_LOAD (SCHEMA, TABLE, TYPE_OF_ETL, CORRECTLY_LOADED,TOTAL_COUNT_SOURCE,TOTAL_COUNT_SDU) VALUES", list_to_load)#загружаем в таблицу для мониторинга

	conn_AIS.close()#закрываем коннекшны
	conn_CH_SBD.close()#закрываем коннекшны 

def S_STATUS():
	conn_CH_SBD_secrets=Connection.get_connection_from_secrets(conn_id="Clickhouse-5")#скрытый коннекшн 5го сервера
	conn_CH_SBD=connect(host=conn_CH_SBD_secrets.host,port=conn_CH_SBD_secrets.port,user=conn_CH_SBD_secrets.login, password=conn_CH_SBD_secrets.password, connect_timeout=3600)
	cursor_CH_SBD = conn_CH_SBD.cursor()
	conn_AIS_secrets=Connection.get_connection_from_secrets(conn_id="AIS_NASELENIE")#скрытый коннекшн источника
	conn_AIS = mysql.connector.connect(database = conn_AIS_secrets.schema, user =conn_AIS_secrets.login, password =conn_AIS_secrets.password, host = conn_AIS_secrets.host, port = conn_AIS_secrets.port)
	cursor_AIS = conn_AIS.cursor()

	cursor_CH_SBD.execute("TRUNCATE TABLE AKIMAT_ALMATY_PASPORT1.S_STATUS")

	cursor_AIS.execute(f"""SELECT
 ID_STATUS AS "ID_STATUS"
, NAME_STATUS AS `NAME_STATUS`
, NAME_STATUS_ AS `NAME_STATUS_`
, SIGN_ACTUAL AS `SIGN_ACTUAL`
, SIGN_DELETE AS `SIGN_DELETE`
FROM PASPORT1.`S_STATUS`
""")

	flag = True
	while flag:
		req_AIS = cursor_AIS.fetchmany(50000)
		if len(req_AIS) == 0:
			flag=False
		cursor_CH_SBD.executemany("""INSERT INTO AKIMAT_ALMATY_PASPORT1.S_STATUS (ID_STATUS, NAME_STATUS, NAME_STATUS_, SIGN_ACTUAL, SIGN_DELETE) VALUES""", req_AIS)

	cursor_CH_SBD.execute(f"SELECT COUNT(*) FROM AKIMAT_ALMATY_PASPORT1.S_STATUS")
	sbd_count_rows_value = cursor_CH_SBD.fetchone()#считаем кол-во данных в СДУ

	cursor_AIS.execute(f"SELECT COUNT(*) FROM PASPORT1.S_STATUS")
	origin_count_rows_value = cursor_AIS.fetchone()#считаем кол-во данных на источнике

	a = 0#если не совпадают
	if int(sbd_count_rows_value[0]) == int(origin_count_rows_value[0]):
		a = 1#если совпадают
	tuple_to_load = ("SELECT COUNT(*) FROM AKIMAT_ALMATY_PASPORT1.S_STATUS", "S_STATUS", "TRUNC", a,  origin_count_rows_value[0],sbd_count_rows_value[0])
	list_to_load = [(tuple_to_load)]

	cursor_CH_SBD.executemany("INSERT INTO AKIMAT_ALMATY_PASPORT1.CHECK_LOAD (SCHEMA, TABLE, TYPE_OF_ETL, CORRECTLY_LOADED,TOTAL_COUNT_SOURCE,TOTAL_COUNT_SDU) VALUES", list_to_load)#загружаем в таблицу для мониторинга

	conn_AIS.close()#закрываем коннекшны
	conn_CH_SBD.close()#закрываем коннекшны 

def S_TYPE_APARTMENT():
	conn_CH_SBD_secrets=Connection.get_connection_from_secrets(conn_id="Clickhouse-5")#скрытый коннекшн 5го сервера
	conn_CH_SBD=connect(host=conn_CH_SBD_secrets.host,port=conn_CH_SBD_secrets.port,user=conn_CH_SBD_secrets.login, password=conn_CH_SBD_secrets.password, connect_timeout=3600)
	cursor_CH_SBD = conn_CH_SBD.cursor()
	conn_AIS_secrets=Connection.get_connection_from_secrets(conn_id="AIS_NASELENIE")#скрытый коннекшн источника
	conn_AIS = mysql.connector.connect(database = conn_AIS_secrets.schema, user =conn_AIS_secrets.login, password =conn_AIS_secrets.password, host = conn_AIS_secrets.host, port = conn_AIS_secrets.port)
	cursor_AIS = conn_AIS.cursor()

	cursor_CH_SBD.execute("SELECT MAX(TIMESTAMP) AS MAX_TIMESTAMP FROM AKIMAT_ALMATY_PASPORT1.S_TYPE_APARTMENT")
	max_date = cursor_CH_SBD.fetchone()[0]

	cursor_AIS.execute(f"""SELECT
 ID_TYPE_APARTMENT AS "ID_TYPE_APARTMENT"
, NAME_TYPE_APARTMENT AS `NAME_TYPE_APARTMENT`
, NAME_TYPE_APARTMENT_ AS `NAME_TYPE_APARTMENT_`
, SIGN_ACTUAL AS `SIGN_ACTUAL`
, SIGN_DELETE AS `SIGN_DELETE`
, DATE_FORMAT(`TIMESTAMP`, '%d %m %Y %T %f') AS `TIMESTAMP`
FROM PASPORT1.`S_TYPE_APARTMENT` where `TIMESTAMP` > '{max_date}'
""")

	flag = True
	while flag:
		req_AIS = cursor_AIS.fetchmany(50000)
		if len(req_AIS) == 0:
			flag=False
		cursor_CH_SBD.executemany("""INSERT INTO AKIMAT_ALMATY_PASPORT1.S_TYPE_APARTMENT (ID_TYPE_APARTMENT, NAME_TYPE_APARTMENT, NAME_TYPE_APARTMENT_, SIGN_ACTUAL, SIGN_DELETE, TIMESTAMP) VALUES""", req_AIS)

	cursor_CH_SBD.execute(f"SELECT COUNT(*) FROM AKIMAT_ALMATY_PASPORT1.S_TYPE_APARTMENT")
	sbd_count_rows_value = cursor_CH_SBD.fetchone()#считаем кол-во данных в СДУ

	cursor_AIS.execute(f"SELECT COUNT(*) FROM PASPORT1.S_TYPE_APARTMENT")
	origin_count_rows_value = cursor_AIS.fetchone()#считаем кол-во данных на источнике

	a = 0#если не совпадают
	if int(sbd_count_rows_value[0]) == int(origin_count_rows_value[0]):
		a = 1#если совпадают
	tuple_to_load = ("SELECT COUNT(*) FROM AKIMAT_ALMATY_PASPORT1.S_TYPE_APARTMENT", "S_TYPE_APARTMENT", "TRUNC", a,  origin_count_rows_value[0],sbd_count_rows_value[0])
	list_to_load = [(tuple_to_load)]

	cursor_CH_SBD.executemany("INSERT INTO AKIMAT_ALMATY_PASPORT1.CHECK_LOAD (SCHEMA, TABLE, TYPE_OF_ETL, CORRECTLY_LOADED,TOTAL_COUNT_SOURCE,TOTAL_COUNT_SDU) VALUES", list_to_load)#загружаем в таблицу для мониторинга

	conn_AIS.close()#закрываем коннекшны
	conn_CH_SBD.close()#закрываем коннекшны 

def S_TYPE_DECLARATION():
	conn_CH_SBD_secrets=Connection.get_connection_from_secrets(conn_id="Clickhouse-5")#скрытый коннекшн 5го сервера
	conn_CH_SBD=connect(host=conn_CH_SBD_secrets.host,port=conn_CH_SBD_secrets.port,user=conn_CH_SBD_secrets.login, password=conn_CH_SBD_secrets.password, connect_timeout=3600)
	cursor_CH_SBD = conn_CH_SBD.cursor()
	conn_AIS_secrets=Connection.get_connection_from_secrets(conn_id="AIS_NASELENIE")#скрытый коннекшн источника
	conn_AIS = mysql.connector.connect(database = conn_AIS_secrets.schema, user =conn_AIS_secrets.login, password =conn_AIS_secrets.password, host = conn_AIS_secrets.host, port = conn_AIS_secrets.port)
	cursor_AIS = conn_AIS.cursor()

	cursor_CH_SBD.execute("TRUNCATE TABLE AKIMAT_ALMATY_PASPORT1.S_TYPE_DECLARATION")

	cursor_AIS.execute(f"""SELECT
 ID_TYPE_DECLARATION AS "ID_TYPE_DECLARATION"
, NAME_TYPE_DECLARATION AS `NAME_TYPE_DECLARATION`
, COMMENTS AS `COMMENTS`
, SIGN_ACTUAL AS `SIGN_ACTUAL`
, SIGN_DELETE AS `SIGN_DELETE`
FROM PASPORT1.`S_TYPE_DECLARATION`
""")

	flag = True
	while flag:
		req_AIS = cursor_AIS.fetchmany(50000)
		if len(req_AIS) == 0:
			flag=False
		cursor_CH_SBD.executemany("""INSERT INTO AKIMAT_ALMATY_PASPORT1.S_TYPE_DECLARATION (ID_TYPE_DECLARATION, NAME_TYPE_DECLARATION, COMMENTS, SIGN_ACTUAL, SIGN_DELETE) VALUES""", req_AIS)

	cursor_CH_SBD.execute(f"SELECT COUNT(*) FROM AKIMAT_ALMATY_PASPORT1.S_TYPE_DECLARATION")
	sbd_count_rows_value = cursor_CH_SBD.fetchone()#считаем кол-во данных в СДУ

	cursor_AIS.execute(f"SELECT COUNT(*) FROM PASPORT1.S_TYPE_DECLARATION")
	origin_count_rows_value = cursor_AIS.fetchone()#считаем кол-во данных на источнике

	a = 0#если не совпадают
	if int(sbd_count_rows_value[0]) == int(origin_count_rows_value[0]):
		a = 1#если совпадают
	tuple_to_load = ("SELECT COUNT(*) FROM AKIMAT_ALMATY_PASPORT1.S_TYPE_DECLARATION", "S_TYPE_DECLARATION", "TRUNC", a,  origin_count_rows_value[0],sbd_count_rows_value[0])
	list_to_load = [(tuple_to_load)]

	cursor_CH_SBD.executemany("INSERT INTO AKIMAT_ALMATY_PASPORT1.CHECK_LOAD (SCHEMA, TABLE, TYPE_OF_ETL, CORRECTLY_LOADED,TOTAL_COUNT_SOURCE,TOTAL_COUNT_SDU) VALUES", list_to_load)#загружаем в таблицу для мониторинга

	conn_AIS.close()#закрываем коннекшны
	conn_CH_SBD.close()#закрываем коннекшны 

def S_TYPE_REGISTRATION():
	conn_CH_SBD_secrets=Connection.get_connection_from_secrets(conn_id="Clickhouse-5")#скрытый коннекшн 5го сервера
	conn_CH_SBD=connect(host=conn_CH_SBD_secrets.host,port=conn_CH_SBD_secrets.port,user=conn_CH_SBD_secrets.login, password=conn_CH_SBD_secrets.password, connect_timeout=3600)
	cursor_CH_SBD = conn_CH_SBD.cursor()
	conn_AIS_secrets=Connection.get_connection_from_secrets(conn_id="AIS_NASELENIE")#скрытый коннекшн источника
	conn_AIS = mysql.connector.connect(database = conn_AIS_secrets.schema, user =conn_AIS_secrets.login, password =conn_AIS_secrets.password, host = conn_AIS_secrets.host, port = conn_AIS_secrets.port)
	cursor_AIS = conn_AIS.cursor()

	cursor_CH_SBD.execute("TRUNCATE TABLE AKIMAT_ALMATY_PASPORT1.S_TYPE_REGISTRATION")

	cursor_AIS.execute(f"""SELECT
 ID_TYPE_REGISTRATION AS "ID_TYPE_REGISTRATION"
, NAME_TYPE_REGISTRATION AS `NAME_TYPE_REGISTRATION`
, SIGN_ACTUAL AS `SIGN_ACTUAL`
, SIGN_DELETE AS `SIGN_DELETE`
FROM PASPORT1.`S_TYPE_REGISTRATION`
""")

	flag = True
	while flag:
		req_AIS = cursor_AIS.fetchmany(50000)
		if len(req_AIS) == 0:
			flag=False
		cursor_CH_SBD.executemany("""INSERT INTO AKIMAT_ALMATY_PASPORT1.S_TYPE_REGISTRATION (ID_TYPE_REGISTRATION, NAME_TYPE_REGISTRATION, SIGN_ACTUAL, SIGN_DELETE) VALUES""", req_AIS)

	cursor_CH_SBD.execute(f"SELECT COUNT(*) FROM AKIMAT_ALMATY_PASPORT1.S_TYPE_REGISTRATION")
	sbd_count_rows_value = cursor_CH_SBD.fetchone()#считаем кол-во данных в СДУ

	cursor_AIS.execute(f"SELECT COUNT(*) FROM PASPORT1.S_TYPE_REGISTRATION")
	origin_count_rows_value = cursor_AIS.fetchone()#считаем кол-во данных на источнике

	a = 0#если не совпадают
	if int(sbd_count_rows_value[0]) == int(origin_count_rows_value[0]):
		a = 1#если совпадают
	tuple_to_load = ("SELECT COUNT(*) FROM AKIMAT_ALMATY_PASPORT1.S_TYPE_REGISTRATION", "S_TYPE_REGISTRATION", "TRUNC", a,  origin_count_rows_value[0],sbd_count_rows_value[0])
	list_to_load = [(tuple_to_load)]

	cursor_CH_SBD.executemany("INSERT INTO AKIMAT_ALMATY_PASPORT1.CHECK_LOAD (SCHEMA, TABLE, TYPE_OF_ETL, CORRECTLY_LOADED,TOTAL_COUNT_SOURCE,TOTAL_COUNT_SDU) VALUES", list_to_load)#загружаем в таблицу для мониторинга

	conn_AIS.close()#закрываем коннекшны
	conn_CH_SBD.close()#закрываем коннекшны 

def S_ULI():
	conn_CH_SBD_secrets=Connection.get_connection_from_secrets(conn_id="Clickhouse-5")#скрытый коннекшн 5го сервера
	conn_CH_SBD=connect(host=conn_CH_SBD_secrets.host,port=conn_CH_SBD_secrets.port,user=conn_CH_SBD_secrets.login, password=conn_CH_SBD_secrets.password, connect_timeout=3600)
	cursor_CH_SBD = conn_CH_SBD.cursor()
	conn_AIS_secrets=Connection.get_connection_from_secrets(conn_id="AIS_NASELENIE")#скрытый коннекшн источника
	conn_AIS = mysql.connector.connect(database = conn_AIS_secrets.schema, user =conn_AIS_secrets.login, password =conn_AIS_secrets.password, host = conn_AIS_secrets.host, port = conn_AIS_secrets.port)
	cursor_AIS = conn_AIS.cursor()

	cursor_CH_SBD.execute("SELECT MAX(TIMESTAMP) AS MAX_TIMESTAMP FROM AKIMAT_ALMATY_PASPORT1.S_ULI")
	max_date = cursor_CH_SBD.fetchone()[0]

	cursor_AIS.execute(f"""SELECT
 ID_STREET_UNIQUE AS "ID_STREET_UNIQUE"
, ID_PLACE_UNIQUE AS `ID_PLACE_UNIQUE`
, ID_STREET AS `ID_STREET`
, NAME_STREET AS `NAME_STREET`
, NAME_STREET_ AS `NAME_STREET_`
, SIGN_ACTUAL AS `SIGN_ACTUAL`
, SIGN_DELETE AS `SIGN_DELETE`
, DATE_FORMAT(`TIMESTAMP`, '%d %m %Y %T %f') AS `TIMESTAMP`
FROM PASPORT1.`S_ULI` where `TIMESTAMP` > '{max_date}'
""")

	flag = True
	while flag:
		req_AIS = cursor_AIS.fetchmany(50000)
		if len(req_AIS) == 0:
			flag=False
		cursor_CH_SBD.executemany("""INSERT INTO AKIMAT_ALMATY_PASPORT1.S_ULI (ID_STREET_UNIQUE, ID_PLACE_UNIQUE, ID_STREET, NAME_STREET, NAME_STREET_, SIGN_ACTUAL, SIGN_DELETE, TIMESTAMP) VALUES""", req_AIS)

	cursor_CH_SBD.execute(f"SELECT COUNT(*) FROM AKIMAT_ALMATY_PASPORT1.S_ULI")
	sbd_count_rows_value = cursor_CH_SBD.fetchone()#считаем кол-во данных в СДУ

	cursor_AIS.execute(f"SELECT COUNT(*) FROM PASPORT1.S_ULI")
	origin_count_rows_value = cursor_AIS.fetchone()#считаем кол-во данных на источнике

	a = 0#если не совпадают
	if int(sbd_count_rows_value[0]) == int(origin_count_rows_value[0]):
		a = 1#если совпадают
	tuple_to_load = ("SELECT COUNT(*) FROM AKIMAT_ALMATY_PASPORT1.S_ULI", "S_ULI", "TRUNC", a,  origin_count_rows_value[0],sbd_count_rows_value[0])
	list_to_load = [(tuple_to_load)]

	cursor_CH_SBD.executemany("INSERT INTO AKIMAT_ALMATY_PASPORT1.CHECK_LOAD (SCHEMA, TABLE, TYPE_OF_ETL, CORRECTLY_LOADED,TOTAL_COUNT_SOURCE,TOTAL_COUNT_SDU) VALUES", list_to_load)#загружаем в таблицу для мониторинга

	conn_AIS.close()#закрываем коннекшны
	conn_CH_SBD.close()#закрываем коннекшны 

def UBYL():
	conn_CH_SBD_secrets=Connection.get_connection_from_secrets(conn_id="Clickhouse-5")#скрытый коннекшн 5го сервера
	conn_CH_SBD=connect(host=conn_CH_SBD_secrets.host,port=conn_CH_SBD_secrets.port,user=conn_CH_SBD_secrets.login, password=conn_CH_SBD_secrets.password, connect_timeout=3600)
	cursor_CH_SBD = conn_CH_SBD.cursor()
	conn_AIS_secrets=Connection.get_connection_from_secrets(conn_id="AIS_NASELENIE")#скрытый коннекшн источника
	conn_AIS = mysql.connector.connect(database = conn_AIS_secrets.schema, user =conn_AIS_secrets.login, password =conn_AIS_secrets.password, host = conn_AIS_secrets.host, port = conn_AIS_secrets.port)
	cursor_AIS = conn_AIS.cursor()

	cursor_CH_SBD.execute("SELECT MAX(TIMESTAMP) AS MAX_TIMESTAMP FROM AKIMAT_ALMATY_PASPORT1.UBYL")
	max_date = cursor_CH_SBD.fetchone()[0]

	cursor_AIS.execute(f"""SELECT
 ID_GET_OUT AS "ID_GET_OUT"
, ID_POINT AS `ID_POINT`
, ID_GET_IN AS `ID_GET_IN`
, ID_OPER AS `ID_OPER`
, ID_REASON_GET_OUT AS `ID_REASON_GET_OUT`
, ID_PURPOSE_GET_OUT AS `ID_PURPOSE_GET_OUT`
, ID_APARTMENT_IN AS `ID_APARTMENT_IN`
, ID_COUNTRY_IN AS `ID_COUNTRY_IN`
, ID_REGION_IN AS `ID_REGION_IN`
, ID_PLACE_IN AS `ID_PLACE_IN`
, ID_BUNCH AS `ID_BUNCH`
, ID_DOCUMENT AS `ID_DOCUMENT`
, DATE_FORMAT(`DATE_INPUT`, '%d %m %Y %T %f') AS `DATE_INPUT`
, DATE_FORMAT(`DATE_GIVING`, '%d %m %Y %T %f') AS `DATE_GIVING`
, DATE_FORMAT(`DATE_END_TERM`, '%d %m %Y %T %f') AS `DATE_END_TERM`
, DATE_FORMAT(`DATE_REGISTRATION`, '%d %m %Y %T %f') AS `DATE_REGISTRATION`
, BOSS AS `BOSS`
, SIGN_STAT AS `SIGN_STAT`
, COMMENTS AS `COMMENTS`
, SIGN_ACTUAL AS `SIGN_ACTUAL`
, SIGN_DELETE AS `SIGN_DELETE`
, DATE_FORMAT(`TIMESTAMP`, '%d %m %Y %T %f') AS `TIMESTAMP`
FROM PASPORT1.`UBYL` where `TIMESTAMP` > '{max_date}'
""")

	flag = True
	while flag:
		req_AIS = cursor_AIS.fetchmany(50000)
		if len(req_AIS) == 0:
			flag=False
		cursor_CH_SBD.executemany("""INSERT INTO AKIMAT_ALMATY_PASPORT1.UBYL (ID_GET_OUT, ID_POINT, ID_GET_IN, ID_OPER, ID_REASON_GET_OUT, ID_PURPOSE_GET_OUT, ID_APARTMENT_IN, ID_COUNTRY_IN, ID_REGION_IN, ID_PLACE_IN, ID_BUNCH, ID_DOCUMENT, DATE_INPUT, DATE_GIVING, DATE_END_TERM, DATE_REGISTRATION, BOSS, SIGN_STAT, COMMENTS, SIGN_ACTUAL, SIGN_DELETE, TIMESTAMP) VALUES""", req_AIS)

	cursor_CH_SBD.execute(f"SELECT COUNT(*) FROM AKIMAT_ALMATY_PASPORT1.UBYL")
	sbd_count_rows_value = cursor_CH_SBD.fetchone()#считаем кол-во данных в СДУ

	cursor_AIS.execute(f"SELECT COUNT(*) FROM PASPORT1.UBYL")
	origin_count_rows_value = cursor_AIS.fetchone()#считаем кол-во данных на источнике

	a = 0#если не совпадают
	if int(sbd_count_rows_value[0]) == int(origin_count_rows_value[0]):
		a = 1#если совпадают
	tuple_to_load = ("SELECT COUNT(*) FROM AKIMAT_ALMATY_PASPORT1.UBYL", "UBYL", "TRUNC", a,  origin_count_rows_value[0],sbd_count_rows_value[0])
	list_to_load = [(tuple_to_load)]

	cursor_CH_SBD.executemany("INSERT INTO AKIMAT_ALMATY_PASPORT1.CHECK_LOAD (SCHEMA, TABLE, TYPE_OF_ETL, CORRECTLY_LOADED,TOTAL_COUNT_SOURCE,TOTAL_COUNT_SDU) VALUES", list_to_load)#загружаем в таблицу для мониторинга

	conn_AIS.close()#закрываем коннекшны
	conn_CH_SBD.close()#закрываем коннекшны 

def W_DOCUMENTS():
	conn_CH_SBD_secrets=Connection.get_connection_from_secrets(conn_id="Clickhouse-5")#скрытый коннекшн 5го сервера
	conn_CH_SBD=connect(host=conn_CH_SBD_secrets.host,port=conn_CH_SBD_secrets.port,user=conn_CH_SBD_secrets.login, password=conn_CH_SBD_secrets.password, connect_timeout=3600)
	cursor_CH_SBD = conn_CH_SBD.cursor()
	conn_AIS_secrets=Connection.get_connection_from_secrets(conn_id="AIS_NASELENIE")#скрытый коннекшн источника
	conn_AIS = mysql.connector.connect(database = conn_AIS_secrets.schema, user =conn_AIS_secrets.login, password =conn_AIS_secrets.password, host = conn_AIS_secrets.host, port = conn_AIS_secrets.port)
	cursor_AIS = conn_AIS.cursor()

	cursor_CH_SBD.execute("SELECT MAX(TIMESTAMP) AS MAX_TIMESTAMP FROM AKIMAT_ALMATY_PASPORT1.W_DOCUMENTS")
	max_date = cursor_CH_SBD.fetchone()[0]

	cursor_AIS.execute(f"""SELECT
 ID_DOCUMENT AS "ID_DOCUMENT"
, ID_POINT AS `ID_POINT`
, ID_PEOPLE_UNIQUE AS `ID_PEOPLE_UNIQUE`
, ID_TYPE_DOC AS `ID_TYPE_DOC`
, SERIES_DOC AS `SERIES_DOC`
, NOMBER_DOC AS `NOMBER_DOC`
, ORGAN_DOC AS `ORGAN_DOC`
, DATE_FORMAT(`DATE_DOC`, '%d %m %Y %T %f') AS `DATE_DOC`
, DATE_FORMAT(`DATE_END_DOC`, '%d %m %Y %T %f') AS `DATE_END_DOC`
, COMMENTS_DOC AS `COMMENTS_DOC`
, SIGN_MAKE AS `SIGN_MAKE`
, PER_ID AS `PER_ID`
, PER_LOC_ID AS `PER_LOC_ID`
, SIGN_ACTUAL AS `SIGN_ACTUAL`
, SIGN_DELETE AS `SIGN_DELETE`
, DATE_FORMAT(`TIMESTAMP`, '%d %m %Y %T %f') AS `TIMESTAMP`
FROM PASPORT1.`W_DOCUMENTS` where `TIMESTAMP` > '{max_date}'
""")

	flag = True
	while flag:
		req_AIS = cursor_AIS.fetchmany(50000)
		if len(req_AIS) == 0:
			flag=False
		cursor_CH_SBD.executemany("""INSERT INTO AKIMAT_ALMATY_PASPORT1.W_DOCUMENTS (ID_DOCUMENT, ID_POINT, ID_PEOPLE_UNIQUE, ID_TYPE_DOC, SERIES_DOC, NOMBER_DOC, ORGAN_DOC, DATE_DOC, DATE_END_DOC, COMMENTS_DOC, SIGN_MAKE, PER_ID, PER_LOC_ID, SIGN_ACTUAL, SIGN_DELETE, TIMESTAMP) VALUES""", req_AIS)

	cursor_CH_SBD.execute(f"SELECT COUNT(*) FROM AKIMAT_ALMATY_PASPORT1.W_DOCUMENTS")
	sbd_count_rows_value = cursor_CH_SBD.fetchone()#считаем кол-во данных в СДУ

	cursor_AIS.execute(f"SELECT COUNT(*) FROM PASPORT1.W_DOCUMENTS")
	origin_count_rows_value = cursor_AIS.fetchone()#считаем кол-во данных на источнике

	a = 0#если не совпадают
	if int(sbd_count_rows_value[0]) == int(origin_count_rows_value[0]):
		a = 1#если совпадают
	tuple_to_load = ("SELECT COUNT(*) FROM AKIMAT_ALMATY_PASPORT1.W_DOCUMENTS", "W_DOCUMENTS", "TRUNC", a,  origin_count_rows_value[0],sbd_count_rows_value[0])
	list_to_load = [(tuple_to_load)]

	cursor_CH_SBD.executemany("INSERT INTO AKIMAT_ALMATY_PASPORT1.CHECK_LOAD (SCHEMA, TABLE, TYPE_OF_ETL, CORRECTLY_LOADED,TOTAL_COUNT_SOURCE,TOTAL_COUNT_SDU) VALUES", list_to_load)#загружаем в таблицу для мониторинга

	conn_AIS.close()#закрываем коннекшны
	conn_CH_SBD.close()#закрываем коннекшны 

def W_LIVE():
	conn_CH_SBD_secrets=Connection.get_connection_from_secrets(conn_id="Clickhouse-5")#скрытый коннекшн 5го сервера
	conn_CH_SBD=connect(host=conn_CH_SBD_secrets.host,port=conn_CH_SBD_secrets.port,user=conn_CH_SBD_secrets.login, password=conn_CH_SBD_secrets.password, connect_timeout=3600)
	cursor_CH_SBD = conn_CH_SBD.cursor()
	conn_AIS_secrets=Connection.get_connection_from_secrets(conn_id="AIS_NASELENIE")#скрытый коннекшн источника
	conn_AIS = mysql.connector.connect(database = conn_AIS_secrets.schema, user =conn_AIS_secrets.login, password =conn_AIS_secrets.password, host = conn_AIS_secrets.host, port = conn_AIS_secrets.port)
	cursor_AIS = conn_AIS.cursor()

	cursor_CH_SBD.execute("SELECT MAX(TIMESTAMP) AS MAX_TIMESTAMP FROM AKIMAT_ALMATY_PASPORT1.W_LIVE")
	max_date = cursor_CH_SBD.fetchone()[0]

	cursor_AIS.execute(f"""SELECT
 ID_LIVE AS "ID_LIVE"
, ID_APARTMENT AS `ID_APARTMENT`
, ID_PEOPLE_UNIQUE AS `ID_PEOPLE_UNIQUE`
, ID_BLOOD_TIES AS `ID_BLOOD_TIES`
, ID_GET_IN AS `ID_GET_IN`
, DATE_FORMAT(`DATE_REGISTRATION`, '%d %m %Y %T %f') AS `DATE_REGISTRATION`
, DATE_FORMAT(`DATE_UPDATE`, '%d %m %Y %T %f') AS `DATE_UPDATE`
, DATE_FORMAT(`DATE_END_REGISTRATION`, '%d %m %Y %T %f') AS `DATE_END_REGISTRATION`
, SIGN_IN_ORDER AS `SIGN_IN_ORDER`
, SIGN_ACTUAL AS `SIGN_ACTUAL`
, SIGN_DELETE AS `SIGN_DELETE`
, DATE_FORMAT(`TIMESTAMP`, '%d %m %Y %T %f') AS `TIMESTAMP`
FROM PASPORT1.`W_LIVE` where `TIMESTAMP` > '{max_date}'
""")

	flag = True
	while flag:
		req_AIS = cursor_AIS.fetchmany(50000)
		if len(req_AIS) == 0:
			flag=False
		cursor_CH_SBD.executemany("""INSERT INTO AKIMAT_ALMATY_PASPORT1.W_LIVE (ID_LIVE, ID_APARTMENT, ID_PEOPLE_UNIQUE, ID_BLOOD_TIES, ID_GET_IN, DATE_REGISTRATION, DATE_UPDATE, DATE_END_REGISTRATION, SIGN_IN_ORDER, SIGN_ACTUAL, SIGN_DELETE, TIMESTAMP) VALUES""", req_AIS)

	cursor_CH_SBD.execute(f"SELECT COUNT(*) FROM AKIMAT_ALMATY_PASPORT1.W_LIVE")
	sbd_count_rows_value = cursor_CH_SBD.fetchone()#считаем кол-во данных в СДУ

	cursor_AIS.execute(f"SELECT COUNT(*) FROM PASPORT1.W_LIVE")
	origin_count_rows_value = cursor_AIS.fetchone()#считаем кол-во данных на источнике

	a = 0#если не совпадают
	if int(sbd_count_rows_value[0]) == int(origin_count_rows_value[0]):
		a = 1#если совпадают
	tuple_to_load = ("SELECT COUNT(*) FROM AKIMAT_ALMATY_PASPORT1.W_LIVE", "W_LIVE", "TRUNC", a,  origin_count_rows_value[0],sbd_count_rows_value[0])
	list_to_load = [(tuple_to_load)]

	cursor_CH_SBD.executemany("INSERT INTO AKIMAT_ALMATY_PASPORT1.CHECK_LOAD (SCHEMA, TABLE, TYPE_OF_ETL, CORRECTLY_LOADED,TOTAL_COUNT_SOURCE,TOTAL_COUNT_SDU) VALUES", list_to_load)#загружаем в таблицу для мониторинга

	conn_AIS.close()#закрываем коннекшны
	conn_CH_SBD.close()#закрываем коннекшны 

def ZAYAV():
	conn_CH_SBD_secrets=Connection.get_connection_from_secrets(conn_id="Clickhouse-5")#скрытый коннекшн 5го сервера
	conn_CH_SBD=connect(host=conn_CH_SBD_secrets.host,port=conn_CH_SBD_secrets.port,user=conn_CH_SBD_secrets.login, password=conn_CH_SBD_secrets.password, connect_timeout=3600)
	cursor_CH_SBD = conn_CH_SBD.cursor()
	conn_AIS_secrets=Connection.get_connection_from_secrets(conn_id="AIS_NASELENIE")#скрытый коннекшн источника
	conn_AIS = mysql.connector.connect(database = conn_AIS_secrets.schema, user =conn_AIS_secrets.login, password =conn_AIS_secrets.password, host = conn_AIS_secrets.host, port = conn_AIS_secrets.port)
	cursor_AIS = conn_AIS.cursor()

	cursor_CH_SBD.execute("SELECT MAX(TIMESTAMP) AS MAX_TIMESTAMP FROM AKIMAT_ALMATY_PASPORT1.ZAYAV")
	max_date = cursor_CH_SBD.fetchone()[0]

	cursor_AIS.execute(f"""SELECT
 ID_DECLARATION AS "ID_DECLARATION"
, ID_POINT AS `ID_POINT`
, ID_APARTMENT AS `ID_APARTMENT`
, ID_TYPE_DECLARATION AS `ID_TYPE_DECLARATION`
, ID_BOSS AS `ID_BOSS`
, SIGN_TYPE_REG AS `SIGN_TYPE_REG`
, DATE_FORMAT(`DATE_DECLARATION`, '%d %m %Y %T %f') AS `DATE_DECLARATION`
, RESOLUTION AS `RESOLUTION`
, SIGN_REGISTRATION AS `SIGN_REGISTRATION`
, DATE_FORMAT(`DATE_RESOLUTION`, '%d %m %Y %T %f') AS `DATE_RESOLUTION`
, DATE_FORMAT(`DATE_REGISTRATION`, '%d %m %Y %T %f') AS `DATE_REGISTRATION`
, COMMENTS AS `COMMENTS`
, AMOUNT_LODGER_ALL AS `AMOUNT_LODGER_ALL`
, AMOUNT_LODGER_BIG AS `AMOUNT_LODGER_BIG`
, AMOUNT_VISA AS `AMOUNT_VISA`
, SIGN_ACTUAL AS `SIGN_ACTUAL`
, SIGN_DELETE AS `SIGN_DELETE`
, DATE_FORMAT(`TIMESTAMP`, '%d %m %Y %T %f') AS `TIMESTAMP`
, DATE_FORMAT(`DATE_COMMIT`, '%d %m %Y %T %f') AS `DATE_COMMIT`
, ID_POINT_COMMIT AS `ID_POINT_COMMIT`
, ID_OPER_COMMIT AS `ID_OPER_COMMIT`
FROM PASPORT1.`ZAYAV` where `TIMESTAMP` > '{max_date}'
""")

	flag = True
	while flag:
		req_AIS = cursor_AIS.fetchmany(50000)
		if len(req_AIS) == 0:
			flag=False
		cursor_CH_SBD.executemany("""INSERT INTO AKIMAT_ALMATY_PASPORT1.ZAYAV (ID_DECLARATION, ID_POINT, ID_APARTMENT, ID_TYPE_DECLARATION, ID_BOSS, SIGN_TYPE_REG, DATE_DECLARATION, RESOLUTION, SIGN_REGISTRATION, DATE_RESOLUTION, DATE_REGISTRATION, COMMENTS, AMOUNT_LODGER_ALL, AMOUNT_LODGER_BIG, AMOUNT_VISA, SIGN_ACTUAL, SIGN_DELETE, TIMESTAMP, DATE_COMMIT, ID_POINT_COMMIT, ID_OPER_COMMIT) VALUES""", req_AIS)

	cursor_CH_SBD.execute(f"SELECT COUNT(*) FROM AKIMAT_ALMATY_PASPORT1.ZAYAV")
	sbd_count_rows_value = cursor_CH_SBD.fetchone()#считаем кол-во данных в СДУ

	cursor_AIS.execute(f"SELECT COUNT(*) FROM PASPORT1.ZAYAV")
	origin_count_rows_value = cursor_AIS.fetchone()#считаем кол-во данных на источнике

	a = 0#если не совпадают
	if int(sbd_count_rows_value[0]) == int(origin_count_rows_value[0]):
		a = 1#если совпадают
	tuple_to_load = ("SELECT COUNT(*) FROM AKIMAT_ALMATY_PASPORT1.ZAYAV", "ZAYAV", "TRUNC", a,  origin_count_rows_value[0],sbd_count_rows_value[0])
	list_to_load = [(tuple_to_load)]

	cursor_CH_SBD.executemany("INSERT INTO AKIMAT_ALMATY_PASPORT1.CHECK_LOAD (SCHEMA, TABLE, TYPE_OF_ETL, CORRECTLY_LOADED,TOTAL_COUNT_SOURCE,TOTAL_COUNT_SDU) VALUES", list_to_load)#загружаем в таблицу для мониторинга

	conn_AIS.close()#закрываем коннекшны
	conn_CH_SBD.close()#закрываем коннекшны 

with DAG("ais_naselenie", description="Регламентная загрузка АИС Население акимата Алматы.", start_date=datetime(2022, 10, 31), schedule_interval="30 1 1 1-12 *", catchup=False) as dag:
	hash_name = Variable.get("hash_password")

	ADAM = PythonOperator(
		owner='Bakhtiyar',
 		task_id='ADAM',
		python_callable=ADAM,
	)
	CW_BUNCH = PythonOperator(
		owner='Bakhtiyar',
 		task_id='CW_BUNCH',
		python_callable=CW_BUNCH,
	)
	DOMA = PythonOperator(
		owner='Bakhtiyar',
 		task_id='DOMA',
		python_callable=DOMA,
	)
	PATER = PythonOperator(
		owner='Bakhtiyar',
 		task_id='PATER',
		python_callable=PATER,
	)
	PRIBYL = PythonOperator(
		owner='Bakhtiyar',
 		task_id='PRIBYL',
		python_callable=PRIBYL,
	)
	S_COMPANY = PythonOperator(
		owner='Bakhtiyar',
 		task_id='S_COMPANY',
		python_callable=S_COMPANY,
	)
	S_COUNTRIES = PythonOperator(
		owner='Bakhtiyar',
 		task_id='S_COUNTRIES',
		python_callable=S_COUNTRIES,
	)
	S_DOC = PythonOperator(
		owner='Bakhtiyar',
 		task_id='S_DOC',
		python_callable=S_DOC,
	)
	S_NAC = PythonOperator(
		owner='Bakhtiyar',
 		task_id='S_NAC',
		python_callable=S_NAC,
	)
	S_NSPNKT = PythonOperator(
		owner='Bakhtiyar',
 		task_id='S_NSPNKT',
		python_callable=S_NSPNKT,
	)
	S_OPERAC = PythonOperator(
		owner='Bakhtiyar',
 		task_id='S_OPERAC',
		python_callable=S_OPERAC,
	)
	S_RAY = PythonOperator(
		owner='Bakhtiyar',
 		task_id='S_RAY',
		python_callable=S_RAY,
	)
	S_SEX = PythonOperator(
		owner='Bakhtiyar',
 		task_id='S_SEX',
		python_callable=S_SEX,
	)
	S_STATES = PythonOperator(
		owner='Bakhtiyar',
 		task_id='S_STATES',
		python_callable=S_STATES,
	)
	S_STATUS = PythonOperator(
		owner='Bakhtiyar',
 		task_id='S_STATUS',
		python_callable=S_STATUS,
	)
	S_TYPE_APARTMENT = PythonOperator(
		owner='Bakhtiyar',
 		task_id='S_TYPE_APARTMENT',
		python_callable=S_TYPE_APARTMENT,
	)
	S_TYPE_DECLARATION = PythonOperator(
		owner='Bakhtiyar',
 		task_id='S_TYPE_DECLARATION',
		python_callable=S_TYPE_DECLARATION,
	)
	S_TYPE_REGISTRATION = PythonOperator(
		owner='Bakhtiyar',
 		task_id='S_TYPE_REGISTRATION',
		python_callable=S_TYPE_REGISTRATION,
	)
	S_ULI = PythonOperator(
		owner='Bakhtiyar',
 		task_id='S_ULI',
		python_callable=S_ULI,
	)
	UBYL = PythonOperator(
		owner='Bakhtiyar',
 		task_id='UBYL',
		python_callable=UBYL,
	)
	W_DOCUMENTS = PythonOperator(
		owner='Bakhtiyar',
 		task_id='W_DOCUMENTS',
		python_callable=W_DOCUMENTS,
	)
	W_LIVE = PythonOperator(
		owner='Bakhtiyar',
 		task_id='W_LIVE',
		python_callable=W_LIVE,
	)
	ZAYAV = PythonOperator(
		owner='Bakhtiyar',
 		task_id='ZAYAV',
		python_callable=ZAYAV,
	)
	
	ADAM>>CW_BUNCH>>DOMA>>PATER>>PRIBYL>>S_COMPANY>>S_COUNTRIES>>S_DOC>>S_NAC>>S_NSPNKT>>S_OPERAC>>S_RAY>>S_SEX>>S_STATES>>S_STATUS>>S_TYPE_APARTMENT>>S_TYPE_DECLARATION>>S_TYPE_REGISTRATION>>S_ULI>>UBYL>>W_DOCUMENTS>>W_LIVE>>ZAYAV