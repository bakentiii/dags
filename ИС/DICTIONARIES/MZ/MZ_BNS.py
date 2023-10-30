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

def get_5_connection():
	try:
		conn_5_secrets = Connection.get_connection_from_secrets(conn_id="Clickhouse-5")##скрытй коннекшн источника    
		return connect(host=conn_5_secrets.host, port=conn_5_secrets.port, password=conn_5_secrets.password, user=conn_5_secrets.login, connect_timeout=3600)
	except Exception as ex:
		print(f'Error connection of 5 server - {ex}')

def insert_values(data, schema, table):
	local_conn = get_17_connection()

	try:
		#sql = f"""INSERT INTO "{schema}".{table.lower()} VALUES({', '.join(['%s' for _ in range(n_columns)])});"""
		sql = f"""INSERT INTO BNS.{table} VALUES""" 

		with local_conn.cursor() as cursor:
			cursor.executemany(sql,data)
			print(f'--> Inserted {len(data)} rows to {schema}.{table}')
		local_conn.commit()
	finally:
		local_conn.close()

def truncate_table(table):
	local_conn = get_17_connection()

	try:
		sql = f"""TRUNCATE TABLE BNS.{table};"""
		with local_conn.cursor() as cursor:
			cursor.execute(sql)
		local_conn.commit()
		print(f'--> Truncated table {table} in BNS schema')
	finally:
		local_conn.close()
		print('Truncate exiting')

def extract_and_load_data(tagret_schema,target_table,source_table):

	conn_5 = get_5_connection()

	try:
		with conn_5.cursor() as cursor:
			
			data = cursor.execute(f"""SELECT * FROM {tagret_schema}.{source_table};""")

			truncate_table(target_table)

			while True:
				print('fetching')
				data = cursor.fetchmany(50000)
				print('fetched')
				print(f'--> {len(data)} rows fetched')

				if not len(data):
					print(f'--> break')
					break

				insert_values(data,tagret_schema,target_table)
	finally:
		conn_5.close()

def main(*args, **kwargs):
	

	extract_and_load_data(
		tagret_schema = kwargs['target_schema'],
		target_table = kwargs['target_table'],
		source_table = kwargs['source_table']

	)
        
with DAG("Transfer_dictionaries",default_args= {'owner': 'Bakhtiyar'}, description="Load table",start_date=datetime.datetime(2023, 5, 31),schedule_interval=None,catchup=False, tags = ['mz', 'bns']) as dag:

	TABLES = {
                'HABORTIONTYPE_ERSB' : { 'target_table' : 'RBZHFV_HABORTIONTYPE_ERSB','source_table' : 'MZ_REGISTERS_BASE'},
                'HADDRESSTYPE' : { 'target_table' : 'RBZHFV_HADDRESSTYPE','source_table' : 'MZ_REGISTERS_BASE'},
                'HADDRESSTYPE_ERSB' : { 'target_table' : 'RBZHFV_HADDRESSTYPE_ERSB','source_table' : 'MZ_REGISTERS_BASE'},
                'HADMISSIONTOTHISHOSPITAL_ERSB' : { 'target_table' : 'RBZHFV_HADMISSIONTOTHISHOSPITAL_ERSB','source_table' : 'MZ_REGISTERS_BASE'},
                'HANAESTHESIA_ERSB' : { 'target_table' : 'RBZHFV_HANAESTHESIA_ERSB','source_table' : 'MZ_REGISTERS_BASE'},
                'HBDEATHTYPE_ERSB' : { 'target_table' : 'RBZHFV_HBDEATHTYPE_ERSB','source_table' : 'MZ_REGISTERS_BASE'},
                'HBEDPROFILE_ERSB' : { 'target_table' : 'RBZHFV_HBEDPROFILE_ERSB','source_table' : 'MZ_REGISTERS_BASE'},
                'HBIOSEX_ERSB' : { 'target_table' : 'RBZHFV_HBIOSEX_ERSB','source_table' : 'MZ_REGISTERS_BASE'},
                'HBLOODGROUPS_ERSB' : { 'target_table' : 'RBZHFV_HBLOODGROUPS_ERSB','source_table' : 'MZ_REGISTERS_BASE'},
                'HCANCERSTAGE_ERSB' : { 'target_table' : 'RBZHFV_HCANCERSTAGE_ERSB','source_table' : 'MZ_REGISTERS_BASE'},
                'HCHILDDEATHTYPES_ERSB' : { 'target_table' : 'RBZHFV_HCHILDDEATHTYPES_ERSB','source_table' : 'MZ_REGISTERS_BASE'},
                'HCOMPLICATIONRT_ERSB' : { 'target_table' : 'RBZHFV_HCOMPLICATIONRT_ERSB','source_table' : 'MZ_REGISTERS_BASE'},
                'HCOMPLICATION_ERSB' : { 'target_table' : 'RBZHFV_HCOMPLICATION_ERSB','source_table' : 'MZ_REGISTERS_BASE'},
                'HDAYHOSPITALCARDTYPES_ERSB' : { 'target_table' : 'RBZHFV_HDAYHOSPITALCARDTYPES_ERSB','source_table' : 'MZ_REGISTERS_BASE'},
                'HDELIVERYTYPE_ERSB' : { 'target_table' : 'RBZHFV_HDELIVERYTYPE_ERSB','source_table' : 'MZ_REGISTERS_BASE'},
                'HDIAGNOSISTYPE_ERSB' : { 'target_table' : 'RBZHFV_HDIAGNOSISTYPE_ERSB','source_table' : 'MZ_REGISTERS_BASE'},
                'HDIAGTYPE_ERSB' : { 'target_table' : 'RBZHFV_HDIAGTYPE_ERSB','source_table' : 'MZ_REGISTERS_BASE'},
                'HDISABLEDDISCHARGE_ERSB' : { 'target_table' : 'RBZHFV_HDISABLEDDISCHARGE_ERSB','source_table' : 'MZ_REGISTERS_BASE'},
                'HDOCTYPE_ERSB' : { 'target_table' : 'RBZHFV_HDOCTYPE_ERSB','source_table' : 'MZ_REGISTERS_BASE'},
                'HDRGWEIGHTTYPES_ERSB' : { 'target_table' : 'RBZHFV_HDRGWEIGHTTYPES_ERSB','source_table' : 'MZ_REGISTERS_BASE'},
                'HDRG_ERSB' : { 'target_table' : 'RBZHFV_HDRG_ERSB','source_table' : 'MZ_REGISTERS_BASE'},
                'HDRUGSUSEDPLACES_ERSB' : { 'target_table' : 'RBZHFV_HDRUGSUSEDPLACES_ERSB','source_table' : 'MZ_REGISTERS_BASE'},
                'HEDUCATION_ERSB' : { 'target_table' : 'RBZHFV_HEDUCATION_ERSB','source_table' : 'MZ_REGISTERS_BASE'},
                'HELIMINATEDFROMSPECIALIZEDHOSPITAL_ERSB' : { 'target_table' : 'RBZHFV_HELIMINATEDFROMSPECIALIZEDHOSPITAL_ERSB','source_table' : 'MZ_REGISTERS_BASE'},
                'HEQUIPMENT_ERSB' : { 'target_table' : 'RBZHFV_HEQUIPMENT_ERSB','source_table' : 'MZ_REGISTERS_BASE'},
                'HEXTRENTYPE_ERSB' : { 'target_table' : 'RBZHFV_HEXTRENTYPE_ERSB','source_table' : 'MZ_REGISTERS_BASE'},
                'HFORTHERAPEUTICPATIENTS_ERSB' : { 'target_table' : 'RBZHFV_HFORTHERAPEUTICPATIENTS_ERSB','source_table' : 'MZ_REGISTERS_BASE'},
                'HFREQUENCYUSENARKO_ERSB' : { 'target_table' : 'RBZHFV_HFREQUENCYUSENARKO_ERSB','source_table' : 'MZ_REGISTERS_BASE'},
                'HHOSPITALIZED_ERSB' : { 'target_table' : 'RBZHFV_HHOSPITALIZED_ERSB','source_table' : 'MZ_REGISTERS_BASE'},
                'HHOSPITALSTAYRESULT_ERSB' : { 'target_table' : 'RBZHFV_HHOSPITALSTAYRESULT_ERSB','source_table' : 'MZ_REGISTERS_BASE'},
                'HINCASEOFDEATH_ERSB' : { 'target_table' : 'RBZHFV_HINCASEOFDEATH_ERSB','source_table' : 'MZ_REGISTERS_BASE'},
                'HINPATIENTHELPTYPES_ERSB' : { 'target_table' : 'RBZHFV_HINPATIENTHELPTYPES_ERSB','source_table' : 'MZ_REGISTERS_BASE'},
                'HINVALIDITYDISCHARGE_ERSB' : { 'target_table' : 'RBZHFV_HINVALIDITYDISCHARGE_ERSB','source_table' : 'MZ_REGISTERS_BASE'},
                'HLOCALIZATION_ERSB' : { 'target_table' : 'RBZHFV_HLOCALIZATION_ERSB','source_table' : 'MZ_REGISTERS_BASE'},
                'HLOCATION_ERSB' : { 'target_table' : 'RBZHFV_HLOCATION_ERSB','source_table' : 'MZ_REGISTERS_BASE'},
                'HMARRIEDTYPE' : { 'target_table' : 'RBZHFV_HMARRIEDTYPE','source_table' : 'MZ_REGISTERS_BASE'},
                'HMEPDRUGS_ERSB' : { 'target_table' : 'RBZHFV_HMEPDRUGS_ERSB','source_table' : 'MZ_REGISTERS_BASE'},
                'HMEPSSERV_ERSB' : { 'target_table' : 'RBZHFV_HMEPSSERV_ERSB','source_table' : 'MZ_REGISTERS_BASE'},
                'HMEPSSURGPROC_ERSB' : { 'target_table' : 'RBZHFV_HMEPSSURGPROC_ERSB','source_table' : 'MZ_REGISTERS_BASE'},
                'HMETASTASISLOCALIZATION_ERSB' : { 'target_table' : 'RBZHFV_HMETASTASISLOCALIZATION_ERSB','source_table' : 'MZ_REGISTERS_BASE'},
                'HMETHODRT_ERSB' : { 'target_table' : 'RBZHFV_HMETHODRT_ERSB','source_table' : 'MZ_REGISTERS_BASE'},
                'HMETHODUSINGNARKO_ERSB' : { 'target_table' : 'RBZHFV_HMETHODUSINGNARKO_ERSB','source_table' : 'MZ_REGISTERS_BASE'},
                'HMORPHOLOGY_ERSB' : { 'target_table' : 'RBZHFV_HMORPHOLOGY_ERSB','source_table' : 'MZ_REGISTERS_BASE'},
                'HMULTIPLICITIES_ERSB' : { 'target_table' : 'RBZHFV_HMULTIPLICITIES_ERSB','source_table' : 'MZ_REGISTERS_BASE'},
                'HNATIONALITY' : { 'target_table' : 'RBZHFV_HNATIONALITY','source_table' : 'MZ_REGISTERS_BASE'},
                'HNATIONALITY_ERSB' : { 'target_table' : 'RBZHFV_HNATIONALITY_ERSB','source_table' : 'MZ_REGISTERS_BASE'},
                'HNATUREHELDTREAT_ERSB' : { 'target_table' : 'RBZHFV_HNATUREHELDTREAT_ERSB','source_table' : 'MZ_REGISTERS_BASE'},
                'HNEWBORNOUTCOMES_ERSB' : { 'target_table' : 'RBZHFV_HNEWBORNOUTCOMES_ERSB','source_table' : 'MZ_REGISTERS_BASE'},
                'HOTHERTYPESPECTREAT_ERSB' : { 'target_table' : 'RBZHFV_HOTHERTYPESPECTREAT_ERSB','source_table' : 'MZ_REGISTERS_BASE'},
                'HPERIODCONCOMITANTDISEASES_ERSB' : { 'target_table' : 'RBZHFV_HPERIODCONCOMITANTDISEASES_ERSB','source_table' : 'MZ_REGISTERS_BASE'},
                'HPURPOSERESEARCH_ERSB' : { 'target_table' : 'RBZHFV_HPURPOSERESEARCH_ERSB','source_table' : 'MZ_REGISTERS_BASE'},
                'HRADIOMODIFIER_ERSB' : { 'target_table' : 'RBZHFV_HRADIOMODIFIER_ERSB','source_table' : 'MZ_REGISTERS_BASE'},
                'HREASONPARTTREAT_ERSB' : { 'target_table' : 'RBZHFV_HREASONPARTTREAT_ERSB','source_table' : 'MZ_REGISTERS_BASE'},
                'HRECOMMENDATION_ERSB' : { 'target_table' : 'RBZHFV_HRECOMMENDATION_ERSB','source_table' : 'MZ_REGISTERS_BASE'},
                'HREHABILITATIONTYPES_ERSB' : { 'target_table' : 'RBZHFV_HREHABILITATIONTYPES_ERSB','source_table' : 'MZ_REGISTERS_BASE'},
                'HREZISTENNOST_ERSB' : { 'target_table' : 'RBZHFV_HREZISTENNOST_ERSB','source_table' : 'MZ_REGISTERS_BASE'},
                'HRHFACTORS_ERSB' : { 'target_table' : 'RBZHFV_HRHFACTORS_ERSB','source_table' : 'MZ_REGISTERS_BASE'},
                'HRISKGROUP_ERSB' : { 'target_table' : 'RBZHFV_HRISKGROUP_ERSB','source_table' : 'MZ_REGISTERS_BASE'},
                'HRT_ERSB' : { 'target_table' : 'RBZHFV_HRT_ERSB','source_table' : 'MZ_REGISTERS_BASE'},
                'HSICK_ERSB' : { 'target_table' : 'RBZHFV_HSICK_ERSB','source_table' : 'MZ_REGISTERS_BASE'},
                'HSIDEEFFECTSCT_ERSB' : { 'target_table' : 'RBZHFV_HSIDEEFFECTSCT_ERSB','source_table' : 'MZ_REGISTERS_BASE'},
                'HSIDEEFFECTSHRT_ERSB' : { 'target_table' : 'RBZHFV_HSIDEEFFECTSHRT_ERSB','source_table' : 'MZ_REGISTERS_BASE'},
                'HSIDEEFFECTSIT_ERSB' : { 'target_table' : 'RBZHFV_HSIDEEFFECTSIT_ERSB','source_table' : 'MZ_REGISTERS_BASE'},
                'HSIDEEFFECTSTIBR_ERSB' : { 'target_table' : 'RBZHFV_HSIDEEFFECTSTIBR_ERSB','source_table' : 'MZ_REGISTERS_BASE'},
                'HSIDEEFFECTSTT_ERSB' : { 'target_table' : 'RBZHFV_HSIDEEFFECTSTT_ERSB','source_table' : 'MZ_REGISTERS_BASE'},
                'HSOCTYPE' : { 'target_table' : 'RBZHFV_HSOCTYPE','source_table' : 'MZ_REGISTERS_BASE'},
                'HSOCTYPE_ERSB' : { 'target_table' : 'RBZHFV_HSOCTYPE_ERSB','source_table' : 'MZ_REGISTERS_BASE'},
                'HSOURCEOFLIVELIHOOD_ERSB' : { 'target_table' : 'RBZHFV_HSOURCEOFLIVELIHOOD_ERSB','source_table' : 'MZ_REGISTERS_BASE'},
                'HSTAGESCT_ERSB' : { 'target_table' : 'RBZHFV_HSTAGESCT_ERSB','source_table' : 'MZ_REGISTERS_BASE'},
                'HSTATE' : { 'target_table' : 'RBZHFV_HSTATE','source_table' : 'MZ_REGISTERS_BASE'},
                'HTERRITORYUNITTYPE_ERSB' : { 'target_table' : 'RBZHFV_HTERRITORYUNITTYPE_ERSB','source_table' : 'MZ_REGISTERS_BASE'},
                'HTHERAPYRESULT_ERSB' : { 'target_table' : 'RBZHFV_HTHERAPYRESULT_ERSB','source_table' : 'MZ_REGISTERS_BASE'},
                'HTOTALSIZETREAT_ERSB' : { 'target_table' : 'RBZHFV_HTOTALSIZETREAT_ERSB','source_table' : 'MZ_REGISTERS_BASE'},
                'HTRAUMATYPE_ERSB' : { 'target_table' : 'RBZHFV_HTRAUMATYPE_ERSB','source_table' : 'MZ_REGISTERS_BASE'},
                'HTYPECASE_ERSB' : { 'target_table' : 'RBZHFV_HTYPECASE_ERSB','source_table' : 'MZ_REGISTERS_BASE'},
                'HTYPECT_ERSB' : { 'target_table' : 'RBZHFV_HTYPECT_ERSB','source_table' : 'MZ_REGISTERS_BASE'},
                'HTYPEDRUGSNARKO_ERSB' : { 'target_table' : 'RBZHFV_HTYPEDRUGSNARKO_ERSB','source_table' : 'MZ_REGISTERS_BASE'},
                'HTYPEHRT_ERSB' : { 'target_table' : 'RBZHFV_HTYPEHRT_ERSB','source_table' : 'MZ_REGISTERS_BASE'},
                'HTYPERT_ERSB' : { 'target_table' : 'RBZHFV_HTYPERT_ERSB','source_table' : 'MZ_REGISTERS_BASE'},
                'HTYPETREAT_ERSB' : { 'target_table' : 'RBZHFV_HTYPETREAT_ERSB','source_table' : 'MZ_REGISTERS_BASE'},
                'HTYPSRCFIN_ERSB' : { 'target_table' : 'RBZHFV_HTYPSRCFIN_ERSB','source_table' : 'MZ_REGISTERS_BASE'},
                'HVARIANTDIAG_ERSB' : { 'target_table' : 'RBZHFV_HVARIANTDIAG_ERSB','source_table' : 'MZ_REGISTERS_BASE'},
                'HWAYRT_ERSB' : { 'target_table' : 'RBZHFV_HWAYRT_ERSB','source_table' : 'MZ_REGISTERS_BASE'},
                'HWHERERECEIVED_ERSB' : { 'target_table' : 'RBZHFV_HWHERERECEIVED_ERSB','source_table' : 'MZ_REGISTERS_BASE'},
                'SPMKB' : { 'target_table' : 'RBZHFV_SPMKB','source_table' : 'MZ_REGISTERS_BASE'},
                'SPMKB_PROTIV' : { 'target_table' : 'RBZHFV_SPMKB_PROTIV','source_table' : 'MZ_REGISTERS_BASE'},
                'SPMKB_PROTIV_LINEAR' : { 'target_table' : 'RBZHFV_SPMKB_PROTIV_LINEAR','source_table' : 'MZ_REGISTERS_BASE'},
                'SP_ADR' : { 'target_table' : 'RBZHFV_SP_ADR','source_table' : 'MZ_REGISTERS_BASE'},
                'SP_APP' : { 'target_table' : 'RBZHFV_SP_APP','source_table' : 'MZ_REGISTERS_BASE'},
                'SP_AUDIT' : { 'target_table' : 'RBZHFV_SP_AUDIT','source_table' : 'MZ_REGISTERS_BASE'},
                'SP_BEREM' : { 'target_table' : 'RBZHFV_SP_BEREM','source_table' : 'MZ_REGISTERS_BASE'},
                'SP_BEREM_BER' : { 'target_table' : 'RBZHFV_SP_BEREM_BER','source_table' : 'MZ_REGISTERS_BASE'},
                'SP_BIOPS' : { 'target_table' : 'RBZHFV_SP_BIOPS','source_table' : 'MZ_REGISTERS_BASE'},
                'SP_BIOPS_PR' : { 'target_table' : 'RBZHFV_SP_BIOPS_PR','source_table' : 'MZ_REGISTERS_BASE'},
                'SP_BLOOD_GROUP' : { 'target_table' : 'RBZHFV_SP_BLOOD_GROUP','source_table' : 'MZ_REGISTERS_BASE'},
                'SP_CAUSE_DEATH' : { 'target_table' : 'RBZHFV_SP_CAUSE_DEATH','source_table' : 'MZ_REGISTERS_BASE'},
                'SP_CILINDR_OAM' : { 'target_table' : 'RBZHFV_SP_CILINDR_OAM','source_table' : 'MZ_REGISTERS_BASE'},
                'SP_CITMAZ' : { 'target_table' : 'RBZHFV_SP_CITMAZ','source_table' : 'MZ_REGISTERS_BASE'},
                'SP_COLOR_OAM' : { 'target_table' : 'RBZHFV_SP_COLOR_OAM','source_table' : 'MZ_REGISTERS_BASE'},
                'SP_CONTRACEPTION' : { 'target_table' : 'RBZHFV_SP_CONTRACEPTION','source_table' : 'MZ_REGISTERS_BASE'},
                'SP_DGROUP' : { 'target_table' : 'RBZHFV_SP_DGROUP','source_table' : 'MZ_REGISTERS_BASE'},
                'SP_DIAG_ADDITION' : { 'target_table' : 'RBZHFV_SP_DIAG_ADDITION','source_table' : 'MZ_REGISTERS_BASE'},
                'SP_DIAG_PROPERTY' : { 'target_table' : 'RBZHFV_SP_DIAG_PROPERTY','source_table' : 'MZ_REGISTERS_BASE'},
                'SP_DISP_GROUP_WATCH' : { 'target_table' : 'RBZHFV_SP_DISP_GROUP_WATCH','source_table' : 'MZ_REGISTERS_BASE'},
                'SP_DOS' : { 'target_table' : 'RBZHFV_SP_DOS','source_table' : 'MZ_REGISTERS_BASE'},
                'SP_EDUCATION' : { 'target_table' : 'RBZHFV_SP_EDUCATION','source_table' : 'MZ_REGISTERS_BASE'},
                'SP_ERR' : { 'target_table' : 'RBZHFV_SP_ERR','source_table' : 'MZ_REGISTERS_BASE'},
                'SP_ESOFAG' : { 'target_table' : 'RBZHFV_SP_ESOFAG','source_table' : 'MZ_REGISTERS_BASE'},
                'SP_FIN' : { 'target_table' : 'RBZHFV_SP_FIN','source_table' : 'MZ_REGISTERS_BASE'},
                'SP_GASTRO' : { 'target_table' : 'RBZHFV_SP_GASTRO','source_table' : 'MZ_REGISTERS_BASE'},
                'SP_GLUKOZA' : { 'target_table' : 'RBZHFV_SP_GLUKOZA','source_table' : 'MZ_REGISTERS_BASE'},
                'SP_GR_DU' : { 'target_table' : 'RBZHFV_SP_GR_DU','source_table' : 'MZ_REGISTERS_BASE'},
                'SP_HEALTHGROUP' : { 'target_table' : 'RBZHFV_SP_HEALTHGROUP','source_table' : 'MZ_REGISTERS_BASE'},
                'SP_INV' : { 'target_table' : 'RBZHFV_SP_INV','source_table' : 'MZ_REGISTERS_BASE'},
                'SP_ISHOD' : { 'target_table' : 'RBZHFV_SP_ISHOD','source_table' : 'MZ_REGISTERS_BASE'},
                'SP_ISHOD_BER' : { 'target_table' : 'RBZHFV_SP_ISHOD_BER','source_table' : 'MZ_REGISTERS_BASE'},
                'SP_ISHOD_BEREM' : { 'target_table' : 'RBZHFV_SP_ISHOD_BEREM','source_table' : 'MZ_REGISTERS_BASE'},
                'SP_ISHOD_REB' : { 'target_table' : 'RBZHFV_SP_ISHOD_REB','source_table' : 'MZ_REGISTERS_BASE'},
                'SP_ISHOD_SCREEN' : { 'target_table' : 'RBZHFV_SP_ISHOD_SCREEN','source_table' : 'MZ_REGISTERS_BASE'},
                'SP_KATEG_DIAG' : { 'target_table' : 'RBZHFV_SP_KATEG_DIAG','source_table' : 'MZ_REGISTERS_BASE'},
                'SP_KOLON' : { 'target_table' : 'RBZHFV_SP_KOLON','source_table' : 'MZ_REGISTERS_BASE'},
                'SP_KONC' : { 'target_table' : 'RBZHFV_SP_KONC','source_table' : 'MZ_REGISTERS_BASE'},
                'SP_LS' : { 'target_table' : 'RBZHFV_SP_LS','source_table' : 'MZ_REGISTERS_BASE'},
                'SP_MAMM' : { 'target_table' : 'RBZHFV_SP_MAMM','source_table' : 'MZ_REGISTERS_BASE'},
                'SP_MANUFACTURERS' : { 'target_table' : 'RBZHFV_SP_MANUFACTURERS','source_table' : 'MZ_REGISTERS_BASE'},
                'SP_NAPR_BER' : { 'target_table' : 'RBZHFV_SP_NAPR_BER','source_table' : 'MZ_REGISTERS_BASE'},
                'SP_OBL' : { 'target_table' : 'RBZHFV_SP_OBL','source_table' : 'MZ_REGISTERS_BASE'},
                'SP_OTD' : { 'target_table' : 'RBZHFV_SP_OTD','source_table' : 'MZ_REGISTERS_BASE'},
                'SP_PIVO' : { 'target_table' : 'RBZHFV_SP_PIVO','source_table' : 'MZ_REGISTERS_BASE'},
                'SP_POS' : { 'target_table' : 'RBZHFV_SP_POS','source_table' : 'MZ_REGISTERS_BASE'},
                'SP_PREP' : { 'target_table' : 'RBZHFV_SP_PREP','source_table' : 'MZ_REGISTERS_BASE'},
                'SP_PRICH_DEATH' : { 'target_table' : 'RBZHFV_SP_PRICH_DEATH','source_table' : 'MZ_REGISTERS_BASE'},
                'SP_PRICH_END' : { 'target_table' : 'RBZHFV_SP_PRICH_END','source_table' : 'MZ_REGISTERS_BASE'},
                'SP_PRICH_ERSB' : { 'target_table' : 'RBZHFV_SP_PRICH_ERSB','source_table' : 'MZ_REGISTERS_BASE'},
                'SP_PRICH_GEMOTRANS' : { 'target_table' : 'RBZHFV_SP_PRICH_GEMOTRANS','source_table' : 'MZ_REGISTERS_BASE'},
                'SP_PRISK' : { 'target_table' : 'RBZHFV_SP_PRISK','source_table' : 'MZ_REGISTERS_BASE'},
                'SP_PR_SMERTI' : { 'target_table' : 'RBZHFV_SP_PR_SMERTI','source_table' : 'MZ_REGISTERS_BASE'},
                'SP_PSA' : { 'target_table' : 'RBZHFV_SP_PSA','source_table' : 'MZ_REGISTERS_BASE'},
                'SP_REGION' : { 'target_table' : 'RBZHFV_SP_REGION','source_table' : 'MZ_REGISTERS_BASE'},
                'SP_REZ_OSM' : { 'target_table' : 'RBZHFV_SP_REZ_OSM','source_table' : 'MZ_REGISTERS_BASE'},
                'SP_RISK' : { 'target_table' : 'RBZHFV_SP_RISK','source_table' : 'MZ_REGISTERS_BASE'},
                'SP_SIMP_ALLERG' : { 'target_table' : 'RBZHFV_SP_SIMP_ALLERG','source_table' : 'MZ_REGISTERS_BASE'},
                'SP_SNAT_BER' : { 'target_table' : 'RBZHFV_SP_SNAT_BER','source_table' : 'MZ_REGISTERS_BASE'},
                'SP_SOST' : { 'target_table' : 'RBZHFV_SP_SOST','source_table' : 'MZ_REGISTERS_BASE'},
                'SP_SOST_ZD' : { 'target_table' : 'RBZHFV_SP_SOST_ZD','source_table' : 'MZ_REGISTERS_BASE'},
                'SP_SPEC' : { 'target_table' : 'RBZHFV_SP_SPEC','source_table' : 'MZ_REGISTERS_BASE'},
                'SP_STEP_ROD' : { 'target_table' : 'RBZHFV_SP_STEP_ROD','source_table' : 'MZ_REGISTERS_BASE'},
                'SP_TARGET_GROUP' : { 'target_table' : 'RBZHFV_SP_TARGET_GROUP','source_table' : 'MZ_REGISTERS_BASE'},
                'SP_TIPDI' : { 'target_table' : 'RBZHFV_SP_TIPDI','source_table' : 'MZ_REGISTERS_BASE'},
                'SP_TIP_MEROP' : { 'target_table' : 'RBZHFV_SP_TIP_MEROP','source_table' : 'MZ_REGISTERS_BASE'},
                'SP_TIP_OBNAR' : { 'target_table' : 'RBZHFV_SP_TIP_OBNAR','source_table' : 'MZ_REGISTERS_BASE'},
                'SP_UCH' : { 'target_table' : 'RBZHFV_SP_UCH','source_table' : 'MZ_REGISTERS_BASE'},
                'SP_UROVEN' : { 'target_table' : 'RBZHFV_SP_UROVEN','source_table' : 'MZ_REGISTERS_BASE'},
                'SP_VIDPOS' : { 'target_table' : 'RBZHFV_SP_VIDPOS','source_table' : 'MZ_REGISTERS_BASE'},
                'SP_VID_ALLERG' : { 'target_table' : 'RBZHFV_SP_VID_ALLERG','source_table' : 'MZ_REGISTERS_BASE'},
                'SP_VINO' : { 'target_table' : 'RBZHFV_SP_VINO','source_table' : 'MZ_REGISTERS_BASE'},
                'SP_VODKA' : { 'target_table' : 'RBZHFV_SP_VODKA','source_table' : 'MZ_REGISTERS_BASE'},
                'SP_VZIAT' : { 'target_table' : 'RBZHFV_SP_VZIAT','source_table' : 'MZ_REGISTERS_BASE'},
                'DICBENEFITTYPES' : { 'target_table' : 'RPN_DICBENEFITTYPES','source_table' : 'MZ_RPN'},
                'DICCITIZENSHIPS' : { 'target_table' : 'RPN_DICCITIZENSHIPS','source_table' : 'MZ_RPN'},
                'DICNATIONALITIES' : { 'target_table' : 'RPN_DICNATIONALITIES','source_table' : 'MZ_RPN'},
                'DICRELATIONDEGREES' : { 'target_table' : 'RPN_DICRELATIONDEGREES','source_table' : 'MZ_RPN'},
                'DICSEX' : { 'target_table' : 'RPN_DICSEX','source_table' : 'MZ_RPN'},
                'HDEATHREGTYPECACHE' : { 'target_table' : 'RPN_HDEATHREGTYPECACHE','source_table' : 'MZ_RPN'},
                'HDEATHTYPECACHE' : { 'target_table' : 'RPN_HDEATHTYPECACHE','source_table' : 'MZ_RPN'},
                'HLOCATIONCACHE' : { 'target_table' : 'RPN_HLOCATIONCACHE','source_table' : 'MZ_RPN'},
                'HREASDREGTYPECACHE' : { 'target_table' : 'RPN_HREASDREGTYPECACHE','source_table' : 'MZ_RPN'},
                'HSICKCACHE' : { 'target_table' : 'RPN_HSICKCACHE','source_table' : 'MZ_RPN'},
                'HVIDDEATHCACHE' : { 'target_table' : 'RPN_HVIDDEATHCACHE','source_table' : 'MZ_RPN'},
                'ORGHEALTHCARECACHE' : { 'target_table' : 'RPN_ORGHEALTHCARECACHE','source_table' : 'MZ_RPN'},
                'ORGHEALTHCARECHANGEHISTORY' : { 'target_table' : 'RPN_ORGHEALTHCARECHANGEHISTORY','source_table' : 'MZ_RPN'},
                'ORGHEALTHCARECODESCACHE' : { 'target_table' : 'RPN_ORGHEALTHCARECODESCACHE','source_table' : 'MZ_RPN'},
                'ORGHEALTHCAREIDENTIFIERS' : { 'target_table' : 'RPN_ORGHEALTHCAREIDENTIFIERS','source_table' : 'MZ_RPN'},
                'ORGHEALTHCARESTAFFINFO' : { 'target_table' : 'RPN_ORGHEALTHCARESTAFFINFO','source_table' : 'MZ_RPN'},
                'D_COUNTRY' : { 'target_table' : 'RPN_CORPSES_D_COUNTRY','source_table' : 'MZ_RPN_CORPSES'},
                'D_GENDER' : { 'target_table' : 'RPN_CORPSES_D_GENDER','source_table' : 'MZ_RPN_CORPSES'},
                'D_RELATION_DEGREE' : { 'target_table' : 'RPN_CORPSES_D_RELATION_DEGREE','source_table' : 'MZ_RPN_CORPSES'},
                'SP_CERT' : { 'target_table' : 'SUMT_SP_CERT','source_table' : 'MZ_SUMT'},
                'SP_COMPLECT' : { 'target_table' : 'SUMT_SP_COMPLECT','source_table' : 'MZ_SUMT'},
                'SP_COMPLECTS_SULO' : { 'target_table' : 'SUMT_SP_COMPLECTS_SULO','source_table' : 'MZ_SUMT'},
                'SP_COUNTRY' : { 'target_table' : 'SUMT_SP_COUNTRY','source_table' : 'MZ_SUMT'},
                'SP_DICTIONARY' : { 'target_table' : 'SUMT_SP_DICTIONARY','source_table' : 'MZ_SUMT'},
                'SP_DOC_TYPE' : { 'target_table' : 'SUMT_SP_DOC_TYPE','source_table' : 'MZ_SUMT'},
                'SP_ED_IZM' : { 'target_table' : 'SUMT_SP_ED_IZM','source_table' : 'MZ_SUMT'},
                'SP_ED_IZM_EFF' : { 'target_table' : 'SUMT_SP_ED_IZM_EFF','source_table' : 'MZ_SUMT'},
                'SP_ELECTRO' : { 'target_table' : 'SUMT_SP_ELECTRO','source_table' : 'MZ_SUMT'},
                'SP_FINANS' : { 'target_table' : 'SUMT_SP_FINANS','source_table' : 'MZ_SUMT'},
                'SP_FINANSIROVANIE' : { 'target_table' : 'SUMT_SP_FINANSIROVANIE','source_table' : 'MZ_SUMT'},
                'SP_FINANSIROVANIE_PROG' : { 'target_table' : 'SUMT_SP_FINANSIROVANIE_PROG','source_table' : 'MZ_SUMT'},
                'SP_FSOBST' : { 'target_table' : 'SUMT_SP_FSOBST','source_table' : 'MZ_SUMT'},
                'SP_FUNC_PODR' : { 'target_table' : 'SUMT_SP_FUNC_PODR','source_table' : 'MZ_SUMT'},
                'SP_KIND_EFFICIENCY' : { 'target_table' : 'SUMT_SP_KIND_EFFICIENCY','source_table' : 'MZ_SUMT'},
                'SP_LOCALITY_STATUS' : { 'target_table' : 'SUMT_SP_LOCALITY_STATUS','source_table' : 'MZ_SUMT'},
                'SP_MEDHELPTYPE' : { 'target_table' : 'SUMT_SP_MEDHELPTYPE','source_table' : 'MZ_SUMT'},
                'SP_MED_POMOSH' : { 'target_table' : 'SUMT_SP_MED_POMOSH','source_table' : 'MZ_SUMT'},
                'SP_MESTO_DISLOKACII' : { 'target_table' : 'SUMT_SP_MESTO_DISLOKACII','source_table' : 'MZ_SUMT'},
                'SP_MT_TYPE' : { 'target_table' : 'SUMT_SP_MT_TYPE','source_table' : 'MZ_SUMT'},
                'SP_NOMENKL' : { 'target_table' : 'SUMT_SP_NOMENKL','source_table' : 'MZ_SUMT'},
                'SP_NORMATIV' : { 'target_table' : 'SUMT_SP_NORMATIV','source_table' : 'MZ_SUMT'},
                'SP_NORMATIVE_TECHNIC' : { 'target_table' : 'SUMT_SP_NORMATIVE_TECHNIC','source_table' : 'MZ_SUMT'},
                'SP_NORMATIVE_TECHNIC_2021_05_06' : { 'target_table' : 'SUMT_SP_NORMATIVE_TECHNIC_2021_05_06','source_table' : 'MZ_SUMT'},
                'SP_OBL' : { 'target_table' : 'SUMT_SP_OBL','source_table' : 'MZ_SUMT'},
                'SP_OKPOLU_CABINET' : { 'target_table' : 'SUMT_SP_OKPOLU_CABINET','source_table' : 'MZ_SUMT'},
                'SP_PARAM_POMESH' : { 'target_table' : 'SUMT_SP_PARAM_POMESH','source_table' : 'MZ_SUMT'},
                'SP_PERIOD_OBSLUJ' : { 'target_table' : 'SUMT_SP_PERIOD_OBSLUJ','source_table' : 'MZ_SUMT'},
                'SP_PLOSHAD' : { 'target_table' : 'SUMT_SP_PLOSHAD','source_table' : 'MZ_SUMT'},
                'SP_POSTAVSHIK_MT' : { 'target_table' : 'SUMT_SP_POSTAVSHIK_MT','source_table' : 'MZ_SUMT'},
                'SP_PRAVO_VLADEN' : { 'target_table' : 'SUMT_SP_PRAVO_VLADEN','source_table' : 'MZ_SUMT'},
                'SP_PRICH_PRIOBR' : { 'target_table' : 'SUMT_SP_PRICH_PRIOBR','source_table' : 'MZ_SUMT'},
                'SP_PRICH_SNYAT_BALANS' : { 'target_table' : 'SUMT_SP_PRICH_SNYAT_BALANS','source_table' : 'MZ_SUMT'},
                'SP_PROFIL' : { 'target_table' : 'SUMT_SP_PROFIL','source_table' : 'MZ_SUMT'},
                'SP_PROFIL167' : { 'target_table' : 'SUMT_SP_PROFIL167','source_table' : 'MZ_SUMT'},
                'SP_PROGRAMM_OBESPECH' : { 'target_table' : 'SUMT_SP_PROGRAMM_OBESPECH','source_table' : 'MZ_SUMT'},
                'SP_REGION' : { 'target_table' : 'SUMT_SP_REGION','source_table' : 'MZ_SUMT'},
                'SP_SHTAT_SPEC' : { 'target_table' : 'SUMT_SP_SHTAT_SPEC','source_table' : 'MZ_SUMT'},
                'SP_SOST' : { 'target_table' : 'SUMT_SP_SOST','source_table' : 'MZ_SUMT'},
                'SP_SPOSOB_USLOVIA_POSTAVKI' : { 'target_table' : 'SUMT_SP_SPOSOB_USLOVIA_POSTAVKI','source_table' : 'MZ_SUMT'},
                'SP_STATUS' : { 'target_table' : 'SUMT_SP_STATUS','source_table' : 'MZ_SUMT'},
                'SP_STR_STAND' : { 'target_table' : 'SUMT_SP_STR_STAND','source_table' : 'MZ_SUMT'},
                'SP_TARIF' : { 'target_table' : 'SUMT_SP_TARIF','source_table' : 'MZ_SUMT'},
                'SP_TECHNICS' : { 'target_table' : 'SUMT_SP_TECHNICS','source_table' : 'MZ_SUMT'},
                'SP_TECHNICS_167' : { 'target_table' : 'SUMT_SP_TECHNICS_167','source_table' : 'MZ_SUMT'},
                'SP_TECHNICS_GROUP' : { 'target_table' : 'SUMT_SP_TECHNICS_GROUP','source_table' : 'MZ_SUMT'},
                'SP_TECHNICS_NMIRK' : { 'target_table' : 'SUMT_SP_TECHNICS_NMIRK','source_table' : 'MZ_SUMT'},
                'SP_TECHNICS_PODRAZDEL' : { 'target_table' : 'SUMT_SP_TECHNICS_PODRAZDEL','source_table' : 'MZ_SUMT'},
                'SP_TECHNICS_RAZDEL' : { 'target_table' : 'SUMT_SP_TECHNICS_RAZDEL','source_table' : 'MZ_SUMT'},
                'SP_TECHNICS_SULO' : { 'target_table' : 'SUMT_SP_TECHNICS_SULO','source_table' : 'MZ_SUMT'},
                'SP_TYPE_EFFICIENCY' : { 'target_table' : 'SUMT_SP_TYPE_EFFICIENCY','source_table' : 'MZ_SUMT'},
                'SP_TYPE_ORG' : { 'target_table' : 'SUMT_SP_TYPE_ORG','source_table' : 'MZ_SUMT'},
                'SP_UROV' : { 'target_table' : 'SUMT_SP_UROV','source_table' : 'MZ_SUMT'},
                'SP_VENTILIATSIA' : { 'target_table' : 'SUMT_SP_VENTILIATSIA','source_table' : 'MZ_SUMT'},
                'SP_VODOSNAB' : { 'target_table' : 'SUMT_SP_VODOSNAB','source_table' : 'MZ_SUMT'},
                'SP_VOZMOJ_ULUCH' : { 'target_table' : 'SUMT_SP_VOZMOJ_ULUCH','source_table' : 'MZ_SUMT'},
                'SP_ZAYAVKA_TYPE' : { 'target_table' : 'SUMT_SP_ZAYAVKA_TYPE','source_table' : 'MZ_SUMT'},
                'HBEDPROFILE' : { 'target_table' : 'SUR_HBEDPROFILE','source_table' : 'MZ_SUR'},
                'HBIOSEX' : { 'target_table' : 'SUR_HBIOSEX','source_table' : 'MZ_SUR'},
                'HPERSONALTYPE' : { 'target_table' : 'SUR_HPERSONALTYPE','source_table' : 'MZ_SUR'},
                'HPOSTFUNC' : { 'target_table' : 'SUR_HPOSTFUNC','source_table' : 'MZ_SUR'},
                'HTYPSRCFIN' : { 'target_table' : 'SUR_HTYPSRCFIN','source_table' : 'MZ_SUR'},
                'ORGHEALTHCARE' : { 'target_table' : 'SUR_ORGHEALTHCARE','source_table' : 'MZ_SUR'},
                'ORGHEALTHCARECODES' : { 'target_table' : 'SUR_ORGHEALTHCARECODES','source_table' : 'MZ_SUR'},
	}
	


	tasks = []

	for key, value in TABLES.items():

		params = {
			'target_schema'  : value['source_table'],
			'target_table' : value['target_table'],
			'source_table' : key
		}

		task_ = PythonOperator(
            task_id=f"{value['source_table']}.{key}",
            trigger_rule='all_done',
            python_callable=main,
            op_kwargs=params
        )

		tasks.append(task_)
	chain(*tasks)