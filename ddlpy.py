import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models.connection import Connection
from airflow.models.baseoperator import chain
from airflow.models import Variable
import psycopg2# postgres conn


def get_local_postgres_connection():
	try:
		conn_postgresSDU_secret = Connection.get_connection_from_secrets(conn_id="local_postgres_ch_backend")#скрытый коннекшн нашего postgres
		return psycopg2.connect(
			dbname=conn_postgresSDU_secret.schema,
			user=conn_postgresSDU_secret.login,
			password=conn_postgresSDU_secret.password,
			host=conn_postgresSDU_secret.host,
			port=conn_postgresSDU_secret.port)
	except Exception as ex:
		print(f'Error connection of SBD_POSTGRES - {ex}')


def extract_and_load_data(tagret_schema,target_table,load_date,n_columns, hash_indexes):

	local_conn = get_local_postgres_connection()

	try:
		with local_conn.cursor() as cursor:
			TABLES = ['grst_accordance_doc',
                        'grst_agent',
                        'grst_agent_type',
                        'grst_arrest',
                        'grst_cause',
                        'grst_country',
                        'grst_currency',
                        'grst_division',
                        'grst_document_issuer',
                        'grst_document_kind',
                        'grst_enterprise_type',
                        'grst_events_sep',
                        'grst_file',
                        'grst_government_number_history',
                        'grst_history',
                        'grst_info',
                        'grst_inspection',
                        'grst_kato',
                        'grst_machinery',
                        'grst_machinery',
                        'grst_machinery_explain_view',
                        'grst_machinery_info_adjustment_request',
                        'grst_mark',
                        'grst_message',
                        'grst_model',
                        'grst_model_explain',
                        'grst_move',
                        'grst_old',
                        'grst_opf',
                        'grst_plant',
                        'grst_plant_explain',
                        'grst_pledge',
                        'grst_position',
                        'grst_privilege',
                        'grst_region',
                        'grst_replacement_unit',
                        'grst_report_link',
                        'grst_report_status',
                        'grst_role',
                        'grst_role_privilege',
                        'grst_role_privilege_explain',
                        'grst_speciality',
                        'grst_street_type',
                        'grst_template',
                        'grst_tmp_uuid_array',
                        'grst_tractor_driver',
                        'grst_type',
                        'grst_unit',
                        'grst_user',
                        'grst_view',
                        'knew',
                        'kold',
                        'kr',
                        'kr2',
                        'msh_vet_san_act',
                        'view_history',
                        'view_user']
            for table in TABLES:
                data = f"""
                            SELECT 'CREATE TABLE ' || table_name || ' (' || string_agg(column_name || ' ' || data_type || 
                                CASE WHEN character_maximum_length IS NOT NULL THEN '(' || character_maximum_length || ')' ELSE '' END ||
                                CASE WHEN is_nullable = 'NO' THEN ' NOT NULL' ELSE '' END, ', ') || ');'
                            FROM information_schema.columns
                            WHERE table_schema = 'MSH_GRST'
                            AND table_name = '{table}'
                            GROUP BY table_schema, table_name;
                        """
                cursor.execute(data)
                ddl_statement = cursor.fetchone()[0]

                output_file = f"/opt/app/airflow/dags/ddl_output.txt"
                 with open(output_file, "w") as f:
                    f.write(ddl_statement)
	finally:
		local_conn.close()



with DAG("insert_ddl", description="Load ddl",start_date=datetime.datetime(2023, 8, 16),schedule_interval=None,catchup=False, tags = ['postgres', 'ddl']) as dag:

    task_ = PythonOperator(
            owner = 'Bakhtiyar'
            task_id=f"insert_ddl",
            python_callable=extract_and_load_data,
        )