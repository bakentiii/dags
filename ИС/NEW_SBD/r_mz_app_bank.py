import datetime
import hashlib
from typing import Union, List, Tuple, Optional

import psycopg2
import pymssql
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models.connection import Connection
from airflow.models.baseoperator import chain
from airflow.models import Variable



def get_source_conn() -> any:
    """
    Geting connection creadentials from airflow secrets and performing connection with database

    Returns:
        connection with database
    """
    try:
        conn_secrets = Connection.get_connection_from_secrets(conn_id="ENTER_CONNECTION_ID")
        return pymssql.connect(database=conn_secrets.schema, host=conn_secrets.host, port=conn_secrets.port, user=conn_secrets.login, password=conn_secrets.password)
    except Exception as ex:
        raise ex


def get_target_conn() -> any:
    """
    Geting connection creadentials from airflow secrets and performing connection with database

    Returns:
        connection with database
    """
    try:
        conn_secrets = Connection.get_connection_from_secrets(conn_id="postgres_11")
        return psycopg2.connect(dbname=conn_secrets.schema, user=conn_secrets.login, password=conn_secrets.password, host=conn_secrets.host, port=conn_secrets.port)
    except Exception as ex:
        raise ex


def hash(value: any, hash_keyword: str) -> Union[str, None]:
    """
    Hashing data with SHA256 algorithm in Uppercase format or None if value is null

    :param value: value to hash
    :hash_keyword: `соль` with which we hash value

    Returns:
        hashed value or None
    """
    if value is None:
        return value

    hash_data = f"{str(value).lower()}{hash_keyword}".encode("utf-8")
    result = hashlib.sha256(hash_data).hexdigest()
    return result.upper()
    

def hash_columns(values: List[any], indexes: List[int], hash_keyword: str) -> Tuple[Union[str, None]]:
    """
    Looping columns by index to Hash

    :param values: List of values to hash
    :param indexes: index of values in column for hash, index range starts from 0[inluding]
    :param hash_keyword: `соль` with which we hash values

    Returns:
        tupled values with hashed by column index
    """
    tmp = list(values)
    for index in indexes:
        tmp[index] = hash(values[index], hash_keyword)
    return tuple(tmp)


def insert_values(data: List[List[any]], schema: str, table: str, n_columns: int, cursor: any) -> None:
    """
    Inserts data into target table

    :param data: list of tupled values
    :param schema: schema name in target database
    :param table: table name in target database `schema`
    :param n_columns: amount of columns in target database `schema`.`table`
    :param cursor: cursor of the target connection
    """
    try:
        sql = f"""INSERT INTO "{schema}"."{table}" VALUES ( {', '.join(['%s']*n_columns)} )"""
        cursor.executemany(sql, data)

    except Exception as ex:
        print('[-] ERROR: An approximate problem, the amount of columns being filled does not match')
        raise ex


def truncate_table(schema: str, table: str, cursor: any) -> None:
    """
    Truncate target database.schema.table to insert new data from source database

    :param schema: schema name in target database
    :param table: table name in target database.schema
    :param cursor: cursor of the target connection
    """
    try:
        sql = f"""TRUNCATE TABLE "{schema}"."{table}";"""
        print(sql)
        cursor.execute(sql)
    except Exception as ex:
        raise ex


def extract_transform_load(source_schema: str, target_schema: str,
                           source_table: str, target_table: str,
                           hash_column_indexes: List[int], n_columns: int,
                           sql: Union[str, None], batch_size: int = 10000) -> None:
    """
    Extracts data from source database, hashing data if needs, and load into target database
    
    :param source_schema: schema name in source database
    :param source_table: table name in source database `source_schema`
    :param target_schema: schema name in target database
    :param target_table: table name in target database `target_schema`
    :param hash_column_indexes: index of values in column for hash, index range starts from 0[inluding]
    :param n_columns: amount of columns in target database `target_schema`.`target_table`
    :param sql: custom sql query to select data from source database
    :param batch_size: data batch size to fetch from source database 
    """
    SDU_LOAD_DATE = datetime.datetime.now() + datetime.timedelta(hours=6)   # loading time into SDU
    HASH_KEYWORD = Variable.get("hash_password")    # `соль` with which we hash value

    if sql is None:
        # if there no custom sql query perform defualting query
        sql = f'SELECT * FROM "{source_schema}"."{source_table}"'

    conn_source = get_source_conn()
    conn_target = get_target_conn()

    try:
        with conn_source.cursor() as cursor_source, \
             conn_target.cursor() as cursor_target:

            cursor_source.execute(sql)

            truncate_table(target_schema, target_table, cursor_target)
            conn_target.commit()

            while True:
                data = cursor_source.fetchmany(batch_size)

                if not len(data): 
                    # break, if there no data
                    break

                # adding column SDU_LOAD_DATE
                data = [[*list(i), get_dag_configs('owner'), SDU_LOAD_DATE] for i in data]

                # hashing data in columns by indexes
                data = [*map(lambda row: hash_columns(row, indexes=hash_column_indexes, hash_keyword=HASH_KEYWORD), data)]

                insert_values(data, target_schema, target_table, n_columns, cursor_target)
                conn_target.commit()
    except Exception as ex:
        raise ex
    finally:
        conn_source.close()




def main(*args, **kwargs):
    extract_transform_load(
        source_schema=kwargs['source_schema'],
        target_schema=kwargs['target_schema'],
        source_table=kwargs['source_table'],
        target_table=kwargs['target_table'],
        hash_column_indexes=kwargs['hash_column_indexes'],
        n_columns=kwargs['n_columns'],
        sql=kwargs['sql'])


def get_dag_configs(key):
    configs = {
        'dag_id': "r_mz_app_bank",
        'owner': "Bakhtiyar",
        'start_date': datetime.datetime.strptime('31/07/2023', '%d/%m/%Y'),
        'schedule_interval': "0 0 * * *",
        'tags': ['mssql', 'mz', 'reglament'],
    }

    return configs[key]


with DAG(
    dag_id=get_dag_configs('dag_id'),
    default_args={'owner': get_dag_configs('owner')},
    start_date=get_dag_configs('start_date'), 
    schedule_interval=get_dag_configs('schedule_interval'),
    catchup=False,
    tags=get_dag_configs('tags')
) as dag:

#   ...
#   Reglament update, truncate load case

    METADATA = [
        {
            "source_schema": "dbo",
            "target_schema": "R_MZ_APP_BANK",
            "tables": [
                {
                    "source_table": "SQL Results$",
                    "hash_column_indexes": [],
                    "n_columns": 3,
                    "sql": None
                },
                {
                    "source_table": "ActiveSicks",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "AddInfoCardScreening",
                    "hash_column_indexes": [],
                    "n_columns": 80,
                    "sql": None
                },
                {
                    "source_table": "AddInfoStatisticalCard",
                    "hash_column_indexes": [],
                    "n_columns": 24,
                    "sql": None
                },
                {
                    "source_table": "Addresses",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "AmbulanceActives",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "Apartments",
                    "hash_column_indexes": [],
                    "n_columns": 12,
                    "sql": None
                },
                {
                    "source_table": "AteCodes",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "Ates",
                    "hash_column_indexes": [],
                    "n_columns": 18,
                    "sql": None
                },
                {
                    "source_table": "BlobContainers",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "Buildings",
                    "hash_column_indexes": [],
                    "n_columns": 12,
                    "sql": None
                },
                {
                    "source_table": "CallingDoctor",
                    "hash_column_indexes": [],
                    "n_columns": 31,
                    "sql": None
                },
                {
                    "source_table": "CallingDoctorSicks",
                    "hash_column_indexes": [],
                    "n_columns": 11,
                    "sql": None
                },
                {
                    "source_table": "CallingDoctorStatusesHistory",
                    "hash_column_indexes": [],
                    "n_columns": 10,
                    "sql": None
                },
                {
                    "source_table": "CallingTimeSettings",
                    "hash_column_indexes": [],
                    "n_columns": 12,
                    "sql": None
                },
                {
                    "source_table": "CardScreeningRiskFactor",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "ClaimDiagnoses",
                    "hash_column_indexes": [],
                    "n_columns": 12,
                    "sql": None
                },
                {
                    "source_table": "ClaimStatuses",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "Claims",
                    "hash_column_indexes": [],
                    "n_columns": 36,
                    "sql": None
                },
                {
                    "source_table": "ClaimsServices",
                    "hash_column_indexes": [],
                    "n_columns": 12,
                    "sql": None
                },
                {
                    "source_table": "ContractTarificators",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "Contracts",
                    "hash_column_indexes": [],
                    "n_columns": 12,
                    "sql": None
                },
                {
                    "source_table": "CorrectionCoefficients",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "DiagnosPatients",
                    "hash_column_indexes": [],
                    "n_columns": 19,
                    "sql": None
                },
                {
                    "source_table": "DiagnosPregnancyExt",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "DictionariesUpdateRules",
                    "hash_column_indexes": [],
                    "n_columns": 11,
                    "sql": None
                },
                {
                    "source_table": "DispensaryAccountPatients",
                    "hash_column_indexes": [],
                    "n_columns": 18,
                    "sql": None
                },
                {
                    "source_table": "ErrorSexSicks",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "ExecutedServices",
                    "hash_column_indexes": [],
                    "n_columns": 27,
                    "sql": None
                },
                {
                    "source_table": "ExecutedServicesResults",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "FuncStructure",
                    "hash_column_indexes": [],
                    "n_columns": 11,
                    "sql": None
                },
                {
                    "source_table": "GeneratedSequenceByMO",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "GridSchedule",
                    "hash_column_indexes": [],
                    "n_columns": 19,
                    "sql": None
                },
                {
                    "source_table": "GridScheduleAdditional",
                    "hash_column_indexes": [],
                    "n_columns": 18,
                    "sql": None
                },
                {
                    "source_table": "GridScheduleBreaks",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "GridScheduleRecords",
                    "hash_column_indexes": [],
                    "n_columns": 26,
                    "sql": None
                },
                {
                    "source_table": "GridScheduleReserves",
                    "hash_column_indexes": [],
                    "n_columns": 11,
                    "sql": None
                },
                {
                    "source_table": "Holidays",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "HospitalActives",
                    "hash_column_indexes": [],
                    "n_columns": 11,
                    "sql": None
                },
                {
                    "source_table": "InspectionResultPatients",
                    "hash_column_indexes": [],
                    "n_columns": 20,
                    "sql": None
                },
                {
                    "source_table": "JournalListCard",
                    "hash_column_indexes": [],
                    "n_columns": 17,
                    "sql": None
                },
                {
                    "source_table": "ListDictionariesAPP",
                    "hash_column_indexes": [],
                    "n_columns": 11,
                    "sql": None
                },
                {
                    "source_table": "ListObjectsForLog",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "ListPost",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "MOActives",
                    "hash_column_indexes": [],
                    "n_columns": 24,
                    "sql": None
                },
                {
                    "source_table": "MaterialResources",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MedicalFacilities",
                    "hash_column_indexes": [],
                    "n_columns": 11,
                    "sql": None
                },
                {
                    "source_table": "Messages",
                    "hash_column_indexes": [],
                    "n_columns": 12,
                    "sql": None
                },
                {
                    "source_table": "MessagesSentHistory",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "MessagesToSent",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "MisMOServiceData",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MoBaseRates",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "NewBornActives",
                    "hash_column_indexes": [],
                    "n_columns": 20,
                    "sql": None
                },
                {
                    "source_table": "NonWorkDays",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "ObjectsChangeLog",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "OrgHealthCare",
                    "hash_column_indexes": [],
                    "n_columns": 16,
                    "sql": None
                },
                {
                    "source_table": "OrgHealthCareCodes",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "OrgHealthCareTypSrcFins",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "PatientAttachments",
                    "hash_column_indexes": [],
                    "n_columns": 14,
                    "sql": None
                },
                {
                    "source_table": "PatientCards",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "PatientVisits",
                    "hash_column_indexes": [],
                    "n_columns": 19,
                    "sql": None
                },
                {
                    "source_table": "Patients",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "PerformedEmergencyDiagnoses",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "PerformedEmergencyServiceInfo",
                    "hash_column_indexes": [],
                    "n_columns": 12,
                    "sql": None
                },
                {
                    "source_table": "PerformedTarificatorConclusions",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "PerformedTarificators",
                    "hash_column_indexes": [],
                    "n_columns": 36,
                    "sql": None
                },
                {
                    "source_table": "Person",
                    "hash_column_indexes": [
                        10,
                        11,
                        14,
                        19
                    ],
                    "n_columns": 22,
                    "sql": None
                },
                {
                    "source_table": "PersonAddresses",
                    "hash_column_indexes": [],
                    "n_columns": 12,
                    "sql": None
                },
                {
                    "source_table": "PersonEmployments",
                    "hash_column_indexes": [],
                    "n_columns": 10,
                    "sql": None
                },
                {
                    "source_table": "PersonPhones",
                    "hash_column_indexes": [],
                    "n_columns": 10,
                    "sql": None
                },
                {
                    "source_table": "Personal",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "PoliclinicActives",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "Post",
                    "hash_column_indexes": [],
                    "n_columns": 13,
                    "sql": None
                },
                {
                    "source_table": "ReferralDiagnoses",
                    "hash_column_indexes": [],
                    "n_columns": 12,
                    "sql": None
                },
                {
                    "source_table": "ReferralServices",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "Referrals",
                    "hash_column_indexes": [],
                    "n_columns": 30,
                    "sql": None
                },
                {
                    "source_table": "RegionalTimeSettingsByMO",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "RepAttachedPopulationByPeriod",
                    "hash_column_indexes": [],
                    "n_columns": 16,
                    "sql": None
                },
                {
                    "source_table": "RepDetailForm12_128",
                    "hash_column_indexes": [],
                    "n_columns": 4,
                    "sql": None
                },
                {
                    "source_table": "RepForm12_128",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "Rights",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "Roles",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "RolesRights",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "RolesSystem",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "RoomFStruct",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "ScreeningFact",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "ScreeningPlan",
                    "hash_column_indexes": [],
                    "n_columns": 10,
                    "sql": None
                },
                {
                    "source_table": "ScreeningPlanSettings",
                    "hash_column_indexes": [],
                    "n_columns": 4,
                    "sql": None
                },
                {
                    "source_table": "ScreeningTargetGroupAges",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "ScreeningTargetGroupBioSex",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "ScreeningTargetGroupSicks",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "Services",
                    "hash_column_indexes": [],
                    "n_columns": 11,
                    "sql": None
                },
                {
                    "source_table": "ServicesConfirmationSettings",
                    "hash_column_indexes": [],
                    "n_columns": 11,
                    "sql": None
                },
                {
                    "source_table": "SickAccount",
                    "hash_column_indexes": [],
                    "n_columns": 4,
                    "sql": None
                },
                {
                    "source_table": "SickNotMain",
                    "hash_column_indexes": [],
                    "n_columns": 4,
                    "sql": None
                },
                {
                    "source_table": "SickPregnancy",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "SickTrauma",
                    "hash_column_indexes": [],
                    "n_columns": 4,
                    "sql": None
                },
                {
                    "source_table": "StatisticalCardReferralDiagnoses",
                    "hash_column_indexes": [],
                    "n_columns": 11,
                    "sql": None
                },
                {
                    "source_table": "StatisticalCards",
                    "hash_column_indexes": [],
                    "n_columns": 24,
                    "sql": None
                },
                {
                    "source_table": "SupportInfo",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "Table_1",
                    "hash_column_indexes": [],
                    "n_columns": 3,
                    "sql": None
                },
                {
                    "source_table": "Tarificator",
                    "hash_column_indexes": [],
                    "n_columns": 20,
                    "sql": None
                },
                {
                    "source_table": "TarificatorCoefficients",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "TarificatorCosts",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "TarificatorMO",
                    "hash_column_indexes": [],
                    "n_columns": 10,
                    "sql": None
                },
                {
                    "source_table": "TarificatorServices",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "TerritoryServiceDoctors",
                    "hash_column_indexes": [],
                    "n_columns": 11,
                    "sql": None
                },
                {
                    "source_table": "TerritoryServices",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "UserRights",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "UserRoles",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "Users",
                    "hash_column_indexes": [],
                    "n_columns": 17,
                    "sql": None
                },
                {
                    "source_table": "ValidationsForMaxCount",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "equipmentLeasing",
                    "hash_column_indexes": [],
                    "n_columns": 12,
                    "sql": None
                },
                {
                    "source_table": "hActiveDeliveryStatuses",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "hActiveStatuses",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "hAddPersonSources",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "hAddressType",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "hAgeCriteriaEx",
                    "hash_column_indexes": [],
                    "n_columns": 12,
                    "sql": None
                },
                {
                    "source_table": "hAgeGroups",
                    "hash_column_indexes": [],
                    "n_columns": 10,
                    "sql": None
                },
                {
                    "source_table": "hAnalysis",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "hAteTypes",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "hAttachPMSPType",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "hAttachStatus",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "hBioSex",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "hBirthTypes",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "hCVid",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "hCallingEndTypes",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "hCallingStatuses",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "hCancelReceptionReason",
                    "hash_column_indexes": [],
                    "n_columns": 10,
                    "sql": None
                },
                {
                    "source_table": "hCategoriesExempts",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "hCategoryPost",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "hCharSick",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "hCholesterolLevels",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "hCitizenship",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "hClaimStatuses",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "hClaimTypes",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "hColonoskopyResult",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "hCorrectionCoefficientTypes",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "hDiagType",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "hDiagnosisType",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "hDurationType",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "hEcgResults",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "hEmploymentKinds",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "hEmploymentTypes",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "hExecutionPlaces",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "hGBD",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "hGlucoseLevels",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "hGridScheduleTypes",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "hHaemocultTestResult",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "hHealthGroup",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "hHospitalStayResult",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "hInvalidGroup",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "hIshodType",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "hListCardTypePeriod",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "hLocalityProfiles",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "hMammographyResultsEx",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "hMedFcl",
                    "hash_column_indexes": [],
                    "n_columns": 10,
                    "sql": None
                },
                {
                    "source_table": "hMedicineChest",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "hMoCoefficientTypes",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "hNationality",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "hPerformedStatuses",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "hPersonalType",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "hPhoneTypes",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "hPost",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "hPostFunc",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "hPriznPost",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "hProctosigmoidoscopeResultEx",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "hReasonRemoveAccount",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "hRecessType",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "hReferralRejectionReason",
                    "hash_column_indexes": [],
                    "n_columns": 10,
                    "sql": None
                },
                {
                    "source_table": "hReferralStates",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "hReferralStatuses",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "hReferralTypes",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "hRngFStruct",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "hScreeningAges",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "hScreeningPassingStatusses",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "hScreeningTargetGroups",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "hScreeningValuesEx",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "hSenderTypes",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "hServ",
                    "hash_column_indexes": [],
                    "n_columns": 14,
                    "sql": None
                },
                {
                    "source_table": "hServType",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "hServiceTypes",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "hSick",
                    "hash_column_indexes": [],
                    "n_columns": 13,
                    "sql": None
                },
                {
                    "source_table": "hSmearResults",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "hStaffTypes",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "hState",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "hStateHealth",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "hStatusPost",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "hSubjects",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "hSubordination",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "hSystems",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "hTarificatorClasses",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "hTarificatorCoefficientSubTypes",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "hTarificatorCoefficientTypes",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "hTarificatorCostTypes",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "hTarificatorGobmpTypes",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "hTarificatorMethods",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "hTerritoryUnitType",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "hTherapyResult",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "hTraumaType",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "hTreatmentReasons",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "hTypMResour",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "hTypSrcFin",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "hType25Card",
                    "hash_column_indexes": [],
                    "n_columns": 10,
                    "sql": None
                },
                {
                    "source_table": "hTypeAccount",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "hUrgencyTypes",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "hUterusBiopsyResult",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "hVisitTypes",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "historyPost",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "listServicesLeasing",
                    "hash_column_indexes": [],
                    "n_columns": 11,
                    "sql": None
                },
                {
                    "source_table": "sysdiagrams",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                }
            ]
        }
    ]

    tasks = []

    for schema in METADATA:
        for table in schema['tables']:
            params = {
                'source_schema': schema['source_schema'],
                'target_schema': schema['target_schema'],
                'source_table': table['source_table'],
                'target_table': table['source_table'].upper(),
                'hash_column_indexes': table['hash_column_indexes'],
                'n_columns': table['n_columns'],
                'sql': table['sql']
            }

            task = PythonOperator(
                task_id=f"{schema['target_schema']}.{table['source_table'].upper()}",
                python_callable=main,
                op_kwargs=params,
                trigger_rule='all_done'
            )

            tasks.append(task)

    chain(*tasks)