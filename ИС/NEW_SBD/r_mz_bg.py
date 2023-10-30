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
        'dag_id': "r_mz_bg",
        'owner': "Bakhtiyar",
        'start_date': datetime.datetime.strptime('', '%d/%m/%Y'),
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
            "target_schema": "R_MZ_BG",
            "tables": [
                {
                    "source_table": "ApplicationErrors",
                    "hash_column_indexes": [
                        2
                    ],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "BlockAutoBooking",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "Etd_FilterFinanceSource",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "Etd_FinanceSourceExtra",
                    "hash_column_indexes": [],
                    "n_columns": 4,
                    "sql": None
                },
                {
                    "source_table": "InputDataAudit",
                    "hash_column_indexes": [],
                    "n_columns": 11,
                    "sql": None
                },
                {
                    "source_table": "LogSyncWithSUR",
                    "hash_column_indexes": [],
                    "n_columns": 3,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_AbolitionAssignment",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_AbolitionGraph",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_AbsencePersInStac",
                    "hash_column_indexes": [],
                    "n_columns": 11,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_AcademicPost",
                    "hash_column_indexes": [],
                    "n_columns": 16,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_AcademicVacation",
                    "hash_column_indexes": [
                        5,
                        6,
                        8
                    ],
                    "n_columns": 11,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_AccessExtSystemByRegionAndMO",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_AccountGroup",
                    "hash_column_indexes": [
                        18,
                        19,
                        20
                    ],
                    "n_columns": 36,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_AccountGroupCard",
                    "hash_column_indexes": [
                        0,
                        1
                    ],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_AccrAffair",
                    "hash_column_indexes": [
                        18
                    ],
                    "n_columns": 23,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_AccrComMember",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_AccrComissions",
                    "hash_column_indexes": [
                        4,
                        5
                    ],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_AccrDemands",
                    "hash_column_indexes": [],
                    "n_columns": 10,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_AccrOrgHealth",
                    "hash_column_indexes": [],
                    "n_columns": 4,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_AccrSittingList",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_AccrVoting",
                    "hash_column_indexes": [
                        3
                    ],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ActDirection",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ActExecutor",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ActInit",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ActObject",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ActObligat",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ActPlace",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ActPrescription",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ActResponse",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ActTimeExecut",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_Action",
                    "hash_column_indexes": [],
                    "n_columns": 11,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_Actions_Element",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_Actions_Section",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_Actions_Tab",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ActiveSicks",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_Activity",
                    "hash_column_indexes": [
                        6,
                        7
                    ],
                    "n_columns": 15,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ActivityAttrs",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ActivityType",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_AddCharact",
                    "hash_column_indexes": [],
                    "n_columns": 25,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_AddCharactGroup",
                    "hash_column_indexes": [
                        2
                    ],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_AddCharactTemplate",
                    "hash_column_indexes": [
                        4,
                        6
                    ],
                    "n_columns": 17,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_AddCharactValues",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_AddContTeam",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_AdditCharact",
                    "hash_column_indexes": [],
                    "n_columns": 12,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_AdditionTariff",
                    "hash_column_indexes": [],
                    "n_columns": 10,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_AdditionalCharacters",
                    "hash_column_indexes": [
                        9
                    ],
                    "n_columns": 13,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_AddressElements",
                    "hash_column_indexes": [
                        0
                    ],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_AddressExecution",
                    "hash_column_indexes": [
                        2
                    ],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_Addresses",
                    "hash_column_indexes": [
                        8
                    ],
                    "n_columns": 11,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_Ads",
                    "hash_column_indexes": [],
                    "n_columns": 10,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_Allergy",
                    "hash_column_indexes": [
                        9,
                        11,
                        12,
                        15
                    ],
                    "n_columns": 18,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_Alliance",
                    "hash_column_indexes": [
                        4
                    ],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_AllowVTMUServicesByDayHospital",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_AmbulanceActives",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_Analysis",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_AnesthesiaCodes",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_AnonymousCode",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_Answer",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_Answers",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_AssaySample",
                    "hash_column_indexes": [],
                    "n_columns": 11,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_Attach",
                    "hash_column_indexes": [
                        0,
                        1,
                        14
                    ],
                    "n_columns": 18,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_AttachPeriod",
                    "hash_column_indexes": [],
                    "n_columns": 10,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_AttachmentRequests",
                    "hash_column_indexes": [
                        0,
                        5
                    ],
                    "n_columns": 12,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_AttendingDoctor",
                    "hash_column_indexes": [
                        7,
                        8,
                        9,
                        10,
                        11
                    ],
                    "n_columns": 17,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_AvailableDrugs",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_AvailableMedProducts",
                    "hash_column_indexes": [],
                    "n_columns": 4,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_AvailablePreparationHistory",
                    "hash_column_indexes": [],
                    "n_columns": 15,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_AvailablePreparations",
                    "hash_column_indexes": [],
                    "n_columns": 23,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_AvailableProducedDrugs",
                    "hash_column_indexes": [],
                    "n_columns": 4,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_AverageAnSalary",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_BedProfileAssociatTypSrcFinFrorMO",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_BedProfileCost",
                    "hash_column_indexes": [],
                    "n_columns": 10,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_BedProfileFilter",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_BedProfileForChild",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_BedProfileGroupForBpHelpTypes",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_BedProfileServices",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_BedProfileServicesTypes",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_BedStatus",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_Beds",
                    "hash_column_indexes": [],
                    "n_columns": 13,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_BedsWorkNormatives",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_Benefits",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_BigDiagnosticCategory",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_BirthReg",
                    "hash_column_indexes": [
                        31
                    ],
                    "n_columns": 40,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_BirthRegCriteriaBirth",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_BirthWomanActives",
                    "hash_column_indexes": [],
                    "n_columns": 10,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_BlobDataValues",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_BookingAverageInfo",
                    "hash_column_indexes": [],
                    "n_columns": 13,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_BookingBedProfileInfo",
                    "hash_column_indexes": [],
                    "n_columns": 10,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_BookingBedProfileWeekDays",
                    "hash_column_indexes": [],
                    "n_columns": 10,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_BookingMaxHospitalizationCount",
                    "hash_column_indexes": [],
                    "n_columns": 10,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_BookingOrgHealthCareSettings",
                    "hash_column_indexes": [],
                    "n_columns": 13,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_BookingPlannedHospitals",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_BookingReserveBedProfile",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_BookingReserveFund",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_BookingSynchronizationSettings",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_BookingTemp",
                    "hash_column_indexes": [],
                    "n_columns": 15,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_BookingValueContractInfo",
                    "hash_column_indexes": [],
                    "n_columns": 13,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_BpHelpTypesFilter",
                    "hash_column_indexes": [],
                    "n_columns": 16,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_BudgetMoRateValues",
                    "hash_column_indexes": [],
                    "n_columns": 13,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_BudgetRepublicPays",
                    "hash_column_indexes": [],
                    "n_columns": 13,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_BudgetRepublicRateValues",
                    "hash_column_indexes": [],
                    "n_columns": 11,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_BudgetStateRateValues",
                    "hash_column_indexes": [],
                    "n_columns": 12,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_Buffer",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_BuildingPowerIndexes",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_CCGRegisterICD",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_CCGRegisterOper",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_CCGroup",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_CMCCMembers",
                    "hash_column_indexes": [],
                    "n_columns": 21,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_CMCachingTables",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_CMEventPool",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_CMListeningTables",
                    "hash_column_indexes": [],
                    "n_columns": 10,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_CTU_FLC",
                    "hash_column_indexes": [],
                    "n_columns": 13,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_CTU_compound",
                    "hash_column_indexes": [],
                    "n_columns": 10,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_CTU_context",
                    "hash_column_indexes": [],
                    "n_columns": 11,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_CT_FLC",
                    "hash_column_indexes": [],
                    "n_columns": 13,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_CT_FieldFilter",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_CT_FieldValues",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_CT_MakeCharact",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_CT_MakeRecords",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_CT_compound",
                    "hash_column_indexes": [],
                    "n_columns": 16,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_CT_context",
                    "hash_column_indexes": [],
                    "n_columns": 11,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_CallingDoctor",
                    "hash_column_indexes": [],
                    "n_columns": 29,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_CallingDoctorInspection",
                    "hash_column_indexes": [],
                    "n_columns": 10,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_CallingTimeSpans",
                    "hash_column_indexes": [],
                    "n_columns": 11,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_CapableStatusPerson",
                    "hash_column_indexes": [
                        8
                    ],
                    "n_columns": 11,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_CardFilesAttach",
                    "hash_column_indexes": [
                        0
                    ],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_CardFilesDescription",
                    "hash_column_indexes": [],
                    "n_columns": 11,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_CasesBeg",
                    "hash_column_indexes": [],
                    "n_columns": 26,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_CasesEnd",
                    "hash_column_indexes": [],
                    "n_columns": 19,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_CasesEnds",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_Category",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_Cell",
                    "hash_column_indexes": [],
                    "n_columns": 17,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_CellElement",
                    "hash_column_indexes": [],
                    "n_columns": 24,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_CellElementComplexText",
                    "hash_column_indexes": [],
                    "n_columns": 18,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_CellElementEvents",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_CellElementFrameParam",
                    "hash_column_indexes": [],
                    "n_columns": 10,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_CellElementIfaceParam",
                    "hash_column_indexes": [],
                    "n_columns": 11,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_CellElementParam",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_CellParam",
                    "hash_column_indexes": [],
                    "n_columns": 13,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_CertAffair",
                    "hash_column_indexes": [],
                    "n_columns": 25,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_CertApAffair",
                    "hash_column_indexes": [],
                    "n_columns": 15,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_CertApCom",
                    "hash_column_indexes": [],
                    "n_columns": 11,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_CertApComMember",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_CertApSitting",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_CertApVoting",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_CertComMember",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_CertComProfiles",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_CertComSpeciality",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_CertComissions",
                    "hash_column_indexes": [],
                    "n_columns": 14,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_CertOrgHealth",
                    "hash_column_indexes": [],
                    "n_columns": 4,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_Certifiable",
                    "hash_column_indexes": [],
                    "n_columns": 4,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_CertificateNotice",
                    "hash_column_indexes": [],
                    "n_columns": 11,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ChangesList",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_CharDiagn",
                    "hash_column_indexes": [],
                    "n_columns": 15,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_CharNazn",
                    "hash_column_indexes": [],
                    "n_columns": 14,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_Charact",
                    "hash_column_indexes": [],
                    "n_columns": 48,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_CharactAttributes",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_CharactAttributesSet",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_CharactBuffer",
                    "hash_column_indexes": [],
                    "n_columns": 12,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_CharactDataPieces",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_CharactRules",
                    "hash_column_indexes": [],
                    "n_columns": 11,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_CharactTemplate",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_CharactTemplate2",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_CharactTemplateBuffer",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_CharactTemplateComposition",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_CharactTemplateFieldValue",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_CharactTemplateSet",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_CharactTemplateUser2",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_CharactVariantView",
                    "hash_column_indexes": [],
                    "n_columns": 15,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_CharactVariantViewValues",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_CheckShedule",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_Clear_Nums",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ColumnList",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_Comments",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_Commissioners",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_CommonParam",
                    "hash_column_indexes": [],
                    "n_columns": 16,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_Complaints",
                    "hash_column_indexes": [],
                    "n_columns": 15,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_CompletedPrescriptions",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_Complications",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ConcomitantDiagnoses",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_CondSet",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ConditionPart",
                    "hash_column_indexes": [],
                    "n_columns": 10,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ConditionPartExpr",
                    "hash_column_indexes": [],
                    "n_columns": 14,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ConditionSet",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ConditionsForDefectsOneTwo",
                    "hash_column_indexes": [],
                    "n_columns": 10,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ConditionsForDefectsOneTwo_backup",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ConditionsInOncologyBySickFilter",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ConfigGroupInModules",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ConflictAttach",
                    "hash_column_indexes": [],
                    "n_columns": 12,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ConformityMepsServDrugs",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ConformityMepsSurgProcDrugs",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ConnectFromOut",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ConsultationSettings",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ContTeam",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_Contact",
                    "hash_column_indexes": [],
                    "n_columns": 19,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_Contacts",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ContraceptionData",
                    "hash_column_indexes": [],
                    "n_columns": 10,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_CorpseRegistry",
                    "hash_column_indexes": [],
                    "n_columns": 16,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_CorrespTable",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_Courses",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_CreateProtocolErrors",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_DBChangerLog",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_DCCMembers",
                    "hash_column_indexes": [],
                    "n_columns": 11,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_DCCNaznDescription",
                    "hash_column_indexes": [],
                    "n_columns": 22,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_DCCNaznDescriptionGAI",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_DCDataElementParam_IC",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_DCDataElementParam_Table",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_DCDefaultDEParamAttributes",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_DCDefaultFieldsAttributes",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_DCDefaultTablesAttributes",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_DCFields",
                    "hash_column_indexes": [],
                    "n_columns": 14,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_DCFixRecordSet",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_DCFlkChecks_Fields",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_DCFlkChecks_Objects",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_DCFlkChecks_Tables",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_DCFlkParams_Fields",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_DCFlkParams_Objects",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_DCFlkParams_Tables",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_DCIncludeCondition",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_DCNameSpases",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_DCObjectStruct",
                    "hash_column_indexes": [],
                    "n_columns": 10,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_DCObjects",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_DCObjectsConditionSet",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_DCRecords",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_DCSavedStruct",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_DCTables",
                    "hash_column_indexes": [],
                    "n_columns": 10,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_DEField",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_DEParameter",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_DIC_CAPABLE_STATUS",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_DIC_COUNTRY",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_DIC_COURT",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_DIC_DISTRICTS",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_DIC_DOCUMENT_INVALIDITY",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_DIC_DOCUMENT_ORGANIZATION",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_DIC_DOCUMENT_TYPE",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_DIC_EXCLUDE_REASON",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_DIC_GP_TERRITORIAL",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_DIC_MESSAGE_RESULT",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_DIC_NATIONALITY",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_DIC_PARTICIPANT",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_DIC_PERSON_STATUS",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_DIC_REGION",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_DIC_SEX",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_DataColumn",
                    "hash_column_indexes": [],
                    "n_columns": 11,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_DataColumnExpr",
                    "hash_column_indexes": [],
                    "n_columns": 14,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_DataElement",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_DataElement2",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_DataElementParam",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_DataElementParam_ComplexText",
                    "hash_column_indexes": [],
                    "n_columns": 11,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_DataElementParam_Element",
                    "hash_column_indexes": [],
                    "n_columns": 11,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_DataElementParam_Section",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_DataElementParam_Spec",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_DataParam",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_DataPiece",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_DataPiece_Caption",
                    "hash_column_indexes": [],
                    "n_columns": 11,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_DataPiece_XSL",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_DataReport",
                    "hash_column_indexes": [],
                    "n_columns": 12,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_DataReportCell",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_DataReportSection",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_DataReportTable",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_DataSource",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_DataSourceParam",
                    "hash_column_indexes": [],
                    "n_columns": 19,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_DateExp",
                    "hash_column_indexes": [],
                    "n_columns": 13,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_DateSendExp",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_DaysOfRest",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_DeathReg",
                    "hash_column_indexes": [],
                    "n_columns": 39,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_DeathRegOtherImpConditions",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_DefExecTimes",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_DefectGroup",
                    "hash_column_indexes": [],
                    "n_columns": 4,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_DefectItem",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_DefectTypeSick",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_Demand",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_DemandJobPlace",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_DemographicData",
                    "hash_column_indexes": [
                        7,
                        8,
                        16
                    ],
                    "n_columns": 19,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_DesDocOut",
                    "hash_column_indexes": [],
                    "n_columns": 16,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_Devices",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_DevicesCharact",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_DevicesName",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_DevicesXMLDataTypes",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_Diagnos",
                    "hash_column_indexes": [],
                    "n_columns": 27,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_DiagnosClinicalData",
                    "hash_column_indexes": [],
                    "n_columns": 18,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_DiagnosContactLink",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_DiagnosExtensionProperties",
                    "hash_column_indexes": [],
                    "n_columns": 13,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_DiagnosesChemotherapy",
                    "hash_column_indexes": [],
                    "n_columns": 4,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_DiagnosesList",
                    "hash_column_indexes": [],
                    "n_columns": 11,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_Diagnosis",
                    "hash_column_indexes": [],
                    "n_columns": 12,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_DiagnosisCanNotBeMain",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_DiagnosisComplications",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_DiagnosisToMepsSurgProc",
                    "hash_column_indexes": [],
                    "n_columns": 11,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_Dicts",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_DigitalSignatures",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_DigitalSignaturesHistory",
                    "hash_column_indexes": [],
                    "n_columns": 10,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_DirectionListResearches",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_DischargeCriteria",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_DiseasesHeavyFormPropertyRelations",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_DiseasesHeavyFormPropertySicks",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_DiseasesHeavyFormSicks",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_DocList",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_DocListTab",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_DocParams",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_DocsList",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_DocsPack",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_DocsVer",
                    "hash_column_indexes": [],
                    "n_columns": 11,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_DoctorForTerritory",
                    "hash_column_indexes": [],
                    "n_columns": 11,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_Doctors",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_Document",
                    "hash_column_indexes": [],
                    "n_columns": 10,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_DocumentTemplate",
                    "hash_column_indexes": [],
                    "n_columns": 18,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_DrgByDoubleSickAndSurgParentSick",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_DrgByDoubleSicks",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_DrgByDoubleSicksAndSurg",
                    "hash_column_indexes": [],
                    "n_columns": 10,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_DrgByDoubleSicksAndSurgParent",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_DrgFactSurgProc",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_DrgFactSurgProcBackup",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_DrgMultiParamsForPerinatology",
                    "hash_column_indexes": [],
                    "n_columns": 14,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_DrgSicks",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_DrgSurgProcs",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_DrgWeights",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_DrugPersonMaxNumber",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_DrugPersons",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_DrugReserves",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_DrugStructures",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_Drugs",
                    "hash_column_indexes": [],
                    "n_columns": 23,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_DrugstoreRequests",
                    "hash_column_indexes": [],
                    "n_columns": 15,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_DrugstoreRequestsHistory",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_Dtransp",
                    "hash_column_indexes": [],
                    "n_columns": 20,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_DtranspInfoFields",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_DurationExecCorrect",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_EISZ_gbl_Person_ADDRESS",
                    "hash_column_indexes": [],
                    "n_columns": 21,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_EMDOutFL",
                    "hash_column_indexes": [],
                    "n_columns": 23,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_EMDPatients",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_EPZAccountGroup",
                    "hash_column_indexes": [],
                    "n_columns": 16,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_EPZDiagnos",
                    "hash_column_indexes": [],
                    "n_columns": 21,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_EPassportRequests",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_EServiceRequestsJournal",
                    "hash_column_indexes": [],
                    "n_columns": 22,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_Education",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_EgpDiagSicks",
                    "hash_column_indexes": [],
                    "n_columns": 4,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_EiszPilotAreaOrgHealthCares",
                    "hash_column_indexes": [],
                    "n_columns": 4,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_EkoZoneMepCoefficients",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ElementNames",
                    "hash_column_indexes": [],
                    "n_columns": 4,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ElementParam",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ElementParamF",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ElementParamM",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ElementSys",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ElementSysLink",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_EmailSettings",
                    "hash_column_indexes": [],
                    "n_columns": 10,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_EmmCallInfoFields",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_EmmCalls",
                    "hash_column_indexes": [],
                    "n_columns": 52,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_EmmCallsRecords",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_EmmDepartInfoFields",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_EmmDeparts",
                    "hash_column_indexes": [],
                    "n_columns": 33,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_EmmTeam",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_Epicrisis",
                    "hash_column_indexes": [],
                    "n_columns": 16,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_EpicrisisData",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_EpicrisisDataBackup",
                    "hash_column_indexes": [],
                    "n_columns": 3,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ErrorAgeSicks",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ErrorDeathReasonSicks",
                    "hash_column_indexes": [],
                    "n_columns": 4,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ErrorSexBedProfiles",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ErrorSexSicks",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ErrorSpecConsultByBeds",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_EventOperations",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ExServItems",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ExServItemsNotExec",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_Examinations",
                    "hash_column_indexes": [],
                    "n_columns": 56,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ExecAction",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ExecCondSet",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ExecRepeatValue",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ExecuteOperation",
                    "hash_column_indexes": [],
                    "n_columns": 16,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ExecuteOperationWorkPlace",
                    "hash_column_indexes": [],
                    "n_columns": 19,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ExecutedSurgicalOperations",
                    "hash_column_indexes": [],
                    "n_columns": 15,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ExpCriteria",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ExpForCom",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ExpForExp",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ExpList",
                    "hash_column_indexes": [],
                    "n_columns": 12,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ExpResult",
                    "hash_column_indexes": [],
                    "n_columns": 11,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ExpThem",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ExpThemOrg",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ExpThemOrgIndSet",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_Expenditure",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_Expr",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ExprPart",
                    "hash_column_indexes": [],
                    "n_columns": 12,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ExternalDirection",
                    "hash_column_indexes": [],
                    "n_columns": 19,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ExternalDirectionRegMaterial",
                    "hash_column_indexes": [],
                    "n_columns": 18,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ExternalDirectionServices",
                    "hash_column_indexes": [],
                    "n_columns": 21,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ExternalDirectionSurgicalOperations",
                    "hash_column_indexes": [],
                    "n_columns": 11,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ExternalForm066Journal",
                    "hash_column_indexes": [],
                    "n_columns": 25,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ExternalFp",
                    "hash_column_indexes": [],
                    "n_columns": 15,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ExternalMo",
                    "hash_column_indexes": [],
                    "n_columns": 23,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ExternalMoContactInfo",
                    "hash_column_indexes": [],
                    "n_columns": 11,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ExternalMoPersonal",
                    "hash_column_indexes": [],
                    "n_columns": 17,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ExternalMoPersonalEduAdd",
                    "hash_column_indexes": [],
                    "n_columns": 17,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ExternalMoPersonalEduPrim",
                    "hash_column_indexes": [],
                    "n_columns": 12,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ExternalMoPersonalSkills",
                    "hash_column_indexes": [],
                    "n_columns": 16,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ExternalMoPersonalStaff",
                    "hash_column_indexes": [],
                    "n_columns": 13,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ExternalMoPowerIndexes",
                    "hash_column_indexes": [],
                    "n_columns": 14,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ExternalMoRegDoc",
                    "hash_column_indexes": [],
                    "n_columns": 15,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ExternalPersonalEduAfterDiplom",
                    "hash_column_indexes": [],
                    "n_columns": 11,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ExternalSystemsHandbooks",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ExtraBudgetaryFunds",
                    "hash_column_indexes": [],
                    "n_columns": 11,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ExtraNoticeInfectDeseases",
                    "hash_column_indexes": [],
                    "n_columns": 12,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ExtraNoticeInfections",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ExtraNoticePoisonings",
                    "hash_column_indexes": [],
                    "n_columns": 10,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ExtraNoticeTubers",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ExtraNotices",
                    "hash_column_indexes": [],
                    "n_columns": 15,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_FAQ",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_FactCompletedPrescriptions",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_FactorBrigades",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_FactorsCondition",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_FileRepository",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_FileSetIds",
                    "hash_column_indexes": [],
                    "n_columns": 3,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_FillFieldsCall",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_FinBudgProgram",
                    "hash_column_indexes": [],
                    "n_columns": 15,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_Fines",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_Forms1",
                    "hash_column_indexes": [],
                    "n_columns": 19,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_FreeBedsByDay",
                    "hash_column_indexes": [],
                    "n_columns": 14,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_FreeBedsCountByDay",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_FreeBedsCountList",
                    "hash_column_indexes": [],
                    "n_columns": 12,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_FreeBedsCountList__log",
                    "hash_column_indexes": [],
                    "n_columns": 16,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_FuncStructureFilter",
                    "hash_column_indexes": [],
                    "n_columns": 12,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_GBDARActual",
                    "hash_column_indexes": [],
                    "n_columns": 11,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_GBDFLActual",
                    "hash_column_indexes": [],
                    "n_columns": 11,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_GBDParamActual",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_GBDYLActual",
                    "hash_column_indexes": [],
                    "n_columns": 11,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_GBDschedule",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_GenDocNum",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_GenDocNumRules",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_GovContest",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_GovContestParticipant",
                    "hash_column_indexes": [],
                    "n_columns": 13,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_GovContestParticipation",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_GovContestVacancy",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_GovPersonal",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_GovPersonalEdu",
                    "hash_column_indexes": [],
                    "n_columns": 11,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_GrafExp",
                    "hash_column_indexes": [],
                    "n_columns": 14,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_GrafExpOrg",
                    "hash_column_indexes": [],
                    "n_columns": 15,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_GrafExpOrgResult",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_GridSched",
                    "hash_column_indexes": [],
                    "n_columns": 27,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_GridSchedCancel",
                    "hash_column_indexes": [],
                    "n_columns": 13,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_GroupExamination",
                    "hash_column_indexes": [],
                    "n_columns": 16,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_GroupProf",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_GroupsUr",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_Guardianship",
                    "hash_column_indexes": [],
                    "n_columns": 11,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_GynecologicExtensionDiagnos",
                    "hash_column_indexes": [],
                    "n_columns": 20,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_HAnalysis",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_HBSubscribe",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_HBSubscribers",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_HBloodAnalisisDialysis",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_HCVBTests",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_HCVCTests",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_HIVTests",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_HImmunoResearch",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_HMedicineChest",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_HORights",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_HOUserRights",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_HOUserSettings",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_HOUsers",
                    "hash_column_indexes": [],
                    "n_columns": 11,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_HOrganizedType",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_HReactionKind",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_HSchemeCT",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_HTumorLocalizationImmunoResearch",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_HealthAndTargetGroups",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_HealthGroup",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_HealthOrganMeps",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_HistChangeRec",
                    "hash_column_indexes": [],
                    "n_columns": 12,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_HistoryAccountHealthGroup",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_HistoryAccountPost",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_HistoryMatResState",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_HistoryMoveInStac",
                    "hash_column_indexes": [],
                    "n_columns": 15,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_HistoryOperationListPost",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_HistoryRec",
                    "hash_column_indexes": [],
                    "n_columns": 10,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_HistoryRecHier",
                    "hash_column_indexes": [],
                    "n_columns": 11,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_HistoryRefListRT",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_HistoryRefPost",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_HivPersons",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_HospList",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_HospitalActives",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_HospitalConfirm",
                    "hash_column_indexes": [],
                    "n_columns": 15,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_HospitalConfirmBackup",
                    "hash_column_indexes": [],
                    "n_columns": 3,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_HospitalDirection",
                    "hash_column_indexes": [],
                    "n_columns": 32,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_HospitalDirectionBackup",
                    "hash_column_indexes": [],
                    "n_columns": 3,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_HospitalDirectionBlobs",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_HospitalDirectionConsultations",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_HospitalDirectionLocks",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_HospitalDirectionPermissions",
                    "hash_column_indexes": [],
                    "n_columns": 10,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_HospitalDirectionSP",
                    "hash_column_indexes": [],
                    "n_columns": 21,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_HospitalNotice",
                    "hash_column_indexes": [],
                    "n_columns": 32,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_HospitalNoticeBackup",
                    "hash_column_indexes": [],
                    "n_columns": 3,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_HospitalNoticeFilter",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_HospitalNumberCounters",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_HospitalRefusal",
                    "hash_column_indexes": [],
                    "n_columns": 12,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_HospitalRefusalBackup",
                    "hash_column_indexes": [],
                    "n_columns": 3,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_HospitalReplacingTypSrcFin",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_HospitalSicks",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_HospitalTherapyResult",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_HpnFilter",
                    "hash_column_indexes": [],
                    "n_columns": 14,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ICI",
                    "hash_column_indexes": [],
                    "n_columns": 15,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_IdTab",
                    "hash_column_indexes": [],
                    "n_columns": 20,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_IfaceAction",
                    "hash_column_indexes": [],
                    "n_columns": 14,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_IfaceActionGroup",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_IfaceActionParam",
                    "hash_column_indexes": [],
                    "n_columns": 13,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_IfaceEntryParam",
                    "hash_column_indexes": [],
                    "n_columns": 12,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_IfaceReAttrs",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_IfaceReqParam",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_IfaceSection",
                    "hash_column_indexes": [],
                    "n_columns": 18,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_IfaceSectionParam",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_IfaceTableList",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ImmunoHistoChemestry",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ImmunoResearchByTumorLocalization",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_InPatientAdditional",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_InPatientCoefficients",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_InPatientMepLogs",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_InPatientOutEpicrisis",
                    "hash_column_indexes": [],
                    "n_columns": 12,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_InPatients",
                    "hash_column_indexes": [],
                    "n_columns": 77,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_InPatientsLeasing",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_InPatientsNarkoData",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_InPatientsOnkoData",
                    "hash_column_indexes": [],
                    "n_columns": 43,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_InPatientsPsihData",
                    "hash_column_indexes": [],
                    "n_columns": 17,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_Inbox",
                    "hash_column_indexes": [],
                    "n_columns": 13,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_IncludeTemplate",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_IndExpSpecActive",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_Indicator",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_IndicatorSet",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_InfectionFactors",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_InfectionSources",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_InqService",
                    "hash_column_indexes": [],
                    "n_columns": 4,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_Inspection",
                    "hash_column_indexes": [],
                    "n_columns": 14,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_InspectionDiag",
                    "hash_column_indexes": [],
                    "n_columns": 14,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_InspectionHTML",
                    "hash_column_indexes": [],
                    "n_columns": 12,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_InspectionHTMLEpicrisis",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_InterfaceSet",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_IssuePreparations",
                    "hash_column_indexes": [],
                    "n_columns": 10,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_JoinElem",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_JournalVisitsMS",
                    "hash_column_indexes": [],
                    "n_columns": 14,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_K_RZD",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_KindReserv",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_LabExamination",
                    "hash_column_indexes": [],
                    "n_columns": 17,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_LaborActivity",
                    "hash_column_indexes": [],
                    "n_columns": 18,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_LicAffair",
                    "hash_column_indexes": [],
                    "n_columns": 22,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_LicAppendixList",
                    "hash_column_indexes": [],
                    "n_columns": 11,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_LicComMember",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_LicComissions",
                    "hash_column_indexes": [],
                    "n_columns": 10,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_LicDocs",
                    "hash_column_indexes": [],
                    "n_columns": 11,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_LicDocsDVer",
                    "hash_column_indexes": [],
                    "n_columns": 10,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_LicDocsData",
                    "hash_column_indexes": [],
                    "n_columns": 10,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_LicDuplicates",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_LicInfr",
                    "hash_column_indexes": [],
                    "n_columns": 4,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_LicOrgHealth",
                    "hash_column_indexes": [],
                    "n_columns": 4,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_LicPersonal",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_LinkProfActionPerformToServices",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_LinkedResultDevice",
                    "hash_column_indexes": [],
                    "n_columns": 17,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_LinkedResultOperation",
                    "hash_column_indexes": [],
                    "n_columns": 16,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_LinkedResultOperationWorkPlace",
                    "hash_column_indexes": [],
                    "n_columns": 18,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_LisCardMaterialSample",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_LisCharact",
                    "hash_column_indexes": [],
                    "n_columns": 16,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_LisCharactDataType",
                    "hash_column_indexes": [],
                    "n_columns": 4,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_LisCharactGroup",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_LisCharactType",
                    "hash_column_indexes": [],
                    "n_columns": 4,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_LisCharactValue",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_LisCheckCard",
                    "hash_column_indexes": [],
                    "n_columns": 11,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_LisCheckCardLot",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_LisCheckCardStatus",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_LisControlMaterial",
                    "hash_column_indexes": [],
                    "n_columns": 12,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_LisControlMaterialLot",
                    "hash_column_indexes": [],
                    "n_columns": 11,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_LisControlMaterialResult",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_LisDeviceDescription",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_LisExServItems",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_LisLimitIndices",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_LisOrder",
                    "hash_column_indexes": [],
                    "n_columns": 14,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_LisOrderMaterial",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_LisOrderMaterialSample",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_LisOrderMaterialSampleStatusHistory",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_LisOrderOperation",
                    "hash_column_indexes": [],
                    "n_columns": 11,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_LisOrderResult",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_LisOrderStatusHistory",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_LisWorkPlace",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_LisWorkPlaceStruct",
                    "hash_column_indexes": [],
                    "n_columns": 12,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ListAdditData",
                    "hash_column_indexes": [],
                    "n_columns": 10,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ListNotValidCertificate",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ListPersonPlanEdu",
                    "hash_column_indexes": [],
                    "n_columns": 11,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ListPlanGrafik",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_LoadProtocolErrors",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_LoadProtocols",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_LoadingProviderMeta",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_Localities",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_LocalityAddresses",
                    "hash_column_indexes": [],
                    "n_columns": 11,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_LocalityDoctors",
                    "hash_column_indexes": [],
                    "n_columns": 10,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_LockGridSched",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_LogOperationDict",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_LogOperationRec",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_MOActives",
                    "hash_column_indexes": [],
                    "n_columns": 22,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_MODevices",
                    "hash_column_indexes": [],
                    "n_columns": 12,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_MODevicesServ",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_MODevicesServCharact",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_MOImages",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_MORegistr",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_MOSettings",
                    "hash_column_indexes": [],
                    "n_columns": 11,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_MOforExp",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_MSSServices",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_MainInfo",
                    "hash_column_indexes": [],
                    "n_columns": 10,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_MainSickForExtended",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_Married",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_MatGrExamAssociate",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_MatNum",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_MaterialResultValues",
                    "hash_column_indexes": [],
                    "n_columns": 10,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_MaterialResultValuesDefault",
                    "hash_column_indexes": [],
                    "n_columns": 10,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_MeasureParamNaznExec",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_MedDeathReg",
                    "hash_column_indexes": [],
                    "n_columns": 27,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_MedDocNum",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_MedDocs",
                    "hash_column_indexes": [],
                    "n_columns": 14,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_MedProdDoseCorrect",
                    "hash_column_indexes": [],
                    "n_columns": 14,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_MedProdExecTemplate",
                    "hash_column_indexes": [],
                    "n_columns": 27,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_MedProdExecVariant",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_MedProdGroup",
                    "hash_column_indexes": [],
                    "n_columns": 16,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_MedProdItem",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_MedProdPeriodicityTemplate",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_MedProduct",
                    "hash_column_indexes": [],
                    "n_columns": 32,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_MedProductReserves",
                    "hash_column_indexes": [],
                    "n_columns": 4,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_MedProductTemplate",
                    "hash_column_indexes": [],
                    "n_columns": 18,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_MedProducts",
                    "hash_column_indexes": [],
                    "n_columns": 11,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_MedRegAddData",
                    "hash_column_indexes": [],
                    "n_columns": 34,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_MedRegAddDataMother",
                    "hash_column_indexes": [],
                    "n_columns": 24,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_MedicalOrganBudgetaryPrograms",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_MedicalOrganCommunals",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_MedicalOrganMSSServices",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_MedicalOrganPlanValues",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_MedicalResearch",
                    "hash_column_indexes": [],
                    "n_columns": 13,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_MedstatFormedReports",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_MedstatReportColumnNames",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_MedstatReportColumns",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_MedstatReportDataArray",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_MedstatReportGraphNames",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_MedstatReportGraphs",
                    "hash_column_indexes": [],
                    "n_columns": 4,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_MedstatReportNames",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_MedstatReportRowNames",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_MedstatReportRowNames_2017",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_MedstatReportRows",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_MedstatReports",
                    "hash_column_indexes": [],
                    "n_columns": 4,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_MepAnaestDrugs",
                    "hash_column_indexes": [],
                    "n_columns": 11,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_MepDrugPrice",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_MepDrugRelationByCardType",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_MepDrugs",
                    "hash_column_indexes": [],
                    "n_columns": 15,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_MepFactAnaestDrugs",
                    "hash_column_indexes": [],
                    "n_columns": 11,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_MepFactDialysisInfo",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_MepFactDrugs",
                    "hash_column_indexes": [],
                    "n_columns": 11,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_MepFactIntensiveDrugs",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_MepFactServs",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_MepFactSurgProc",
                    "hash_column_indexes": [],
                    "n_columns": 20,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_MepFactSurgProcDrugs",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_MepOperations",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_MepServAndMepDrugRelationByCardType",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_MepServCountDialysis",
                    "hash_column_indexes": [],
                    "n_columns": 13,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_MepServRelationByCardType",
                    "hash_column_indexes": [],
                    "n_columns": 11,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_MepServs",
                    "hash_column_indexes": [],
                    "n_columns": 11,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_MepSicks",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_MepSurgProcDrugs",
                    "hash_column_indexes": [],
                    "n_columns": 11,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_MepSurgProcs",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_Meps",
                    "hash_column_indexes": [],
                    "n_columns": 26,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_MepsServAssociate",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_MepsServPrice",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_MepsSickDayClinic",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_MepsSurgProcAssociate",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_MepsSurgProcForPayTypes",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_MethodConfirmDiag",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_MethodRadiationTherapy",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_MissingStatusPerson",
                    "hash_column_indexes": [],
                    "n_columns": 11,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_MoChemotherapyReg",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_MoDirectionSettings",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_MoDrgBaseRates",
                    "hash_column_indexes": [],
                    "n_columns": 11,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_MoEkoSettings",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_MoRegionalBudget",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_MoRepublicanBudgetBaseRate",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_MoSmpSettings",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_MoVsmpSettings",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_MoWorkingByTS",
                    "hash_column_indexes": [],
                    "n_columns": 4,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ModuleAccess",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_MonPeriods",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_MonitoringTemplate",
                    "hash_column_indexes": [],
                    "n_columns": 13,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_MonitoringTemplateRecs",
                    "hash_column_indexes": [],
                    "n_columns": 12,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_MonthType",
                    "hash_column_indexes": [],
                    "n_columns": 4,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_MorbidEventResponses",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_MoveInStac",
                    "hash_column_indexes": [],
                    "n_columns": 11,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_MoveInStacStateTimeSetting",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_NarcoDirectionForUse",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_NarcoExtensionDiagnos",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_NativeReportRight",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_NativeReports",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_NaznExec",
                    "hash_column_indexes": [],
                    "n_columns": 33,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_NaznExecProperty",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_NaznExecToFirstNazn",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_NaznMaterial",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_NaznNotExec",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_NaznSheetState",
                    "hash_column_indexes": [],
                    "n_columns": 12,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_NaznTeam",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_NeedHistory",
                    "hash_column_indexes": [],
                    "n_columns": 24,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_Needs",
                    "hash_column_indexes": [],
                    "n_columns": 25,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_NewBornActives",
                    "hash_column_indexes": [],
                    "n_columns": 15,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_NewBornData",
                    "hash_column_indexes": [],
                    "n_columns": 28,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_NewBornDiagnoses",
                    "hash_column_indexes": [],
                    "n_columns": 10,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_NewBornInfos",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_NewbornAidKit",
                    "hash_column_indexes": [],
                    "n_columns": 15,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_NextStageRebilitationSettings",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_Nlinks",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_NoSubordinateMO",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_Norm",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_NormCalcRule",
                    "hash_column_indexes": [],
                    "n_columns": 11,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_NormPost",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_NormResult",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_NormSet",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_NormTimeCom",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_NormTimeExp",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_NormValue",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_NormValuePart",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_NormValuePartRule",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_NotWorkDays",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_NoticeTargetHSickMepsSurgProc",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_Notics",
                    "hash_column_indexes": [],
                    "n_columns": 12,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_NumCases",
                    "hash_column_indexes": [],
                    "n_columns": 4,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_NumHist",
                    "hash_column_indexes": [],
                    "n_columns": 4,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_NumResult",
                    "hash_column_indexes": [],
                    "n_columns": 4,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ObjectAttrSpec",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ObjectProps",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ObjectVariant",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_Obstetrics",
                    "hash_column_indexes": [],
                    "n_columns": 17,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_OncoDispansersList",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_OncohematologicalSicksAcuteFormDiagnoses",
                    "hash_column_indexes": [],
                    "n_columns": 4,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_OncologyExtensionDiagnos",
                    "hash_column_indexes": [],
                    "n_columns": 14,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_OncologyExtensionDiagnosMetastasisLocalization",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_OnlineCountersForPatientsAdmissionReg",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_OnlineCountersForWaitList",
                    "hash_column_indexes": [],
                    "n_columns": 12,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_OperationAverageInfo",
                    "hash_column_indexes": [],
                    "n_columns": 11,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_OperationResult",
                    "hash_column_indexes": [],
                    "n_columns": 12,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_OperationRulesCheckCard",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_OperationStatus",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_OperationsBySicks",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_OperationsCanNotBeMain",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_OrderResultStatus",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_OrgHealthCareBedProfiles",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_OrgHealthCareCodes",
                    "hash_column_indexes": [],
                    "n_columns": 10,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_OrgHealthCareDayHospitalCardTypes",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_OrgHealthCareGroupForBpHelpTypes",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_OrgHealthCareNoticeTargets",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_OrgHealthCareVariants",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_OrgHealthSettings",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_OrgTechnika",
                    "hash_column_indexes": [],
                    "n_columns": 11,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_Otkaz",
                    "hash_column_indexes": [],
                    "n_columns": 11,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_OtkazReg",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_OutBox",
                    "hash_column_indexes": [],
                    "n_columns": 14,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_OutCasesInformationRules",
                    "hash_column_indexes": [],
                    "n_columns": 11,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_OutPatients",
                    "hash_column_indexes": [],
                    "n_columns": 17,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_OuzTemplates",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_PAccessLocalMO",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_PAddress",
                    "hash_column_indexes": [],
                    "n_columns": 12,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_PDeathReg",
                    "hash_column_indexes": [],
                    "n_columns": 47,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_PDeathRegCode",
                    "hash_column_indexes": [],
                    "n_columns": 40,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_PDeathRegComplicatedDeliveries",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_PDeathRegCriteriaBirth",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_PDeathRegCriteriaBirths",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_PDeathRegDisAndCompBefore",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_PDeathRegDisAndCompDuring",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_PDeathRegOperationalBenefits",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_PDeathRegOtherAccompaniedState",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_PDeathRegOtherDiseaseChild",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_PDeathRegOtherDiseasesMother",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_PDoc",
                    "hash_column_indexes": [],
                    "n_columns": 16,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_PSoc",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_PSocDisability",
                    "hash_column_indexes": [],
                    "n_columns": 10,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_PWork",
                    "hash_column_indexes": [],
                    "n_columns": 11,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ParamDataAgrExpr",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ParamDataExpr",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_PartogrammValues",
                    "hash_column_indexes": [],
                    "n_columns": 11,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_Partogramms",
                    "hash_column_indexes": [],
                    "n_columns": 13,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_PatientAdmissionDrugs",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_PatientAdmissionServices",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_PatientContacts",
                    "hash_column_indexes": [],
                    "n_columns": 10,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_PatientDaybook",
                    "hash_column_indexes": [],
                    "n_columns": 14,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_PatientFines",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_PatientTrainings",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_Patients",
                    "hash_column_indexes": [],
                    "n_columns": 22,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_PatientsAdmissionRefuse",
                    "hash_column_indexes": [],
                    "n_columns": 3,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_PatientsAdmissionRegister",
                    "hash_column_indexes": [],
                    "n_columns": 83,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_PatientsInvitation",
                    "hash_column_indexes": [],
                    "n_columns": 14,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_PatternForExternalSystems",
                    "hash_column_indexes": [],
                    "n_columns": 11,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_PaymentScaleFilter",
                    "hash_column_indexes": [],
                    "n_columns": 14,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_PaymentScaleOrgHealthCare",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_Pemail",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_PerfomedDrugsExclude",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_PerfomedServicesExclude",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_Performers",
                    "hash_column_indexes": [],
                    "n_columns": 11,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_PeriodFormer",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_PeriodMonth",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_PeriodType",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_Periodicity",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_PeriodicityRules",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_PeriodicityRules_Group",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_PeriodicityRules_Reg",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_PeriodicityTemplate",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_PermittedDrugsForHospitals",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_PermittedOperationsForHospitals",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_PermittedServicesForHospitals",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_PersInStac",
                    "hash_column_indexes": [],
                    "n_columns": 21,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_Person",
                    "hash_column_indexes": [
                        8,
                        9,
                        25
                    ],
                    "n_columns": 29,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_PersonCache",
                    "hash_column_indexes": [],
                    "n_columns": 17,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_PersonIdentification",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_PersonPhones",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_PersonPlanEdu",
                    "hash_column_indexes": [],
                    "n_columns": 10,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_PersonSched",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_PersonSchedTemplate",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_PersonalInfo",
                    "hash_column_indexes": [],
                    "n_columns": 16,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_PersonalLogin",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_PersonalTabel",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_PersonsInGroup",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_Phone",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_Phones",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_PlaceCall",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_PlanGrafik",
                    "hash_column_indexes": [],
                    "n_columns": 12,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_PlanPerformReg",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_PlanPower",
                    "hash_column_indexes": [],
                    "n_columns": 10,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_PolyPpmrs",
                    "hash_column_indexes": [],
                    "n_columns": 19,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_PolyclinicActives",
                    "hash_column_indexes": [],
                    "n_columns": 4,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_PositiveFluorographyActives",
                    "hash_column_indexes": [],
                    "n_columns": 4,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_PostInfoCache",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_PostLoginRelation",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_PostSpeciality",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_PostgraduateEducation",
                    "hash_column_indexes": [],
                    "n_columns": 20,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_PovodsLinks",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_Predicate",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_PreferVariantService",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_Pregnancies",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_PreparationActualBalances",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_PreparationComings",
                    "hash_column_indexes": [],
                    "n_columns": 4,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_PreparationIssues",
                    "hash_column_indexes": [],
                    "n_columns": 10,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_Preparations",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_PrepareSurv",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_Prescription",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_PresentList",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_PretendProfSkill",
                    "hash_column_indexes": [],
                    "n_columns": 10,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_PrevPregnancyEnd",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_PriborGroup",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_PriborItem",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_PriorityReplaceBrigades",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_PrivateSicks",
                    "hash_column_indexes": [],
                    "n_columns": 4,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ProducedDrugs",
                    "hash_column_indexes": [],
                    "n_columns": 4,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ProfAction",
                    "hash_column_indexes": [],
                    "n_columns": 22,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ProfActionNotices",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ProfActionPerfGroup",
                    "hash_column_indexes": [],
                    "n_columns": 17,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ProfActionPerfReg",
                    "hash_column_indexes": [],
                    "n_columns": 17,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ProfActionPerfTime_Group",
                    "hash_column_indexes": [],
                    "n_columns": 23,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ProfActionPerfTime_Reg",
                    "hash_column_indexes": [],
                    "n_columns": 24,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ProfActionPerform",
                    "hash_column_indexes": [],
                    "n_columns": 14,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ProfActionPerformReg",
                    "hash_column_indexes": [],
                    "n_columns": 20,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ProfActionPerformTotal",
                    "hash_column_indexes": [],
                    "n_columns": 11,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ProfActionPeriodicity_Group",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ProfActionPeriodicity_Reg",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ProfActions",
                    "hash_column_indexes": [],
                    "n_columns": 13,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ProfCallTemplate",
                    "hash_column_indexes": [],
                    "n_columns": 10,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ProphExecuteAccount",
                    "hash_column_indexes": [],
                    "n_columns": 11,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ProphMeasure",
                    "hash_column_indexes": [],
                    "n_columns": 30,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_Prophylactics",
                    "hash_column_indexes": [],
                    "n_columns": 10,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ProtDiagnos",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ProtNazn",
                    "hash_column_indexes": [],
                    "n_columns": 12,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ProtServices",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_Protocol",
                    "hash_column_indexes": [],
                    "n_columns": 25,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ProtocolAmbulatory",
                    "hash_column_indexes": [],
                    "n_columns": 16,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ProtocolAmbulatoryServ",
                    "hash_column_indexes": [],
                    "n_columns": 11,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ProtocolCriterion",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ProtocolHosp",
                    "hash_column_indexes": [],
                    "n_columns": 23,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ProtocolHospCriterion",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ProtocolHospOper",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ProtocolHospServ",
                    "hash_column_indexes": [],
                    "n_columns": 11,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ProtocolHospServMin",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ProtocolOper",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ProtocolServ",
                    "hash_column_indexes": [],
                    "n_columns": 12,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ProtocolServMin",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_Protocols",
                    "hash_column_indexes": [],
                    "n_columns": 11,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ProtocolsInteractionsDevice",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ProvisionHealthServicesToCountriesByDiagnoses",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_Purpose",
                    "hash_column_indexes": [],
                    "n_columns": 16,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_PurposeOrderNum",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_QGRParam",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_QGRParamField",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_QGenReportList",
                    "hash_column_indexes": [],
                    "n_columns": 17,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_QuarterMonthMapping",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_QueryField",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_QueryInfo",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_QueryParam",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_QueryUnit",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_Question",
                    "hash_column_indexes": [],
                    "n_columns": 10,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_Questionnaire",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_Questions",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_QuestionsList",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_QueueLinker",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_QueueOfHosp",
                    "hash_column_indexes": [],
                    "n_columns": 20,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_QuickPreciseAddressSym",
                    "hash_column_indexes": [],
                    "n_columns": 12,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_RWTests",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ReactionOnVaccine",
                    "hash_column_indexes": [],
                    "n_columns": 26,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ReasonOfSickDeathRate",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ReasonRemovAccount",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_RecessSched",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_RecessSchedTemplate",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_RecipientActives",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_RecordCreator",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ReferenceResult",
                    "hash_column_indexes": [],
                    "n_columns": 14,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_RegCandidates",
                    "hash_column_indexes": [],
                    "n_columns": 35,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_RegCandidatesPeriod",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_RegComplOp",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_RegMainOp",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_RegMaterial",
                    "hash_column_indexes": [],
                    "n_columns": 18,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_RegProfAction",
                    "hash_column_indexes": [],
                    "n_columns": 21,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_RegionContact",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_RegionsCommunals",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ReglamentExpr",
                    "hash_column_indexes": [],
                    "n_columns": 15,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ReglamentExprF",
                    "hash_column_indexes": [],
                    "n_columns": 15,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ReglamentExprM",
                    "hash_column_indexes": [],
                    "n_columns": 15,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ReglamentForm",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ReglamentParam",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ReglamentParamF",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ReglamentParamM",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ReglamentReport",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_RehabilitMoList",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_RehabilitSolveBedProfiles",
                    "hash_column_indexes": [],
                    "n_columns": 4,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_RehabilitSolveSicks",
                    "hash_column_indexes": [],
                    "n_columns": 4,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_RelationDicts",
                    "hash_column_indexes": [],
                    "n_columns": 10,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_RelationStates",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_RelationStreet",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_Rep_hMDocType",
                    "hash_column_indexes": [],
                    "n_columns": 16,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_RepeatParam",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ReplacMedProduct",
                    "hash_column_indexes": [],
                    "n_columns": 18,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_Report",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ReportAccess",
                    "hash_column_indexes": [],
                    "n_columns": 4,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ReportAgr",
                    "hash_column_indexes": [],
                    "n_columns": 4,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ReportForm",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ReportFormType",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ReportMonthPeriodStep",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ReportParam",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ReportParamAgr",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ReportParamAgrExpr",
                    "hash_column_indexes": [],
                    "n_columns": 16,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ReportParamExpr",
                    "hash_column_indexes": [],
                    "n_columns": 16,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ReportPeriod",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ReportPeriodByMO",
                    "hash_column_indexes": [],
                    "n_columns": 13,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ReportPeriodCalendar",
                    "hash_column_indexes": [],
                    "n_columns": 16,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ReportRule",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ReportSection",
                    "hash_column_indexes": [],
                    "n_columns": 13,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ReportSectionParam",
                    "hash_column_indexes": [],
                    "n_columns": 10,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ReportTable",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ReqVariantService",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_RequestHistory",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_RequestRecords",
                    "hash_column_indexes": [],
                    "n_columns": 24,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_Requirements",
                    "hash_column_indexes": [],
                    "n_columns": 14,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ResearchServItem",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_Reserve",
                    "hash_column_indexes": [],
                    "n_columns": 17,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ReserveDoseTypes",
                    "hash_column_indexes": [],
                    "n_columns": 4,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_Reserves",
                    "hash_column_indexes": [],
                    "n_columns": 18,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ResponPrevent",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ResponProfAction",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_RestrictSickForExtSystem",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ResultAction",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ResultConfirmation",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ResultDelivery",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ResultsResearches",
                    "hash_column_indexes": [],
                    "n_columns": 23,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_RoomGroup",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_RoomGroupPost",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_RoomItem",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_RowHeader",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_RowIDs",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_RowList",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_Rubrica",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_RuleCalcPost",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_RuleForming",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_SCAddWidget",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_SCBufferParam",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_SCConditionParam",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_SCEntryParam",
                    "hash_column_indexes": [],
                    "n_columns": 11,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_SCEntryPoint",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_SCFillFields",
                    "hash_column_indexes": [],
                    "n_columns": 14,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_SCIParameters",
                    "hash_column_indexes": [],
                    "n_columns": 15,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_SCIQGenReports",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_SCIScenario",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_SCOrderOf",
                    "hash_column_indexes": [],
                    "n_columns": 19,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_SCOutPutParam",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_SCRegistr",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_SECancel",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_SESActives",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ST_Age",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ST_CharactServices",
                    "hash_column_indexes": [],
                    "n_columns": 12,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ST_MaterialRequirement",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ST_MultiServ",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ST_OperationComposition",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ST_ResultConfirmation",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ST_ResultRequirement",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ST_ServicesInTemplate",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ST_TariffEqual",
                    "hash_column_indexes": [],
                    "n_columns": 15,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_SanTechIndex",
                    "hash_column_indexes": [],
                    "n_columns": 13,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_SavedCharactSet",
                    "hash_column_indexes": [],
                    "n_columns": 16,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_SchedWork",
                    "hash_column_indexes": [],
                    "n_columns": 23,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_SchedWorkTemplate",
                    "hash_column_indexes": [],
                    "n_columns": 10,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ScreeningExaminations",
                    "hash_column_indexes": [],
                    "n_columns": 57,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_SecColumn",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_SecRow",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_SecTab",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_SelCriteria",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_SelCriteriaDEParams",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_SelCriteriaGroups",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_SelectedFactorsBirth",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_SelectedFactorsPDeath",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_SendCardByFinTypes",
                    "hash_column_indexes": [],
                    "n_columns": 4,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_SendMedicalOrgans",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ServActualExpenses",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ServAndSurgProcVSMP",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ServAndSurgProcVSMP_DS",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ServApplicationsJournal",
                    "hash_column_indexes": [],
                    "n_columns": 21,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ServApplicationsJournalHistory",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ServExec",
                    "hash_column_indexes": [],
                    "n_columns": 30,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ServExecTemplate",
                    "hash_column_indexes": [],
                    "n_columns": 27,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ServExecVariant",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ServGroup",
                    "hash_column_indexes": [],
                    "n_columns": 17,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ServItem",
                    "hash_column_indexes": [],
                    "n_columns": 11,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ServMaterialAssociate",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ServRegisterAssociate",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ServiceStandarts",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ServiceTemplate",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ServiceValues",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_Services",
                    "hash_column_indexes": [],
                    "n_columns": 31,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ServicesGrouped",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ServicesLink",
                    "hash_column_indexes": [],
                    "n_columns": 11,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_SetExp",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_SettingsHHospitalizedOtkazType",
                    "hash_column_indexes": [],
                    "n_columns": 11,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ShedExec",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ShedExecStatus",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_SheetTransfer",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_SickAndMepDrugRelationByCardType",
                    "hash_column_indexes": [],
                    "n_columns": 11,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_SickAndMepServRelationByCardType",
                    "hash_column_indexes": [],
                    "n_columns": 11,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_SickAndSurgProcRelationByCardType",
                    "hash_column_indexes": [],
                    "n_columns": 10,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_SickExclusive",
                    "hash_column_indexes": [],
                    "n_columns": 4,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_SickGroup",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_SickGroupComp",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_SickGroupForBpHelpTypes",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_SickList",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_SickMepsSurgProc",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_SicksByDoubleCoding",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_SicksWithTraumaTypeSettings",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_SideEffectsChemoTherapy",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_SideEffectsHormonoTherapy",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_SideEffectsImmunoTherapy",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_SideEffectsTIBR",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_SideEffectsTT",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_SignInterAndMepDrugRelationByCardType",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_SignificantInterferences",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_SittingList",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_SocialStatuses",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_SpecialEquipments",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_SpecialistGroup",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_SpecialistItem",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_SpecificService",
                    "hash_column_indexes": [],
                    "n_columns": 16,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_SrcFinForExternalSystems",
                    "hash_column_indexes": [],
                    "n_columns": 10,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_StacPpmrs",
                    "hash_column_indexes": [],
                    "n_columns": 28,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_StandartOperation",
                    "hash_column_indexes": [],
                    "n_columns": 15,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_StatFAQ",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_StateEkoSettings",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_StateSettings",
                    "hash_column_indexes": [],
                    "n_columns": 11,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_StatementFrom",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_StatementFromExpr",
                    "hash_column_indexes": [],
                    "n_columns": 14,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_StayOutcomeAccount",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_StiDataReport",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_StiReport",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_StiReportParam",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_StructTerritorService",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_StudentAttachHistory",
                    "hash_column_indexes": [],
                    "n_columns": 13,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_StudentGroup",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_StudentJobPlace",
                    "hash_column_indexes": [],
                    "n_columns": 13,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_SubordinationGroupForBpHelpTypes",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_SurgProcActualExpenses",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_SurgProcAndMepDrugRelationByCardType",
                    "hash_column_indexes": [],
                    "n_columns": 10,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_SurgProcAndMepServRelationByCardType",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_SurgProcCommonCosts",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_SurgProcComplications",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_SurgicalOperations",
                    "hash_column_indexes": [],
                    "n_columns": 11,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_SurgicalProcedures",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_SvidOrg",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_TBTests",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_TGCard",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_TGCardPregnant",
                    "hash_column_indexes": [],
                    "n_columns": 28,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_TGCardScreening",
                    "hash_column_indexes": [],
                    "n_columns": 67,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_TabFilter",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_TabFilterLink",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_TabRule",
                    "hash_column_indexes": [],
                    "n_columns": 12,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_Tabtable",
                    "hash_column_indexes": [],
                    "n_columns": 77,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_TargetGroup",
                    "hash_column_indexes": [],
                    "n_columns": 19,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_TargetGroupCharacts",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_TariffTable",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_Tarificator",
                    "hash_column_indexes": [],
                    "n_columns": 20,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_TarificatorCoefficients",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_TarificatorCosts",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_Tarifikator",
                    "hash_column_indexes": [],
                    "n_columns": 10,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_TempHospitalNotice",
                    "hash_column_indexes": [],
                    "n_columns": 18,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_TemperList",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_TemperListValues",
                    "hash_column_indexes": [],
                    "n_columns": 16,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_TemplateAttributes",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_TemplateAttributesDescription",
                    "hash_column_indexes": [],
                    "n_columns": 10,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_TemplateAttributesParams",
                    "hash_column_indexes": [],
                    "n_columns": 12,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_TemplateAttributesSet",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_TemplateObject",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_TemplateResearch",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_TemplateSetAttributes",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_TemplateSetAttributesSet",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_Temporary",
                    "hash_column_indexes": [],
                    "n_columns": 3,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_TerritoryService",
                    "hash_column_indexes": [],
                    "n_columns": 14,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_TerritoryServiceMO",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_TestGroup",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_TestPerson",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_TestTemplate",
                    "hash_column_indexes": [],
                    "n_columns": 14,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_TestTheme",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_TestVariant",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_TimeTable",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_Transp",
                    "hash_column_indexes": [],
                    "n_columns": 17,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_TreatmentOncologicalPatient",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_TreatmentOthers",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_TreeDecisions",
                    "hash_column_indexes": [],
                    "n_columns": 16,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_TypeFStruct",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_TypeMedicalFacilities",
                    "hash_column_indexes": [],
                    "n_columns": 13,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_TypePost",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_TypeRadiationTherapy",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_TypeRoomFStruct",
                    "hash_column_indexes": [],
                    "n_columns": 17,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_TypeUseDrugsNarko",
                    "hash_column_indexes": [],
                    "n_columns": 10,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_TypesOfMedicalCare",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_UKLCalc",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_UKLSettings",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ULNomenclatureRel",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_UnionAddressesLog",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_UnknownPersonMaxNumber",
                    "hash_column_indexes": [],
                    "n_columns": 4,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_UnspecSicks",
                    "hash_column_indexes": [],
                    "n_columns": 4,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_UsedDevices",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_UsedDrugs",
                    "hash_column_indexes": [],
                    "n_columns": 17,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_UsedEquipment",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_UsedGoods",
                    "hash_column_indexes": [],
                    "n_columns": 20,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_UserActions",
                    "hash_column_indexes": [],
                    "n_columns": 11,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_UserGuide",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_UserSettings",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_Uz",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_VISErrorMessage",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_VISLog",
                    "hash_column_indexes": [],
                    "n_columns": 14,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_VISLogHistory",
                    "hash_column_indexes": [],
                    "n_columns": 14,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_VISRepl",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_VacationAttributes",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_Vaccination",
                    "hash_column_indexes": [],
                    "n_columns": 38,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ValidDrugsByAnaest",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ValidSocTypeByAge",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ValidateErrors",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ValidateForm066",
                    "hash_column_indexes": [],
                    "n_columns": 12,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ValidateLogs",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ValidateTypes",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ValidationHistory",
                    "hash_column_indexes": [],
                    "n_columns": 10,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ValidationResultInfo",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_Variable",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_VariantDataCommunications",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_VariantDataCommunicationsDevice",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_VedStudJobPlace",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_VedZamPost",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_Visits",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_Voting",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_VsmpDirections",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_VsmpMeps",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_VsmpServs",
                    "hash_column_indexes": [],
                    "n_columns": 4,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_WorkHistory",
                    "hash_column_indexes": [],
                    "n_columns": 10,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_WorkPeriod",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_WorkType",
                    "hash_column_indexes": [],
                    "n_columns": 4,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_WriteOffActs",
                    "hash_column_indexes": [],
                    "n_columns": 14,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_WriteOffPreparation",
                    "hash_column_indexes": [],
                    "n_columns": 11,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_academicDegree",
                    "hash_column_indexes": [],
                    "n_columns": 12,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_academicStatus",
                    "hash_column_indexes": [
                        3,
                        7
                    ],
                    "n_columns": 11,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_actapp_param",
                    "hash_column_indexes": [
                        4
                    ],
                    "n_columns": 11,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_addEducation",
                    "hash_column_indexes": [
                        9,
                        18
                    ],
                    "n_columns": 23,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_addInformationCasesBeg",
                    "hash_column_indexes": [],
                    "n_columns": 21,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_adr_FirstObject",
                    "hash_column_indexes": [
                        7,
                        8,
                        10
                    ],
                    "n_columns": 20,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_adr_Geonim",
                    "hash_column_indexes": [],
                    "n_columns": 14,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_adr_ObjectHistory",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_adr_Regions",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_adr_SecondaryObject",
                    "hash_column_indexes": [],
                    "n_columns": 22,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_adr_TerritoryUnit",
                    "hash_column_indexes": [],
                    "n_columns": 19,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_adr_TerritotyUnitIndx",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ar_ATU",
                    "hash_column_indexes": [],
                    "n_columns": 55,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ar_ByElement",
                    "hash_column_indexes": [],
                    "n_columns": 39,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ar_ByIdSettlement",
                    "hash_column_indexes": [],
                    "n_columns": 15,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ar_hcategorybuilding",
                    "hash_column_indexes": [],
                    "n_columns": 13,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ar_htypeate",
                    "hash_column_indexes": [],
                    "n_columns": 14,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ar_htypeconstruction",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_ar_htypegeonim",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_bedProfileHistory",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_bedSrcFinHistory",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_bedStacTypeHistory",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_bookValue",
                    "hash_column_indexes": [],
                    "n_columns": 11,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_building",
                    "hash_column_indexes": [],
                    "n_columns": 17,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_calcCom",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_calcExp",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_cds_CheckRegisters",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_cds_FinalDiagnoses",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_cds_Forms1",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_countServicesLeasing",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_defendant",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_department",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_disability",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_equipmentLeasing",
                    "hash_column_indexes": [],
                    "n_columns": 10,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_faculty",
                    "hash_column_indexes": [],
                    "n_columns": 10,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_funcStructure",
                    "hash_column_indexes": [],
                    "n_columns": 25,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_gbl_document",
                    "hash_column_indexes": [],
                    "n_columns": 24,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_gbl_person",
                    "hash_column_indexes": [],
                    "n_columns": 57,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_gbl_person_log",
                    "hash_column_indexes": [],
                    "n_columns": 14,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_gbl_work_log",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hAK",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hAPPCategoriesExempts",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hAPPColonoskopyResult",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hAPPHealthGroup",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hAPPListCardTypePeriod",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hAPPMammographyResultsEx",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hAPPProctosigmoidoscopeResultEx",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hAPPReasonRemoveAccount",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hAPPScreeningValuesEx",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hAPPSmearResults",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hAPPStateHealth",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hAPPType25Card",
                    "hash_column_indexes": [],
                    "n_columns": 10,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hAPPTypeAccount",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hAbortionType",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hAccountMod",
                    "hash_column_indexes": [],
                    "n_columns": 10,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hAccountStatuses",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hAccrComType",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hAccrResult",
                    "hash_column_indexes": [],
                    "n_columns": 10,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hAccuracyType",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hActDirection",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hActExecutor",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hActGroup",
                    "hash_column_indexes": [],
                    "n_columns": 11,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hActMDocType",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hActObject",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hActObligat",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hActPlace",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hActResponse",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hActTimeExecut",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hActType",
                    "hash_column_indexes": [],
                    "n_columns": 10,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hAction",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hActionType",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hActionTypeFO",
                    "hash_column_indexes": [],
                    "n_columns": 11,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hActivType",
                    "hash_column_indexes": [],
                    "n_columns": 15,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hActiveStates",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hActiveStatuses",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hActivityMark",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hActivitySysType",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hAddPersonSources",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hAddressActualType",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hAddressType",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hAddressTypes",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hAdmissionToThisHospital",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hAdressLevel",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hAffairStatus",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hAgeCriteria",
                    "hash_column_indexes": [],
                    "n_columns": 11,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hAgeGroups",
                    "hash_column_indexes": [],
                    "n_columns": 11,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hAgeHSick",
                    "hash_column_indexes": [],
                    "n_columns": 11,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hAgeSettings",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hAgeUnknownPatients",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hAlcoholVolumes",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hAllergens",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hAllergicReaction",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hAllianceType",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hAmnioticFluidTypes",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hAnaesthesia",
                    "hash_column_indexes": [],
                    "n_columns": 11,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hAnswerType",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hApComResult",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hArrivalType",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hAssignReason",
                    "hash_column_indexes": [],
                    "n_columns": 10,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hAteTypes",
                    "hash_column_indexes": [],
                    "n_columns": 10,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hAttachPMSPType",
                    "hash_column_indexes": [],
                    "n_columns": 11,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hAttachStatus",
                    "hash_column_indexes": [],
                    "n_columns": 11,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hBDO",
                    "hash_column_indexes": [],
                    "n_columns": 11,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hBDeathType",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hBaseAccount",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hBedProfile",
                    "hash_column_indexes": [],
                    "n_columns": 13,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hBedProfileCodesString",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hBedProfileFilterTypes",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hBedProfileService",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hBedProfileType",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hBedsWorkNormatives",
                    "hash_column_indexes": [],
                    "n_columns": 4,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hBioSex",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hBirthTypes",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hBloodGroups",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hBookingAges",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hBpHelpTypes",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hBudgetProgramAdmins",
                    "hash_column_indexes": [],
                    "n_columns": 10,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hBudgetProgramList",
                    "hash_column_indexes": [],
                    "n_columns": 12,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hBudgetProgramSpecifics",
                    "hash_column_indexes": [],
                    "n_columns": 11,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hBudgetRates",
                    "hash_column_indexes": [],
                    "n_columns": 10,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hBudgetSubProgramList",
                    "hash_column_indexes": [],
                    "n_columns": 13,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hBudgetaryPrograms",
                    "hash_column_indexes": [],
                    "n_columns": 12,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hBuildingType",
                    "hash_column_indexes": [],
                    "n_columns": 11,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hCPlases",
                    "hash_column_indexes": [],
                    "n_columns": 13,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hCPovod",
                    "hash_column_indexes": [],
                    "n_columns": 11,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hCType",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hCVid",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hCalcRuleInterp",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hCallasType",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hCallingEndTypes",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hCallingStatuses",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hCancerStage",
                    "hash_column_indexes": [],
                    "n_columns": 10,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hCapableStatus",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hCapacityWork",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hCardTG",
                    "hash_column_indexes": [],
                    "n_columns": 10,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hCatSick",
                    "hash_column_indexes": [],
                    "n_columns": 10,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hCategCare",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hCategories",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hCategoryPost",
                    "hash_column_indexes": [],
                    "n_columns": 10,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hCauseAbsence",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hCauseOfMedProductChange",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hCauseOfServChange",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hCauseOfServNonExec",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hCauseTak",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hCausesOfAttach",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hCharBirth",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hCharSick",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hCharactTemplateEditMode",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hCheckRegisterProcessingStatuses",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hChildBornType",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hChildDeathTypes",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hCholesterolLevels",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hCitizenship",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hCityCtg",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hClSocType",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hClaimStatuses",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hClaimTypes",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hClassJobDsc",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hClassSick",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hCodFStruct",
                    "hash_column_indexes": [],
                    "n_columns": 10,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hCoefficient",
                    "hash_column_indexes": [],
                    "n_columns": 12,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hCoefficientTypes",
                    "hash_column_indexes": [],
                    "n_columns": 10,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hCommRecomend",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hCommResult",
                    "hash_column_indexes": [],
                    "n_columns": 10,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hCommentTables",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hComparisons",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hComplaintsType",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hComplication",
                    "hash_column_indexes": [],
                    "n_columns": 11,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hComplicationRT",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hComplicationType",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hComplications",
                    "hash_column_indexes": [],
                    "n_columns": 11,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hConfirmationType",
                    "hash_column_indexes": [],
                    "n_columns": 10,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hConsultations",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hContactsType",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hContinueType",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hContraceptionMethods",
                    "hash_column_indexes": [],
                    "n_columns": 10,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hControlArrival",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hCriteriaBirths",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hDCAttributes",
                    "hash_column_indexes": [],
                    "n_columns": 11,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hDCCConclusionGAI",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hDCCDecisions",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hDCConditionSet",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hDCConditionType",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hDCDataType",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hDCObjectStructType",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hDCTableType",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hDCTypeFlkCheck",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hDamageReason",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hDataSource",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hDataSourceParam",
                    "hash_column_indexes": [],
                    "n_columns": 10,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hDataType",
                    "hash_column_indexes": [],
                    "n_columns": 11,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hDataTypeLIS",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hDayHospitalAttachs",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hDayHospitalCardTypes",
                    "hash_column_indexes": [],
                    "n_columns": 10,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hDeathOnResult",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hDeathRegType",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hDeathRegVid",
                    "hash_column_indexes": [],
                    "n_columns": 10,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hDeathType",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hDecision",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hDefectForms",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hDefectHandlings",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hDefectTypes",
                    "hash_column_indexes": [],
                    "n_columns": 13,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hDefects",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hDegree",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hDegreeState",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hDelayType",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hDeliveryType",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hDepartRes",
                    "hash_column_indexes": [],
                    "n_columns": 11,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hDepartStat",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hDetailsDeath",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hDetectType",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hDiagType",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hDiagnosisType",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hDialysisServ",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hDictType",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hDictsItrc",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hDictsNat",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hDiplomClassification",
                    "hash_column_indexes": [],
                    "n_columns": 10,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hDirectionForUse",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hDisabledDischarge",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hDiseasesHeavyFormProperties",
                    "hash_column_indexes": [],
                    "n_columns": 10,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hDiseasesHeavyForms",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hDocOperation",
                    "hash_column_indexes": [],
                    "n_columns": 10,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hDocType",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hDocTypes",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hDocs",
                    "hash_column_indexes": [],
                    "n_columns": 10,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hDoctorTypes",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hDocumentInvalidity",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hDoseType",
                    "hash_column_indexes": [],
                    "n_columns": 12,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hDrg",
                    "hash_column_indexes": [],
                    "n_columns": 12,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hDrgTypes",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hDrgWeightTypes",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hDropType",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hDrugSubstance",
                    "hash_column_indexes": [],
                    "n_columns": 10,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hDrugsUsedPlaces",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hDtranspType",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hDurationType",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hEPRequestType",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hEServiceRequestStatuses",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hEServiceRequestTypes",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hEcgResults",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hEduForm",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hEduTrainingDirection",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hEducType",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hEducation",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hEducationPayTypes",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hEkoZoneTypes",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hElectric",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hElementTypes",
                    "hash_column_indexes": [],
                    "n_columns": 12,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hEliminatedFromSpecializedHospital",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hEmbryoQuantity",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hEmmCPovod",
                    "hash_column_indexes": [],
                    "n_columns": 12,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hEmmUrgency",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hEmploymentKinds",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hEpicrisisDataTypes",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hEquipment",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hErrorMessages",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hExamBehavior",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hExamBreatheAuscultation",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hExamBreatheChest",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hExamBreatheDyspnea",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hExamBreatheRales",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hExamCondition",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hExamConsciousness",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hExamFoodLiver",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hExamFoodPeritoneum",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hExamFoodSpleen",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hExamFoodStomach",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hExamFoodTongue",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hExamHeartNoise",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hExamHeartPulse",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hExamHeartTones",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hExamNeurologyAphasia",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hExamNeurologyCMN",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hExamNeurologyEyeBalls",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hExamNeurologyMeningialSigns",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hExamNeurologyMotorSphere",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hExamNeurologyPainSensitivity",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hExamNeurologyPathologicSigns",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hExamNeurologyTendonReflexes",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hExamPeripherals",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hExamPupilAnizokariya",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hExamPupilLightReaction",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hExamPupilValue",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hExamSkin",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hExamTherapyResult",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hExamUrogenitalMenstruation",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hExamUrogenitalUrine",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hExamZev",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hExcludeReason",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hExecutionPlaces",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hExpType",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hExperience",
                    "hash_column_indexes": [],
                    "n_columns": 11,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hExpertType",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hExprValueType",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hExtSys",
                    "hash_column_indexes": [],
                    "n_columns": 4,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hExternalDirectionStatus",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hExternalSystems",
                    "hash_column_indexes": [],
                    "n_columns": 11,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hExtrenType",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hFSpecialization",
                    "hash_column_indexes": [],
                    "n_columns": 10,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hFactorCalcAdd",
                    "hash_column_indexes": [],
                    "n_columns": 10,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hFactorCalcArea",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hFactorsPregnancies",
                    "hash_column_indexes": [],
                    "n_columns": 11,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hFiltersType",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hFiring",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hForTherapeuticPatients",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hForm1Types",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hFormOrg",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hFormTypeFO",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hFormingFond",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hFrequencyUseCommonToolsNarko",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hFrequencyUseNarko",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hFromAdditionalEducation",
                    "hash_column_indexes": [],
                    "n_columns": 11,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hFuncSetBuild",
                    "hash_column_indexes": [],
                    "n_columns": 10,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hFunction",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hGBD",
                    "hash_column_indexes": [],
                    "n_columns": 4,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hGBDStatusActual",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hGBDStatusProv",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hGas",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hGenDocNumPeriod",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hGestationalAge",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hGlucoseLevels",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hGovEducationType",
                    "hash_column_indexes": [],
                    "n_columns": 10,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hGovEmploymentType",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hGovJobCategory",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hGovPersonalJobType",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hGovServiceType",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hGridScheduleTypes",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hGroupDisability",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hHBNamespace",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hHBSubscriberNode",
                    "hash_column_indexes": [],
                    "n_columns": 10,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hHBSubscriberSystem",
                    "hash_column_indexes": [],
                    "n_columns": 10,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hHaemocultTestResult",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hHandbook",
                    "hash_column_indexes": [],
                    "n_columns": 11,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hHasNotMotherInfo",
                    "hash_column_indexes": [],
                    "n_columns": 3,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hHelpTypes",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hHoliday",
                    "hash_column_indexes": [],
                    "n_columns": 10,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hHospitalDirectionReasons",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hHospitalNoticeStatuses",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hHospitalNoticeTypes",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hHospitalStayResult",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hHospitalized",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hHospitalizedOtkazType",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hHousingConditions",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hHousingQueue",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hICIGroup",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hISQL",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hImmunizationPeriod",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hInAnyMedicalRecord",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hInCaseOfDeath",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hInPatientHelpTypes",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hInPatientPayTypes",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hIndDataType",
                    "hash_column_indexes": [],
                    "n_columns": 10,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hIndExpSpeciality",
                    "hash_column_indexes": [],
                    "n_columns": 10,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hIndResult",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hIndexPwr",
                    "hash_column_indexes": [],
                    "n_columns": 10,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hIndicExpType",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hIndicExpVid",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hIndicators",
                    "hash_column_indexes": [],
                    "n_columns": 13,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hIndicatorsGroup",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hIndicatorsType",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hInformationVolume",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hInitiator",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hInspectionType",
                    "hash_column_indexes": [],
                    "n_columns": 10,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hInstNomenclature",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hInterface",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hInterfaceType",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hIntervalType",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hInvalidityDischarge",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hIshodType",
                    "hash_column_indexes": [],
                    "n_columns": 12,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hIssueReasons",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hJobPlace",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hKATO",
                    "hash_column_indexes": [],
                    "n_columns": 26,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hKMUSpeciality",
                    "hash_column_indexes": [],
                    "n_columns": 11,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hKMUSpecialityNurse",
                    "hash_column_indexes": [],
                    "n_columns": 10,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hKPovod",
                    "hash_column_indexes": [],
                    "n_columns": 12,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hLSappointment",
                    "hash_column_indexes": [],
                    "n_columns": 10,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hLaborPeriod",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hLanguageProficiency",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hLanguages",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hLateDiagnosticReason",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hLevelRespon",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hLicCheckReason",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hLicCheckType",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hLicDocType",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hLicDocs",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hLicResult",
                    "hash_column_indexes": [],
                    "n_columns": 10,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hLisAttestationType",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hLisCharactMaterial",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hLisDeviceConnectType",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hLisErrorDeviceCondition",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hLisFromDirection",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hLisOperationType",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hLisPatientCategory",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hLisProtocolsTypes",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hLisResultSource",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hLisResultStatus",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hLisResultType",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hLisRulesPlanCheckCard",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hLisStatus",
                    "hash_column_indexes": [],
                    "n_columns": 11,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hLisStatusCheckCard",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hLisTechniqueCheckCard",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hLocalityProfiles",
                    "hash_column_indexes": [],
                    "n_columns": 10,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hLocalization",
                    "hash_column_indexes": [],
                    "n_columns": 13,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hLocation",
                    "hash_column_indexes": [],
                    "n_columns": 11,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hMDeathType",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hMDocType",
                    "hash_column_indexes": [],
                    "n_columns": 11,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hMDocView",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hMOImageType",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hMammographyResults",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hMarriedType",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hMatResCheckPeriod",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hMatResState",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hMaterial",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hMaterialType",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hMatureType",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hMedActivity",
                    "hash_column_indexes": [],
                    "n_columns": 10,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hMedFcl",
                    "hash_column_indexes": [],
                    "n_columns": 11,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hMedServLevel",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hMedStage",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hMedTrtmType",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hMedicProductType",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hMedicalData",
                    "hash_column_indexes": [],
                    "n_columns": 10,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hMedicalDayPeriod",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hMedicationType",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hMepChangeReasons",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hMepDrugs",
                    "hash_column_indexes": [],
                    "n_columns": 16,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hMepDrugsFilters",
                    "hash_column_indexes": [],
                    "n_columns": 10,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hMepDrugs_changed",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hMepDrugs_changed2",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hMeps",
                    "hash_column_indexes": [],
                    "n_columns": 11,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hMepsLevels",
                    "hash_column_indexes": [],
                    "n_columns": 15,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hMepsServ",
                    "hash_column_indexes": [],
                    "n_columns": 22,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hMepsServFilters",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hMepsSurgProc",
                    "hash_column_indexes": [],
                    "n_columns": 19,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hMepsSurgProcDayClinic",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hMessageResult",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hMessageType",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hMetastasisLocalization",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hMethodExec",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hMethodRT",
                    "hash_column_indexes": [],
                    "n_columns": 10,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hMethodUsingNarko",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hMethodology",
                    "hash_column_indexes": [],
                    "n_columns": 10,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hMiAdEduOrg",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hMiFacultet",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hMiUZ",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hMilitaryAvailability",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hMilitaryAwards",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hMilitaryRank",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hMilitaryRegCategory",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hMilitaryRegGroup",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hMilitaryRegType",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hMilitaryStaff",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hMoCoefficientTypes",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hModule",
                    "hash_column_indexes": [],
                    "n_columns": 10,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hModules",
                    "hash_column_indexes": [],
                    "n_columns": 12,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hMonth",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hMorphology",
                    "hash_column_indexes": [],
                    "n_columns": 13,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hMoveInStacOutStatuses",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hMoveInStacOutTypes",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hMoveInStacPlacementStatuses",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hMoveInStacPlacementTypes",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hMultiplicities",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hNameTypeFStruct",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hNationality",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hNaturalDisaster",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hNatureHeldTreat",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hNatureSanitizing",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hNeedStatuses",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hNeedTypes",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hNewbornOutcomes",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hNomenclatureResearcher",
                    "hash_column_indexes": [],
                    "n_columns": 11,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hNormType",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hNormValueType",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hNotExec",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hNoticeTargets",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hNumbers",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hObjectFilterType",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hOccasion",
                    "hash_column_indexes": [],
                    "n_columns": 11,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hOperRec",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hOperationDict",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hOperationFROM",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hOperationRec",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hOperationType",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hOperationType2",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hOrgHealthCareTypes",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hOrgPravForm",
                    "hash_column_indexes": [],
                    "n_columns": 11,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hOrgQual",
                    "hash_column_indexes": [],
                    "n_columns": 10,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hOrgTechicsType",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hOrgTechniksType",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hOtherTypeSpecTreat",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hOtkazType",
                    "hash_column_indexes": [],
                    "n_columns": 12,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hOtzyvType",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hOutlayIndication",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hOwnership",
                    "hash_column_indexes": [],
                    "n_columns": 10,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hParticipant",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hPartogrammParametrs",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hPathology",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hPatientAdmissionServiceCategory",
                    "hash_column_indexes": [],
                    "n_columns": 10,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hPatientAdmissionServiceSection",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hPatientAdmissionServices",
                    "hash_column_indexes": [],
                    "n_columns": 10,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hPatientCategory",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hPayType",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hPerech",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hPerformedStatuses",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hPeriodConcomitantDiseases",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hPeriodType",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hPeriodicity",
                    "hash_column_indexes": [],
                    "n_columns": 16,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hPeriodicityType",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hPersDRegType",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hPersonStatus",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hPersonType",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hPersonalType",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hPhaseExecut",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hPhoneType",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hPlaceTakingMaterial",
                    "hash_column_indexes": [],
                    "n_columns": 10,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hPlanPeopleCount",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hPlanValueTypes",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hPost",
                    "hash_column_indexes": [],
                    "n_columns": 10,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hPostEquivalenceResult",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hPostFunc",
                    "hash_column_indexes": [],
                    "n_columns": 10,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hPostgraduateEducationType",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hPredicateType",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hPregnantWomenDeathPeriod",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hPreservType",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hPrevPregnancyEnd",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hPriorityType",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hPriznPost",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hProctosigmoidoscopeResult",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hProducedDrugs",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hProfActionPerformStatus",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hProject",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hPromotions",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hPropertyCategory",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hPropertyType",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hProtocol",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hPublicPurchase",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hPurchasing",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hPurposeResearch",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hQExent",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hQGRFunc",
                    "hash_column_indexes": [],
                    "n_columns": 10,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hQualDoc",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hQuarters",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hQueryFieldType",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hRT",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hRadiomodifier",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hRateValues",
                    "hash_column_indexes": [],
                    "n_columns": 12,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hRaters",
                    "hash_column_indexes": [],
                    "n_columns": 13,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hRates",
                    "hash_column_indexes": [],
                    "n_columns": 10,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hRating",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hReactionCharacter",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hReasDRegType",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hReason",
                    "hash_column_indexes": [],
                    "n_columns": 10,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hReasonDisability",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hReasonMR",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hReasonPartTreat",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hReasonRemovAccount",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hRecessType",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hRecommendation",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hRecovery",
                    "hash_column_indexes": [],
                    "n_columns": 11,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hRefStringType",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hReferralStates",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hReferralStatuses",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hReferralTypes",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hRegType",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hRegionType",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hRegions",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hRegisteredSystems",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hRehabilitationTypes",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hRepeatGosp",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hRequestStatuses",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hResolveType",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hResultSurvey",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hRezistennost",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hRhFactors",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hRightPoss",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hRiskGroup",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hRngFStruct",
                    "hash_column_indexes": [],
                    "n_columns": 10,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hRngMedFcl",
                    "hash_column_indexes": [],
                    "n_columns": 10,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hRngTransp",
                    "hash_column_indexes": [],
                    "n_columns": 10,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hRoleType",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hRsnWork",
                    "hash_column_indexes": [],
                    "n_columns": 10,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hRsnWorkDsc",
                    "hash_column_indexes": [],
                    "n_columns": 10,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hRsnWorkTermination",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hSCIParamType",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hSCInterface",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hSCOrderOfType",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hSCWidget",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hSciPublicationsType",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hScreeningAges",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hScreeningExamNeeds",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hScreeningExamResults",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hScreeningPassingStatusses",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hScreeningTargetGroups",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hScreeningValues",
                    "hash_column_indexes": [],
                    "n_columns": 10,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hSenderTypes",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hServ",
                    "hash_column_indexes": [],
                    "n_columns": 16,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hServApplicationRefusalReasons",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hServApplicationStatuses",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hServType",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hServiceTypes",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hServicesStatuses",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hSetAccountType",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hSetRoom",
                    "hash_column_indexes": [],
                    "n_columns": 10,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hSewerage",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hShiftType",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hSick",
                    "hash_column_indexes": [],
                    "n_columns": 15,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hSickClasses",
                    "hash_column_indexes": [],
                    "n_columns": 10,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hSickExtended",
                    "hash_column_indexes": [],
                    "n_columns": 12,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hSickFilters",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hSickListTypes",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hSickTypes",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hSideEffectsCT",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hSideEffectsHRT",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hSideEffectsIT",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hSideEffectsTIBR",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hSideEffectsTT",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hSign",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hSignSex",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hSignificantInterferences",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hSistEisz",
                    "hash_column_indexes": [],
                    "n_columns": 10,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hSmearResults",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hSocType",
                    "hash_column_indexes": [],
                    "n_columns": 10,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hSocialAwards",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hSourceOfLivelihood",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hSpecName",
                    "hash_column_indexes": [],
                    "n_columns": 11,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hSpecialistGroup",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hSpeciality",
                    "hash_column_indexes": [],
                    "n_columns": 10,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hSpr",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hSqlFunction",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hStaffPosts",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hStaffStatus",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hStaffTypes",
                    "hash_column_indexes": [],
                    "n_columns": 10,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hStage",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hStageCom",
                    "hash_column_indexes": [],
                    "n_columns": 10,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hStageNumbers",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hStagesCT",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hStandartTypes",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hStatMemberAk",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hState",
                    "hash_column_indexes": [],
                    "n_columns": 13,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hStateEmplCategory",
                    "hash_column_indexes": [],
                    "n_columns": 10,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hStatesRaters",
                    "hash_column_indexes": [],
                    "n_columns": 12,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hStatus",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hStatusCall",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hStatusExec",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hStatusPost",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hStayOutcome",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hStudentAttachReason",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hStudentDetachReason",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hStudyConditions",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hSubjects",
                    "hash_column_indexes": [],
                    "n_columns": 10,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hSubordination",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hSubordinationType",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hSubstanceTypes",
                    "hash_column_indexes": [],
                    "n_columns": 10,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hSurgicalConstants",
                    "hash_column_indexes": [],
                    "n_columns": 11,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hSyndrome",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hSystemLIS",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hSystemTypeRec",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hSystems",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hTDElementType",
                    "hash_column_indexes": [],
                    "n_columns": 11,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hTNVED",
                    "hash_column_indexes": [],
                    "n_columns": 16,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hTableAccountTimeType",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hTableStatus",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hTakingAccountType",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hTapType",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hTarificatorClasses",
                    "hash_column_indexes": [],
                    "n_columns": 10,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hTarificatorCoefficientSubTypes",
                    "hash_column_indexes": [],
                    "n_columns": 10,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hTarificatorCoefficientTypes",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hTarificatorCostTypes",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hTarificatorGobmpTypes",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hTarificatorMethods",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hTarificatorServiceTypes",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hTemaAddEdu",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hTemplateType",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hTermCourse",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hTermType",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hTerritoryUnitType",
                    "hash_column_indexes": [],
                    "n_columns": 11,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hThemeTrainings",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hTherapyResult",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hTimeUnits",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hTotalSizeTreat",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hTransferType",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hTranspType",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hTraumaType",
                    "hash_column_indexes": [],
                    "n_columns": 10,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hTreatmentOncology",
                    "hash_column_indexes": [],
                    "n_columns": 10,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hTreatmentReasons",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hTreeListPostType",
                    "hash_column_indexes": [],
                    "n_columns": 10,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hTriglycerideLevels",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hTuberculosisComplicationType",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hTuberculosisContingentType",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hTuberculosisMLU",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hTuberculosisPhase",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hTuberculosisSickType",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hTuberculosisSpecification",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hTypMResour",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hTypRegDoc",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hTypRegion",
                    "hash_column_indexes": [],
                    "n_columns": 11,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hTypRepWork",
                    "hash_column_indexes": [],
                    "n_columns": 10,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hTypRepair",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hTypSrcFin",
                    "hash_column_indexes": [],
                    "n_columns": 10,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hTypStreet",
                    "hash_column_indexes": [],
                    "n_columns": 11,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hTypeAction",
                    "hash_column_indexes": [],
                    "n_columns": 10,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hTypeActionParam",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hTypeAddition",
                    "hash_column_indexes": [],
                    "n_columns": 12,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hTypeAttr",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hTypeAttributes",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hTypeAttributesSet",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hTypeBindTerritor",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hTypeBuild",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hTypeCT",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hTypeCards",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hTypeCase",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hTypeCom",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hTypeCondition",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hTypeControlPos",
                    "hash_column_indexes": [],
                    "n_columns": 10,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hTypeDatePlan",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hTypeDrugsNarko",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hTypeEdu",
                    "hash_column_indexes": [],
                    "n_columns": 11,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hTypeElement",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hTypeElementEvent",
                    "hash_column_indexes": [],
                    "n_columns": 11,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hTypeElementParam",
                    "hash_column_indexes": [],
                    "n_columns": 10,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hTypeElementParamValue",
                    "hash_column_indexes": [],
                    "n_columns": 10,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hTypeElementSys",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hTypeElementSysLink",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hTypeEmployee",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hTypeField",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hTypeFilterLink",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hTypeFloorSpace",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hTypeFrame",
                    "hash_column_indexes": [],
                    "n_columns": 10,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hTypeFurlough",
                    "hash_column_indexes": [],
                    "n_columns": 11,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hTypeHRT",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hTypeIfDef",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hTypeIfInst",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hTypeIface",
                    "hash_column_indexes": [],
                    "n_columns": 10,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hTypeIfaceEntryParam",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hTypeIfaceSection",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hTypeInsurance",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hTypeInterfaceForm",
                    "hash_column_indexes": [],
                    "n_columns": 10,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hTypeInvitation",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hTypeLink",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hTypeListPerson",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hTypeOperation",
                    "hash_column_indexes": [],
                    "n_columns": 10,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hTypeParamFunction",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hTypePriborGroup",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hTypePromptness",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hTypeRT",
                    "hash_column_indexes": [],
                    "n_columns": 10,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hTypeReserv",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hTypeRespon",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hTypeResults",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hTypeRowLink",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hTypeRule",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hTypeRulesLink",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hTypeShedGroup",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hTypeSpecialistGroup",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hTypeSysParam",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hTypeTAParams",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hTypeTableLoad",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hTypeTransportation",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hTypeTreat",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hTypeUnitStructOHC",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hTypeValueSource",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hTypeVisits",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hTypesCalls",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hUKLEstimate",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hUchSpeciality",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hUnitCalendar",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hUnitReferentResult",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hUnits",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hUnitsLis",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hUrgencyTypes",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hUseUnits",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hUserConfigGroup",
                    "hash_column_indexes": [],
                    "n_columns": 11,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hUserConfigParam",
                    "hash_column_indexes": [],
                    "n_columns": 20,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hUterusBiopsyResult",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hUterusContractionsTypes",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hVISQueryResultCode",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hVISQueryStatus",
                    "hash_column_indexes": [],
                    "n_columns": 4,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hVTKGroups",
                    "hash_column_indexes": [],
                    "n_columns": 12,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hVaccinationType",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hValidateElementTypes",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hValidateLogTypes",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hValueSourceType",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hValueUnits",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hValuesDegree",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hValuesDegreeSE",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hVariantDiag",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hVentilation",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hVidDeath",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hVidEMD",
                    "hash_column_indexes": [],
                    "n_columns": 10,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hVisType",
                    "hash_column_indexes": [],
                    "n_columns": 4,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hVisitTypes",
                    "hash_column_indexes": [],
                    "n_columns": 10,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hWaterCold",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hWaterHot",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hWayRT",
                    "hash_column_indexes": [],
                    "n_columns": 10,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hWayUseDrug",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hWeekDays",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hWhereReceived",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hWhoEstablished",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hWhoLivePastThirtyDays",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hWomanDeathType",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hWorkType",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hWriteoffActStatuses",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_handbookList",
                    "hash_column_indexes": [],
                    "n_columns": 10,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_handbookVersions",
                    "hash_column_indexes": [],
                    "n_columns": 12,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_har_categorybuilding",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_har_typeate",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_har_typeconstruction",
                    "hash_column_indexes": [],
                    "n_columns": 4,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_har_typegeonim",
                    "hash_column_indexes": [],
                    "n_columns": 4,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hcategorycitizens",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hdirectiontraining",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_historWorkBed",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_historyPost",
                    "hash_column_indexes": [],
                    "n_columns": 11,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hpartBuilding",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hs_CheckRegisters",
                    "hash_column_indexes": [],
                    "n_columns": 14,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hs_Diagnoses",
                    "hash_column_indexes": [],
                    "n_columns": 14,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hs_Forms1",
                    "hash_column_indexes": [],
                    "n_columns": 4,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_hstatusrec",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_jobDescription",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_kgsList",
                    "hash_column_indexes": [],
                    "n_columns": 13,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_legalEntity",
                    "hash_column_indexes": [
                        0
                    ],
                    "n_columns": 17,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_listPost",
                    "hash_column_indexes": [],
                    "n_columns": 16,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_listRazrTab",
                    "hash_column_indexes": [],
                    "n_columns": 15,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_listResearchesSystemLIS",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_listServicesLeasing",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_listTabtable",
                    "hash_column_indexes": [],
                    "n_columns": 12,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_listTariffTable",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_localDistantMetastas",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_markCom",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_materialResources",
                    "hash_column_indexes": [],
                    "n_columns": 22,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_medicalFacilities",
                    "hash_column_indexes": [],
                    "n_columns": 31,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_militaryRegistration",
                    "hash_column_indexes": [],
                    "n_columns": 18,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_naturalDisasterParticipation",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_orgHealthCare",
                    "hash_column_indexes": [],
                    "n_columns": 23,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_payingServicesLeasing",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_personBloodGroup",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_personInsurance",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_personLanguages",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_personMilitaryAwards",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_personSciPublications",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_personSocialAwards",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_personal",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_personalForeclosures",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_personalLabourBook",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_personalPromotions",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_phc_CheckRegisters",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_phc_Forms1",
                    "hash_column_indexes": [],
                    "n_columns": 3,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_post",
                    "hash_column_indexes": [],
                    "n_columns": 17,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_postEquivalence",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_power",
                    "hash_column_indexes": [],
                    "n_columns": 10,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_primEducation",
                    "hash_column_indexes": [],
                    "n_columns": 20,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_professionalSkill",
                    "hash_column_indexes": [],
                    "n_columns": 15,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_razrTab",
                    "hash_column_indexes": [],
                    "n_columns": 21,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_regDocuments",
                    "hash_column_indexes": [],
                    "n_columns": 11,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_repairBuilding",
                    "hash_column_indexes": [],
                    "n_columns": 10,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_repairWork",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_respForContents",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_roomFStruct",
                    "hash_column_indexes": [],
                    "n_columns": 15,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_service",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_stageCom",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_students",
                    "hash_column_indexes": [],
                    "n_columns": 13,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_surServicesTriggerTable",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_tmp_IdTab",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_tmp_RelationDicts",
                    "hash_column_indexes": [],
                    "n_columns": 10,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_tmp_gbl_document",
                    "hash_column_indexes": [],
                    "n_columns": 17,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_tmp_gbl_person",
                    "hash_column_indexes": [],
                    "n_columns": 47,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_transport",
                    "hash_column_indexes": [],
                    "n_columns": 15,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_urLegalEntity",
                    "hash_column_indexes": [],
                    "n_columns": 10,
                    "sql": None
                },
                {
                    "source_table": "MZ_t_zamPost",
                    "hash_column_indexes": [],
                    "n_columns": 7,
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
                    "source_table": "OutputDataAudit",
                    "hash_column_indexes": [],
                    "n_columns": 6,
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