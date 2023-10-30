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
        conn_secrets = Connection.get_connection_from_secrets(conn_id="MZ_RBZHFV_MSSQL")
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
        'dag_id': "r_mz_rbzhfv",
        'owner': "Bakhtiyar",
        'start_date': datetime.datetime.strptime('', '%d/%m/%Y'),
        'schedule_interval': "0 0 * * *",
        'tags': ['mz', 'rbzhfv', 'reglament'],
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
            "target_schema": "R_MZ_RBIZHFV",
            "tables": [
                {
                    "source_table": "ApplicationSetting",
                    "hash_column_indexes": [],
                    "n_columns": 4,
                    "sql": None
                },
                {
                    "source_table": "Audit",
                    "hash_column_indexes": [],
                    "n_columns": 15,
                    "sql": None
                },
                {
                    "source_table": "DRUG_DIC_ATC",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "DRUG_DIC_CommercialName",
                    "hash_column_indexes": [],
                    "n_columns": 14,
                    "sql": None
                },
                {
                    "source_table": "DRUG_DIC_DrugForm",
                    "hash_column_indexes": [],
                    "n_columns": 15,
                    "sql": None
                },
                {
                    "source_table": "DRUG_DIC_MNN",
                    "hash_column_indexes": [],
                    "n_columns": 12,
                    "sql": None
                },
                {
                    "source_table": "DRUG_DIC_WrappingForm",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "DeadBer_karta",
                    "hash_column_indexes": [],
                    "n_columns": 14,
                    "sql": None
                },
                {
                    "source_table": "Del_ber_karta",
                    "hash_column_indexes": [],
                    "n_columns": 13,
                    "sql": None
                },
                {
                    "source_table": "NotificationMessages",
                    "hash_column_indexes": [],
                    "n_columns": 10,
                    "sql": None
                },
                {
                    "source_table": "Notifications",
                    "hash_column_indexes": [],
                    "n_columns": 24,
                    "sql": None
                },
                {
                    "source_table": "OkpoluHistory",
                    "hash_column_indexes": [],
                    "n_columns": 13,
                    "sql": None
                },
                {
                    "source_table": "ProfileData",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "Profiles",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "RemoveDublicate",
                    "hash_column_indexes": [],
                    "n_columns": 4,
                    "sql": None
                },
                {
                    "source_table": "Roles",
                    "hash_column_indexes": [],
                    "n_columns": 4,
                    "sql": None
                },
                {
                    "source_table": "Sp_err",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "UserParams",
                    "hash_column_indexes": [],
                    "n_columns": 19,
                    "sql": None
                },
                {
                    "source_table": "Users",
                    "hash_column_indexes": [],
                    "n_columns": 22,
                    "sql": None
                },
                {
                    "source_table": "UsersInRoles",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "Version",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "ber_faktor",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "ber_gospital",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "ber_gospital_diag",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "ber_karta",
                    "hash_column_indexes": [],
                    "n_columns": 192,
                    "sql": None
                },
                {
                    "source_table": "ber_karta_netr",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "ber_karta_netr_GCVP",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "ber_karta_osob",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "ber_karta_reb",
                    "hash_column_indexes": [],
                    "n_columns": 13,
                    "sql": None
                },
                {
                    "source_table": "ber_karta_work",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "ber_obsled",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "ber_osl",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "ber_osmotr",
                    "hash_column_indexes": [],
                    "n_columns": 20,
                    "sql": None
                },
                {
                    "source_table": "ber_osmotr_full",
                    "hash_column_indexes": [],
                    "n_columns": 73,
                    "sql": None
                },
                {
                    "source_table": "ber_patronaj",
                    "hash_column_indexes": [],
                    "n_columns": 20,
                    "sql": None
                },
                {
                    "source_table": "ber_pred",
                    "hash_column_indexes": [],
                    "n_columns": 11,
                    "sql": None
                },
                {
                    "source_table": "ber_school",
                    "hash_column_indexes": [],
                    "n_columns": 11,
                    "sql": None
                },
                {
                    "source_table": "berem_d",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "def_update",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "diag_addition",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "dop_usl",
                    "hash_column_indexes": [],
                    "n_columns": 11,
                    "sql": None
                },
                {
                    "source_table": "finans",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "hAbortionType_ersb",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "hAddressType",
                    "hash_column_indexes": [],
                    "n_columns": 10,
                    "sql": None
                },
                {
                    "source_table": "hAddressType_ersb",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "hAdmissionToThisHospital_ersb",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "hAnaesthesia_ersb",
                    "hash_column_indexes": [],
                    "n_columns": 11,
                    "sql": None
                },
                {
                    "source_table": "hBdeathType_ersb",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "hBedProfile_ersb",
                    "hash_column_indexes": [],
                    "n_columns": 12,
                    "sql": None
                },
                {
                    "source_table": "hBioSex_ersb",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "hBloodGroups_ersb",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "hCancerStage_ersb",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "hChildDeathTypes_ersb",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "hComplicationRT_ersb",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "hComplication_ersb",
                    "hash_column_indexes": [],
                    "n_columns": 11,
                    "sql": None
                },
                {
                    "source_table": "hDayHospitalCardTypes_ersb",
                    "hash_column_indexes": [],
                    "n_columns": 10,
                    "sql": None
                },
                {
                    "source_table": "hDeliveryType_ersb",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "hDiagType_ersb",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "hDiagnosisType_ersb",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "hDisabledDischarge_ersb",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "hDocType_ersb",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "hDrgWeightTypes_ersb",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "hDrg_ersb",
                    "hash_column_indexes": [],
                    "n_columns": 11,
                    "sql": None
                },
                {
                    "source_table": "hDrugsUsedPlaces_ersb",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "hEducation_ersb",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "hEliminatedFromSpecializedHospital_ersb",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "hEquipment_ersb",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "hExtrenType_ersb",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "hForTherapeuticPatients_ersb",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "hFrequencyUseNarko_ersb",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "hHospitalStayResult_ersb",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "hHospitalized_ersb",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "hInCaseOfDeath_ersb",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "hInPatientHelpTypes_ersb",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "hInvalidityDischarge_ersb",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "hLocalization_ersb",
                    "hash_column_indexes": [],
                    "n_columns": 12,
                    "sql": None
                },
                {
                    "source_table": "hLocation_ersb",
                    "hash_column_indexes": [],
                    "n_columns": 11,
                    "sql": None
                },
                {
                    "source_table": "hMarriedType",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "hMepDrugs_ersb",
                    "hash_column_indexes": [],
                    "n_columns": 13,
                    "sql": None
                },
                {
                    "source_table": "hMepsServ_ersb",
                    "hash_column_indexes": [],
                    "n_columns": 21,
                    "sql": None
                },
                {
                    "source_table": "hMepsSurgProc_ersb",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "hMetastasisLocalization_ersb",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "hMethodRT_ersb",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "hMethodUsingNarko_ersb",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "hMorphology_ersb",
                    "hash_column_indexes": [],
                    "n_columns": 13,
                    "sql": None
                },
                {
                    "source_table": "hMultiplicities_ersb",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "hNationality",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "hNationality_ersb",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "hNatureHeldTreat_ersb",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "hNewbornOutcomes_ersb",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "hOtherTypeSpecTreat_ersb",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "hPeriodConcomitantDiseases_ersb",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "hPurposeResearch_ersb",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "hRT_ersb",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "hRadiomodifier_ersb",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "hReasonPartTreat_ersb",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "hRecommendation_ersb",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "hRehabilitationTypes_ersb",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "hRezistennost_ersb",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "hRhFactors_ersb",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "hRiskGroup_ersb",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "hSick_ersb",
                    "hash_column_indexes": [],
                    "n_columns": 15,
                    "sql": None
                },
                {
                    "source_table": "hSideEffectsCT_ersb",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "hSideEffectsHRT_ersb",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "hSideEffectsIT_ersb",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "hSideEffectsTIBR_ersb",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "hSideEffectsTT_ersb",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "hSocType",
                    "hash_column_indexes": [],
                    "n_columns": 10,
                    "sql": None
                },
                {
                    "source_table": "hSocType_ersb",
                    "hash_column_indexes": [],
                    "n_columns": 10,
                    "sql": None
                },
                {
                    "source_table": "hSourceOfLivelihood_ersb",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "hStagesCT_ersb",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "hState",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "hTerritoryUnitType_ersb",
                    "hash_column_indexes": [],
                    "n_columns": 11,
                    "sql": None
                },
                {
                    "source_table": "hTherapyResult_ersb",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "hTotalSizeTreat_ersb",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "hTraumaType_ersb",
                    "hash_column_indexes": [],
                    "n_columns": 11,
                    "sql": None
                },
                {
                    "source_table": "hTypSrcFin_ersb",
                    "hash_column_indexes": [],
                    "n_columns": 10,
                    "sql": None
                },
                {
                    "source_table": "hTypeCT_ersb",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "hTypeCase_ersb",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "hTypeDrugsNarko_ersb",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "hTypeHRT_ersb",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "hTypeRT_ersb",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "hTypeTreat_ersb",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "hVariantDiag_ersb",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "hWayRT_ersb",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "hWhereReceived_ersb",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "human",
                    "hash_column_indexes": [],
                    "n_columns": 53,
                    "sql": None
                },
                {
                    "source_table": "human_address",
                    "hash_column_indexes": [],
                    "n_columns": 20,
                    "sql": None
                },
                {
                    "source_table": "human_all_diag",
                    "hash_column_indexes": [],
                    "n_columns": 11,
                    "sql": None
                },
                {
                    "source_table": "human_all_gosp",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "human_all_oper",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "human_all_travm",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "human_allerg",
                    "hash_column_indexes": [],
                    "n_columns": 10,
                    "sql": None
                },
                {
                    "source_table": "human_blood_bio",
                    "hash_column_indexes": [],
                    "n_columns": 32,
                    "sql": None
                },
                {
                    "source_table": "human_citmaz",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "human_diag",
                    "hash_column_indexes": [],
                    "n_columns": 41,
                    "sql": None
                },
                {
                    "source_table": "human_disp",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "human_disp_flur",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "human_disp_merop",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "human_disp_prep",
                    "hash_column_indexes": [],
                    "n_columns": 12,
                    "sql": None
                },
                {
                    "source_table": "human_disp_sop",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "human_elektrokardio",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "human_flur",
                    "hash_column_indexes": [],
                    "n_columns": 10,
                    "sql": None
                },
                {
                    "source_table": "human_gemotrans",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "human_glukoza",
                    "hash_column_indexes": [],
                    "n_columns": 10,
                    "sql": None
                },
                {
                    "source_table": "human_health_group",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "human_issled",
                    "hash_column_indexes": [],
                    "n_columns": 10,
                    "sql": None
                },
                {
                    "source_table": "human_mamm",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "human_nasled",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "human_oak",
                    "hash_column_indexes": [],
                    "n_columns": 26,
                    "sql": None
                },
                {
                    "source_table": "human_oam",
                    "hash_column_indexes": [],
                    "n_columns": 31,
                    "sql": None
                },
                {
                    "source_table": "human_prik",
                    "hash_column_indexes": [],
                    "n_columns": 15,
                    "sql": None
                },
                {
                    "source_table": "human_screen",
                    "hash_column_indexes": [],
                    "n_columns": 109,
                    "sql": None
                },
                {
                    "source_table": "human_screen_diag",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "human_screen_osmotr",
                    "hash_column_indexes": [],
                    "n_columns": 23,
                    "sql": None
                },
                {
                    "source_table": "human_uzd",
                    "hash_column_indexes": [],
                    "n_columns": 27,
                    "sql": None
                },
                {
                    "source_table": "human_vich",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "karta_d",
                    "hash_column_indexes": [],
                    "n_columns": 35,
                    "sql": None
                },
                {
                    "source_table": "kato",
                    "hash_column_indexes": [],
                    "n_columns": 20,
                    "sql": None
                },
                {
                    "source_table": "klass_usl",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "nos_ber",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "nosfert_dinam",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "nosfert_egp",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "nosfert_egp_list",
                    "hash_column_indexes": [],
                    "n_columns": 3,
                    "sql": None
                },
                {
                    "source_table": "notice_update",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "ns15",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "okpolu",
                    "hash_column_indexes": [],
                    "n_columns": 28,
                    "sql": None
                },
                {
                    "source_table": "ps_visit",
                    "hash_column_indexes": [],
                    "n_columns": 17,
                    "sql": None
                },
                {
                    "source_table": "request_kart",
                    "hash_column_indexes": [],
                    "n_columns": 18,
                    "sql": None
                },
                {
                    "source_table": "sp_adr",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "sp_app",
                    "hash_column_indexes": [],
                    "n_columns": 4,
                    "sql": None
                },
                {
                    "source_table": "sp_audit",
                    "hash_column_indexes": [],
                    "n_columns": 11,
                    "sql": None
                },
                {
                    "source_table": "sp_berem",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "sp_berem_ber",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "sp_biops",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "sp_biops_pr",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "sp_blood_group",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "sp_cause_death",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "sp_cilindr_oam",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "sp_citmaz",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "sp_color_oam",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "sp_contraception",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "sp_dgroup",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "sp_diag_addition",
                    "hash_column_indexes": [],
                    "n_columns": 11,
                    "sql": None
                },
                {
                    "source_table": "sp_diag_property",
                    "hash_column_indexes": [],
                    "n_columns": 10,
                    "sql": None
                },
                {
                    "source_table": "sp_disp_group_watch",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "sp_dos",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "sp_education",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "sp_esofag",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "sp_fin",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "sp_gastro",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "sp_glukoza",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "sp_gr_du",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "sp_healthgroup",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "sp_inv",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "sp_ishod",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "sp_ishod_ber",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "sp_ishod_berem",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "sp_ishod_reb",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "sp_ishod_screen",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "sp_kateg_diag",
                    "hash_column_indexes": [],
                    "n_columns": 10,
                    "sql": None
                },
                {
                    "source_table": "sp_kolon",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "sp_konc",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "sp_ls",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "sp_mamm",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "sp_manufacturers",
                    "hash_column_indexes": [],
                    "n_columns": 12,
                    "sql": None
                },
                {
                    "source_table": "sp_napr_ber",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "sp_obl",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "sp_otd",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "sp_pivo",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "sp_pos",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "sp_pr_smerti",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "sp_prep",
                    "hash_column_indexes": [],
                    "n_columns": 26,
                    "sql": None
                },
                {
                    "source_table": "sp_prich_death",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "sp_prich_end",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "sp_prich_ersb",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "sp_prich_gemotrans",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "sp_prisk",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "sp_psa",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "sp_region",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "sp_rez_osm",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "sp_risk",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "sp_simp_allerg",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "sp_snat_ber",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "sp_sost",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "sp_sost_zd",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "sp_spec",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "sp_step_rod",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "sp_target_group",
                    "hash_column_indexes": [],
                    "n_columns": 11,
                    "sql": None
                },
                {
                    "source_table": "sp_tip_merop",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "sp_tip_obnar",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "sp_tipdi",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "sp_uch",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "sp_uroven",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "sp_vid_allerg",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "sp_vidpos",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "sp_vino",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "sp_vodka",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "sp_vziat",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "spis_otd",
                    "hash_column_indexes": [],
                    "n_columns": 15,
                    "sql": None
                },
                {
                    "source_table": "spis_spec",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "spis_vra",
                    "hash_column_indexes": [],
                    "n_columns": 26,
                    "sql": None
                },
                {
                    "source_table": "spis_vra_post",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "spmkb",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "spmkb_protiv",
                    "hash_column_indexes": [],
                    "n_columns": 4,
                    "sql": None
                },
                {
                    "source_table": "spmkb_protiv_linear",
                    "hash_column_indexes": [],
                    "n_columns": 3,
                    "sql": None
                },
                {
                    "source_table": "surgery",
                    "hash_column_indexes": [],
                    "n_columns": 10,
                    "sql": None
                },
                {
                    "source_table": "tar_usl",
                    "hash_column_indexes": [],
                    "n_columns": 14,
                    "sql": None
                },
                {
                    "source_table": "version_help",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "vid_usl",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "vra_otd",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "vra_spec",
                    "hash_column_indexes": [],
                    "n_columns": 5,
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