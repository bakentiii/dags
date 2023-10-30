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
        'dag_id': "r_mz_islo",
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
            "target_schema": "R_MZ_ISLO",
            "tables": [
                {
                    "source_table": "ATC_Class",
                    "hash_column_indexes": [],
                    "n_columns": 11,
                    "sql": None
                },
                {
                    "source_table": "ATC_Transformed",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "AverageVolume",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "BalanceRegistries",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "BalanceRegistryHistory",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "BalanceRegistryStatuses",
                    "hash_column_indexes": [],
                    "n_columns": 4,
                    "sql": None
                },
                {
                    "source_table": "Category",
                    "hash_column_indexes": [],
                    "n_columns": 11,
                    "sql": None
                },
                {
                    "source_table": "CategorySKF",
                    "hash_column_indexes": [],
                    "n_columns": 10,
                    "sql": None
                },
                {
                    "source_table": "Contract",
                    "hash_column_indexes": [
                        8,
                        33
                    ],
                    "n_columns": 40,
                    "sql": None
                },
                {
                    "source_table": "ContractDetails",
                    "hash_column_indexes": [],
                    "n_columns": 11,
                    "sql": None
                },
                {
                    "source_table": "ContractPharmacies",
                    "hash_column_indexes": [
                        2,
                        3,
                        4,
                        8,
                        9
                    ],
                    "n_columns": 12,
                    "sql": None
                },
                {
                    "source_table": "Contractors",
                    "hash_column_indexes": [],
                    "n_columns": 21,
                    "sql": None
                },
                {
                    "source_table": "Corporation",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "Customer",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "DR_DicBoxes",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "DR_DicDosageForms",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "DR_DicProducerTypes",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "DR_DicProducers",
                    "hash_column_indexes": [],
                    "n_columns": 11,
                    "sql": None
                },
                {
                    "source_table": "DR_RegisterBoxes",
                    "hash_column_indexes": [],
                    "n_columns": 12,
                    "sql": None
                },
                {
                    "source_table": "DeliveryInvoiceDetails",
                    "hash_column_indexes": [],
                    "n_columns": 14,
                    "sql": None
                },
                {
                    "source_table": "DeliveryInvoices",
                    "hash_column_indexes": [
                        1,
                        6
                    ],
                    "n_columns": 17,
                    "sql": None
                },
                {
                    "source_table": "DeliveryPlanDetails",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "DeliveryPlans",
                    "hash_column_indexes": [],
                    "n_columns": 12,
                    "sql": None
                },
                {
                    "source_table": "District",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "DocNumbers",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "Drug",
                    "hash_column_indexes": [],
                    "n_columns": 49,
                    "sql": None
                },
                {
                    "source_table": "DrugAtcCode",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "DrugDosageForm",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "DrugFormType",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "DrugMNN",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "DrugMeasure",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "DrugProducer",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "DrugType",
                    "hash_column_indexes": [
                        2,
                        6,
                        7,
                        11
                    ],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "DrugUseMethod",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "Drugstore",
                    "hash_column_indexes": [],
                    "n_columns": 14,
                    "sql": None
                },
                {
                    "source_table": "ERDBCheckForRegions",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "ERDBHumanDiags",
                    "hash_column_indexes": [],
                    "n_columns": 10,
                    "sql": None
                },
                {
                    "source_table": "FreeMedPriceDetails",
                    "hash_column_indexes": [],
                    "n_columns": 15,
                    "sql": None
                },
                {
                    "source_table": "FreeMedServiceType",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "Globals",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "Goods",
                    "hash_column_indexes": [],
                    "n_columns": 12,
                    "sql": None
                },
                {
                    "source_table": "HelpLibrary",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "Inventory",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "IsloErsbIntegrationMessage",
                    "hash_column_indexes": [],
                    "n_columns": 13,
                    "sql": None
                },
                {
                    "source_table": "JournalAccountingSupplies",
                    "hash_column_indexes": [],
                    "n_columns": 22,
                    "sql": None
                },
                {
                    "source_table": "Logs",
                    "hash_column_indexes": [],
                    "n_columns": 11,
                    "sql": None
                },
                {
                    "source_table": "MZ_DicDoseType",
                    "hash_column_indexes": [],
                    "n_columns": 14,
                    "sql": None
                },
                {
                    "source_table": "MZ_DicOrgPravForm",
                    "hash_column_indexes": [],
                    "n_columns": 12,
                    "sql": None
                },
                {
                    "source_table": "MZ_DicState",
                    "hash_column_indexes": [],
                    "n_columns": 14,
                    "sql": None
                },
                {
                    "source_table": "MethodCall",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "News",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "Nosology",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "Nosology_Icd",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "OpenPeriod",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "OpenPeriodDetails",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "Organization",
                    "hash_column_indexes": [],
                    "n_columns": 16,
                    "sql": None
                },
                {
                    "source_table": "OutpatientJournalAccounting",
                    "hash_column_indexes": [],
                    "n_columns": 18,
                    "sql": None
                },
                {
                    "source_table": "PasswordPolicyExceptedUsers",
                    "hash_column_indexes": [],
                    "n_columns": 3,
                    "sql": None
                },
                {
                    "source_table": "PatientSearchLogs",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "PatientVersion",
                    "hash_column_indexes": [
                        2
                    ],
                    "n_columns": 20,
                    "sql": None
                },
                {
                    "source_table": "PeriodCloseDates",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "PolyclPlanByProgram",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "Polyclinic",
                    "hash_column_indexes": [
                        14,
                        16,
                        17,
                        18
                    ],
                    "n_columns": 25,
                    "sql": None
                },
                {
                    "source_table": "PolyclinicSpec",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "PostDoctor",
                    "hash_column_indexes": [
                        6,
                        7,
                        10,
                        15
                    ],
                    "n_columns": 18,
                    "sql": None
                },
                {
                    "source_table": "Price",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "PriceDetails",
                    "hash_column_indexes": [],
                    "n_columns": 30,
                    "sql": None
                },
                {
                    "source_table": "PriceDetailsDrug",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "PriceMonitoring",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "Producer",
                    "hash_column_indexes": [
                        4
                    ],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "Product",
                    "hash_column_indexes": [],
                    "n_columns": 21,
                    "sql": None
                },
                {
                    "source_table": "ProductChar",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "ProductIncomeOutcome",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "ProductMNN",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "ProductMnnSulo",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "ProductStore",
                    "hash_column_indexes": [],
                    "n_columns": 13,
                    "sql": None
                },
                {
                    "source_table": "ProductStore010Program",
                    "hash_column_indexes": [],
                    "n_columns": 18,
                    "sql": None
                },
                {
                    "source_table": "ProductVersion",
                    "hash_column_indexes": [],
                    "n_columns": 21,
                    "sql": None
                },
                {
                    "source_table": "Product_pay",
                    "hash_column_indexes": [],
                    "n_columns": 30,
                    "sql": None
                },
                {
                    "source_table": "Program",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "ProvideIds",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "ProvideIds2",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "Provider",
                    "hash_column_indexes": [
                        5,
                        6,
                        11,
                        12,
                        13
                    ],
                    "n_columns": 16,
                    "sql": None
                },
                {
                    "source_table": "RecipeConsolidated",
                    "hash_column_indexes": [],
                    "n_columns": 13,
                    "sql": None
                },
                {
                    "source_table": "RecipeConsolidatedStatuses",
                    "hash_column_indexes": [],
                    "n_columns": 4,
                    "sql": None
                },
                {
                    "source_table": "RecipeDeleted",
                    "hash_column_indexes": [],
                    "n_columns": 29,
                    "sql": None
                },
                {
                    "source_table": "RecipeEGovLog",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "RecipeGroups",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "RecipeSerNumbers",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "Recipe_pay",
                    "hash_column_indexes": [],
                    "n_columns": 31,
                    "sql": None
                },
                {
                    "source_table": "Recipe_validity",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "Region",
                    "hash_column_indexes": [],
                    "n_columns": 16,
                    "sql": None
                },
                {
                    "source_table": "Reports",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "RoleBasedSystemSettings",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "RoleMemberships",
                    "hash_column_indexes": [],
                    "n_columns": 4,
                    "sql": None
                },
                {
                    "source_table": "Roles",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "SURRegionMap",
                    "hash_column_indexes": [],
                    "n_columns": 5,
                    "sql": None
                },
                {
                    "source_table": "ServiceLogs",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "ServiceUsers",
                    "hash_column_indexes": [],
                    "n_columns": 9,
                    "sql": None
                },
                {
                    "source_table": "Settings",
                    "hash_column_indexes": [],
                    "n_columns": 8,
                    "sql": None
                },
                {
                    "source_table": "SkfServiceException",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "SkfServiceSheduler",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "Spec",
                    "hash_column_indexes": [],
                    "n_columns": 20,
                    "sql": None
                },
                {
                    "source_table": "SpecVersion",
                    "hash_column_indexes": [],
                    "n_columns": 17,
                    "sql": None
                },
                {
                    "source_table": "StationaryJournalAccounting",
                    "hash_column_indexes": [],
                    "n_columns": 18,
                    "sql": None
                },
                {
                    "source_table": "SurMO",
                    "hash_column_indexes": [],
                    "n_columns": 11,
                    "sql": None
                },
                {
                    "source_table": "SystemSettings",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "TbCard",
                    "hash_column_indexes": [],
                    "n_columns": 19,
                    "sql": None
                },
                {
                    "source_table": "Tender",
                    "hash_column_indexes": [],
                    "n_columns": 13,
                    "sql": None
                },
                {
                    "source_table": "TenderRegional",
                    "hash_column_indexes": [],
                    "n_columns": 7,
                    "sql": None
                },
                {
                    "source_table": "Translation",
                    "hash_column_indexes": [],
                    "n_columns": 10,
                    "sql": None
                },
                {
                    "source_table": "Users",
                    "hash_column_indexes": [],
                    "n_columns": 31,
                    "sql": None
                },
                {
                    "source_table": "UsersEasyPassword",
                    "hash_column_indexes": [],
                    "n_columns": 4,
                    "sql": None
                },
                {
                    "source_table": "WebConfigSettings",
                    "hash_column_indexes": [],
                    "n_columns": 6,
                    "sql": None
                },
                {
                    "source_table": "WrongRecords",
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
                    "source_table": "\u0411\u043e\u043b\u044c\u043d\u044b\u0435",
                    "hash_column_indexes": [
                        14,
                        15,
                        16,
                        19,
                        21
                    ],
                    "n_columns": 24,
                    "sql": None
                },
                {
                    "source_table": "\u0412\u0440\u0430\u0447\u0438",
                    "hash_column_indexes": [
                        3,
                        4,
                        5,
                        7
                    ],
                    "n_columns": 10,
                    "sql": None
                },
                {
                    "source_table": "\u0420\u0435\u0446\u0435\u043f\u0442\u044b",
                    "hash_column_indexes": [],
                    "n_columns": 44,
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