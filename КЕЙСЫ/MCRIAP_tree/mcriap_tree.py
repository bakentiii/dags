from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models.connection import Connection
from airflow.models.baseoperator import chain
from clickhouse_driver import Client, connect
import datetime

def get_5_connection():
    try:
        conn_5_secrets = Connection.get_connection_from_secrets(conn_id="Clickhouse-5")##скрытй коннекшн источника    
        return connect(host=conn_5_secrets.host, port=conn_5_secrets.port, password=conn_5_secrets.password, user=conn_5_secrets.login, connect_timeout=3600)
    except Exception as ex:
        print(f'Error connection of 5 server - {ex}')

def insert_values(data):
    conn_5 = get_5_connection()
    try:
        sql = f"""INSERT INTO DWH_CDM.MCRIAP_ELICENSE_TREE VALUES"""
        with conn_5.cursor() as cursor:
            cursor.executemany(sql, data)
            print(f'[+] Inserted {len(data)} rows to DWH_CDM.MCRIAP_ELICENSE_TREE')
    finally:
        conn_5.close()

def truncate_table():
    conn_5 = get_5_connection()
    try:
        sql = f"""TRUNCATE TABLE DWH_CDM.MCRIAP_ELICENSE_TREE;"""
        with conn_5.cursor() as cursor:
            cursor.execute(sql)
        print(f'[+] TABLE DWH_CDM.MCRIAP_ELICENSE_TREE TRUNCATED')
    finally:
        conn_5.close()


def extract_and_load():
    conn_source = get_5_connection()
    truncate_table()
    try:
        with conn_source.cursor() as cursor:

            ID = [39142,
                3447,
                2956,
                2867,
                2875,
                2876,
                2877,
                2889,
                2894,
                2837,
                1679,
                2990,
                3028,
                2997,
                3894,
                59,
                3011,
                3159,
                3341,
                3290,
                23270,
                23287,
                188,
                1495,
                33554,
                550,
                25680,
                73,
                38963,
                25573,
                72,
                38957,
                41684,
                2141,
                26745,
                26422,
                26646,
                26401,
                31727,
                29711,
                3418,
                3389,
                3439,
                3143,
                3223,
                3234,
                3248,
                3194,
                3102,
                3106,
                3109,
                3113,
                3010,
                2992,
                3041,
                3045,
                2183,
                2206,
                2267,
                2474,
                2600,
                2647,
                2649,
                2669,
                2705,
                2708,
                2823,
                1909,
                1913,
                1935,
                2002,
                2106,
                2127,
                25695,
                25878,
                25756,
                26221,
                26175,
                26197,
                25594,
                67,
                38291,
                3053,
                3114,
                3008,
                3122,
                3124,
                3202,
                3222,
                3421,
                25765,
                3070,
                3072,
                3081,
                3088,
                3091,
                3098,
                3100,
                82,
                38966,
                42243,
                3236,
                3286,
                3294,
                3417,
                3167,
                3036,
                26786,
                26773,
                26486,
                26680,
                3089,
                26738,
                3322,
                3251,
                3354,
                26753,
                89,
                38978,
                33555,
                40123,
                1494,
                276,
                32347,
                32353,
                32387,
                544,
                1288]

            for id_ in ID:
                SQL = f"""select
                            case
                                when NAME_2_LEVEL is null and NAME_3_LEVEL is null and NAME_4_LEVEL is null and NAME_5_LEVEL is null and NAME_6_LEVEL is null then 1
                                when NAME_2_LEVEL is not null and NAME_3_LEVEL is null and NAME_4_LEVEL is null and NAME_5_LEVEL is null and NAME_6_LEVEL is null then 2
                                when NAME_2_LEVEL is not null and NAME_3_LEVEL is not null and NAME_4_LEVEL is null and NAME_5_LEVEL is null and NAME_6_LEVEL is null then 3
                                when NAME_2_LEVEL is not null and NAME_3_LEVEL is not null and NAME_4_LEVEL is not null and NAME_5_LEVEL is null and NAME_6_LEVEL is null then 4
                                when NAME_2_LEVEL is not null and NAME_3_LEVEL is not null and NAME_4_LEVEL is not null and NAME_5_LEVEL is not null and NAME_6_LEVEL is null then 5
                                when NAME_2_LEVEL is not null and NAME_3_LEVEL is not null and NAME_4_LEVEL is not null and NAME_5_LEVEL is not null and NAME_6_LEVEL is not null then 6
                            end as LEVEL_NUM,
                            case 
                                when LEVEL_NUM = 1 then NAME_1_LEVEL
                                when LEVEL_NUM = 2 then CONCAT(NAME_1_LEVEL,' -> ',NAME_2_LEVEL)
                                when LEVEL_NUM = 3 then CONCAT(NAME_1_LEVEL,' -> ',NAME_2_LEVEL, ' -> ',NAME_3_LEVEL)
                                when LEVEL_NUM = 4 then CONCAT(NAME_1_LEVEL,' -> ',NAME_2_LEVEL, ' -> ',NAME_3_LEVEL, ' -> ',NAME_4_LEVEL)
                                when LEVEL_NUM = 5 then CONCAT(NAME_1_LEVEL,' -> ',NAME_2_LEVEL, ' -> ',NAME_3_LEVEL, ' -> ',NAME_4_LEVEL, ' -> ',NAME_5_LEVEL)
                                when LEVEL_NUM = 6 then CONCAT(NAME_1_LEVEL,' -> ',NAME_2_LEVEL, ' -> ',NAME_3_LEVEL, ' -> ',NAME_4_LEVEL, ' -> ',NAME_5_LEVEL, ' -> ',NAME_6_LEVEL)
                            end as FULL_NAME,
                            if(a.ID = 0,null,a.ID) as ID_1_LEVEL,
                            a.CODE AS CODE_1_LEVEL,
                            a.NAMERU as NAME_1_LEVEL,
                            a.hist AS ACTUAL_1_LEVEL,
                            if(b.ID = 0,null,b.ID) as ID_2_LEVEL,
                            b.PARENT AS PARENT_2_LEVEL,
                            b.CODE AS CODE_2_LEVEL,
                            b.NAMERU as NAME_2_LEVEL,
                            b.hist AS ACTUAL_2_LEVEL,
                            if(c.ID = 0,null,c.ID) as ID_3_LEVEL,
                            c.PARENT AS PARENT_3_LEVEL,
                            c.CODE AS CODE_3_LEVEL,
                            c.NAMERU as NAME_3_LEVEL,
                            c.hist AS ACTUAL_3_LEVEL,
                            if(d.ID = 0,null,d.ID) as ID_4_LEVEL,
                            d.PARENT AS PARENT_4_LEVEL,
                            d.CODE AS CODE_4_LEVEL,
                            d.NAMERU as NAME_4_LEVEL,
                            d.hist AS ACTUAL_4_LEVEL,
                            if(e.ID = 0,null,e.ID) as ID_5_LEVEL,
                            e.PARENT AS PARENT_5_LEVEL,
                            e.CODE AS CODE_5_LEVEL,
                            e.NAMERU as NAME_5_LEVEL,
                            e.hist AS ACTUAL_5_LEVEL,
                            if(f.ID = 0,null,f.ID) as ID_6_LEVEL,
                            f.PARENT AS PARENT_6_LEVEL,
                            f.CODE AS CODE_6_LEVEL,
                            f.NAMERU as NAME_6_LEVEL,
                            f.hist AS ACTUAL_6_LEVEL
                            from (
                                select ID,
                                NAMERU,
                                (case when ISDELETED = 1 and HISTORYDATE is not null then '(Удален, Истор)' when ISDELETED = 1 then '(Удален)' when HISTORYDATE is not null then '(Истор)' else '' end) as hist, CODE
                                from MCRIAP_ELICENSEV3.ACTIVITYTYPES
                                where PARENT is NULL and ID in({_id})
                            ) a
                            left join
                            (select ID,PARENT, NAMERU,(case when ISDELETED = 1 and HISTORYDATE is not null then '(Удален, Истор)' when ISDELETED = 1 then '(Удален)' when HISTORYDATE is not null then '(Истор)' else '' end) as hist,CODE
                                from MCRIAP_ELICENSEV3.ACTIVITYTYPES ) b ON a.ID = b.PARENT
                            left join
                            (select ID,PARENT, NAMERU,(case when ISDELETED = 1 and HISTORYDATE is not null then '(Удален, Истор)' when ISDELETED = 1 then '(Удален)' when HISTORYDATE is not null then '(Истор)' else '' end) as hist,CODE
                                from MCRIAP_ELICENSEV3.ACTIVITYTYPES) c ON b.ID = c.PARENT
                            left join
                            (select ID,PARENT, NAMERU,(case when ISDELETED = 1 and HISTORYDATE is not null then '(Удален, Истор)' when ISDELETED = 1 then '(Удален)' when HISTORYDATE is not null then '(Истор)' else '' end) as hist,CODE
                                from MCRIAP_ELICENSEV3.ACTIVITYTYPES ) d ON c.ID = d.PARENT
                            left join
                            (select ID,PARENT, NAMERU,(case when ISDELETED = 1 and HISTORYDATE is not null then '(Удален, Истор)' when ISDELETED = 1 then '(Удален)' when HISTORYDATE is not null then '(Истор)' else '' end) as hist,CODE
                                from MCRIAP_ELICENSEV3.ACTIVITYTYPES ) e ON d.ID = e.PARENT
                            left join
                            (select ID,PARENT, NAMERU,(case when ISDELETED = 1 and HISTORYDATE is not null then '(Удален, Истор)' when ISDELETED = 1 then '(Удален)' when HISTORYDATE is not null then '(Истор)' else '' end) as hist,CODE
                                from MCRIAP_ELICENSEV3.ACTIVITYTYPES ) f ON e.ID = f.PARENT"""
                data = cursor.execute(SQL)

                while True:
                    data = cursor.fetchmany(50000)
                    print(f'[+] {len(data)} rows fetched')

                    if not len(data):
                        print(f'[+] breaked')
                        break

                    insert_values(data)
    finally:
        conn_source.close()


with DAG(
    dag_id='MCRIAP_ELICENSE_V3_TREE',
    default_args={'owner':'Bakhtiyar'},
    start_date=datetime.datetime(2023, 10, 3), 
    schedule_interval = None,
    catchup = False,
    tags=['mcriap']
) as dag:

    task_ = PythonOperator(
        task_id=f"MCRIAP_ELICENSE_V3_TREE",
        trigger_rule='all_done',
        python_callable=extract_and_load
    )