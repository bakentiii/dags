from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models.connection import Connection
from airflow.models.baseoperator import chain
from clickhouse_driver import Client, connect
import datetime


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
    conn_17 = get_17_connection()
    try:
        sql = f"""INSERT INTO {schema}.{table} VALUES"""
        with conn_17.cursor() as cursor:
            cursor.executemany(sql, data)
            print(f'[+] Inserted {len(data)} rows to {schema}.{table}')
    finally:
        conn_17.close()


def truncate_table(schema, table):
    conn_17 = get_17_connection()
    try:
        sql = f"""TRUNCATE TABLE {schema}.{table};"""
        with conn_17.cursor() as cursor:
            cursor.execute(sql)
        print(f'[+] TABLE {schema}.{table} TRUNCATED')
    finally:
        conn_17.close()




def extract_and_load(target_schema, target_table, SQL, BATCH_SIZE=5000):
    SDU_LOAD_IN_DT = datetime.datetime.now() + datetime.timedelta(hours=6)

    conn_source = get_5_connection()
    try:
        with conn_source.cursor() as cursor:
            data = cursor.execute(SQL)

            truncate_table(target_schema, target_table)

            while True:
                data = cursor.fetchmany(50000)
                print(f'[+] {len(data)} rows fetched')

                if not len(data):
                    print(f'[+] breaked')
                    break

                insert_values(data, target_schema, target_table)
    finally:
        conn_source.close()




def main(*args, **kwargs):
    extract_and_load(
        target_schema=kwargs['target_schema'],
        target_table=kwargs['target_table'],
        SQL=kwargs['sql'],
        BATCH_SIZE=10000)



with DAG(
    dag_id='DWH_CDM_RCHL_TABLES_MNVO',
    default_args={'owner':'Bakhtiyar'},
    start_date=datetime.datetime(2023, 7, 19), 
    schedule_interval = '0 0 * * *',
    catchup = False,
    tags=['rchl', 'dwh_cdm']
) as dag:

    TABLES = {
        "NOBD_MP_P1_V1": {
            'sql': """
            SELECT --TOP 15
distinct s.ID AS ID,
sa.SHIFT_ID AS KOLVO_SMEN_OBUCH
FROM MON_NOBD.SCHOOL s
INNER JOIN MON_NOBD.SCHOOL_ATTR sa ON sa.SCHOOL_ID = s.ID AND sa.EDU_PERIOD_ID = toInt32(0)
left JOIN MON_NOBD.SCHOOL_SPEC ss ON sa.ID = ss.SCHOOL_ATTR_ID 
LEFT JOIN MON_NOBD.D_SCHOOLSPEC_TYPE sst ON sst.ID = ss.SPEC_TYPE_ID
WHERE sa.ID = ss.SCHOOL_ATTR_ID AND ( sst.CODE LIKE '02.1.%' OR sst.CODE LIKE '02.2.%' OR sst.CODE LIKE '02.3.%' OR sst.CODE LIKE '02.4.%' OR sst.CODE LIKE '02.5.%' OR sst.CODE LIKE '07.%' OR sst.CODE IN ('02.6.1', '02.6.2', '02.6.3', '02.6.4', '08.3', '08.4', '08.5', '08.6', '09.3', '09.4') )
AND sa.DATE_CLOSE IS NULL"""
        },
      
        "NOBD_MP_P2_V1": {
            'sql': """
                    SELECT --128 --NUMBER OF ROWS
        --TOP 15 
        distinct
        s.ID AS ID,
        s.BIN AS BIN_ORGANIZ_OBRZAOV, 
        area.RNAME AS OBLAST_RUS, 
        area.KNAME AS OBLAST_KAZ, 
        region.RNAME AS RAYON_RU, 
        region.KNAME AS RAYON_KAZ, 
        locality.RNAME AS NAS_PUNKT_RU, 
        locality.KNAME AS NAS_PUNKT_KAZ, 
        s.KK_NAME AS NAIMENOV_ORGANIZ_OBRAZOV_KZ, 
        s.RU_NAME AS NAIMENOV_ORGANIZ_OBRAZOV_RU,
        s.KK_FULLNAME AS FULL_NAME_ORGANIZ_OBRAZOV_KZ,
        s.RU_FULLNAME AS FULL_NAME_ORGANIZ_OBRAZOV_RU,
        ts.RNAME AS TIP_ORGANIZAC_OBRAZOV,
        EDUORG_TYPE.RNAME as VID_ORG,
        --( SELECT STRING_AGG(sst.rname, ', ') FROM MON_NOBD.school_spec LEFT JOIN MON_NOBD.d_schoolSpec_type sst ON sst.id = MON_NOBD.school_spec.spec_type_id WHERE sa.id = MON_NOBD.school_spec.school_attr_id ) AS 'Виды организации',
        areatype.RNAME AS TERRITOR_PRINADLEZH,
        sa.GEO_COORD AS COORDINATES

        FROM MON_NOBD.SCHOOL s
        INNER JOIN MON_NOBD.SCHOOL_ATTR sa ON sa.SCHOOL_ID = s.ID AND sa.EDU_PERIOD_ID = toInt32(0)
        -- КАТО
        INNER JOIN MON_NOBD.D_REGION locality ON sa.REGION_ID = locality.ID 
        LEFT JOIN MON_NOBD.D_REGION area ON area.ID = locality.AREA_ID
        LEFT JOIN MON_NOBD.D_REGION region ON region.ID = locality.DISTRICT_ID
        -- связь со справочниками значений по организации
        LEFT JOIN MON_NOBD.D_TYPE_SCHOOL ts ON ts.ID = sa.TYPE_SCHOOL_ID
        LEFT JOIN MON_NOBD.D_AREATYPE areatype ON areatype.ID = sa.AREATYPE_ID
        -- условия организации
        LEFT JOIN MON_NOBD.SCHOOL_SPEC ss ON sa.ID = ss.SCHOOL_ATTR_ID 
        LEFT JOIN MON_NOBD.D_SCHOOLSPEC_TYPE sst ON sst.ID = ss.SPEC_TYPE_ID
        LEFT JOIN 
        (
        SELECT SS.SCHOOL_ATTR_ID, trim(BOTH '[]' FROM toString(groupArray(SST.RNAME))) AS RNAME
        FROM MON_NOBD.SCHOOL_SPEC SS
        LEFT JOIN MON_NOBD.D_SCHOOLSPEC_TYPE SST ON SST.ID = SS.SPEC_TYPE_ID 
        GROUP BY SS.SCHOOL_ATTR_ID--, SST.RNAME 
        --ORDER BY SS.SCHOOL_ATTR_ID
        ) EDUORG_TYPE ON EDUORG_TYPE.SCHOOL_ATTR_ID = sa.ID 
        WHERE sst.CODE LIKE '06.%'
        AND sa.DATE_CLOSE IS NULL"""
                },
            
        "NOBD_MP_P3_V1": {
            'sql': """
                    SELECT --11 132 --NUMBER OF ROWS
        --TOP 15 
        DISTINCT 
        s.ID AS ID,
        s.BIN AS BIN_ORGANIZ_OBRZAOV, 
        area.RNAME AS OBLAST_RUS, 
        area.KNAME AS OBLAST_KAZ, 
        region.RNAME AS RAYON_RU, 
        region.KNAME AS RAYON_KAZ, 
        locality.RNAME AS NAS_PUNKT_RU, 
        locality.KNAME AS NAS_PUNKT_KAZ, 
        s.KK_NAME AS NAIMENOV_ORGANIZ_OBRAZOV_KZ, 
        s.RU_NAME AS NAIMENOV_ORGANIZ_OBRAZOV_RU,
        s.KK_FULLNAME AS FULL_NAME_ORGANIZ_OBRAZOV_KZ,
        s.RU_FULLNAME AS FULL_NAME_ORGANIZ_OBRAZOV_RU,
        ts.RNAME AS TIP_ORGANIZAC_OBRAZOV,
        EDUORG_TYPE.RNAME as VID_ORG,
        --( SELECT STRING_AGG(sst.rname, ', ') FROM MON_NOBD.school_spec LEFT JOIN MON_NOBD.d_schoolSpec_type sst ON sst.id = MON_NOBD.school_spec.spec_type_id WHERE sa.id = MON_NOBD.school_spec.school_attr_id ) AS 'Виды организации',
        areatype.RNAME AS TERRITOR_PRINADLEZH,
        sa.GEO_COORD AS COORDINATES

        FROM MON_NOBD.SCHOOL s
        INNER JOIN MON_NOBD.SCHOOL_ATTR sa ON sa.SCHOOL_ID = s.ID AND sa.EDU_PERIOD_ID = toInt32(0)
        -- КАТО
        INNER JOIN MON_NOBD.D_REGION locality ON sa.REGION_ID = locality.ID 
        LEFT JOIN MON_NOBD.D_REGION area ON area.ID = locality.AREA_ID
        LEFT JOIN MON_NOBD.D_REGION region ON region.ID = locality.DISTRICT_ID
        -- связь со справочниками значений по организации
        LEFT JOIN MON_NOBD.D_TYPE_SCHOOL ts ON ts.ID = sa.TYPE_SCHOOL_ID
        LEFT JOIN MON_NOBD.D_AREATYPE areatype ON areatype.ID = sa.AREATYPE_ID
        -- условия организации
        LEFT JOIN MON_NOBD.SCHOOL_SPEC ss ON sa.ID = ss.SCHOOL_ATTR_ID 
        LEFT JOIN MON_NOBD.D_SCHOOLSPEC_TYPE sst ON sst.ID = ss.SPEC_TYPE_ID
        LEFT JOIN 
        (
        SELECT SS.SCHOOL_ATTR_ID, trim(BOTH '[]' FROM toString(groupArray(SST.RNAME))) AS RNAME
        FROM MON_NOBD.SCHOOL_SPEC SS
        LEFT JOIN MON_NOBD.D_SCHOOLSPEC_TYPE SST ON SST.ID = SS.SPEC_TYPE_ID 
        GROUP BY SS.SCHOOL_ATTR_ID--, SST.RNAME 
        --ORDER BY SS.SCHOOL_ATTR_ID
        ) EDUORG_TYPE ON EDUORG_TYPE.SCHOOL_ATTR_ID = sa.ID 
        WHERE sa.ID = ss.SCHOOL_ATTR_ID AND sst.CODE IN ('01.1', '01.2', '01.3', '01.4', '01.5', '01.6', '08.1', '08.2')
        AND sa.DATE_CLOSE IS NULL"""
                },
            
        "NOBD_MP_P4_V1": {
            'sql': """
                    SELECT --11 723 --NUMBER OF ROWS
        --TOP 15 
        distinct
        s.ID AS ID,
        s.BIN AS BIN_ORGANIZ_OBRZAOV, 
        area.RNAME AS OBLAST_RUS, 
        area.KNAME AS OBLAST_KAZ, 
        region.RNAME AS RAYON_RU, 
        region.KNAME AS RAYON_KAZ, 
        locality.RNAME AS NAS_PUNKT_RU, 
        locality.KNAME AS NAS_PUNKT_KAZ, 
        s.KK_NAME AS NAIMENOV_ORGANIZ_OBRAZOV_KZ, 
        s.RU_NAME AS NAIMENOV_ORGANIZ_OBRAZOV_RU,
        s.KK_FULLNAME AS FULL_NAME_ORGANIZ_OBRAZOV_KZ,
        s.RU_FULLNAME AS FULL_NAME_ORGANIZ_OBRAZOV_RU,
        ts.RNAME AS TIP_ORGANIZAC_OBRAZOV,
        EDUORG_TYPE.RNAME as VID_ORG,
        --( SELECT STRING_AGG(sst.rname, ', ') FROM MON_NOBD.school_spec LEFT JOIN MON_NOBD.d_schoolSpec_type sst ON sst.id = MON_NOBD.school_spec.spec_type_id WHERE sa.id = MON_NOBD.school_spec.school_attr_id ) AS 'Виды организации',
        areatype.RNAME AS TERRITOR_PRINADLEZH,
        sa.GEO_COORD AS COORDINATES

        FROM MON_NOBD.SCHOOL s
        INNER JOIN MON_NOBD.SCHOOL_ATTR sa ON sa.SCHOOL_ID = s.ID AND sa.EDU_PERIOD_ID = toInt32(0)
        -- КАТО
        INNER JOIN MON_NOBD.D_REGION locality ON sa.REGION_ID = locality.ID 
        LEFT JOIN MON_NOBD.D_REGION area ON area.ID = locality.AREA_ID
        LEFT JOIN MON_NOBD.D_REGION region ON region.ID = locality.DISTRICT_ID
        -- связь со справочниками значений по организации
        LEFT JOIN MON_NOBD.D_TYPE_SCHOOL ts ON ts.ID = sa.TYPE_SCHOOL_ID
        LEFT JOIN MON_NOBD.D_AREATYPE areatype ON areatype.ID = sa.AREATYPE_ID
        -- условия организации
        LEFT JOIN MON_NOBD.SCHOOL_SPEC ss ON sa.ID = ss.SCHOOL_ATTR_ID 
        LEFT JOIN MON_NOBD.D_SCHOOLSPEC_TYPE sst ON sst.ID = ss.SPEC_TYPE_ID
        LEFT JOIN 
        (
        SELECT SS.SCHOOL_ATTR_ID, trim(BOTH '[]' FROM toString(groupArray(SST.RNAME))) AS RNAME
        FROM MON_NOBD.SCHOOL_SPEC SS
        LEFT JOIN MON_NOBD.D_SCHOOLSPEC_TYPE SST ON SST.ID = SS.SPEC_TYPE_ID 
        GROUP BY SS.SCHOOL_ATTR_ID--, SST.RNAME 
        --ORDER BY SS.SCHOOL_ATTR_ID
        ) EDUORG_TYPE ON EDUORG_TYPE.SCHOOL_ATTR_ID = sa.ID 
        WHERE sa.ID = ss.SCHOOL_ATTR_ID AND ( sst.CODE LIKE '02.1.%' OR sst.CODE LIKE '02.2.%' OR sst.CODE LIKE '02.3.%' OR sst.CODE LIKE '02.4.%' OR sst.CODE LIKE '02.5.%' OR sst.CODE LIKE '07.%' OR sst.CODE IN ('02.6.1', '02.6.2', '02.6.3', '02.6.4', '08.3', '08.4', '08.5', '08.6', '09.3', '09.4') )
        AND sa.DATE_CLOSE IS NULL"""
                },
            
        "NOBD_MP_P5_V1": {
            'sql': """
                    SELECT --783 --NUMBER OF ROWS
        --TOP 15 
        distinct
        s.ID AS ID,
        s.BIN AS BIN_ORGANIZ_OBRZAOV, 
        area.RNAME AS OBLAST_RUS, 
        area.KNAME AS OBLAST_KAZ, 
        region.RNAME AS RAYON_RU, 
        region.KNAME AS RAYON_KAZ, 
        locality.RNAME AS NAS_PUNKT_RU, 
        locality.KNAME AS NAS_PUNKT_KAZ, 
        s.KK_NAME AS NAIMENOV_ORGANIZ_OBRAZOV_KZ, 
        s.RU_NAME AS NAIMENOV_ORGANIZ_OBRAZOV_RU,
        s.KK_FULLNAME AS FULL_NAME_ORGANIZ_OBRAZOV_KZ,
        s.RU_FULLNAME AS FULL_NAME_ORGANIZ_OBRAZOV_RU,
        ts.RNAME AS TIP_ORGANIZAC_OBRAZOV,
        EDUORG_TYPE.RNAME as VID_ORG,
        --( SELECT STRING_AGG(sst.rname, ', ') FROM MON_NOBD.school_spec LEFT JOIN MON_NOBD.d_schoolSpec_type sst ON sst.id = MON_NOBD.school_spec.spec_type_id WHERE sa.id = MON_NOBD.school_spec.school_attr_id ) AS 'Виды организации',
        areatype.RNAME AS TERRITOR_PRINADLEZH,
        sa.GEO_COORD AS COORDINATES

        FROM MON_NOBD.SCHOOL s
        INNER JOIN MON_NOBD.SCHOOL_ATTR sa ON sa.SCHOOL_ID = s.ID AND sa.EDU_PERIOD_ID = toInt32(0)
        -- КАТО
        INNER JOIN MON_NOBD.D_REGION locality ON sa.REGION_ID = locality.ID 
        LEFT JOIN MON_NOBD.D_REGION area ON area.ID = locality.AREA_ID
        LEFT JOIN MON_NOBD.D_REGION region ON region.ID = locality.DISTRICT_ID
        -- связь со справочниками значений по организации
        LEFT JOIN MON_NOBD.D_TYPE_SCHOOL ts ON ts.ID = sa.TYPE_SCHOOL_ID
        LEFT JOIN MON_NOBD.D_AREATYPE areatype ON areatype.ID = sa.AREATYPE_ID
        -- условия организации
        LEFT JOIN MON_NOBD.SCHOOL_SPEC ss ON sa.ID = ss.SCHOOL_ATTR_ID 
        LEFT JOIN MON_NOBD.D_SCHOOLSPEC_TYPE sst ON sst.ID = ss.SPEC_TYPE_ID
        LEFT JOIN 
        (
        SELECT SS.SCHOOL_ATTR_ID, trim(BOTH '[]' FROM toString(groupArray(SST.RNAME))) AS RNAME
        FROM MON_NOBD.SCHOOL_SPEC SS
        LEFT JOIN MON_NOBD.D_SCHOOLSPEC_TYPE SST ON SST.ID = SS.SPEC_TYPE_ID 
        GROUP BY SS.SCHOOL_ATTR_ID--, SST.RNAME 
        --ORDER BY SS.SCHOOL_ATTR_ID
        ) EDUORG_TYPE ON EDUORG_TYPE.SCHOOL_ATTR_ID = sa.ID 
        WHERE sa.ID = ss.SCHOOL_ATTR_ID AND ( sa.ID = ss.SCHOOL_ATTR_ID AND sst.CODE LIKE '03.%' )
        AND sa.DATE_CLOSE IS NULL"""
                },
            
        "NOBD_MP_P6_V1": {
            'sql': """
                    SELECT --5 --NUMBER OF ROWS
        --TOP 15 
        s.ID AS ID,
        s.BIN AS BIN_ORGANIZ_OBRZAOV, 
        area.RNAME AS OBLAST_RUS, 
        area.KNAME AS OBLAST_KAZ, 
        region.RNAME AS RAYON_RU, 
        region.KNAME AS RAYON_KAZ, 
        locality.RNAME AS NAS_PUNKT_RU, 
        locality.KNAME AS NAS_PUNKT_KAZ, 
        s.KK_NAME AS NAIMENOV_ORGANIZ_OBRAZOV_KZ, 
        s.RU_NAME AS NAIMENOV_ORGANIZ_OBRAZOV_RU,
        s.KK_FULLNAME AS FULL_NAME_ORGANIZ_OBRAZOV_KZ,
        s.RU_FULLNAME AS FULL_NAME_ORGANIZ_OBRAZOV_RU,
        ts.RNAME AS TIP_ORGANIZAC_OBRAZOV,
        EDUORG_TYPE.RNAME as VID_ORG,
        --( SELECT STRING_AGG(sst.rname, ', ') FROM MON_NOBD.school_spec LEFT JOIN MON_NOBD.d_schoolSpec_type sst ON sst.id = MON_NOBD.school_spec.spec_type_id WHERE sa.id = MON_NOBD.school_spec.school_attr_id ) AS 'Виды организации',
        areatype.RNAME AS TERRITOR_PRINADLEZH,
        sa.GEO_COORD AS COORDINATES

        FROM MON_NOBD.SCHOOL s
        INNER JOIN MON_NOBD.SCHOOL_ATTR sa ON sa.SCHOOL_ID = s.ID AND sa.EDU_PERIOD_ID = toInt32(0)
        -- КАТО
        INNER JOIN MON_NOBD.D_REGION locality ON sa.REGION_ID = locality.ID 
        LEFT JOIN MON_NOBD.D_REGION area ON area.ID = locality.AREA_ID
        LEFT JOIN MON_NOBD.D_REGION region ON region.ID = locality.DISTRICT_ID
        -- связь со справочниками значений по организации
        LEFT JOIN MON_NOBD.D_TYPE_SCHOOL ts ON ts.ID = sa.TYPE_SCHOOL_ID
        LEFT JOIN MON_NOBD.D_AREATYPE areatype ON areatype.ID = sa.AREATYPE_ID
        -- условия организации
        LEFT JOIN MON_NOBD.SCHOOL_SPEC ss ON sa.ID = ss.SCHOOL_ATTR_ID 
        LEFT JOIN MON_NOBD.D_SCHOOLSPEC_TYPE sst ON sst.ID = ss.SPEC_TYPE_ID
        LEFT JOIN 
        (
        SELECT SS.SCHOOL_ATTR_ID, trim(BOTH '[]' FROM toString(groupArray(SST.RNAME))) AS RNAME
        FROM MON_NOBD.SCHOOL_SPEC SS
        LEFT JOIN MON_NOBD.D_SCHOOLSPEC_TYPE SST ON SST.ID = SS.SPEC_TYPE_ID 
        GROUP BY SS.SCHOOL_ATTR_ID--, SST.RNAME 
        --ORDER BY SS.SCHOOL_ATTR_ID
        ) EDUORG_TYPE ON EDUORG_TYPE.SCHOOL_ATTR_ID = sa.ID 
        WHERE sa.ID = ss.SCHOOL_ATTR_ID AND ( sa.ID = ss.SCHOOL_ATTR_ID AND sst.CODE IN ('02.3.5') )
        AND sa.DATE_CLOSE IS NULL"""
                },
            
        "NOBD_MP_P8_V1": {
            'sql': """
                    SELECT --312 --NUMBER OF ROWS
        --TOP 15 
        s.ID AS ID,
        s.BIN AS BIN_ORGANIZ_OBRZAOV, 
        area.RNAME AS OBLAST_RUS, 
        area.KNAME AS OBLAST_KAZ, 
        region.RNAME AS RAYON_RU, 
        region.KNAME AS RAYON_KAZ, 
        locality.RNAME AS NAS_PUNKT_RU, 
        locality.KNAME AS NAS_PUNKT_KAZ, 
        s.KK_NAME AS NAIMENOV_ORGANIZ_OBRAZOV_KZ, 
        s.RU_NAME AS NAIMENOV_ORGANIZ_OBRAZOV_RU,
        s.KK_FULLNAME AS FULL_NAME_ORGANIZ_OBRAZOV_KZ,
        s.RU_FULLNAME AS FULL_NAME_ORGANIZ_OBRAZOV_RU,
        ts.RNAME AS TIP_ORGANIZAC_OBRAZOV,
        EDUORG_TYPE.RNAME as VID_ORG,
        --( SELECT STRING_AGG(sst.rname, ', ') FROM MON_NOBD.school_spec LEFT JOIN MON_NOBD.d_schoolSpec_type sst ON sst.id = MON_NOBD.school_spec.spec_type_id WHERE sa.id = MON_NOBD.school_spec.school_attr_id ) AS 'Виды организации',
        areatype.RNAME AS TERRITOR_PRINADLEZH,
        sa.GEO_COORD AS COORDINATES

        FROM MON_NOBD.SCHOOL s
        INNER JOIN MON_NOBD.SCHOOL_ATTR sa ON sa.SCHOOL_ID = s.ID AND sa.EDU_PERIOD_ID = toInt32(0)
        -- КАТО
        INNER JOIN MON_NOBD.D_REGION locality ON sa.REGION_ID = locality.ID 
        LEFT JOIN MON_NOBD.D_REGION area ON area.ID = locality.AREA_ID
        LEFT JOIN MON_NOBD.D_REGION region ON region.ID = locality.DISTRICT_ID
        -- связь со справочниками значений по организации
        LEFT JOIN MON_NOBD.D_TYPE_SCHOOL ts ON ts.ID = sa.TYPE_SCHOOL_ID
        LEFT JOIN MON_NOBD.D_AREATYPE areatype ON areatype.ID = sa.AREATYPE_ID
        -- условия организации
        LEFT JOIN MON_NOBD.SCHOOL_SPEC ss ON sa.ID = ss.SCHOOL_ATTR_ID 
        LEFT JOIN MON_NOBD.D_SCHOOLSPEC_TYPE sst ON sst.ID = ss.SPEC_TYPE_ID
        LEFT JOIN 
        (
        SELECT SS.SCHOOL_ATTR_ID, trim(BOTH '[]' FROM toString(groupArray(SST.RNAME))) AS RNAME
        FROM MON_NOBD.SCHOOL_SPEC SS
        LEFT JOIN MON_NOBD.D_SCHOOLSPEC_TYPE SST ON SST.ID = SS.SPEC_TYPE_ID 
        GROUP BY SS.SCHOOL_ATTR_ID--, SST.RNAME 
        --ORDER BY SS.SCHOOL_ATTR_ID
        ) EDUORG_TYPE ON EDUORG_TYPE.SCHOOL_ATTR_ID = sa.ID 
        WHERE sa.ID = ss.SCHOOL_ATTR_ID AND sa.ID = ss.SCHOOL_ATTR_ID AND sst.CODE IN ('08.7', '08.8', '08.9', '08.93')
        AND sa.DATE_CLOSE IS NULL"""
                },
            
        "NOBD_MP_P9_V1": {
            'sql': """
                    SELECT --114 --NUMBER OF ROWS
        --TOP 15 
        s.ID AS ID,
        s.BIN AS BIN_ORGANIZ_OBRZAOV, 
        area.RNAME AS OBLAST_RUS, 
        area.KNAME AS OBLAST_KAZ, 
        region.RNAME AS RAYON_RU, 
        region.KNAME AS RAYON_KAZ, 
        locality.RNAME AS NAS_PUNKT_RU, 
        locality.KNAME AS NAS_PUNKT_KAZ, 
        s.KK_NAME AS NAIMENOV_ORGANIZ_OBRAZOV_KZ, 
        s.RU_NAME AS NAIMENOV_ORGANIZ_OBRAZOV_RU,
        s.KK_FULLNAME AS FULL_NAME_ORGANIZ_OBRAZOV_KZ,
        s.RU_FULLNAME AS FULL_NAME_ORGANIZ_OBRAZOV_RU,
        ts.RNAME AS TIP_ORGANIZAC_OBRAZOV,
        EDUORG_TYPE.RNAME as VID_ORG,
        --( SELECT STRING_AGG(sst.rname, ', ') FROM MON_NOBD.school_spec LEFT JOIN MON_NOBD.d_schoolSpec_type sst ON sst.id = MON_NOBD.school_spec.spec_type_id WHERE sa.id = MON_NOBD.school_spec.school_attr_id ) AS 'Виды организации',
        areatype.RNAME AS TERRITOR_PRINADLEZH,
        sa.GEO_COORD AS COORDINATES

        FROM MON_NOBD.SCHOOL s
        INNER JOIN MON_NOBD.SCHOOL_ATTR sa ON sa.SCHOOL_ID = s.ID AND sa.EDU_PERIOD_ID = toInt32(0)
        -- КАТО
        INNER JOIN MON_NOBD.D_REGION locality ON sa.REGION_ID = locality.ID 
        LEFT JOIN MON_NOBD.D_REGION area ON area.ID = locality.AREA_ID
        LEFT JOIN MON_NOBD.D_REGION region ON region.ID = locality.DISTRICT_ID
        -- связь со справочниками значений по организации
        LEFT JOIN MON_NOBD.D_TYPE_SCHOOL ts ON ts.ID = sa.TYPE_SCHOOL_ID
        LEFT JOIN MON_NOBD.D_AREATYPE areatype ON areatype.ID = sa.AREATYPE_ID
        -- условия организации
        LEFT JOIN MON_NOBD.SCHOOL_SPEC ss ON sa.ID = ss.SCHOOL_ATTR_ID 
        LEFT JOIN MON_NOBD.D_SCHOOLSPEC_TYPE sst ON sst.ID = ss.SPEC_TYPE_ID
        LEFT JOIN 
        (
        SELECT SS.SCHOOL_ATTR_ID, trim(BOTH '[]' FROM toString(groupArray(SST.RNAME))) AS RNAME
        FROM MON_NOBD.SCHOOL_SPEC SS
        LEFT JOIN MON_NOBD.D_SCHOOLSPEC_TYPE SST ON SST.ID = SS.SPEC_TYPE_ID 
        GROUP BY SS.SCHOOL_ATTR_ID--, SST.RNAME 
        --ORDER BY SS.SCHOOL_ATTR_ID
        ) EDUORG_TYPE ON EDUORG_TYPE.SCHOOL_ATTR_ID = sa.ID 
        WHERE sa.ID = ss.SCHOOL_ATTR_ID AND sa.ID = ss.SCHOOL_ATTR_ID AND sst.CODE LIKE '09.%'
        AND sa.DATE_CLOSE IS NULL"""
                },
            
        "NOBD_MP_P10_V1": {
            'sql': """
                    SELECT DISTINCT --9 968 373
        s.ID AS ID_ORGANIZACII_OBRAZOVANII,
        st.ID AS ID_UCHASHEGOSYA, 
        e.EDU_ID ID_OBUCHENIYA,
        st.IIN AS IIN,
        st.LASTNAME AS SURNAME,
        st.FIRSTNAME AS NAME,
        COALESCE(st.MIDDLENAME, '') AS MIDDLENAME,
        sta.BIRTHDATE AS BIRTHDATE,
        grade.RNAME AS PARALLEL,
        lang.RNAME AS YAZIK_OBUCH
        FROM MON_NOBD.SCHOOL s
        INNER JOIN MON_NOBD.SCHOOL_ATTR sa ON sa.SCHOOL_ID = s.ID AND sa.EDU_PERIOD_ID = toInt32(0)
        INNER JOIN MON_NOBD.EDUCATION e ON e.SCHOOL_ID = s.ID AND e.EDU_PERIOD_ID = toInt32(0)
        INNER JOIN MON_NOBD.STUDENT_ATTR sta ON sta.STUDENT_ID = e.STUDENT_ID AND sta.EDU_PERIOD_ID = toInt32(0)
        INNER JOIN MON_NOBD.STUDENT st ON st.ID = e.STUDENT_ID
        inner join MON_NOBD.SCHOOL_SPEC on sa.ID = MON_NOBD.SCHOOL_SPEC.SCHOOL_ATTR_ID
        left join MON_NOBD.D_SCHOOLSPEC_TYPE sst ON sst.ID = MON_NOBD.SCHOOL_SPEC.SPEC_TYPE_ID
        LEFT JOIN MON_NOBD.D_CLASS_NUM2 grade ON e.CLASS_NUM_ID = grade.ID
        LEFT JOIN MON_NOBD.D_LANG lang ON lang.ID = e.LANG_ID
        WHERE sst.CODE LIKE '02.1.%' 
        OR sst.CODE LIKE '02.2.%' 
        OR sst.CODE LIKE '02.3.%' 
        OR sst.CODE LIKE '02.4.%' 
        OR sst.CODE LIKE '02.5.%' 
        OR sst.CODE LIKE '07.%' 
        OR sst.CODE IN ('02.6.1', '02.6.2', '02.6.3', '02.6.4', '08.3', '08.4', '08.5', '08.6', '09.3', '09.4') 
        AND sa.DATE_CLOSE IS NULL"""
                },
            
        "NOBD_MP_P11_V1": {
            'sql': """
                    --Script #1
        SELECT 
        S.ID AS ID 
        , GR1.RNAME AS CLASS_1 
        , GR2.RNAME AS CLASS_2
        , GR3.RNAME AS CLASS_3 
        , GR4.RNAME AS CLASS_4
        , CC.CLASS_CNT AS KOLVO_KLASSOV
        FROM MON_NOBD.SCHOOL S
        INNER JOIN 
        (
        SELECT ATTR.ID 
        , ATTR.SCHOOL_ID
        , ATTR.DATE_CLOSE
        FROM MON_NOBD.SCHOOL_ATTR ATTR
        WHERE ATTR.EDU_PERIOD_ID = 0
        ) SA ON SA.SCHOOL_ID = S.ID 
        INNER JOIN MON_NOBD.SCHOOL_COMBCLASS CC ON CC.SCHOOL_ATTR_ID = SA.ID
        LEFT JOIN MON_NOBD.D_CLASS_NUM2 GR1 ON GR1.ID = CC.CLASS1_NUM_ID 
        LEFT JOIN MON_NOBD.D_CLASS_NUM2 GR2 ON GR2.ID = CC.CLASS2_NUM_ID 
        LEFT JOIN MON_NOBD.D_CLASS_NUM2 GR3 ON GR3.ID = CC.CLASS3_NUM_ID 
        LEFT JOIN MON_NOBD.D_CLASS_NUM2 GR4 ON GR4.ID = CC.CLASS4_NUM_ID 
        WHERE SA.DATE_CLOSE IS NULL 
        AND (
        CC.CLASS1_NUM_ID = 3 
        OR CC.CLASS2_NUM_ID = 3 
        OR CC.CLASS3_NUM_ID = 3 
        OR CC.CLASS4_NUM_ID = 3)
        AND SA.ID IN (
        SELECT DISTINCT SS.SCHOOL_ATTR_ID AS SCHOOL_ATTR_ID
        FROM MON_NOBD.SCHOOL_SPEC SS 
        INNER JOIN MON_NOBD.SCHOOL_ATTR SA ON SA.ID = SS.SCHOOL_ATTR_ID 
        LEFT JOIN MON_NOBD.D_SCHOOLSPEC_TYPE SST ON SST.ID = SS.SPEC_TYPE_ID 
        WHERE 
        (
        SST.CODE LIKE '02.1.%' 
        OR SST.CODE LIKE '02.2.%' 
        OR SST.CODE LIKE '02.3.%' 
        OR SST.CODE LIKE '02.4.%' 
        OR SST.CODE LIKE '02.5.%' 
        OR SST.CODE LIKE '07.%' 
        OR SST.CODE IN ('02.6.1', '02.6.2', '02.6.3', '02.6.4', '08.3', '08.4', '08.5', '08.6', '09.3', '09.4')
        )
        AND SA.DATE_CLOSE IS NULL
        )
        ORDER BY S.ID

        --Script #2
        SELECT 
        S.ID AS ID 
        , GR1.RNAME AS CLASS_1 
        , GR2.RNAME AS CLASS_2
        , GR3.RNAME AS CLASS_3 
        , GR4.RNAME AS CLASS_4
        , CC.CLASS_CNT AS KOLVO_KLASSOV
        FROM MON_NOBD.SCHOOL S
        INNER JOIN 
        (
        SELECT ATTR.ID 
        , ATTR.SCHOOL_ID
        , ATTR.DATE_CLOSE
        FROM MON_NOBD.SCHOOL_ATTR ATTR
        WHERE ATTR.EDU_PERIOD_ID = 0
        ) SA ON SA.SCHOOL_ID = S.ID 
        INNER JOIN MON_NOBD.SCHOOL_COMBCLASS CC ON CC.SCHOOL_ATTR_ID = SA.ID
        LEFT JOIN MON_NOBD.D_CLASS_NUM2 GR1 ON GR1.ID = CC.CLASS1_NUM_ID 
        LEFT JOIN MON_NOBD.D_CLASS_NUM2 GR2 ON GR2.ID = CC.CLASS2_NUM_ID 
        LEFT JOIN MON_NOBD.D_CLASS_NUM2 GR3 ON GR3.ID = CC.CLASS3_NUM_ID 
        LEFT JOIN MON_NOBD.D_CLASS_NUM2 GR4 ON GR4.ID = CC.CLASS4_NUM_ID 
        WHERE SA.DATE_CLOSE IS NULL 
        AND (
        CC.CLASS1_NUM_ID = 11
        OR CC.CLASS2_NUM_ID = 11 
        OR CC.CLASS3_NUM_ID = 11 
        OR CC.CLASS4_NUM_ID = 11)
        AND SA.ID IN (
        SELECT DISTINCT SS.SCHOOL_ATTR_ID AS SCHOOL_ATTR_ID
        FROM MON_NOBD.SCHOOL_SPEC SS 
        INNER JOIN MON_NOBD.SCHOOL_ATTR SA ON SA.ID = SS.SCHOOL_ATTR_ID 
        LEFT JOIN MON_NOBD.D_SCHOOLSPEC_TYPE SST ON SST.ID = SS.SPEC_TYPE_ID 
        WHERE 
        (
        SST.CODE LIKE '02.1.%' 
        OR SST.CODE LIKE '02.2.%' 
        OR SST.CODE LIKE '02.3.%' 
        OR SST.CODE LIKE '02.4.%' 
        OR SST.CODE LIKE '02.5.%' 
        OR SST.CODE LIKE '07.%' 
        OR SST.CODE IN ('02.6.1', '02.6.2', '02.6.3', '02.6.4', '08.3', '08.4', '08.5', '08.6', '09.3', '09.4')
        )
        AND SA.DATE_CLOSE IS NULL
        )
        ORDER BY S.ID

        --Script #3
        SELECT 
        S.ID AS ID 
        , GR1.RNAME AS CLASS_1 
        , GR2.RNAME AS CLASS_2
        , GR3.RNAME AS CLASS_3 
        , GR4.RNAME AS CLASS_4
        , CC.CLASS_CNT AS KOLVO_KLASSOV
        FROM MON_NOBD.SCHOOL S
        INNER JOIN 
        (
        SELECT ATTR.ID 
        , ATTR.SCHOOL_ID
        , ATTR.DATE_CLOSE
        FROM MON_NOBD.SCHOOL_ATTR ATTR
        WHERE ATTR.EDU_PERIOD_ID = 0
        ) SA ON SA.SCHOOL_ID = S.ID 
        INNER JOIN MON_NOBD.SCHOOL_COMBCLASS CC ON CC.SCHOOL_ATTR_ID = SA.ID
        LEFT JOIN MON_NOBD.D_CLASS_NUM2 GR1 ON GR1.ID = CC.CLASS1_NUM_ID 
        LEFT JOIN MON_NOBD.D_CLASS_NUM2 GR2 ON GR2.ID = CC.CLASS2_NUM_ID 
        LEFT JOIN MON_NOBD.D_CLASS_NUM2 GR3 ON GR3.ID = CC.CLASS3_NUM_ID 
        LEFT JOIN MON_NOBD.D_CLASS_NUM2 GR4 ON GR4.ID = CC.CLASS4_NUM_ID 
        WHERE SA.DATE_CLOSE IS NULL 
        AND (
        CC.CLASS1_NUM_ID = 13
        OR CC.CLASS2_NUM_ID = 13 
        OR CC.CLASS3_NUM_ID = 13 
        OR CC.CLASS4_NUM_ID = 13)
        AND SA.ID IN (
        SELECT DISTINCT SS.SCHOOL_ATTR_ID AS SCHOOL_ATTR_ID
        FROM MON_NOBD.SCHOOL_SPEC SS 
        INNER JOIN MON_NOBD.SCHOOL_ATTR SA ON SA.ID = SS.SCHOOL_ATTR_ID 
        LEFT JOIN MON_NOBD.D_SCHOOLSPEC_TYPE SST ON SST.ID = SS.SPEC_TYPE_ID 
        WHERE 
        (
        SST.CODE LIKE '02.1.%' 
        OR SST.CODE LIKE '02.2.%' 
        OR SST.CODE LIKE '02.3.%' 
        OR SST.CODE LIKE '02.4.%' 
        OR SST.CODE LIKE '02.5.%' 
        OR SST.CODE LIKE '07.%' 
        OR SST.CODE IN ('02.6.1', '02.6.2', '02.6.3', '02.6.4', '08.3', '08.4', '08.5', '08.6', '09.3', '09.4')
        )
        AND SA.DATE_CLOSE IS NULL
        )
        ORDER BY S.ID"""
                },
            
        "NOBD_MP_P12_V1": {
            'sql': """
                    SELECT DISTINCT --8 354
        s.ID AS ID,
        e1.TOT_CNT AS PHYSICS,
        e2.TOT_CNT AS CHEMISTRY,
        e3.TOT_CNT AS BIOLOGY,
        e4.TOT_CNT AS IT
        FROM MON_NOBD.SCHOOL s
        INNER JOIN MON_NOBD.SCHOOL_ATTR sa ON sa.SCHOOL_ID = s.ID AND sa.EDU_PERIOD_ID = toInt32(0)
        -- условия организации
        inner join MON_NOBD.SCHOOL_SPEC on sa.ID = MON_NOBD.SCHOOL_SPEC.SCHOOL_ATTR_ID
        left join MON_NOBD.D_SCHOOLSPEC_TYPE sst ON sst.ID = MON_NOBD.SCHOOL_SPEC.SPEC_TYPE_ID
        LEFT JOIN ( SELECT TOT_CNT, SCHOOL_ATTR_ID FROM MON_NOBD.SCHOOL_CABINET WHERE CABINET_TYPE_ID = toInt32(2)) e1 ON e1.SCHOOL_ATTR_ID = sa.ID
        LEFT JOIN ( SELECT TOT_CNT, SCHOOL_ATTR_ID FROM MON_NOBD.SCHOOL_CABINET WHERE CABINET_TYPE_ID = toInt32(3)) e2 ON e2.SCHOOL_ATTR_ID = sa.ID
        LEFT JOIN ( SELECT TOT_CNT, SCHOOL_ATTR_ID FROM MON_NOBD.SCHOOL_CABINET WHERE CABINET_TYPE_ID = toInt32(4)) e3 ON e3.SCHOOL_ATTR_ID = sa.ID
        LEFT JOIN ( SELECT TOT_CNT, SCHOOL_ATTR_ID FROM MON_NOBD.SCHOOL_CABINET WHERE CABINET_TYPE_ID = toInt32(19)) e4 ON e4.SCHOOL_ATTR_ID = sa.ID
        where sst.CODE LIKE '02.1.%' OR sst.CODE LIKE '02.2.%' OR sst.CODE LIKE '02.3.%' OR sst.CODE LIKE '02.4.%' OR sst.CODE LIKE '02.5.%' OR sst.CODE LIKE '07.%' OR sst.CODE IN ('02.6.1', '02.6.2', '02.6.3', '02.6.4', '08.3', '08.4', '08.5', '08.6', '09.3', '09.4')
        AND sa.DATE_CLOSE IS NULL"""
                },
            
        "NOBD_MP_P13_V1": {
            'sql': """
                    SELECT S.ID 
        , CASE 
        WHEN SA.ID IN 
        (
        SELECT FA.SCHOOL_ATTR_ID
        FROM MON_NOBD.SCHOOL_FREEACCESS FA 
        WHERE FA.FREE_ACCESS_ID IN (3,9)
        ) THEN 'Да'
        ELSE 'Нет'
        END RAMP_AVAILABILITY
        FROM MON_NOBD.SCHOOL S 
        INNER JOIN 
        (
        SELECT SC.SCHOOL_ID
        , SC.ID
        , SC.DATE_CLOSE
        FROM MON_NOBD.SCHOOL_ATTR SC
        WHERE SC.EDU_PERIOD_ID = 0 
        ) SA ON SA.SCHOOL_ID = S.ID 
        WHERE SA.DATE_CLOSE IS NULL 
        AND SA.ID IN (
        SELECT SS.SCHOOL_ATTR_ID 
        FROM MON_NOBD.SCHOOL_SPEC SS 
        LEFT JOIN MON_NOBD.D_SCHOOLSPEC_TYPE SST ON SST.ID = SS.SPEC_TYPE_ID 
        WHERE SST.CODE LIKE '03.%'
        )
        AND SA.ID IN (
        SELECT FA.SCHOOL_ATTR_ID
        FROM MON_NOBD.SCHOOL_FREEACCESS FA 
        WHERE FA.FREE_ACCESS_ID IN (3,9)
        )"""
                },
            
        "NOBD_MP_P14_V1": {
            'sql': """
                    SELECT DISTINCT --108 644
        s.ID AS ID_ORGANIZATION_OBRAZ,
        t.ID AS ID_UCHITELYA, 
        tj.JOBINFO_ID AS ID_SVEDENI_O_RABOTE,
        t.IIN AS IIN,
        t.LASTNAME AS SURNAME,
        t.FIRSTNAME AS NAME,
        COALESCE(t.MIDDLENAME, '') AS MIDDLENAME,
        tea.BIRTHDATE AS BIRTHDATE
        FROM MON_NOBD.SCHOOL s
        INNER JOIN MON_NOBD.SCHOOL_ATTR sa ON sa.SCHOOL_ID = s.ID AND sa.EDU_PERIOD_ID = toInt32(0)
        INNER JOIN MON_NOBD.TEACHJOB tj ON tj.SCHOOL_ID = s.ID AND tj.EDU_PERIOD_ID = toInt32(0)
        INNER JOIN MON_NOBD.TEACHER t ON t.ID = tj.TEACHER_ID
        INNER JOIN MON_NOBD.TEACHER_ATTR tea ON tea.TEACHER_ID = t.ID AND tea.EDU_PERIOD_ID = toInt32(0)
        inner join MON_NOBD.SCHOOL_SPEC on sa.ID = MON_NOBD.SCHOOL_SPEC.SCHOOL_ATTR_ID
        left join MON_NOBD.D_SCHOOLSPEC_TYPE sst ON sst.ID = MON_NOBD.SCHOOL_SPEC.SPEC_TYPE_ID
        where sst.CODE IN ('01.1', '01.2', '01.3', '01.4', '01.5', '01.6', '08.1', '08.2')
        AND sa.DATE_CLOSE IS NULL
        -- условия персонал
        AND tj.REC_STATUS = toInt32(0) 
        AND tj.STAFF_STAT_ID IN ('1') 
        AND tj.POST_TYPE_ID IN ('3', '5', '61', '62', '63', '64', '65', '66', '67', '68', '69', '70', '71', '72', '73', '74', '75', '85', '100')
        """
                },
            
        "NOBD_MP_P15_V1": {
            'sql': """
                    SELECT distinct --966 181
        s.ID AS ID_ORGANIZATION_OBRAZ,
        st.ID AS ID_UCHASHEGOSYA, 
        e.EDU_ID ID_OBUCHENIYA,
        st.IIN AS IIN,
        st.LASTNAME AS SURNAME,
        st.FIRSTNAME AS NAME,
        COALESCE(st.MIDDLENAME, '') AS MIDDLENAME,
        sta.BIRTHDATE AS BIRTHDATE
        FROM MON_NOBD.SCHOOL s
        INNER JOIN MON_NOBD.SCHOOL_ATTR sa ON sa.SCHOOL_ID = s.ID AND sa.EDU_PERIOD_ID = toInt32(0)
        INNER JOIN MON_NOBD.EDUCATION e ON e.SCHOOL_ID = s.ID AND e.EDU_PERIOD_ID = toInt32(0)
        INNER JOIN MON_NOBD.STUDENT_ATTR sta ON sta.STUDENT_ID = e.STUDENT_ID AND sta.EDU_PERIOD_ID = toInt32(0)
        INNER JOIN MON_NOBD.STUDENT st ON st.ID = e.STUDENT_ID
        -- условия организации
        inner join MON_NOBD.SCHOOL_SPEC on sa.ID = MON_NOBD.SCHOOL_SPEC.SCHOOL_ATTR_ID
        left join MON_NOBD.D_SCHOOLSPEC_TYPE sst ON sst.ID = MON_NOBD.SCHOOL_SPEC.SPEC_TYPE_ID
        where sst.CODE IN ('01.1', '01.2', '01.3', '01.4', '01.5', '01.6', '08.1', '08.2')
        AND sa.DATE_CLOSE IS NULL
        AND e.EDU_STATUS = toInt32(0)"""
                },
            
        "NOBD_MP_P16_V1": {
            'sql': """
                    SELECT distinct --966 181
        s.ID AS ID_ORGANIZATION_OBRAZ,
        st.ID AS ID_UCHASHEGOSYA, 
        e.EDU_ID ID_OBUCHENIYA,
        st.IIN AS IIN,
        st.LASTNAME AS SURNAME,
        st.FIRSTNAME AS NAME,
        COALESCE(st.MIDDLENAME, '') AS MIDDLENAME,
        sta.BIRTHDATE AS BIRTHDATE
        FROM MON_NOBD.SCHOOL s
        INNER JOIN MON_NOBD.SCHOOL_ATTR sa ON sa.SCHOOL_ID = s.ID AND sa.EDU_PERIOD_ID = toInt32(0)
        INNER JOIN MON_NOBD.EDUCATION e ON e.SCHOOL_ID = s.ID AND e.EDU_PERIOD_ID = toInt32(0)
        INNER JOIN MON_NOBD.STUDENT_ATTR sta ON sta.STUDENT_ID = e.STUDENT_ID AND sta.EDU_PERIOD_ID = toInt32(0)
        INNER JOIN MON_NOBD.STUDENT st ON st.ID = e.STUDENT_ID
        -- условия организации
        inner join MON_NOBD.SCHOOL_SPEC on sa.ID = MON_NOBD.SCHOOL_SPEC.SCHOOL_ATTR_ID
        left join MON_NOBD.D_SCHOOLSPEC_TYPE sst ON sst.ID = MON_NOBD.SCHOOL_SPEC.SPEC_TYPE_ID
        where sst.CODE IN ('01.1', '01.2', '01.3', '01.4', '01.5', '01.6', '08.1', '08.2')
        AND sa.DATE_CLOSE IS NULL
        AND e.EDU_STATUS = toInt32(0)"""
                },
            
        "NOBD_MP_P17_V1": {
            'sql': """
                    SELECT 
        S.ID 
        , ST.ID 
        , E.EDU_ID
        , ST.IIN 
        , ST.LASTNAME 
        , ST.FIRSTNAME 
        , ST.MIDDLENAME 
        , STA.BIRTHDATE
        , E.GROUP_NAME
        , DGT.RNAME 
        , SG.SEAT_CNT 
        , SG.CHILD_CNT 
        FROM MON_NOBD.SCHOOL S 
        INNER JOIN 
        (
        SELECT AT.SCHOOL_ID
        , AT.DATE_CLOSE
        , AT.ID
        FROM MON_NOBD.SCHOOL_ATTR AT
        WHERE AT.EDU_PERIOD_ID = 0
        ) SA ON SA.SCHOOL_ID = S.ID 
        INNER JOIN 
        (
        SELECT EDU.SCHOOL_ID
        , EDU.STUDENT_ID
        , EDU.EDU_ID
        , EDU.GROUP_NAME
        , EDU.EDU_STATUS
        FROM MON_NOBD.EDUCATION EDU 
        WHERE EDU.EDU_PERIOD_ID = 0
        ) E ON E.SCHOOL_ID = S.ID 
        INNER JOIN MON_NOBD.STUDENT ST ON ST.ID = E.STUDENT_ID
        INNER JOIN 
        (
        SELECT STUD.STUDENT_ID
        , STUD.BIRTHDATE
        FROM MON_NOBD.STUDENT_ATTR STUD 
        WHERE STUD.EDU_PERIOD_ID = 0
        ) STA ON STA.STUDENT_ID = E.STUDENT_ID 
        INNER JOIN MON_NOBD.SCHOOL_GROUP SG ON SG.SCHOOL_ATTR_ID = SA.ID
        AND SG.GROUP_NAME = E.GROUP_NAME
        INNER JOIN MON_NOBD.SCHOOL_ALLGROUP ALLG ON ALLG.SCHOOL_ATTR_ID = SA.ID
        AND ALLG.GROUP_NAME = E.GROUP_NAME
        LEFT JOIN MON_NOBD.D_GROUPAGE_TYPE DGT ON DGT.ID = ALLG.GROUPAGE_TYPE_ID 
        WHERE SA.DATE_CLOSE IS NULL 
        AND E.EDU_STATUS = 0
        AND SA.ID IN 
        (
        SELECT SS.SCHOOL_ATTR_ID 
        FROM MON_NOBD.SCHOOL_SPEC SS 
        LEFT JOIN MON_NOBD.D_SCHOOLSPEC_TYPE SST ON SST.ID = SS.SPEC_TYPE_ID 
        WHERE SST.CODE IN ('01.1', '01.2', '01.3', '01.4', '01.5', '01.6', '08.1', '08.2') 
        )"""
                },
            
        "NOBD_MP_P18_V1": {
            'sql': """
                    SELECT S.ID 
        , SG.GROUP_NAME 
        , SG.SEAT_CNT 
        , E.KOLVO
        FROM MON_NOBD.SCHOOL S
        INNER JOIN 
        (
        SELECT AT.SCHOOL_ID
        , AT.DATE_CLOSE
        , AT.ID
        FROM MON_NOBD.SCHOOL_ATTR AT
        WHERE AT.EDU_PERIOD_ID = 0
        ) SA ON SA.SCHOOL_ID = S.ID 
        INNER JOIN MON_NOBD.SCHOOL_GROUP SG ON SG.SCHOOL_ATTR_ID = SA.ID
        LEFT JOIN 
        (
        SELECT EDU.SCHOOL_ID 
        , EDU.GROUP_NAME
        , COUNT(EDU.SCHOOL_ID) AS KOLVO
        FROM MON_NOBD.EDUCATION EDU 
        WHERE EDU.EDU_STATUS = 0
        AND EDU.EDU_PERIOD_ID = 0
        GROUP BY EDU.SCHOOL_ID, EDU.GROUP_NAME
        ) E ON E.SCHOOL_ID = S.ID 
        AND E.GROUP_NAME = SG.GROUP_NAME 
        WHERE SA.DATE_CLOSE IS NULL 
        AND SA.ID IN (
        SELECT SS.SCHOOL_ATTR_ID 
        FROM MON_NOBD.SCHOOL_SPEC SS 
        LEFT JOIN MON_NOBD.D_SCHOOLSPEC_TYPE SST ON SST.ID = SS.SPEC_TYPE_ID 
        WHERE SST.CODE IN ('01.1', '01.2', '01.3', '01.4', '01.5', '01.6', '08.1', '08.2') 
        ) 
        ORDER BY S.ID
        """
                },
            
        "NOBD_MP_P19_V1": {
            'sql': """
                    SELECT DISTINCT--11 132
        s.ID AS ID,
        ts.RNAME AS TIP_ORGANIZACII_OBRAZOV,
        sst.RNAME AS VIDY_ORGANIZACII

        FROM MON_NOBD.SCHOOL s
        INNER JOIN MON_NOBD.SCHOOL_ATTR sa ON sa.SCHOOL_ID = s.ID AND sa.EDU_PERIOD_ID = toInt32(0)
        -- связь со справочниками значений по организации
        LEFT JOIN MON_NOBD.D_TYPE_SCHOOL ts ON ts.ID = sa.TYPE_SCHOOL_ID
        -- условия организации
        inner join MON_NOBD.SCHOOL_SPEC on sa.ID = MON_NOBD.SCHOOL_SPEC.SCHOOL_ATTR_ID
        left join MON_NOBD.D_SCHOOLSPEC_TYPE sst ON sst.ID = MON_NOBD.SCHOOL_SPEC.SPEC_TYPE_ID

        where sst.CODE IN ('01.1', '01.2', '01.3', '01.4', '01.5', '01.6', '08.1', '08.2')
        AND sa.DATE_CLOSE IS NULL"""
                },
            
        "NOBD_MP_P20_V1": {
            'sql': """
                    SELECT DISTINCT --238 199
        s.ID AS ID_ORGANIZACII_OBR,
        sg.GROUP_NAME AS NAZVANIYE_GRUPPI,
        sg.SEAT_CNT AS CHISLO_MEST_V_GRUPPE,
        groupage_type.RNAME AS GOD_VOSPIT_OBUCH,
        sg2.AGE AS VOZRAST_POSTUPLENIYA
        FROM MON_NOBD.SCHOOL s
        INNER JOIN MON_NOBD.SCHOOL_ATTR sa ON sa.SCHOOL_ID = s.ID AND sa.EDU_PERIOD_ID = toInt32(0)
        INNER JOIN MON_NOBD.SCHOOL_GROUP sg ON sg.SCHOOL_ATTR_ID = sa.ID
        INNER JOIN MON_NOBD.SCHOOL_ALLGROUP sg2 ON sg2.SCHOOL_ATTR_ID = sa.ID
        -- справочники
        LEFT JOIN MON_NOBD.D_GROUPAGE_TYPE groupage_type ON groupage_type.ID = sg2.GROUPAGE_TYPE_ID
        -- условия организации
        inner join MON_NOBD.SCHOOL_SPEC on sa.ID = MON_NOBD.SCHOOL_SPEC.SCHOOL_ATTR_ID
        left join MON_NOBD.D_SCHOOLSPEC_TYPE sst ON sst.ID = MON_NOBD.SCHOOL_SPEC.SPEC_TYPE_ID
        where sst.CODE IN ('01.1', '01.2', '01.3', '01.4', '01.5', '01.6', '08.1', '08.2')
        AND sa.DATE_CLOSE IS NULL"""
                },
            
        "NOBD_MP_P21_V1": {
            'sql': """
                    SELECT --11 132
        s.ID AS ID_ORGANIZ_OBRAZOV,
        e1.total AS VSEGO_DETEI
        FROM MON_NOBD.SCHOOL s
        INNER JOIN MON_NOBD.SCHOOL_ATTR sa ON sa.SCHOOL_ID = s.ID AND sa.EDU_PERIOD_ID = toInt32(0)
        left join( SELECT COUNT(*) as total,e.SCHOOL_ID as SCHOOL_ID FROM MON_NOBD.EDUCATION e 
        WHERE e.EDU_STATUS = 0 -- статусы в организации (0 - обучается в настоящее время)
        AND e.EDU_PERIOD_ID = 0 -- период сбора данных (0 - актуальные данные)
        group by SCHOOL_ID
        ) e1 on e1.SCHOOL_ID = s.ID 
        inner join MON_NOBD.SCHOOL_SPEC on sa.ID = MON_NOBD.SCHOOL_SPEC.SCHOOL_ATTR_ID
        left join MON_NOBD.D_SCHOOLSPEC_TYPE sst ON sst.ID = MON_NOBD.SCHOOL_SPEC.SPEC_TYPE_ID
        where sst.CODE IN ('01.1', '01.2', '01.3', '01.4', '01.5', '01.6', '08.1', '08.2')
        AND sa.DATE_CLOSE IS NULL"""
                },
            
        "NOBD_MP_P22_V1": {
            'sql': """
                    SELECT 
        S.ID AS "ID ОРГАНИЗАЦИИ ОБРАЗОВАНИЯ",
        ST.ID AS "ID УЧАЩЕГОСЯ", 
        E.EDU_ID "ID ОБУЧЕНИЯ",
        ST.IIN AS "ИИН",
        ST.LASTNAME AS "ФАМИЛИЯ",
        ST.FIRSTNAME AS "ИМЯ",
        COALESCE(ST.MIDDLENAME, '') AS "ОТЧЕСТВО",
        STA.BIRTHDATE AS "ДАТА РОЖДЕНИЯ"

        FROM MON_NOBD.SCHOOL S
        INNER JOIN MON_NOBD.SCHOOL_ATTR SA ON SA.SCHOOL_ID = S.ID 
        INNER JOIN MON_NOBD.EDUCATION E ON E.SCHOOL_ID = S.ID 
        INNER JOIN MON_NOBD.STUDENT_ATTR STA ON STA.STUDENT_ID = E.STUDENT_ID
        INNER JOIN MON_NOBD.STUDENT ST ON ST.ID = E.STUDENT_ID
        INNER JOIN 
        (SELECT SPEC_TYPE_ID,
        SC.SCHOOL_ATTR_ID SCHOOL_ATTR_ID FROM MON_NOBD.SCHOOL_SPEC SC
        LEFT JOIN MON_NOBD.D_SCHOOLSPEC_TYPE SST ON SST.ID = SC.SPEC_TYPE_ID 
        WHERE SST.CODE IN ('01.1', '01.2', '01.3', '01.4', '01.5', '01.6', '08.1', '08.2')) AS EX ON EX.SCHOOL_ATTR_ID=SA.ID
        -- УСЛОВИЯ ОРГАНИЗАЦИИ
        WHERE SA.DATE_CLOSE IS NULL -- ОРГАНИЗАЦИЯ НЕ ЗАКРЫТА В НАСТОЯЩЕЕ ВРЕМЯ
        AND SA.EDU_PERIOD_ID = 0
        AND E.EDU_PERIOD_ID = 0
        AND STA.EDU_PERIOD_ID = 0"""
                },
            
        "NOBD_MP_P25_V1": {
            'sql': """
                    SELECT--318
        s.ID AS ID,
        a1.total AS UCHITEL_DEFECTOLOG,
        a2.total AS DEFEKTOLOG,
        a3.total AS UCHITEL_LOGOPED,
        a4.total AS LOGOPED,
        a5.total AS OLIGOFRENOPEDAGOG,
        a6.total AS SURDOPEDAGOG,
        a7.total AS TIFLOPEDAGOG
        FROM MON_NOBD.SCHOOL s
        INNER JOIN MON_NOBD.SCHOOL_ATTR sa ON sa.SCHOOL_ID = s.ID AND sa.EDU_PERIOD_ID = toInt32(0)
        -- условия организации
        inner join MON_NOBD.SCHOOL_SPEC on sa.ID = MON_NOBD.SCHOOL_SPEC.SCHOOL_ATTR_ID
        left join MON_NOBD.D_SCHOOLSPEC_TYPE sst ON sst.ID = MON_NOBD.SCHOOL_SPEC.SPEC_TYPE_ID
        left join (SELECT COUNT(*) as total,tj.SCHOOL_ID as SCHOOL_ID FROM MON_NOBD.TEACHJOB tj WHERE tj.EDU_PERIOD_ID = toInt32(0) AND tj.REC_STATUS = toInt32(0) AND tj.STAFF_STAT_ID = toInt32(1) AND tj.POST_TYPE_ID = toInt32(69) GROUP BY SCHOOL_ID) a1 on a1.SCHOOL_ID = s.ID
        left join (SELECT COUNT(*) as total,tj.SCHOOL_ID as SCHOOL_ID FROM MON_NOBD.TEACHJOB tj WHERE tj.EDU_PERIOD_ID = toInt32(0) AND tj.REC_STATUS = toInt32(0) AND tj.STAFF_STAT_ID = toInt32(1) AND tj.POST_TYPE_ID = toInt32(347)GROUP BY SCHOOL_ID) a2 on a2.SCHOOL_ID = s.ID
        left join (SELECT COUNT(*) as total,tj.SCHOOL_ID as SCHOOL_ID FROM MON_NOBD.TEACHJOB tj WHERE tj.EDU_PERIOD_ID = toInt32(0) AND tj.REC_STATUS = toInt32(0) AND tj.STAFF_STAT_ID = toInt32(1) AND tj.POST_TYPE_ID = toInt32(66)GROUP BY SCHOOL_ID) a3 on a3.SCHOOL_ID = s.ID
        left join (SELECT COUNT(*) as total,tj.SCHOOL_ID as SCHOOL_ID FROM MON_NOBD.TEACHJOB tj WHERE tj.EDU_PERIOD_ID = toInt32(0) AND tj.REC_STATUS = toInt32(0) AND tj.STAFF_STAT_ID = toInt32(1) AND tj.POST_TYPE_ID = toInt32(348)GROUP BY SCHOOL_ID) a4 on a4.SCHOOL_ID = s.ID
        left join (SELECT COUNT(*) as total,tj.SCHOOL_ID as SCHOOL_ID FROM MON_NOBD.TEACHJOB tj WHERE tj.EDU_PERIOD_ID = toInt32(0) AND tj.REC_STATUS = toInt32(0) AND tj.STAFF_STAT_ID = toInt32(1) AND tj.POST_TYPE_ID = toInt32(71)GROUP BY SCHOOL_ID) a5 on a5.SCHOOL_ID = s.ID
        left join (SELECT COUNT(*) as total,tj.SCHOOL_ID as SCHOOL_ID FROM MON_NOBD.TEACHJOB tj WHERE tj.EDU_PERIOD_ID = toInt32(0) AND tj.REC_STATUS = toInt32(0) AND tj.STAFF_STAT_ID = toInt32(1) AND tj.POST_TYPE_ID = toInt32(68)GROUP BY SCHOOL_ID) a6 on a6.SCHOOL_ID = s.ID
        left join (SELECT COUNT(*) as total,tj.SCHOOL_ID as SCHOOL_ID FROM MON_NOBD.TEACHJOB tj WHERE tj.EDU_PERIOD_ID = toInt32(0) AND tj.REC_STATUS = toInt32(0) AND tj.STAFF_STAT_ID = toInt32(1) AND tj.POST_TYPE_ID = toInt32(67)GROUP BY SCHOOL_ID) a7 on a7.SCHOOL_ID = s.ID
        where sst.CODE IN ('08.7', '08.8', '08.9', '08.93')"""
                },
            
        
        "NOBD_MP_P26_1_V1": {
            'sql': """
                    SELECT DISTINCT
        s.ID AS "ID организации образования",
        e1.total AS "Всего учащихся 1-11/13 классов",
        e2.total AS "Из них, охваченных летним отдыхом"

        FROM MON_NOBD.SCHOOL s
        INNER JOIN (
        SELECT sar.SCHOOL_ID
        , sar.DATE_CLOSE
        , sar.ID as ID
        FROM MON_NOBD.SCHOOL_ATTR sar
        WHERE sar.EDU_PERIOD_ID = toInt32(0)
        ) SA ON SA.SCHOOL_ID = s.ID 
        left join ( SELECT e.SCHOOL_ID as SCHOOL_ID , COUNT(*) as total FROM MON_NOBD.EDUCATION e
        WHERE e.EDU_STATUS IN (0, 4) 
        AND e.EDU_PERIOD_ID = toInt32(0)
        AND e.CLASS_NUM_ID IN (3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15)
        group by SCHOOL_ID
        ) e1 on e1.SCHOOL_ID = s.ID
        left join ( SELECT e.SCHOOL_ID as SCHOOL_ID, COUNT(*) as total FROM MON_NOBD.EDUCATION e 
        WHERE e.EDU_STATUS IN (0, 4) 
        AND e.EDU_PERIOD_ID = toInt32(0)
        AND e.CLASS_NUM_ID IN (3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15) 
        AND e.IS_SUMMER_CAMP = toInt32(1)
        group by SCHOOL_ID
        ) e2 on e2.SCHOOL_ID = s.ID
        where SA.DATE_CLOSE IS NULL
        and SA.ID in( SELECT SCHOOL_ATTR_ID FROM MON_NOBD.SCHOOL_SPEC LEFT JOIN MON_NOBD.D_SCHOOLSPEC_TYPE sst ON sst.ID = MON_NOBD.SCHOOL_SPEC.SPEC_TYPE_ID WHERE sst.CODE LIKE '02.1.%' OR sst.CODE LIKE '02.2.%' OR sst.CODE LIKE '02.3.%' OR sst.CODE LIKE '02.4.%' OR sst.CODE LIKE '02.5.%' OR sst.CODE LIKE '07.%' OR sst.CODE IN ('02.6.1', '02.6.2', '02.6.3', '02.6.4', '08.3', '08.4', '08.5', '08.6', '09.3', '09.4'))"""
                },
            
        "NOBD_MP_P26_V1": {
            'sql': """
                    SELECT --457
        s.ID AS ID,
        CASE WHEN sb.HAS_HOTWATER = '1' THEN 'да' WHEN sb.HAS_HOTWATER = '0' THEN 'нет' END AS NALICHIYE_GORYACH_VODI,
        CASE WHEN sb.IS_IMPWATER = '1' THEN 'да' WHEN sb.IS_IMPWATER='0' THEN 'нет' END AS PRIVOZNAYA_VODA,
        CASE WHEN sb.HAS_DRINKWATER = '1' THEN 'да' WHEN sb.HAS_DRINKWATER='0' THEN 'нет' END AS NALICHIYE_PITEYVOI_VODI,
        toilet_type.RNAME AS NALICHIYE_TUALETOV
        FROM MON_NOBD.SCHOOL s
        INNER JOIN MON_NOBD.SCHOOL_ATTR sa ON sa.SCHOOL_ID = s.ID AND sa.EDU_PERIOD_ID = toInt32(0)
        INNER JOIN MON_NOBD.SCHOOL_BUILD sb ON sb.SCHOOL_ATTR_ID = sa.ID
        -- справочники
        LEFT JOIN MON_NOBD.D_TOILET_TYPE toilet_type ON toilet_type.ID = sb.TOILET_TYPE_ID
        -- условия организации
        inner join MON_NOBD.SCHOOL_SPEC ON sa.ID = MON_NOBD.SCHOOL_SPEC.SCHOOL_ATTR_ID
        left join MON_NOBD.D_SCHOOLSPEC_TYPE sst ON sst.ID = MON_NOBD.SCHOOL_SPEC.SPEC_TYPE_ID
        where sst.CODE IN ('08.7', '08.8', '08.9', '08.93')
        AND sa.DATE_CLOSE IS NULL"""
                },
            
        "NOBD_MP_P28_V1": {
            'sql': """
                    SELECT 
        S.ID AS SCHOOL_ID
        , CASE 
        WHEN SB.HAS_HOTWATER = '1' THEN 'Да'
        WHEN SB.HAS_HOTWATER = '0' THEN 'Нет'
        ELSE NULL
        END AS HOTWATER_AVAILABILITY
        , CASE 
        WHEN SB.IS_IMPWATER = '1' THEN 'Да'
        WHEN SB.IS_IMPWATER = '0' THEN 'Нет'
        ELSE NULL
        END AS IMPORTWATER_AVAILABILITY
        , CASE 
        WHEN SB.HAS_DRINKWATER = '1' THEN 'Да'
        WHEN SB.HAS_DRINKWATER = '0' THEN 'Нет'
        ELSE NULL
        END AS DRINKWATER_AVAILABILITY
        , CASE 
        WHEN SA.HAS_DINROOM = '1' THEN 'Да'
        WHEN SA.HAS_DINROOM = '0' THEN 'Нет'
        ELSE NULL
        END AS DININGROOM_AVAILABILITY
        , DTT.RNAME AS TOILET_AVAILABILITY
        FROM MON_NOBD.SCHOOL S 
        INNER JOIN 
        (
        SELECT AT.SCHOOL_ID
        , AT.DATE_CLOSE
        , AT.ID
        , AT.HAS_DINROOM
        FROM MON_NOBD.SCHOOL_ATTR AT
        WHERE AT.EDU_PERIOD_ID = 0
        ) SA ON SA.SCHOOL_ID = S.ID 
        INNER JOIN MON_NOBD.SCHOOL_BUILD SB ON SB.SCHOOL_ATTR_ID = SA.ID
        LEFT JOIN MON_NOBD.D_TOILET_TYPE DTT ON DTT.ID = SB.TOILET_TYPE_ID 
        WHERE SB.BUILD_EDUUSE_ID = 1
        AND SA.DATE_CLOSE IS NULL
        AND SA.ID IN (
        SELECT DISTINCT SS.SCHOOL_ATTR_ID AS SCHOOL_ATTR_ID
        FROM MON_NOBD.SCHOOL_SPEC SS 
        INNER JOIN MON_NOBD.SCHOOL_ATTR SA ON SA.ID = SS.SCHOOL_ATTR_ID 
        LEFT JOIN MON_NOBD.D_SCHOOLSPEC_TYPE SST ON SST.ID = SS.SPEC_TYPE_ID 
        WHERE SST.CODE IN ('02.3.5')
        )
        """
                },
            
        "NOBD_MP_P29_V1": {
            'sql': """
                    SELECT 
        S.ID AS SCHOOL_ID
        , CASE 
        WHEN SB.HAS_HOTWATER = '1' THEN 'Да'
        WHEN SB.HAS_HOTWATER = '0' THEN 'Нет'
        ELSE NULL
        END AS HOTWATER_AVAILABILITY
        , CASE 
        WHEN SB.IS_IMPWATER = '1' THEN 'Да'
        WHEN SB.IS_IMPWATER = '0' THEN 'Нет'
        ELSE NULL
        END AS IMPORTWATER_AVAILABILITY
        , CASE 
        WHEN SB.HAS_DRINKWATER = '1' THEN 'ДА'
        WHEN SB.HAS_DRINKWATER = '0' THEN 'НЕТ'
        ELSE NULL
        END AS DRINKWATER_AVAILABILITY

        , DTT.RNAME AS TOILET_AVAILABILITY
        FROM MON_NOBD.SCHOOL S
        INNER JOIN
        (
        SELECT AT.SCHOOL_ID
        , AT.DATE_CLOSE
        , AT.ID
        , AT.HAS_DINROOM
        FROM MON_NOBD.SCHOOL_ATTR AT
        WHERE AT.EDU_PERIOD_ID = 0
        ) SA ON SA.SCHOOL_ID = S.ID 
        INNER JOIN MON_NOBD.SCHOOL_BUILD SB ON SB.SCHOOL_ATTR_ID = SA.ID
        LEFT JOIN MON_NOBD.D_TOILET_TYPE DTT ON DTT.ID = SB.TOILET_TYPE_ID 
        WHERE SB.BUILD_EDUUSE_ID = 1
        AND SA.DATE_CLOSE IS NULL
        AND SA.ID IN (
        SELECT DISTINCT SS.SCHOOL_ATTR_ID AS SCHOOL_ATTR_ID
        FROM MON_NOBD.SCHOOL_SPEC SS 
        INNER JOIN MON_NOBD.SCHOOL_ATTR SA ON SA.ID = SS.SCHOOL_ATTR_ID 
        LEFT JOIN MON_NOBD.D_SCHOOLSPEC_TYPE SST ON SST.ID = SS.SPEC_TYPE_ID 
        WHERE SST.CODE LIKE ('09.%')
        )"""
                },
            
        "NOBD_MP_P30_V1": {
            'sql': """
                    SELECT --157
        s.ID AS ID,
        CASE WHEN sb.HAS_HOTWATER = '1' THEN 'да' WHEN sb.HAS_HOTWATER = '0' THEN 'нет' END AS NALICHIYE_GORYACH_VODI,
        CASE WHEN sb.IS_IMPWATER = '1' THEN 'да' WHEN sb.IS_IMPWATER='0' THEN 'нет' END AS PRIVOZNAYA_VODA,
        CASE WHEN sb.HAS_DRINKWATER = '1' THEN 'да' WHEN sb.HAS_DRINKWATER='0' THEN 'нет' END AS NALICHIYE_PITEYVOI_VODI,
        toilet_type.RNAME AS NALICHIYE_TUALETOV
        FROM MON_NOBD.SCHOOL s
        INNER JOIN MON_NOBD.SCHOOL_ATTR sa ON sa.SCHOOL_ID = s.ID AND sa.EDU_PERIOD_ID = toInt32(0)
        INNER JOIN MON_NOBD.SCHOOL_BUILD sb ON sb.SCHOOL_ATTR_ID = sa.ID
        -- справочники
        LEFT JOIN MON_NOBD.D_TOILET_TYPE toilet_type ON toilet_type.ID = sb.TOILET_TYPE_ID
        -- условия организации
        inner join MON_NOBD.SCHOOL_SPEC ON sa.ID = MON_NOBD.SCHOOL_SPEC.SCHOOL_ATTR_ID
        left join MON_NOBD.D_SCHOOLSPEC_TYPE sst ON sst.ID = MON_NOBD.SCHOOL_SPEC.SPEC_TYPE_ID
        where sst.CODE LIKE ('09.%')
        AND sa.DATE_CLOSE IS NULL
        AND sb.BUILD_USE_ID = toInt32(1)"""
                },
            
        "NOBD_MP_P31_V1": {
            'sql': """
                    SELECT DISTINCT --638 004
        s.ID AS ID, 
        st.ID AS ID_UCHASHEGOSYA, 
        e.EDU_ID ID_OBUCHENIYA,
        st.IIN AS IIN,
        st.LASTNAME AS SURNAME,
        st.FIRSTNAME AS NAME,
        COALESCE(st.MIDDLENAME, '') AS MIDDLENAME,
        sta.BIRTHDATE AS BIRTH_DATE,
        profession.RNAME AS SPECIALNOST_I_CLASSIFIKATOR_PROGRAM,
        e.REG_DATE AS DATA_POSTUPLENIYA
        FROM MON_NOBD.SCHOOL s
        INNER JOIN MON_NOBD.SCHOOL_ATTR sa ON s.ID = sa.SCHOOL_ID AND sa.EDU_PERIOD_ID = toInt32(0)
        INNER JOIN MON_NOBD.EDUCATION e ON s.ID = e.SCHOOL_ID AND e.EDU_PERIOD_ID = toInt32(0)
        INNER JOIN MON_NOBD.STUDENT st ON st.ID = e.STUDENT_ID
        INNER JOIN MON_NOBD.STUDENT_ATTR sta ON sta.STUDENT_ID = st.ID AND sta.EDU_PERIOD_ID = toInt32(0)
        -- показатели учащихся
        LEFT JOIN MON_NOBD.D_PROFESSION profession ON profession.ID = e.PROFESSION_ID
        -- условия
        inner join MON_NOBD.SCHOOL_SPEC on sa.ID = MON_NOBD.SCHOOL_SPEC.SCHOOL_ATTR_ID
        left join MON_NOBD.D_SCHOOLSPEC_TYPE sst ON sst.ID = MON_NOBD.SCHOOL_SPEC.SPEC_TYPE_ID
        where sst.CODE LIKE '06.%'
        AND sa.DATE_CLOSE IS NULL -- организация не закрыта в настоящее время
        --
        AND e.EDU_STATUS = toInt32(0)
        AND e.EDUSTATUS_ID = toInt32(1)"""
                },
            
        "NOBD_MP_P32_V1": {
            'sql': """
                    
        SELECT
        s.ID AS "ID организации образования",
        e1.total AS "Всего выбывших студентов (1 год назад)",
        e2.total AS "Всего выбывших студентов (2 года назад)"
        FROM MON_NOBD.SCHOOL s
        INNER JOIN (
        SELECT sar.SCHOOL_ID
        , sar.DATE_CLOSE
        , sar.ID as ID
        FROM MON_NOBD.SCHOOL_ATTR sar
        WHERE sar.EDU_PERIOD_ID = toInt32(0)
        ) SA ON SA.SCHOOL_ID = s.ID
        left join ( WITH CONCAT(SUBSTRING(toString(NOW() - INTERVAL 1 YEAR),1,4), '/01/01 00:00:00') AS BeginDate 
        , CONCAT(SUBSTRING(toString(NOW() - INTERVAL 1 YEAR),1,4), '/12/31 23:59:59') AS EndDate
        SELECT COUNT(*) as total, e.SCHOOL_ID as SCHOOL_ID FROM MON_NOBD.EDUCATION e 
        WHERE e.EDU_STATUS = toInt32(2)
        AND e.EDU_PERIOD_ID = toInt32(0) 
        AND e.OUT_DATE BETWEEN BeginDate AND EndDate -- дата выбытия
        group by SCHOOL_ID
        ) e1 on e1.SCHOOL_ID = s.ID
        left join ( WITH CONCAT(SUBSTRING(toString(NOW() - INTERVAL 2 YEAR),1,4), '/01/01 00:00:00') AS BeginDate 
        , CONCAT(SUBSTRING(toString(NOW() - INTERVAL 2 YEAR),1,4), '/12/31 23:59:59') AS EndDate
        SELECT COUNT(*) as total, e.SCHOOL_ID as SCHOOL_ID FROM MON_NOBD.EDUCATION e 
        WHERE e.EDU_STATUS = toInt32(2)
        AND e.EDU_PERIOD_ID = toInt32(0)
        AND e.OUT_DATE BETWEEN BeginDate AND EndDate -- дата выбытия
        group by SCHOOL_ID
        ) e2 on e2.SCHOOL_ID = s.ID
        -- условия организации
        where SA.ID in (SELECT SCHOOL_ATTR_ID FROM MON_NOBD.SCHOOL_SPEC LEFT JOIN MON_NOBD.D_SCHOOLSPEC_TYPE sst ON sst.ID = MON_NOBD.SCHOOL_SPEC.SPEC_TYPE_ID WHERE sst.CODE LIKE '06.%')
        AND SA.DATE_CLOSE IS NULL"""
                },
            
        "NOBD_MP_P33_V1": {
            'sql': """
                    SELECT
        s.ID AS "ID организации образования",
        e1.total AS "Всего выбывших студентов (1 год назад)",
        e2.total AS "Всего выбывших студентов (2 года назад)"
        FROM MON_NOBD.SCHOOL s
        INNER JOIN (
        SELECT sar.SCHOOL_ID
        , sar.DATE_CLOSE
        , sar.ID as ID
        FROM MON_NOBD.SCHOOL_ATTR sar
        WHERE sar.EDU_PERIOD_ID = toInt32(0)
        ) SA ON SA.SCHOOL_ID = s.ID
        left join ( WITH CONCAT(SUBSTRING(toString(NOW() - INTERVAL 1 YEAR),1,4), '/01/01 00:00:00') AS BeginDate 
        , CONCAT(SUBSTRING(toString(NOW() - INTERVAL 1 YEAR),1,4), '/12/31 23:59:59') AS EndDate
        SELECT COUNT(*) as total, e.SCHOOL_ID as SCHOOL_ID FROM MON_NOBD.EDUCATION e 
        WHERE e.EDU_STATUS = toInt32(1)
        AND e.EDU_PERIOD_ID = toInt32(0) 
        AND e.OUT_DATE BETWEEN BeginDate AND EndDate -- дата выбытия
        group by SCHOOL_ID
        ) e1 on e1.SCHOOL_ID = s.ID
        left join ( WITH CONCAT(SUBSTRING(toString(NOW() - INTERVAL 2 YEAR),1,4), '/01/01 00:00:00') AS BeginDate 
        , CONCAT(SUBSTRING(toString(NOW() - INTERVAL 2 YEAR),1,4), '/12/31 23:59:59') AS EndDate
        SELECT COUNT(*) as total, e.SCHOOL_ID as SCHOOL_ID FROM MON_NOBD.EDUCATION e 
        WHERE e.EDU_STATUS = toInt32(1)
        AND e.EDU_PERIOD_ID = toInt32(0)
        AND e.OUT_DATE BETWEEN BeginDate AND EndDate -- дата выбытия
        group by SCHOOL_ID
        ) e2 on e2.SCHOOL_ID = s.ID
        -- условия организации
        where SA.ID in (SELECT SCHOOL_ATTR_ID FROM MON_NOBD.SCHOOL_SPEC LEFT JOIN MON_NOBD.D_SCHOOLSPEC_TYPE sst ON sst.ID = MON_NOBD.SCHOOL_SPEC.SPEC_TYPE_ID WHERE sst.CODE LIKE '06.%')
        AND SA.DATE_CLOSE IS NULL"""
                },
            
        "NOBD_MP_P34_V1": {
            'sql': """
                    select * from (
        SELECT distinct --136
        s.ID AS ID_ORGANIZATION_OBR,
        CASE WHEN addAttr.HAS_INSTACCRED = '1' THEN 'да' WHEN addAttr.HAS_INSTACCRED = '0' THEN 'нет' END AS PROSHLI_INSTRUKC_AKKREDITAC

        FROM MON_NOBD.SCHOOL s
        INNER JOIN MON_NOBD.SCHOOL_ATTR sa ON sa.SCHOOL_ID = s.ID AND sa.EDU_PERIOD_ID = toInt32(0)
        INNER JOIN MON_NOBD.SCHOOL_ADDATTR addAttr ON addAttr.SCHOOL_ATTR_ID = sa.ID
        -- условия организации
        inner join MON_NOBD.SCHOOL_SPEC on sa.ID = MON_NOBD.SCHOOL_SPEC.SCHOOL_ATTR_ID
        left join MON_NOBD.D_SCHOOLSPEC_TYPE sst ON sst.ID = MON_NOBD.SCHOOL_SPEC.SPEC_TYPE_ID
        WHERE sst.CODE LIKE '06.%'
        ) a
        inner join (
        SELECT 
        s.ID AS ID_ORGANIZATION_OBR,
        accrnr_agency.RNAME AS NAZVANIYE_AKKREDIT_ORGANA_VHODYASHEGO_V_NAC_REEST,
        school_accred.ACCRSTART_DATE AS NACHALO_SROKA_DEISTV_AKKRED,
        school_accred.ACCREND_DATE AS OKONCHANIYE_SROKA_DEISTV_AKKRED

        FROM MON_NOBD.SCHOOL s
        INNER JOIN MON_NOBD.SCHOOL_ATTR sa ON sa.SCHOOL_ID = s.ID AND sa.EDU_PERIOD_ID = toInt32(0)
        INNER JOIN MON_NOBD.SCHOOL_ACCRED school_accred ON school_accred.SCHOOL_ATTR_ID = sa.ID
        -- связь со справочниками значений по организации
        LEFT JOIN MON_NOBD.D_ACCRNR_AGENCY accrnr_agency ON accrnr_agency.ID = school_accred.ACCRNR_AGENCY_ID
        inner join MON_NOBD.SCHOOL_SPEC on sa.ID = MON_NOBD.SCHOOL_SPEC.SCHOOL_ATTR_ID
        left join MON_NOBD.D_SCHOOLSPEC_TYPE sst ON sst.ID = MON_NOBD.SCHOOL_SPEC.SPEC_TYPE_ID
        where sst.CODE LIKE '06.%'
        AND sa.DATE_CLOSE IS NULL -- организация не закрыта в настоящее время
        AND school_accred.IND_TYPE = toInt32(1)
        ) b on a.ID_ORGANIZATION_OBR = b.ID_ORGANIZATION_OBR"""
                },
            
        "NOBD_MP_P35_V1": {
            'sql': """
                    select * from (
        SELECT DISTINCT 
        S.ID AS ID_ORGANIZATION_OBR,
        CASE WHEN ADDATTR.HAS_SPECACCRED=1 THEN 'ДА' WHEN ADDATTR.HAS_SPECACCRED=0 THEN 'НЕТ' END AS PROSHLI_SPECIAL_AKKRED

        FROM MON_NOBD.SCHOOL S
        INNER JOIN MON_NOBD.SCHOOL_ATTR SA ON SA.SCHOOL_ID = S.ID
        INNER JOIN MON_NOBD.SCHOOL_ADDATTR ADDATTR ON ADDATTR.SCHOOL_ATTR_ID = SA.ID
        -- УСЛОВИЯ ОРГАНИЗАЦИИ
        LEFT JOIN 
        (SELECT SPEC_TYPE_ID,
        SC.SCHOOL_ATTR_ID SCHOOL_ATTR_ID FROM MON_NOBD.SCHOOL_SPEC SC
        LEFT JOIN MON_NOBD.D_SCHOOLSPEC_TYPE SST ON SST.ID = SC.SPEC_TYPE_ID 
        WHERE SST.CODE LIKE '06.%') AS EX ON EX.SCHOOL_ATTR_ID=SA.ID -- СООТВЕТСТВИЕ ПО ТИПАМ/ВИДАМ ОРГАНИЗАЦИЙ
        WHERE SA.EDU_PERIOD_ID = toInt32(0)
        AND SA.DATE_CLOSE IS NULL -- ОРГАНИЗАЦИЯ НЕ ЗАКРЫТА В НАСТОЯЩЕЕ ВРЕМЯ
        ) a 
        inner join (
        SELECT 
        s.ID AS ID_ORGANIZATION_OBR,
        accrnr_agency.RNAME AS NAZVANIYE_AKKREDIT_ORGANA_VHODYASHEGO_V_NAC_REEST,
        profession.RNAME AS KOD_I_NAZVANIYE_OBR_PROGRAMMI,
        school_accred.ACCRSTART_DATE AS NACHALO_SROKA_DEISTV_AKKRED,
        school_accred.ACCREND_DATE AS OKONCHANIYE_SROKA_DEISTV_AKKRED

        FROM MON_NOBD.SCHOOL s
        INNER JOIN MON_NOBD.SCHOOL_ATTR sa ON sa.SCHOOL_ID = s.ID AND sa.EDU_PERIOD_ID = toInt32(0)
        INNER JOIN MON_NOBD.SCHOOL_ACCRED school_accred ON school_accred.SCHOOL_ATTR_ID = sa.ID
        -- связь со справочниками значений по организации
        LEFT JOIN MON_NOBD.D_ACCRNR_AGENCY accrnr_agency ON accrnr_agency.ID = school_accred.ACCRNR_AGENCY_ID
        LEFT JOIN MON_NOBD.D_PROFESSION profession ON profession.ID = school_accred.PROFESSION_ID
        inner join MON_NOBD.SCHOOL_SPEC on sa.ID = MON_NOBD.SCHOOL_SPEC.SCHOOL_ATTR_ID
        left join MON_NOBD.D_SCHOOLSPEC_TYPE sst ON sst.ID = MON_NOBD.SCHOOL_SPEC.SPEC_TYPE_ID
        where sst.CODE LIKE '06.%'
        AND sa.DATE_CLOSE IS NULL -- организация не закрыта в настоящее время
        AND school_accred.IND_TYPE = toInt32(2)
        ) b on a.ID_ORGANIZATION_OBR = b.ID_ORGANIZATION_OBR"""
                },
            
        "NOBD_MP_P37_V1": {
            'sql': """
                    
        WITH CONCAT(SUBSTRING(toString(NOW() - INTERVAL 1 YEAR),1,4), '/01/01 00:00:00') AS BeginDate 
        , CONCAT(SUBSTRING(toString(NOW() - INTERVAL 1 YEAR),1,4), '/12/31 23:59:59') AS EndDate
        SELECT distinct S.ID 
        , ST.ID 
        , E.EDU_ID 
        , ST.IIN 
        , ST.LASTNAME 
        , ST.FIRSTNAME 
        , ST.MIDDLENAME 
        , STA.BIRTHDATE 
        , PROFESSION.RNAME 
        , BUDGET.RNAME 
        , E.REG_DATE 
        ,any(sc1.total) as a
        FROM MON_NOBD.SCHOOL S 
        INNER JOIN MON_NOBD.SCHOOL_ATTR SA ON S.ID = SA.SCHOOL_ID AND SA.EDU_PERIOD_ID = toInt32(0)
        INNER JOIN MON_NOBD.EDUCATION E ON S.ID = E.SCHOOL_ID AND E.EDU_PERIOD_ID = toInt32(0)
        INNER JOIN MON_NOBD.STUDENT ST ON ST.ID = E.STUDENT_ID 
        INNER JOIN MON_NOBD.STUDENT_ATTR STA ON STA.STUDENT_ID = ST.ID AND STA.EDU_PERIOD_ID = toInt32(0)
        LEFT JOIN MON_NOBD.D_PROFESSION PROFESSION ON PROFESSION.ID = E.PROFESSION_ID 
        LEFT JOIN MON_NOBD.D_BUDGET_TYPE BUDGET ON BUDGET.ID = E.BUDGET_TYPE_ID 
        LEFT JOIN ( SELECT distinct sc.STUDENT_ATTR_ID as STUDENT_ATTR_ID,sc.TEST_DATE as TEST_DATE , SUM(sc.TOTAL_SCORE) as total FROM MON_NOBD.STUDENT_SCORE sc 
        GROUP BY sc.TEST_CODE,STUDENT_ATTR_ID,TEST_DATE ORDER BY total DESC 
        ) sc1 on sc1.STUDENT_ATTR_ID = STA.ID 
        WHERE SA.ID IN (
        SELECT DISTINCT SS.SCHOOL_ATTR_ID AS SCHOOL_ATTR_ID
        FROM MON_NOBD.SCHOOL_SPEC SS 
        INNER JOIN MON_NOBD.SCHOOL_ATTR SA ON SA.ID = SS.SCHOOL_ATTR_ID 
        LEFT JOIN MON_NOBD.D_SCHOOLSPEC_TYPE SST ON SST.ID = SS.SPEC_TYPE_ID 
        WHERE SST.CODE LIKE '06.%'
        )
        AND SA.DATE_CLOSE IS NULL
        AND E.EDUSTATUS_ID = 1
        AND (E.LEARNYEAR_ID = 1 OR E.COURSE_NUM_ID = 19)
        AND E.REG_DATE BETWEEN BeginDate AND EndDate
        --and ST.ID = 1694760
        --AND sc1.TEST_DATE < E.REG_DATE 
        group by 
        S.ID 
        , ST.ID 
        , E.EDU_ID 
        , ST.IIN 
        , ST.LASTNAME 
        , ST.FIRSTNAME 
        , ST.MIDDLENAME 
        , STA.BIRTHDATE 
        , PROFESSION.RNAME 
        , BUDGET.RNAME 
        , E.REG_DATE"""
                },
            
        "NOBD_MP_P36_1_V1": {
            'sql': """
                    SELECT--130 rows
        s.ID AS "ID",
        e.cnt AS "Всего студентов 1 курса",
        e2.cnt AS "Всего студентов 1 курса, зачисленных условно"

        FROM MON_NOBD.SCHOOL s
        INNER JOIN MON_NOBD.SCHOOL_ATTR sa ON sa.SCHOOL_ID = s.ID AND sa.EDU_PERIOD_ID = toInt32(0)
        left join ( SELECT COUNT(DISTINCT e.EDU_ID) as cnt, e.SCHOOL_ID as SCHOOL_ID FROM MON_NOBD.EDUCATION e 
        LEFT JOIN MON_NOBD.D_PROFESSION profession ON profession.ID = e.PROFESSION_ID
        WHERE e.EDU_STATUS = toInt32(0)
        AND e.EDU_PERIOD_ID = toInt32(0)
        AND ( ( SUBSTRING(profession.CODE, 1,1) < '5' AND e.COURSE_NUM_ID = 19 ) OR ( SUBSTRING(profession.CODE, 1,1) >= '5' AND e.LEARNYEAR_ID = 1 ) ) -- курс / год обучения
        group by SCHOOL_ID
        ) e on e.SCHOOL_ID = s.ID
        left join ( SELECT COUNT(DISTINCT e.EDU_ID) as cnt, e.SCHOOL_ID as SCHOOL_ID FROM MON_NOBD.EDUCATION e 
        LEFT JOIN MON_NOBD.D_PROFESSION profession ON profession.ID = e.PROFESSION_ID
        WHERE e.EDU_STATUS = toInt32(0)
        AND e.EDU_PERIOD_ID = toInt32(0)
        AND ( ( SUBSTRING(profession.CODE, 1,1) < '5' AND e.COURSE_NUM_ID = 19 ) OR ( SUBSTRING(profession.CODE, 1,1) >= '5' AND e.LEARNYEAR_ID = 1 ) ) -- курс / год обучения
        AND e.ADMISS_TYPE_ID = toInt32(6) -- вид зачисления
        group by SCHOOL_ID
        ) e2 on e2.SCHOOL_ID = s.ID
        -- условия организации
        where sa.DATE_CLOSE IS NULL
        and sa.ID in( SELECT SCHOOL_ATTR_ID FROM MON_NOBD.SCHOOL_SPEC LEFT JOIN MON_NOBD.D_SCHOOLSPEC_TYPE sst ON sst.ID = MON_NOBD.SCHOOL_SPEC.SPEC_TYPE_ID WHERE sst.CODE LIKE '06.%')
        """
                },
            
        "NOBD_MP_P38_V1": {
            'sql': """
                    SELECT --130 rows
        s.ID AS "ID",
        e.cnt AS "Всего студентов",
        e2.cnt AS "Всего студентов, переведенных из зарубежных вузов (за прошлый год)"
        FROM MON_NOBD.SCHOOL s
        INNER JOIN MON_NOBD.SCHOOL_ATTR sa ON sa.SCHOOL_ID = s.ID AND sa.EDU_PERIOD_ID = toInt32(0)
        left join ( SELECT COUNT(DISTINCT e.EDU_ID) as cnt, e.SCHOOL_ID as SCHOOL_ID FROM MON_NOBD.EDUCATION e 
        WHERE e.EDU_STATUS = toInt32(0)
        AND e.EDU_PERIOD_ID = toInt32(0)
        group by SCHOOL_ID
        ) e on e.SCHOOL_ID = s.ID
        left join ( SELECT COUNT(DISTINCT e.EDU_ID) as cnt, e.SCHOOL_ID as SCHOOL_ID FROM MON_NOBD.EDUCATION e 
        WHERE e.EDU_STATUS = toInt32(0)
        AND e.EDU_PERIOD_ID = toInt32(0)
        AND e.ADMISS_TYPE_ID = toInt32(3)
        AND toDate(CONCAT(SUBSTRING(REG_DATE,1,4),'-',SUBSTRING(REG_DATE,6,2),'-',SUBSTRING(REG_DATE,9,2))) BETWEEN toDate(CONCAT(toString(toYear(now()) - 1),'-01-01')) AND toDate(CONCAT(toString(toYear(now()) - 1),'-12-31')) -- дата прибытия
        group by
        SCHOOL_ID
        ) e2 on e2.SCHOOL_ID = s.ID
        -- условия организации
        where sa.DATE_CLOSE IS NULL
        and sa.ID in( SELECT SCHOOL_ATTR_ID FROM MON_NOBD.SCHOOL_SPEC LEFT JOIN MON_NOBD.D_SCHOOLSPEC_TYPE sst ON sst.ID = MON_NOBD.SCHOOL_SPEC.SPEC_TYPE_ID WHERE sst.CODE LIKE '06.%')

        -"""
                },
            
        
        "NOBD_MP_P39_V1": {
            'sql': """
                    SELECT DISTINCT --151
        s.ID AS "ID", 
        st.ID AS "ID учащегося", 
        e.EDU_ID "ID обучения",
        st.IIN AS "ИИН",
        st.LASTNAME AS "Фамилия",
        st.FIRSTNAME AS "Имя",
        COALESCE(st.MIDDLENAME, '') AS "Отчество",
        sta.BIRTHDATE AS "Дата рождения",
        profession.RNAME AS "Специальность и классификатор/образовательная программа"
        FROM MON_NOBD.SCHOOL s
        INNER JOIN (
        SELECT sar.SCHOOL_ID
        , sar.DATE_CLOSE
        , sar.ID as ID
        FROM MON_NOBD.SCHOOL_ATTR sar
        WHERE sar.EDU_PERIOD_ID = toInt32(0)
        ) SA ON SA.SCHOOL_ID = s.ID
        INNER JOIN MON_NOBD.EDUCATION e ON s.ID = e.SCHOOL_ID AND e.EDU_PERIOD_ID = toInt32(0)
        INNER JOIN MON_NOBD.STUDENT st ON st.ID = e.STUDENT_ID
        INNER JOIN MON_NOBD.STUDENT_ATTR sta ON sta.STUDENT_ID = st.ID AND sta.EDU_PERIOD_ID = toInt32(0)
        -- показатели учащихся
        LEFT JOIN MON_NOBD.D_PROFESSION profession ON profession.ID = e.PROFESSION_ID
        -- условия
        where SA.DATE_CLOSE IS NULL
        AND e.EDU_STATUS = toInt32(0)
        AND e.ADMISS_TYPE_ID = toInt32(5)
        and SA.ID in (SELECT SCHOOL_ATTR_ID FROM MON_NOBD.SCHOOL_SPEC LEFT JOIN MON_NOBD.D_SCHOOLSPEC_TYPE sst ON sst.ID = MON_NOBD.SCHOOL_SPEC.SPEC_TYPE_ID WHERE sst.CODE LIKE '06.%')"""
                },
            
        "NOBD_MP_P40_V1": {
            'sql': """
                    SELECT 
        s.ID AS "ID",
        e1.total AS "Кол-во зарубежных специалистов"
        FROM MON_NOBD.SCHOOL s
        INNER JOIN MON_NOBD.SCHOOL_ATTR sa ON sa.SCHOOL_ID = s.ID AND sa.EDU_PERIOD_ID = toInt32(0)
        left join (SELECT COUNT(*) as total,tj.SCHOOL_ID as SCHOOL_ID FROM MON_NOBD.TEACHJOB tj 
        WHERE tj.EDU_PERIOD_ID = toInt32(0) 
        AND tj.REC_STATUS = toInt32(0)
        AND tj.IS_FOREIGN = toInt32(1)
        group by SCHOOL_ID) e1 on e1.SCHOOL_ID = s.ID
        inner join MON_NOBD.SCHOOL_SPEC on sa.ID = MON_NOBD.SCHOOL_SPEC.SCHOOL_ATTR_ID
        LEFT JOIN MON_NOBD.D_SCHOOLSPEC_TYPE sst ON sst.ID = MON_NOBD.SCHOOL_SPEC.SPEC_TYPE_ID
        where sst.CODE LIKE '06.%'
        AND sa.DATE_CLOSE IS NULL"""
                },
            
        "NOBD_MP_P41_V1": {
            'sql': """
                    SELECT
        S.ID AS "ID ОРГАНИЗАЦИИ ОБРАЗОВАНИЯ",
        ST.ID AS "ID УЧАЩЕГОСЯ", 
        E.EDU_ID "ID ОБУЧЕНИЯ",
        ST.IIN AS "ИИН",
        ST.LASTNAME AS "ФАМИЛИЯ",
        ST.FIRSTNAME AS "ИМЯ",
        COALESCE(ST.MIDDLENAME, '') AS "ОТЧЕСТВО",
        STA.BIRTHDATE AS "ДАТА РОЖДЕНИЯ"

        FROM MON_NOBD.SCHOOL S
        INNER JOIN MON_NOBD.SCHOOL_ATTR SA ON S.ID = SA.SCHOOL_ID 
        INNER JOIN MON_NOBD.EDUCATION E ON E.SCHOOL_ID = S.ID
        INNER JOIN MON_NOBD.STUDENT_ATTR STA ON STA.STUDENT_ID = E.STUDENT_ID
        INNER JOIN MON_NOBD.STUDENT ST ON ST.ID = E.STUDENT_ID
        INNER JOIN 
        (SELECT SPEC_TYPE_ID,
        SC.SCHOOL_ATTR_ID SCHOOL_ATTR_ID FROM MON_NOBD.SCHOOL_SPEC SC
        LEFT JOIN MON_NOBD.D_SCHOOLSPEC_TYPE SST ON SST.ID = SC.SPEC_TYPE_ID 
        WHERE SST.CODE LIKE '06.%') AS EX ON EX.SCHOOL_ATTR_ID=SA.ID
        WHERE
        -- УСЛОВИЯ ОРГАНИЗАЦИИ
        SA.DATE_CLOSE IS NULL -- ОРГАНИЗАЦИЯ НЕ ЗАКРЫТА В НАСТОЯЩЕЕ ВРЕМЯ
        -- УСЛОВИЯ КОНТИНГЕНТ
        AND E.EDU_STATUS = 2 -- ВЫПУСКНИК
        AND E.STUDEDU_LEVEL_ID IN (9, 10) -- ДОКТОРАНТУРА (НАУЧНО-ПЕДАГОГИЧЕСКОЕ НАПРАВЛЕНИЕ) / ДОКТОРАНТУРА (ПРОФИЛЬНОЕ НАПРАВЛЕНИЕ)"
        AND SA.EDU_PERIOD_ID = 0
        AND E.EDU_PERIOD_ID = 0
        AND STA.EDU_PERIOD_ID = 0"""
                },
            
        "NOBD_MP_P42_V1": {
            'sql': """
                    SELECT 
        S.ID AS "ID ОРГАНИЗАЦИИ ОБРАЗОВАНИЯ",
        ST.ID AS "ID УЧАЩЕГОСЯ", 
        E.EDU_ID "ID ОБУЧЕНИЯ",
        ST.IIN AS "ИИН",
        ST.LASTNAME AS "ФАМИЛИЯ",
        ST.FIRSTNAME AS "ИМЯ",
        COALESCE(ST.MIDDLENAME, '') AS "ОТЧЕСТВО",
        HOSTEL_LIVE.RNAME AS "СВЕДЕНИЯ ОБ ОБЩЕЖИТИИ",
        HOSTEL_NEED.RNAME AS "ПРОЖИВАЕТ В ОБЩЕЖИТИИ"

        FROM MON_NOBD.SCHOOL S
        INNER JOIN MON_NOBD.SCHOOL_ATTR SA ON SA.SCHOOL_ID = S.ID 
        INNER JOIN MON_NOBD.EDUCATION E ON E.SCHOOL_ID = S.ID
        INNER JOIN MON_NOBD.STUDENT_ATTR STA ON STA.STUDENT_ID = E.STUDENT_ID
        INNER JOIN MON_NOBD.STUDENT ST ON ST.ID = E.STUDENT_ID
        -- СВЯЗЬ СО СПРАВОЧНИКАМИ ЗНАЧЕНИЙ ПО КОНТИНГЕНТУ
        LEFT JOIN MON_NOBD.D_HOSTEL_NEED HOSTEL_NEED ON HOSTEL_NEED.ID = E.HOSTEL_NEED_ID
        LEFT JOIN MON_NOBD.D_HOSTEL_LIVE HOSTEL_LIVE ON HOSTEL_LIVE.ID = E.HOSTEL_NEED_ID
        LEFT JOIN 
        (SELECT SPEC_TYPE_ID,
        SC.SCHOOL_ATTR_ID SCHOOL_ATTR_ID FROM MON_NOBD.SCHOOL_SPEC SC
        LEFT JOIN MON_NOBD.D_SCHOOLSPEC_TYPE SST ON SST.ID = SC.SPEC_TYPE_ID 
        WHERE SST.CODE LIKE '03.%') AS EX ON EX.SCHOOL_ATTR_ID=SA.ID
        -- УСЛОВИЯ ОРГАНИЗАЦИИ
        WHERE
        SA.DATE_CLOSE IS NULL -- ОРГАНИЗАЦИЯ НЕ ЗАКРЫТА В НАСТОЯЩЕЕ ВРЕМЯ
        -- УСЛОВИЯ КОНТИНГЕНТ
        AND E.EDU_STATUS = 0 
        AND E.VACATION_REASON_ID IN ('4')
        AND SA.EDU_PERIOD_ID = 0
        AND E.EDU_PERIOD_ID = 0
        AND STA.EDU_PERIOD_ID = 0"""
                },
            
        "NOBD_MP_P43_V1": {
            'sql': """
                    "SELECT 
        DISTINCT
        S.ID AS ""ID ОРГАНИЗАЦИИ ОБРАЗОВАНИЯ"",
        PCUSE.PCTOT_CNT AS ""КОЛИЧЕСТВО КОМПЬЮТЕРОВ, ИСПОЛЬЗУЕМЫХ В ОБРАЗОВАТЕЛЬНОМ ПРОЦЕССЕ""

        FROM MON_NOBD.SCHOOL S
        INNER JOIN MON_NOBD.SCHOOL_ATTR SA ON SA.SCHOOL_ID = S.ID
        INNER JOIN MON_NOBD.SCHOOL_PCUSE PCUSE ON PCUSE.SCHOOL_ATTR_ID = SA.ID
        LEFT JOIN 
        (SELECT SPEC_TYPE_ID,
        SC.SCHOOL_ATTR_ID SCHOOL_ATTR_ID FROM MON_NOBD.SCHOOL_SPEC SC
        LEFT JOIN MON_NOBD.D_SCHOOLSPEC_TYPE SST ON SST.ID = SC.SPEC_TYPE_ID 
        WHERE SST.CODE LIKE '02.1.%' OR 
        SST.CODE LIKE '02.2.%' 
        OR SST.CODE LIKE '02.3.%' 
        OR SST.CODE LIKE '02.4.%' OR SST.CODE LIKE '02.5.%' OR SST.CODE LIKE '07.%' OR 
        SST.CODE IN ('02.6.1', '02.6.2', '02.6.3', '02.6.4', '08.3', '08.4', '08.5', '08.6', '09.3', '09.4') ) AS EX ON EX.SCHOOL_ATTR_ID=SA.ID
        -- УСЛОВИЯ ОРГАНИЗАЦИИ
        WHERE
        SA.DATE_CLOSE IS NULL
        AND PCUSE.RECTYPE = 2
        AND PCUSE.PC_USE_ID = 3
        AND PCUSE.PURCHASEYEAR_ID = 1
        AND SA.EDU_PERIOD_ID = 0"""
                },
            
        
        "NOBD_MP_P44_V1": {
            'sql': """
                    "SELECT 
        DISTINCT 
        S.ID AS ""ID ОРГАНИЗАЦИИ ОБРАЗОВАНИЯ"",
        AGRINTSPEED.RNAME AS ""ДОГОВОРНАЯ СКОРОСТЬ ИНТЕРНЕТА"",
        FACTINTSPEED.RNAME AS ""ФАКТИЧЕСКАЯ СКОРОСТЬ ИНТЕРНЕТА"",
        SA.INTACCESS_CNT AS ""КОЛИЧЕСТВО ТОЧЕК ДОСТУПА""

        FROM MON_NOBD.SCHOOL S
        INNER JOIN MON_NOBD.SCHOOL_ATTR SA ON S.ID = SA.SCHOOL_ID
        -- СПРАВОЧНИКИ
        LEFT JOIN MON_NOBD.D_INTSPEED AGRINTSPEED ON AGRINTSPEED.ID = SA.INTSPEED_ID
        LEFT JOIN MON_NOBD.D_INTSPEED FACTINTSPEED ON FACTINTSPEED.ID = SA.FACT_INTSPEED_ID
        -- УСЛОВИЯ ОРГАНИЗАЦИИ
        LEFT JOIN 
        (SELECT SPEC_TYPE_ID,
        SC.SCHOOL_ATTR_ID SCHOOL_ATTR_ID FROM MON_NOBD.SCHOOL_SPEC SC
        LEFT JOIN MON_NOBD.D_SCHOOLSPEC_TYPE SST ON SST.ID = SC.SPEC_TYPE_ID 
        WHERE SST.CODE LIKE '06.%') AS EX ON EX.SCHOOL_ATTR_ID=SA.ID
        WHERE SA.EDU_PERIOD_ID = 0
        AND SA.DATE_CLOSE IS NULL -- ОРГАНИЗАЦИЯ НЕ ЗАКРЫТА В НАСТОЯЩЕЕ ВРЕМЯ"""
                }
        
        }

    TARGET_SCHEMA = 'DWH_CDM'

    tasks = []

    for key, value in TABLES.items():
        
        params = {
            'target_schema': TARGET_SCHEMA,
            'target_table': key,
            'sql': value['sql']
        }

        task_ = PythonOperator(
            task_id=f"{TARGET_SCHEMA}.{key}",
           # trigger_rule='all_done',
            python_callable=main,
            op_kwargs=params
        )

        tasks.append(task_)

    chain(*tasks)