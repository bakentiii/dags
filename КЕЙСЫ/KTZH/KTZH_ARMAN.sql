drop table RF_CASE.KTZH_IMPORT_ALL ;
-- RF_CASE.KTZH_EXPORT_ALL definition

-- RF_CASE.KTZH_IMPORT_ALL definition

CREATE TABLE RF_CASE.KTZH_IMPORT_ALL
(

    `SENDING_NUM` String,

    `ACCEPT_DATE` String,

    `DISCRETING_DATE` String,

    `SENDING_STATION_CODE` Nullable(UInt32),

    `SENDING_STATION_NAME` Nullable(String),

    `DEST_STATION_CODE` UInt32,

    `DEST_STATION_NAME` String,

    `SENDING_COUNTRY` String,

    `DEST_COUNTRY` String,

    `GNG_CODE` UInt32,

    `GNG_NAME` String,

    `ETSNG_CODE` Nullable(UInt32),

    `ETSNG_NAME` String,

    `SENDER_CODE` UInt32,

    `SENDER_NAME` String,

    `RECEIVER_CODE` UInt16,

    `RECEIVER_NAME` String,

    `CONTAINER_NUM` Nullable(String),

    `CARRIAGE_NUM` Nullable(UInt32),

    `WEIGHT` Nullable(Float32),

    `CARGO_TYPE` Nullable(String),

    `SDU_LOAD_IN_DT` DateTime,

    `TNVAD_CODES` Nullable(String),

    `CODE_CUT` Nullable(String),

    `VALUE_INDICATOR` Nullable(Float32),

    `DISCRETING_DATE2` String,

    `REG_DATE` Date,

    `month` UInt8,

    `week` UInt16,

    `year` UInt16,

    `REG_DATE2` Date,

    `VALUE_INDICATOR1` Nullable(Float32),

    `VALUE_INDICATOR2` Nullable(Float32),

    `type1` String
)
ENGINE = MergeTree()
order by REG_DATE;

insert into RF_CASE.KTZH_IMPORT_ALL

SELECT 
*,
WEIGHT as  VALUE_INDICATOR,
replaceAll(substring(DISCRETING_DATE,1,10),'/','-') as DISCRETING_DATE2,
toDate(DISCRETING_DATE2) as  REG_DATE,
toMonth(REG_DATE) as month,
toWeek(REG_DATE)+1 as week,
toYear(REG_DATE) as year,
if(
toYear(REG_DATE)==toYear(now())-1 ,

date_add(YEAR, 1, REG_DATE),
REG_DATE
) as REG_DATE2,

if(toYear(REG_DATE)==toYear(now())-1,
VALUE_INDICATOR
,0) as VALUE_INDICATOR1,
if(toYear(REG_DATE)==toYear(now()),
VALUE_INDICATOR
,0) as VALUE_INDICATOR2,

if(CODE_CUT is null ,' Не из списка' ,'Из списка') as type1

FROM RF_CASE.KTZH_IMPORT_2023 set1 

left join 
(
	SELECT 
	'1' as TNVAD_CODES,
	substring(TNVED_10,1,8) as CODE_CUT
	FROM RF_CASE.TOVAR_STRAN
	group by TNVED_10
) as set2 
on substring(toString(set1.GNG_CODE),1,8)=set2.CODE_CUT 

where DISCRETING_DATE  is not null 


union all 

SELECT 
*,
WEIGHT as  VALUE_INDICATOR,
replaceAll(substring(DISCRETING_DATE,1,10),'/','-') as DISCRETING_DATE2,
toDate(DISCRETING_DATE2) as  REG_DATE,
toMonth(REG_DATE) as month,
toWeek(REG_DATE)+1 as week,
toYear(REG_DATE) as year,
if(
toYear(REG_DATE)==toYear(now())-1 ,

date_add(YEAR, 1, REG_DATE),
REG_DATE
) as REG_DATE2,

if(toYear(REG_DATE)==toYear(now())-1,
VALUE_INDICATOR
,0) as VALUE_INDICATOR1,
if(toYear(REG_DATE)==toYear(now()),
VALUE_INDICATOR
,0) as VALUE_INDICATOR2,

if(CODE_CUT is null ,' Не из списка' ,'Из списка') as type1

FROM RF_CASE.KTZH_IMPORT_2022 set1 
left join 
(
	SELECT 
	'1' as TNVAD_CODES,
	substring(TNVED_10,1,8) as CODE_CUT
	FROM RF_CASE.TOVAR_STRAN
	group by TNVED_10
) as set2 
on substring(toString(set1.GNG_CODE),1,8)=set2.CODE_CUT 
where DISCRETING_DATE  is not null;

drop table RF_CASE.KTZH_EXPORT_ALL ;
-- RF_CASE.KTZH_EXPORT_ALL definition

-- RF_CASE.KTZH_EXPORT_ALL definition

CREATE TABLE RF_CASE.KTZH_EXPORT_ALL
(

    `SENDING_NUM` String,

    `ACCEPT_DATE` String,

    `DISCRETING_DATE` String,

    `SENDING_STATION_CODE` Nullable(UInt32),

    `SENDING_STATION_NAME` Nullable(String),

    `DEST_STATION_CODE` UInt32,

    `DEST_STATION_NAME` Nullable(String),

    `SENDING_COUNTRY` Nullable(String),

    `DEST_COUNTRY` String,

    `GNG_CODE` UInt32,

    `GNG_NAME` String,

    `ETSNG_CODE` Nullable(UInt32),

    `ETSNG_NAME` String,

    `SENDER_CODE` UInt32,

    `SENDER_NAME` String,

    `RECEIVER_CODE` UInt64,

    `RECEIVER_NAME` String,

    `CONTAINER_NUM` Nullable(String),

    `CARRIAGE_NUM` Nullable(UInt32),

    `WEIGHT` Nullable(Float32),

    `CARGO_TYPE` Nullable(String),

    `SDU_LOAD_IN_DT` DateTime,

    `TNVAD_CODES` Nullable(String),

    `CODE_CUT` Nullable(String),

    `VALUE_INDICATOR` Nullable(Float32),

    `REG_DATE` Date,

    `month` UInt8,

    `week` UInt16,

    `year` UInt16,

    `REG_DATE2` Date,

    `VALUE_INDICATOR1` Nullable(Float32),

    `VALUE_INDICATOR2` Nullable(Float32),

    `type1` String
)
ENGINE = MergeTree()
order by year;

insert into RF_CASE.KTZH_EXPORT_ALL

SELECT 
*,
WEIGHT as  VALUE_INDICATOR,
toDate(SUBSTRING(ACCEPT_DATE,1,10)) as  REG_DATE,
toMonth(REG_DATE) as month,
toWeek(REG_DATE)+1 as week,
toYear(REG_DATE) as year,
if(
toYear(REG_DATE)==toYear(now())-1 ,

date_add(YEAR, 1, REG_DATE),
REG_DATE
) as REG_DATE2,

if(toYear(REG_DATE)==toYear(now())-1,
VALUE_INDICATOR
,0) as VALUE_INDICATOR1,
if(toYear(REG_DATE)==toYear(now()),
VALUE_INDICATOR
,0) as VALUE_INDICATOR2,

if(CODE_CUT is null ,' Не из списка' ,'Из списка') as type1

FROM RF_CASE.KTZH_EXPORT_2023 set1 
left join 
(
	SELECT 
	'1' as TNVAD_CODES,
	substring(TNVED_10,1,8) as CODE_CUT
	FROM RF_CASE.TOVAR_STRAN
	group by TNVED_10
) as set2 
on substring(toString(set1.GNG_CODE),1,8)=set2.CODE_CUT 
where ACCEPT_DATE  is not null 



union all 

SELECT 
*,
WEIGHT as  VALUE_INDICATOR,
toDate(SUBSTRING(ACCEPT_DATE,1,10)) as  REG_DATE,
toMonth(REG_DATE) as month,
toWeek(REG_DATE)+1 as week,
toYear(REG_DATE) as year,
if(
toYear(REG_DATE)==toYear(now())-1 ,

date_add(YEAR, 1, REG_DATE),
REG_DATE
) as REG_DATE2,

if(toYear(REG_DATE)==toYear(now())-1,
VALUE_INDICATOR
,0) as VALUE_INDICATOR1,
if(toYear(REG_DATE)==toYear(now()),
VALUE_INDICATOR
,0) as VALUE_INDICATOR2,

if(CODE_CUT is null ,' Не из списка' ,'Из списка') as type1

FROM RF_CASE.KTZH_EXPORT_2022 set1 
left join 
(
	SELECT 
	'1' as TNVAD_CODES,
	substring(TNVED_10,1,8) as CODE_CUT
	FROM RF_CASE.TOVAR_STRAN
	group by TNVED_10
) as set2 
on substring(toString(set1.GNG_CODE),1,8)=set2.CODE_CUT 

where ACCEPT_DATE  is not null;

drop table RF_CASE.KTZH_ALL;

CREATE TABLE RF_CASE.KTZH_ALL
(

    `SENDING_NUM` String,

    `ACCEPT_DATE` String,

    `DISCRETING_DATE` String,

    `SENDING_STATION_CODE` Nullable(UInt32),

    `SENDING_STATION_NAME` Nullable(String),

    `DEST_STATION_CODE` UInt32,

    `DEST_STATION_NAME` Nullable(String),

    `SENDING_COUNTRY` Nullable(String),

    `DEST_COUNTRY` String,

    `GNG_CODE` UInt32,

    `GNG_NAME` String,

    `ETSNG_CODE` Nullable(UInt32),

    `ETSNG_NAME` String,

    `SENDER_CODE` UInt32,

    `SENDER_NAME` String,

    `RECEIVER_CODE` UInt64,

    `RECEIVER_NAME` String,

    `CONTAINER_NUM` Nullable(String),

    `CARRIAGE_NUM` Nullable(UInt32),

    `WEIGHT` Nullable(Float32),

    `CARGO_TYPE` Nullable(String),

    `SDU_LOAD_IN_DT` DateTime,

    `TNVAD_CODES` Nullable(String),

    `CODE_CUT` Nullable(String),

    `VALUE_INDICATOR` Nullable(Float32),

    `REG_DATE` Date,

    `month` UInt8,

    `week` UInt16,

    `year` UInt16,

    `REG_DATE2` Date,

    `VALUE_INDICATOR1` Nullable(Float32),

    `VALUE_INDICATOR2` Nullable(Float32),

    `type1` String,

    `type2` String
)

ENGINE =MergeTree()
order by RECEIVER_NAME;

insert into RF_CASE.KTZH_ALL 

SELECT SENDING_NUM, ACCEPT_DATE, DISCRETING_DATE, SENDING_STATION_CODE, SENDING_STATION_NAME, DEST_STATION_CODE, DEST_STATION_NAME, SENDING_COUNTRY, DEST_COUNTRY, GNG_CODE, GNG_NAME, ETSNG_CODE, ETSNG_NAME, SENDER_CODE, SENDER_NAME, RECEIVER_CODE, RECEIVER_NAME, CONTAINER_NUM, CARRIAGE_NUM, WEIGHT, CARGO_TYPE, SDU_LOAD_IN_DT, TNVAD_CODES, CODE_CUT, VALUE_INDICATOR, REG_DATE, `month`, week, `year`, REG_DATE2, VALUE_INDICATOR1, VALUE_INDICATOR2, type1
,'Экспорт' as type2
FROM RF_CASE.KTZH_EXPORT_ALL

union all 

SELECT SENDING_NUM, ACCEPT_DATE, DISCRETING_DATE, SENDING_STATION_CODE, SENDING_STATION_NAME, DEST_STATION_CODE, DEST_STATION_NAME, SENDING_COUNTRY, DEST_COUNTRY, GNG_CODE, GNG_NAME, ETSNG_CODE, ETSNG_NAME, SENDER_CODE, SENDER_NAME, RECEIVER_CODE, RECEIVER_NAME, CONTAINER_NUM, CARRIAGE_NUM, WEIGHT, CARGO_TYPE, SDU_LOAD_IN_DT, TNVAD_CODES, CODE_CUT, VALUE_INDICATOR, REG_DATE, `month`, week, `year`, REG_DATE2, VALUE_INDICATOR1, VALUE_INDICATOR2, type1
,'Импорт' as type2
FROM RF_CASE.KTZH_IMPORT_ALL;




