create table NEET_YOUTH.PEOPLE_NEET ENGINE = MergeTree() Order by IIN as 
SELECT DISTINCT 
GBL.IIN  AS IIN
, GBL.SEX_NAME AS SEX_NAME
, GBL.PERSON_AGE AS PERSON_AGE
, AGE_CAT.AGE_CATEGORY  as AGE_CATEGORY
, GBL.KATO AS KATO
, ZS.STATUS AS ZAGS_STATUS
, IF(UCH.UL_IIN IS NULL OR UCH.UL_IIN = '','Не является учредителем ЮЛ','Является учредителем ЮЛ') AS IS_UCHRED
, IF(IP.IP_IIN IS NULL OR IP.IP_IIN = '','Отсутствует ИП','Имеется ИП') AS IS_IP
, IF(GRST.GRST_IIN IS NULL OR GRST.GRST_IIN = '','Отсутсвует КХ/ФХ',GRST.OPF_NAME) AS IS_GRST
, IF(OSMS.OSMS_IIN IS NULL OR OSMS.OSMS_IIN = '','Отсутствует в списке плательщиков ОСМС','Присутствует в списке плательщиков ОСМС') as IS_OSMS
, IF(ESP.ESP_IIN IS NULL OR ESP.ESP_IIN = '','Не является плательщиком ЕСП', 'Является плательщиком ЕСП') AS IS_ESP
, IF(OPV.RNN IS NULL OR OPV.RNN = '','Отсутствуют налоговые отчисления ОПВ последние 2 месяца подряд','Присутствуют налоговые отчисления ОПВ последние 2 месяца подряд') AS IS_OPV_2MONTH
, IF(ST3.IIN IS NULL OR ST3.IIN = '',0,1) AS IS_YOUNG_STUDENT
, IF(DOC.IIN IS NULL OR DOC.IIN = '','Не зарегистрирован диплом/аттестат об образовании','Зарегистрирован диплом/аттестат об образовании') AS HAS_STUDY_DOC
, IF(ST4.IIN IS NULL OR ST4.IIN = '',0,1) AS IS_OLD_STUDENT
, IF(POSOB.IIN IS NULL OR POSOB.IIN = '','Не получает пособия по уходу за ребенком','Получает пособия по уходу за ребенком') AS HAS_CHILD_POSOB
, IF(CHILD.IIN IS NULL OR CHILD.IIN = '','Наличие ребенка (детей более 3-х) либо нет детей','Наличие ребенка (детей не более 3-х)') as HAS_CHILD
, IF(BEZRAB.IIN IS NULL OR BEZRAB.IIN = '','Отсутствует в базе данных официальных безработных','Присутствует в базе данных официальных безработных') as IS_BEZRAB
, IF(SOC.RNN IS NULL OR SOC.RNN = '','Никогда не отчисляли социальных отчислений и платежей', 'Имеются записи социальных отчислений и платежей') as HAS_SOC_OTCHISL
, IF(SR_OBR.STUDENT_IIN IS NULL OR SR_OBR.STUDENT_IIN = '','Отсутствует среднее образование','Присутствует среднее образование') as HAS_SRED_OBR
, IF(NOT_SR_OBR.STUDENT_IIN IS NULL OR NOT_SR_OBR.STUDENT_IIN = '','Отсутствует незаконченное среднее образование','Присутствует незаконченное среднее образование') AS HAS_UNFINISH_SRED_OBR
, IF(TIPO.STUDENT_IIN IS NULL OR TIPO.STUDENT_IIN = '','Отсутствует профессиональное образование','Присутствует профессиональное образование') as HAS_TIPO_OBR
, IF(NOT_TIPO.STUDENT_IIN IS NULL OR NOT_TIPO.STUDENT_IIN = '','Отсутствует незаконченное профессиональное образование','Присутствует незаконченное профессиональное образование') AS HAS_UNFINISH_TIPO_OBR
, IF(VYSWEE.STUDENT_IIN IS NULL OR VYSWEE.STUDENT_IIN = '','Отсутствует высшее образование','Присутствует высшее образование') as HAS_VYSWEE_OBR
, IF(NOT_VYSWEE.STUDENT_IIN IS NULL OR NOT_VYSWEE.STUDENT_IIN = '','Отсутствует незаконченное высшее образование','Присутствует незаконченное высшее образование') as HAS_UNFINISH_VYSWEE_OBR
, IF(MASTER_DEGREE.STUDENT_IIN IS NULL OR MASTER_DEGREE.STUDENT_IIN = '','Отсутствует послевузовское образование','Присутствует послевузовское образование') as HAS_MASTER_DEGREE_OBR
, IF(NOT_MASTER_DEGREE.STUDENT_IIN IS NULL OR NOT_MASTER_DEGREE.STUDENT_IIN = '','Отсутствует незаконченное послевузовское образование','Присутствует незаконченное послевузовское образование') as HAS_UNFINISH_MASTER_DEGREE_OBR
FROM DM_ZEROS.DM_MU_GBL_PERSON_LIVE_KZ_CITZN GBL 
INNER JOIN MON_NOBD.D_AGE_CATEGORIES AGE_CAT ON GBL.PERSON_AGE = AGE_CAT.AGE_ID 
LEFT JOIN (select max(STATUS) as STATUS, IIN  from CKS_CASES.ZAGS_STATUS group by IIN) ZS ON GBL.IIN = ZS.IIN
LEFT JOIN NEET_YOUTH.IS_UL_IIN UCH ON GBL.IIN = UCH.UL_IIN
LEFT JOIN NEET_YOUTH.IS_IP_IIN IP ON GBL.IIN = IP.IP_IIN
LEFT JOIN NEET_YOUTH.IS_GRST_IIN GRST ON GBL.IIN = GRST.GRST_IIN
LEFT JOIN NEET_YOUTH.IS_OSMS_IIN OSMS ON GBL.IIN = OSMS.OSMS_IIN
LEFT JOIN NEET_YOUTH.IS_ESP_IIN ESP ON GBL.IIN = ESP.ESP_IIN
LEFT JOIN (SELECT DISTINCT RNN FROM NEET_YOUTH.OPV_IIN_YEAR) OPV ON GBL.IIN = OPV.RNN
LEFT JOIN NEET_YOUTH.STUDY_END_3_MONTH ST3 ON GBL.IIN = ST3.IIN
LEFT JOIN NEET_YOUTH.HAS_STUDY_DOC DOC ON GBL.IIN = DOC.IIN
LEFT JOIN NEET_YOUTH.STUDY_END_MORE_4_MONTH ST4 ON GBL.IIN = ST4.IIN
LEFT JOIN NEET_YOUTH.NET_POSOB_ZA_REBENKA POSOB ON GBL.IIN = POSOB.IIN
LEFT JOIN NEET_YOUTH.HAS_1_TO_3_CHILD CHILD ON GBL.IIN = CHILD.IIN
LEFT JOIN NEET_YOUTH.BEZRABOTNIYE BEZRAB ON GBL.IIN = BEZRAB.IIN
LEFT JOIN (SELECT DISTINCT RNN FROM SK_FAMILY.SOC_IIN_YEAR) SOC ON GBL.IIN = SOC.RNN
LEFT JOIN (SELECT STUDENT_IIN FROM NEET_YOUTH.NOBD n WHERE IS_FINISHED IN('Завершенное среднее образование','Завершенное образование в организации для детей-сирот и детей, оставшихся без попечения родителей','Завершенное специализированное образование','Завершенное специальное образование') AND ROW_COUNTER = 1) SR_OBR ON GBL.IIN = SR_OBR.STUDENT_IIN
LEFT JOIN (SELECT STUDENT_IIN FROM NEET_YOUTH.NOBD n WHERE IS_FINISHED IN('Незавершенное среднее образование','Незавершенное образование в организации для детей-сирот и детей, оставшихся без попечения родителей','Незавершенное образование в организации дополнительного образования для детей','Незавершенное специализированное образование','Незавершенное специальное образование') AND ROW_COUNTER = 1) NOT_SR_OBR ON GBL.IIN = NOT_SR_OBR.STUDENT_IIN
LEFT JOIN (SELECT STUDENT_IIN FROM NEET_YOUTH.NOBD n WHERE IS_FINISHED IN('Завершенное профессиональное образование')) AS TIPO ON GBL.IIN = TIPO.STUDENT_IIN
LEFT JOIN (SELECT STUDENT_IIN FROM NEET_YOUTH.NOBD n WHERE IS_FINISHED IN('Незавершенное профессиональное образование') AND ROW_COUNTER = 1 ) AS NOT_TIPO ON GBL.IIN = NOT_TIPO.STUDENT_IIN
LEFT JOIN (SELECT STUDENT_IIN FROM NEET_YOUTH.NOBD n WHERE DSL_NAME IN('Первое высшее сокращенное образование','Первое высшее образование','Второе высшее образование','Первое высшее образование (5 лет)/(специалитет)','Интернатура') AND ROW_COUNTER = 1 AND IS_FINISHED LIKE 'Завершенное%') AS VYSWEE ON GBL.IIN = VYSWEE.STUDENT_IIN
LEFT JOIN (SELECT STUDENT_IIN FROM NEET_YOUTH.NOBD n WHERE DSL_NAME IN('Первое высшее сокращенное образование','Первое высшее образование','Второе высшее образование','Первое высшее образование (5 лет)/(специалитет)','Интернатура') AND ROW_COUNTER = 1 AND IS_FINISHED LIKE 'Незавершенное%') AS NOT_VYSWEE ON GBL.IIN = NOT_VYSWEE.STUDENT_IIN
LEFT JOIN (SELECT STUDENT_IIN FROM NEET_YOUTH.NOBD n WHERE DSL_NAME IN('Магистратура (Научно-педагогическое направление)','Магистратура (Профильное направление)','Резидентура','Докторантура (Профильное направление)','Докторантура (Научно-педагогическое направление)','Магистратура (Свидетельство к диплому магистра)','Педагогическая переподготовка','Магистр делового администрирования','Доктор делового администрирования') AND ROW_COUNTER = 1 AND IS_FINISHED LIKE 'Завершенное%') AS MASTER_DEGREE ON GBL.IIN = MASTER_DEGREE.STUDENT_IIN
LEFT JOIN (SELECT STUDENT_IIN FROM NEET_YOUTH.NOBD n WHERE DSL_NAME IN('Магистратура (Научно-педагогическое направление)','Магистратура (Профильное направление)','Резидентура','Докторантура (Профильное направление)','Докторантура (Научно-педагогическое направление)','Магистратура (Свидетельство к диплому магистра)','Педагогическая переподготовка','Магистр делового администрирования','Доктор делового администрирования') AND ROW_COUNTER = 1 AND IS_FINISHED LIKE 'Незавершенное%') AS NOT_MASTER_DEGREE ON GBL.IIN = NOT_MASTER_DEGREE.STUDENT_IIN
WHERE GBL.PERSON_AGE BETWEEN 0 AND 34 
order by IIN