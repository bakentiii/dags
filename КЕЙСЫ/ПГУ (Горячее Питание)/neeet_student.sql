SELECT 
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
, IF(ST3.IIN IS NULL OR ST3.IIN = '','Закончил обучение БОЛЕЕ 3 вкл месяцев назад','Закончил обучение МЕНЕЕ 3 вкл месяцев назад') AS IS_YOUNG_STUDENT
, IF(DOC.IIN IS NULL OR DOC.IIN = '','Не зарегистрирован диплом/аттестат об образовании','Зарегистрирован диплом/аттестат об образовании') AS HAS_STUDY_DOC
, IF(ST4.IIN IS NULL OR ST4.IIN = '','Закончил обучение НИЖЕ 4 вкл месяцев назад','Закончил обучение СВЫШЕ 4 вкл месяцев назад') AS IS_OLD_STUDENT
FROM DM_ZEROS.DM_MU_GBL_PERSON_LIVE_KZ_CITZN GBL 
INNER JOIN MON_NOBD.D_AGE_CATEGORIES AGE_CAT ON GBL.PERSON_AGE = AGE_CAT.AGE_ID 
LEFT JOIN CKS_CASES.ZAGS_STATUS ZS ON GBL.IIN = ZS.IIN
LEFT JOIN NEET_YOUTH.IS_UL_IIN UCH ON GBL.IIN = UCH.UL_IIN
LEFT JOIN NEET_YOUTH.IS_IP_IIN IP ON GBL.IIN = IP.IP_IIN
LEFT JOIN NEET_YOUTH.IS_GRST_IIN GRST ON GBL.IIN = GRST.GRST_IIN
LEFT JOIN NEET_YOUTH.IS_OSMS_IIN OSMS ON GBL.IIN = OSMS.OSMS_IIN
LEFT JOIN NEET_YOUTH.IS_ESP_IIN ESP ON GBL.IIN = ESP.ESP_IIN
LEFT JOIN (SELECT DISTINCT RNN FROM NEET_YOUTH.OPV_IIN_YEAR) OPV ON GBL.IIN = OPV.RNN
LEFT JOIN NEET_YOUTH.STUDY_END_3_MONTH ST3 ON GBL.IIN = ST3.IIN
LEFT JOIN NEET_YOUTH.HAS_STUDY_DOC DOC ON GBL.IIN = DOC.IIN
LEFT JOIN NEET_YOUTH.STUDY_END_MORE_4_MONTH ST4 ON GBL.IIN = ST4.IIN
WHERE GBL.PERSON_AGE BETWEEN 0 AND 34