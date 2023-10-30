-- SOC_KARTA.KATO_FOR_FAMILY definition
drop table SOC_KARTA.KATO_FOR_FAMILY;

CREATE TABLE SOC_KARTA.KATO_FOR_FAMILY (
    `IIN` String COMMENT 'ИИН человека',
    `FULL_KATO_NAME` String COMMENT 'Полный КАТО человека',
    `KATO_2` String COMMENT 'КАТО региона',
    `KATO_2_NAME` String COMMENT 'Регионы',
    `SEX_NAME` String COMMENT 'Гендерная принадлежность',
    `PERSON_AGE` Int64 COMMENT 'Возраст',
    `KATO_4` String COMMENT 'КАТО районы',
    `KATO_4_NAME` String COMMENT 'Районы',
    `KATO_6` String COMMENT 'КАТО Населенных пунктов',
    `KATO` String COMMENT 'КАТО 6 знаков'
) ENGINE = MergeTree()
ORDER BY
    IIN;

insert into
    SOC_KARTA.KATO_FOR_FAMILY
SELECT
    IIN,
    groupArray(FULL_KATO_NAME) [1] as FULL_KATO_NAME,
    groupArray(KATO_2) [1] as KATO_2,
    groupArray(KATO_2_NAME) [1] as KATO_2_NAME,
    groupArray(SEX_NAME) [1] as SEX_NAME,
    groupArray(PERSON_AGE) [1] as PERSON_AGE,
    if(
        groupArray(KATO_4) [1] is null,
        groupArray(KATO_JOIN_4) [1],
        groupArray(KATO_4) [1]
    ) as KATO_4,
    if(
        groupArray(KATO_4_NAME) [1] is null,
        groupArray(KATO_JOIN_4_NAME) [1],
        groupArray(KATO_4_NAME) [1]
    ) as KATO_4_NAME,
    groupArray(KATO_6) [1] as KATO_6,
    groupArray(KATO) [1] as KATO
FROM
     DM_MU.GBL_PERSON_LIVE_KZ_CITZN
group by
    IIN;

-- SOC_KARTA.KATO_FOR_FAMILY definition
drop TABLE SOC_KARTA.SK_FAMILY_QUALITY_IIN2;

CREATE TABLE SOC_KARTA.SK_FAMILY_QUALITY_IIN2 (
    `ID_SK_FAM_IIN` Int32 COMMENT 'ID таблицы',
    `IIN` Nullable(String) COMMENT 'ИИН человека',
    `ID_SK_FAMILY_QUALITY` Int64 COMMENT 'ID Семьи',
    `ID_SK_FAMILY_QUALITY2` Nullable(String) COMMENT 'ID Семьи',
    `INCOME_OOP_1_IIN` Float64 COMMENT 'Общий доход человека за квартал деленый на 3',
    `CNT_EMPLOYABLE_IIN2` UInt8 COMMENT 'Трудоспособный или нет',
    `CNT_EMPLOYABLE_IIN3` UInt8 COMMENT 'Работает или нет',
    `INCOME_LPH_IIN` Nullable(Int64) COMMENT 'Доход ЛПХ',
    `CNT_DV_COM_IIN` Int8 COMMENT 'Количество коммерческого авто',
    `CNT_DV_IIN` Int8 COMMENT 'Количество авто',
    `CNT_NEDV_ZU_IIN` Int32 COMMENT 'Количество земли',
    `CNT_IP_IIN` Nullable(Int8) COMMENT 'Участник ИП',
    `CNT_NEDV_COM_IIN` Int16 COMMENT 'Количество коммерческой недвижимости',
    `CNT_NEDV_IIN` Int16 COMMENT 'Количество недвижимости',
    `CNT_EMPLOYABLE_IIN` Int8 COMMENT 'Количество работающих',
    `CNT_GRST_IIN` Int16 COMMENT 'Количество ГРСТ',
    `CNT_UL_IIN` Nullable(Int8) COMMENT 'Участник ТОО',
    `NEED_MED_IIN` Nullable(Int8) COMMENT 'Нуждаемость в мед страховании ИИН',
    `NEED_MED_IIN2` UInt8 COMMENT 'нуждаемость в мед страховании 1 значить нуждается',
    `NEED_EDU_IIN` Nullable(Int8) COMMENT 'пр нуждаемость в образовании',
    `IS_VILLAGE_IIN` Int8 COMMENT 'проживает в селе/городе',
    `SQ_MET_IIN` Float32 COMMENT 'Общая площадь жилой недвижимости в м2 у иин',
    `IS_STUDENT_IIN` Nullable(Int8) COMMENT 'ИИН студент или нет',
    `IS_DET_SAD_IIN` Int8 COMMENT 'ИИН прикреплен в дет сад или нет',
    `IS_SCHOOL_PRIKR_IIN` Int8 COMMENT 'ИИН прикреплен в школу или нет',
    `INVALID1or2Group` UInt8 COMMENT 'Являеться ли инвалидом 1 или 2 группы',
    `OSMS` Int8 COMMENT 'ОСМС - Медицинское страхование',
    `INVALIDsDETSTVAorREBENOKinvalid` UInt8 COMMENT 'Являеться ли инвалидом с детсва или ребенок инвалид',
    `INVALID3GROUP` UInt8 COMMENT 'Являеться ли инвалидом 3 группы',
    `EDU_SCHOOL` UInt8 COMMENT 'есть ли у человека среднее образование',
    `EDU_SRED` UInt8 COMMENT 'есть ли у человека профессиональное и среднеспециальное образование',
    `EDU_HIGHSCHOOL` UInt8 COMMENT 'есть ли у человека высшее образование',
    `ZEM_NEDV_DESC2` UInt8 COMMENT 'есть земельный участок под сельхозземельные участки (более 1 Га)',
    `ZEM_NEDV_DESC1` UInt8 COMMENT 'есть земельный участок под индивидуальное жилищное строительство',
    `raz` Nullable(Int64) COMMENT 'Нужно для внутренного рассчета тут храниться возраст',
    `DETIDO23` UInt8 COMMENT 'входит ли в категорию учащейся молодежи до 23 лет',
    `DETI18` UInt8 COMMENT 'количество детей до 18 не прикрепленных к садику и к школе',
    `DETI5` UInt8 COMMENT 'Нужно для внутренного рассчета является ли ребенком до 6 лет',
    `CNTDETI5` UInt8 COMMENT 'Нужно для внутренного рассчета количество детей до 6 лет',
    `DETI15` UInt8 COMMENT 'Нужно для внутренного рассчета является ли подростком до 16 лет',
    `CNTDETI15` UInt8 COMMENT 'Нужно для внутренного рассчета количество детей до 16 лет',
    `CNT_DETI18` UInt8 COMMENT 'количество детей до 18 лет',
    `HRONIC` UInt8 COMMENT 'есть ли хроническое заболевание ',
    `DUCHET` String COMMENT 'состоит ли в диспансерном учете ',
    `FULL_KATO_NAME` String COMMENT 'Полный КАТО человека',
    `KATO_2` String COMMENT 'КАТО региона',
    `KATO_2_NAME` String COMMENT 'Регионы',
    `KATO_4` String COMMENT 'КАТО районы',
    `KATO_4_NAME` String COMMENT 'Районы',
    `KATO_6` String COMMENT 'КАТО Населенных пунктов',
    `KATO` String COMMENT 'КАТО 6 знаков',
    `SEX_NAME` String COMMENT 'Гендерная принадлежность',
    `PERSON_AGE` Int16 COMMENT 'Возраст',
    `COUNT_CREDIT` Int32 COMMENT 'Количество кредитов у человека ',
    `GKB_AMOUNT` Nullable(Float32) COMMENT 'Суммарная сумма кредитов',
    `GKB_DEBT_VALUE` Nullable(Float32) COMMENT 'Сумма предстоящих платежей остаток',
    `GKB_DEBT_PASTDUE_VALUE` Nullable(Float32) COMMENT 'Просроченная сумма по кредитам',
    `GKB_PAYMENT_DAYS_OVERDUE` Nullable(Int32) COMMENT 'Максимальное количество дней просрочки по всем действующим кредитам',
    `SSD_PATIENTS` Int8 COMMENT 'Есть ли социально значимые заболевания',
    `FAMILY_TYPE` Int8 COMMENT 'Тип семьи',
    `MEMBER_TYPE` Int64 COMMENT 'Категория человека'
) ENGINE = MergeTree()
Order by
    ID_SK_FAM_IIN;

insert into
    SOC_KARTA.SK_FAMILY_QUALITY_IIN2
SELECT
    ID_SK_FAM_IIN,
    sc.IIN as IIN,
    sc.ID_SK_FAMILY_QUALITY as ID_SK_FAMILY_QUALITY,
    (
        case
            when ID_SK_FAMILY_QUALITY == 0 then IIN
            else toString(ID_SK_FAMILY_QUALITY)
        end
    ) as ID_SK_FAMILY_QUALITY2,
    if(SDD is null, 0, SDD) as INCOME_OOP_1_IIN,
    if(
        IS_TRUDOSPOSOB_IIN = 1,
        1,
        0
    ) as CNT_EMPLOYABLE_IIN2,
    if(
       /* TRUD_STAT_32 = 1,
        1,
        0*/
      INCOME_OOP_3_IIN>0, 
      1, 
      0
    ) as CNT_EMPLOYABLE_IIN3,
    if(INCOME_LPH_IIN is null, 0, INCOME_LPH_IIN) as INCOME_LPH_IIN,
    CNT_DV_COM_IIN,
    CNT_DV_IIN,
    CNT_NEDV_ZU_IIN,
    if(CNT_IP_IIN is null, 0, CNT_IP_IIN) as CNT_IP_IIN,
    CNT_NEDV_COM_IIN,
    if(CNT_NEDV_IIN is null, 0, CNT_NEDV_IIN) as CNT_NEDV_IIN,
    CNT_EMPLOYABLE_IIN,
    CNT_GRST_IIN,
    if(CNT_UL_IIN is null, 0, CNT_UL_IIN) as CNT_UL_IIN,
    NEED_MED_IIN,
    if(NEED_MED_IIN == 0, 1, 0) as NEED_MED_IIN2,
    if(
        set2.status = 0
        and CHILD_TYPE = 1,
        1,
        0
    ) as NEED_EDU_IIN,
    IS_VILLAGE_IIN,
    SQ_MET_IIN,
    IS_STUDENT_IIN,
    IS_DET_SAD_IIN,
    IS_SCHOOL_PRIKR_IIN,
    INVALID1or2Group,
    if(OSMS is null, 0, OSMS) as OSMS,
    if(
        INVALIDsDETSTVAorREBENOKinvalid = 1,
        1,
        0
    ) as INVALIDsDETSTVAorREBENOKinvalid,
    INVALID3GROUP,
    if(raz >= 18, EDU_SCHOOL, 0) as EDU_SCHOOL,
    if(raz >= 18, EDU_SRED, 0) as EDU_SRED,
    if(raz >= 19, EDU_HIGHSCHOOL, 0) as EDU_HIGHSCHOOL,
    if(ZEM_NEDV_DESC == 2, 1, 0) as ZEM_NEDV_DESC2,
    if(ZEM_NEDV_DESC == 1, 1, 0) as ZEM_NEDV_DESC1,
    AGE as raz,
    if(
        raz < 23
        and raz != -1,
        if(
            raz < 18,
            1,
            if(
                IS_SCHOOL_PRIKR_IIN == 1,
                1,
                if(IS_STUDENT_IIN == 1, 1, 0)
            )
        ),
        0
    ) as DETIDO23,
    if(
        raz < 18
        and IS_DET_SAD_IIN == 1
        and IS_SCHOOL_PRIKR_IIN == 1,
        1,
        0
    ) as DETI18,
    if(
        raz <= 6
        and raz >= 2
        and IS_DET_SAD_IIN == 0,
        1,
        0
    ) as DETI5,
    if(
        raz <= 6
        and raz >= 2,
        1,
        0
    ) as CNTDETI5,
    if(
        raz < 16
        and raz > 5
        and IS_SCHOOL_PRIKR_IIN == 0,
        1,
        0
    ) as DETI15,
    if(
        raz < 16
        and raz > 5,
        1,
        0
    ) as CNTDETI15,
    if(
        CHILD_TYPE = 1,
        1,
        0
    ) as CNT_DETI18,
    HRONIC,
    DUCHET,
    FULL_KATO_NAME,
    KATO_2,
    KATO_2_NAME,
    KATO_4,
    KATO_4_NAME,
    KATO_6,
    KATO,
    SEX_NAME,
    raz as PERSON_AGE,
    if(GKB_COUNT_CREDIT is null, 0, GKB_COUNT_CREDIT),
    GKB_AMOUNT,
    GKB_DEBT_VALUE,
    GKB_DEBT_PASTDUE_VALUE,
    GKB_PAYMENT_DAYS_OVERDUE,
    SSD_PATIENTS,
    FAMILY_TYPE,
    MEMBER_TYPE
FROM
    SOC_KARTA.SK_FAMILY_QUALITY_IIN sc
    left join (
        SELECT
            IIN,
            MAX(
                if(
                    CATEGORY_ID in [47, 46, 45, 42, 44, 43, 49, 50, 51, 52, 53, 54, 213, 214],
                    1,
                    0
                )
            ) as INVALID1or2Group,
            MAX(
                if(
                    CATEGORY_ID in [48, 55, 62, 63, 64, 65, 66, 216],
                    1,
                    0
                )
            ) as INVALIDsDETSTVAorREBENOKinvalid,
            MAX(
                if(
                    CATEGORY_ID in [56, 57, 58, 59, 60, 61, 215],
                    1,
                    0
                )
            ) as INVALID3GROUP
        FROM
            SOC_KARTA.SR_PERSON_SOURCE
        group by
            IIN
    ) sps on sc.IIN = sps.IIN
    left join (
        SELECT
            IIN,
            max(EDU_SCHOOL) as EDU_SCHOOL,
            max(EDU_SRED) as EDU_SRED,
            max(EDU_HIGHSCHOOL) as EDU_HIGHSCHOOL,
            MIN(STATUS) as status
        FROM
            SOC_KARTA.SK_EDU_TMP_MON
        group by
            IIN
    ) set2 on sc.IIN = set2.IIN
    left join (
        SELECT
            IIN,
            'D-UCHET' as DUCHET,
            MAX(if(ISCHRONICAL is not null, 1, 0)) as HRONIC
        FROM
            SOC_KARTA.ACTIVEDISPANSERPEOPLE
        group by
            IIN
    ) hro on sc.IIN = hro.IIN
    left join (
        SELECT
            IIN,
            FULL_KATO_NAME,
            substring(KATO_2, 1, 2) as KATO_2,
            KATO_2_NAME,
            substring(KATO_4, 1, 4) as KATO_4,
            KATO_4_NAME,
            substring(KATO_6, 1, 6) as KATO_6,
            SEX_NAME,
            substring(KATO, 1, 9) as KATO
        FROM
            SOC_KARTA.KATO_FOR_FAMILY
    ) hro2 on sc.IIN = hro2.IIN
    left join (
        SELECT
            IIN,
            CHILD_TYPE as CHILD_TYPE
        FROM
            SOC_KARTA.SK_FAMILY_MEMBER
        where
            CHILD_TYPE = 1
    ) as set3 on sc.IIN = set3.IIN
       left join 
    (
		select 
		ID_SK_FAMILY_QUALITY,
		FLOOR(SUM_TOTAL/CNT/3) as SDD
		from 
		SOC_KARTA.SK_FAMILY_QUALITY_IIN m
		left join 
		(
			select 
			ID_SK_FAMILY_QUALITY, 
			sum(INCOME_TOTAL_IIN) SUM_TOTAL,
			count(IIN) as CNT
			from SOC_KARTA.SK_FAMILY_QUALITY_IIN
			group by ID_SK_FAMILY_QUALITY 
		) s on s.ID_SK_FAMILY_QUALITY=m.ID_SK_FAMILY_QUALITY 
        /*SELECT
            ID,
            MAX(SDD) as SDD
        FROM
            SOC_KARTA.SK_FAMILY_QUALITY
        group by
            ID*/
    ) --toInt64(sc.ID_SK_FAMILY_QUALITY) = toInt64(set5.ID);
       as set5 on sc.ID_SK_FAMILY_QUALITY = set5.ID_SK_FAMILY_QUALITY;

-- SOC_KARTA.SK_FAMILY_QUALITY_IIN3 definition
drop table SOC_KARTA.SK_FAMILY_QUALITY_IIN3;

CREATE TABLE SOC_KARTA.SK_FAMILY_QUALITY_IIN3 (
    `IIN` Nullable(String) COMMENT 'ИИН человека',
    `kol` UInt64 COMMENT 'Количество людей в семье',
    `ID_SK_FAMILY_QUALITY` Int64 COMMENT 'ID Семьи',
    `ID_SK_FAMILY_QUALITY2` Nullable(String) COMMENT 'ID Семьи',
    `INCOME_OOP_1_IIN` Float64 COMMENT 'Общий доход человека за квартал деленый на 3',
    `CNT_EMPLOYABLE_IIN2` UInt8 COMMENT 'Трудоспособный или нет',
    `CNT_EMPLOYABLE_IIN3` UInt8 COMMENT 'Работает или нет',
    `INCOME_LPH_IIN` Int64 COMMENT 'Доход ЛПХ',
    `CNT_DV_COM_IIN` Int32 COMMENT 'Количество коммерческого авто',
    `CNT_DV_IIN` Int32 COMMENT 'Количество авто',
    `CNT_NEDV_ZU_IIN` Int32 COMMENT 'Количество земли',
    `CNT_IP_IIN` Int16 COMMENT 'Количество земли',
    `CNT_NEDV_COM_IIN` Int16 COMMENT 'Количество коммерческой недвижимости',
    `CNT_NEDV_IIN` Int16 COMMENT 'Количество недвижимости',
    `CNT_EMPLOYABLE_IIN` Int8 COMMENT 'Количество работающих',
    `CNT_GRST_IIN` Int16 COMMENT 'Количество ГРСТ',
    `CNT_UL_IIN` Int16 COMMENT 'Участник ТОО',
    `NEED_MED_IIN` Int8 COMMENT 'Нуждаемость в мед страховании ИИН',
    `NEED_MED_IIN2` UInt8 COMMENT 'нуждаемость в мед страховании 1 значить нуждается',
    `NEED_EDU_IIN` Int8 COMMENT 'пр нуждаемость в образовании',
    `IS_VILLAGE_IIN` Int8 COMMENT 'проживает в селе/городе',
    `SQ_MET_IIN` Float32 COMMENT 'Общая площадь жилой недвижимости в м2 у иин',
    `IS_STUDENT_IIN` Int8 COMMENT 'ИИН студент или нет',
    `IS_DET_SAD_IIN` Int8 COMMENT 'ИИН прикреплен в дет сад или нет',
    `IS_SCHOOL_PRIKR_IIN` Int8 COMMENT 'ИИН прикреплен в школу или нет',
    `INVALID1or2Group` UInt8 COMMENT 'Являеться ли инвалидом 1 или 2 группы',
    `OSMS` Int16 COMMENT 'ОСМС - Медицинское страхование',
    `INVALIDsDETSTVAorREBENOKinvalid` UInt8 COMMENT 'Являеться ли инвалидом с детсва или ребенок инвалид',
    `INVALID3GROUP` UInt8 COMMENT 'Являеться ли инвалидом 3 группы',
    `EDU_SCHOOL` UInt8 COMMENT 'есть ли у человека среднее образование',
    `EDU_SRED` UInt8 COMMENT 'есть ли у человека профессиональное и среднеспециальное образование',
    `EDU_HIGHSCHOOL` UInt8 COMMENT 'есть ли у человека высшее образование',
    `ZEM_NEDV_DESC2` UInt8 COMMENT 'есть земельный участок под сельхозземельные участки (более 1 Га)',
    `ZEM_NEDV_DESC1` UInt8 COMMENT 'есть земельный участок под индивидуальное жилищное строительство',
    `raz` Int64 COMMENT 'Нужно для внутренного рассчета тут храниться возраст',
    `DETIDO23` UInt8 COMMENT 'входит ли в категорию учащейся молодежи до 23 лет',
    `DETI18` UInt8 COMMENT 'количество детей до 18 не прикрепленных к садику и к школе',
    `CNT_DETI18` UInt8 COMMENT 'количество детей до 18 лет',
    `HRONIC` UInt8 COMMENT 'есть ли хроническое заболевание ',
    `DUCHET` String COMMENT 'состоит ли в диспансерном учете ',
    `FULL_KATO_NAME` String COMMENT 'Полный КАТО человека',
    `KATO_2` String COMMENT 'КАТО региона',
    `KATO_2_NAME` String COMMENT 'Регионы',
    `KATO_4` String COMMENT 'КАТО районы',
    `KATO_4_NAME` String COMMENT 'Районы',
    `KATO_6` String COMMENT 'КАТО Населенных пунктов',
    `KATO` String COMMENT 'КАТО 6 знаков',
    `SEX_NAME` String COMMENT 'Гендерная принадлежность',
    `PERSON_AGE` Int64 COMMENT 'Возраст',
    `COUNT_CREDIT` Int32 COMMENT 'Количество кредитов у человека ',
    `GKB_AMOUNT` Nullable(Float32) COMMENT 'Суммарная сумма кредитов',
    `GKB_DEBT_VALUE` Nullable(Float32) COMMENT 'Сумма предстоящих платежей остаток',
    `GKB_DEBT_PASTDUE_VALUE` Nullable(Float32) COMMENT 'Просроченная сумма по кредитам',
    `GKB_PAYMENT_DAYS_OVERDUE` Nullable(Int32) COMMENT 'Максимальное количество дней просрочки по всем действующим кредитам',
    `SSD_PATIENTS` Int8 COMMENT 'Есть ли социально значимые заболевания',
    `FAMILY_TYPE` Int8 COMMENT 'Тип семьи',
    `MEMBER_TYPE` Int64 COMMENT 'Категория человека',
    `DETI5` UInt8 COMMENT 'Нужно для внутренного рассчета является ли ребенком до 6 лет',
    `CNTDETI5` UInt8 COMMENT 'Нужно для внутренного рассчета количество детей до 6 лет',
    `DETI15` UInt8 COMMENT 'Нужно для внутренного рассчета является ли подростком до 16 лет',
    `CNTDETI15` UInt8 COMMENT 'Нужно для внутренного рассчета количество детей до 16 лет'
) ENGINE = MergeTree()
ORDER BY
    kol SETTINGS index_granularity = 8192;

insert into
    SOC_KARTA.SK_FAMILY_QUALITY_IIN3
SELECT
    IIN,
    count(*) as kol,
    arraySort(groupArray(ID_SK_FAMILY_QUALITY)) [kol] as ID_SK_FAMILY_QUALITY,
    (
        case
            when ID_SK_FAMILY_QUALITY == 0 then IIN
            else toString(ID_SK_FAMILY_QUALITY)
        end
    ) as ID_SK_FAMILY_QUALITY2,
    groupArray(INCOME_OOP_1_IIN) [kol] as INCOME_OOP_1_IIN,
    groupArray(CNT_EMPLOYABLE_IIN2) [kol] as CNT_EMPLOYABLE_IIN2,
    groupArray(CNT_EMPLOYABLE_IIN3) [kol] as CNT_EMPLOYABLE_IIN3,
    groupArray(INCOME_LPH_IIN) [kol] INCOME_LPH_IIN,
    groupArray(CNT_DV_COM_IIN) [kol] as CNT_DV_COM_IIN,
    groupArray(CNT_DV_IIN) [kol] as CNT_DV_IIN,
    groupArray(CNT_NEDV_ZU_IIN) [kol] as CNT_NEDV_ZU_IIN,
    groupArray(CNT_IP_IIN) [kol] as CNT_IP_IIN,
    groupArray(CNT_NEDV_COM_IIN) [kol] as CNT_NEDV_COM_IIN,
    groupArray(CNT_NEDV_IIN) [kol] as CNT_NEDV_IIN,
    groupArray(CNT_EMPLOYABLE_IIN) [kol] as CNT_EMPLOYABLE_IIN,
    groupArray(CNT_GRST_IIN) [kol] as CNT_GRST_IIN,
    groupArray(CNT_UL_IIN) [kol] as CNT_UL_IIN,
    groupArray(NEED_MED_IIN) [kol] as NEED_MED_IIN,
    groupArray(NEED_MED_IIN2) [kol] as NEED_MED_IIN2,
    groupArray(NEED_EDU_IIN) [kol] as NEED_EDU_IIN,
    groupArray(IS_VILLAGE_IIN) [kol] as IS_VILLAGE_IIN,
    groupArray(SQ_MET_IIN) [kol] as SQ_MET_IIN,
    groupArray(IS_STUDENT_IIN) [kol] as IS_STUDENT_IIN,
    groupArray(IS_DET_SAD_IIN) [kol] as IS_DET_SAD_IIN,
    groupArray(IS_SCHOOL_PRIKR_IIN) [kol] as IS_SCHOOL_PRIKR_IIN,
    groupArray(INVALID1or2Group) [kol] as INVALID1or2Group,
    groupArray(OSMS) [kol] as OSMS,
    groupArray(INVALIDsDETSTVAorREBENOKinvalid) [kol] as INVALIDsDETSTVAorREBENOKinvalid,
    groupArray(INVALID3GROUP) [kol] as INVALID3GROUP,
    groupArray(EDU_SCHOOL) [kol] as EDU_SCHOOL,
    groupArray(EDU_SRED) [kol] as EDU_SRED,
    groupArray(EDU_HIGHSCHOOL) [kol] as EDU_HIGHSCHOOL,
    groupArray(ZEM_NEDV_DESC2) [kol] as ZEM_NEDV_DESC2,
    groupArray(ZEM_NEDV_DESC1) [kol] as ZEM_NEDV_DESC1,
    groupArray(raz) [kol] as raz,
    groupArray(DETIDO23) [kol] as DETIDO23,
    groupArray(DETI18) [kol] as DETI18,
    groupArray(CNT_DETI18) [kol] as CNT_DETI18,
    groupArray(HRONIC) [kol] as HRONIC,
    groupArray(DUCHET) [kol] as DUCHET,
    groupArray(FULL_KATO_NAME) [kol] as FULL_KATO_NAME,
    groupArray(KATO_2) [kol] as KATO_2,
    groupArray(KATO_2_NAME) [kol] as KATO_2_NAME,
    groupArray(KATO_4) [kol] as KATO_4,
    groupArray(KATO_4_NAME) [kol] as KATO_4_NAME,
    groupArray(KATO_6) [kol] as KATO_6,
    groupArray(KATO) [kol] as KATO,
    groupArray(SEX_NAME) [kol] as SEX_NAME,
    groupArray(PERSON_AGE) [kol] as PERSON_AGE,
    MAX(COUNT_CREDIT) as COUNT_CREDIT,
    MAX(GKB_AMOUNT) as GKB_AMOUNT,
    MAX(GKB_DEBT_VALUE) as GKB_DEBT_VALUE,
    MAX(GKB_DEBT_PASTDUE_VALUE) as GKB_DEBT_PASTDUE_VALUE,
    MAX(GKB_PAYMENT_DAYS_OVERDUE) as GKB_PAYMENT_DAYS_OVERDUE,
    groupArray(SSD_PATIENTS) [kol] as SSD_PATIENTS,
    groupArray(FAMILY_TYPE) [kol] as FAMILY_TYPE,
    groupArray(MEMBER_TYPE) [kol] as MEMBER_TYPE,
    groupArray(DETI5) [kol] as DETI5,
    groupArray(CNTDETI5) [kol] as CNTDETI5,
    groupArray(DETI15) [kol] as DETI15,
    groupArray(CNTDETI15) [kol] as CNTDETI15
FROM
    SOC_KARTA.SK_FAMILY_QUALITY_IIN2
group by
    IIN;

-- SOC_KARTA.SK_FAMILY_VITRINA_KATO
drop TABLE SOC_KARTA.SK_FAMILY_VITRINA_KATO;

CREATE TABLE SOC_KARTA.SK_FAMILY_VITRINA_KATO (
    `ID_SK_FAMILY_QUALITY2` String COMMENT 'ID Семьи',
    `count_iin` UInt64 COMMENT 'Количество людей в семье',
    `one` UInt64 COMMENT 'Женщина по MEMBER TYPE нужно для рассчета главного в семье для опредление като семьи',
    `two` UInt64 COMMENT 'Мужщина по MEMBER TYPE нужно для рассчета главного в семье для опредление като семьи',
    `coun` UInt64 COMMENT 'остальные по MEMBER TYPE нужно для рассчета главного в семье для опредление като семьи',
    `FULL_KATO_NAME` String COMMENT 'Полный КАТО человека',
    `KATO_2` String COMMENT 'КАТО региона',
    `KATO_2_NAME` String COMMENT 'Регионы',
    `KATO_4` String COMMENT 'КАТО районы',
    `KATO_4_NAME` String COMMENT 'Районы',
    `KATO_6` String COMMENT 'КАТО Населенных пунктов',
    `KATO` String COMMENT 'КАТО 6 знаков'
) ENGINE = MergeTree()
Order By
    ID_SK_FAMILY_QUALITY2;

insert into
    SOC_KARTA.SK_FAMILY_VITRINA_KATO
select
    ID_SK_FAMILY_QUALITY2,
    count_iin,
    one,
    two,
    coun,
    FULL_KATO_NAME,
    KATO_2,
    KATO_2_NAME,
    KATO_4,
    KATO_4_NAME,
    KATO_6,
    KATO
from
    (
        select
            ID_SK_FAMILY_QUALITY2,
            count(distinct(IIN)) as count_iin,
            indexOf(groupArray(MEMBER_TYPE), 1) as one,
            indexOf(groupArray(MEMBER_TYPE), 2) as two,
            indexOf(groupArray(MEMBER_TYPE), 3) as three,
            indexOf(groupArray(MEMBER_TYPE), 4) as four,
            indexOf(groupArray(MEMBER_TYPE), 5) as five,
            indexOf(groupArray(MEMBER_TYPE), 13) as thirteen,
            if(
                one > 0,
                one,
                if(
                    two > 0,
                    two,
                    if(
                        three > 0,
                        three,
                        if(four > 0, four, if(five > 0, five, thirteen))
                    )
                )
            ) as coun,
            if(
                groupArray(FULL_KATO_NAME) [coun] == '',
                'Без прописки',
                groupArray(FULL_KATO_NAME) [coun]
            ) as FULL_KATO_NAME,
            if(
                groupArray(KATO_2) [coun] == '',
                'Без прописки',
                groupArray(KATO_2) [coun]
            ) as KATO_2,
            if(
                groupArray(KATO_2_NAME) [coun] == '',
                'Без прописки',
                groupArray(KATO_2_NAME) [coun]
            ) as KATO_2_NAME,
            if(
                groupArray(KATO_4) [coun] == '',
                'Без прописки',
                groupArray(KATO_4) [coun]
            ) as KATO_4,
             multiIf(
                groupArray(KATO_4_NAME) [coun] == '' and KATO_4 =='0' ,     
                'Без прописки', groupArray(KATO_4_NAME) [coun] == '' and KATO_4!='0' ,
                KATO_2_NAME,
                groupArray(KATO_4_NAME) [coun]
            ) as KATO_4_NAME,
            if(
                groupArray(KATO_6) [coun] == '',
                'Без прописки',
                groupArray(KATO_6) [coun]
            ) as KATO_6,
            if(
                groupArray(KATO) [coun] == '',
                'Без прописки',
                groupArray(KATO) [coun]
            ) as KATO
        FROM
            SOC_KARTA.SK_FAMILY_QUALITY_IIN3
        where
            ID_SK_FAMILY_QUALITY != 0
        group by
            ID_SK_FAMILY_QUALITY2
    );

-- SOC_KARTA.SK_FAMILY_VITRINA_BEFORE_MAIN definition
drop TABLE SOC_KARTA.SK_FAMILY_VITRINA_BEFORE_MAIN;

CREATE TABLE SOC_KARTA.SK_FAMILY_VITRINA_BEFORE_MAIN (
    `ID_SK_FAMILY_QUALITY2` Nullable(String) COMMENT 'ID Семьи',
    `SUM_CNT_DETI18` UInt8 COMMENT 'Количество детей до 18 лет не прикрепленные к школе,садику',
    `AVG_INCOME_OOP_1_IIN` Float64 COMMENT 'СДД на одного человека',
    `SUM_CNT_EMPLOYABLE_IIN2` UInt64 COMMENT 'Количество трудоспособных и не работающих члены семьи',
    `SUM_CNT_EMPLOYABLE_IIN3` UInt64 COMMENT 'Количество работающих',
    `SUM_CNT_NEDV_IIN` Int64 COMMENT 'Количество недвижимости',
    `AVG_SQ_MET_IIN` Float64 COMMENT 'Кв м на члена семьи',
    `SUM_CNT_DV_IIN` Int64 COMMENT 'Количество авто в семье',
    `SUM_CNT_DV_COM_IIN` Int64 COMMENT 'Количество коммерческого авто в семье',
    `SUM_CNT_GRST_IIN` Int64 COMMENT 'Количество ГРСТ в семье',
    `SUM_ZEM_NEDV_DESC2` UInt64 COMMENT 'Количество земельных участков под сельхозземельные участки (более 1 Га)',
    `SUM_ZEM_NEDV_DESC1` UInt64 COMMENT 'Количество земельных участков под индивидуальное жилищное строительство',
    `SUM_CNT_NEDV_COM_IIN` Int64 COMMENT 'Количество коммерческой недвижимости',
    `SUM_INCOME_LPH_IIN` Int64 COMMENT 'Сумма ЛПХ семьи',
    `SUM_CNT_IP_IIN` Int64 COMMENT 'Количество ИП в семье',
    `SUM_CNT_UL_IIN` Int64 COMMENT 'Количество ТОО в семье',
    `SUM_DETIDO23` UInt64 COMMENT 'Количество несовершеннолетних детей или учащейся молодежи до 23 лет',
    `SUM_INVALID1or2Group` UInt64 COMMENT 'Количество инвалидов 1 или 2 группы в семье',
    `SUM_INVALIDsDETSTVAorREBENOKinvalid` UInt64 COMMENT 'Количество инвалидов с детсва или ребенок инвалид',
    `SUM_INVALID3GROUP` UInt64 COMMENT 'Количество инвалидов 3 группы в семье',
    `SUM_HRONIC` UInt64 COMMENT 'Количество членов семьи с храническим заболеванием',
    `SUM_DUCHET` UInt64 COMMENT 'Количество членов семьи с Д-учетом',
    `SUM_NEED_EDU_IIN` Int64 COMMENT 'Количество членов семьи нуждающиеся в образований',
    `SUM_NEED_MED_IIN2` UInt64 COMMENT 'Количество членов семьи нуждающиеся в мед страхований',
    `SUM_EDU_HIGHSCHOOL` UInt64 COMMENT 'Количество людей с высшем образованием',
    `SUM_EDU_SRED` UInt64 COMMENT 'Количество людей с среднем образованием',
    `SUM_EDU_SCHOOL` UInt64 COMMENT 'Количество людей с школьном образованием',
    `SUM_OSMS` Int64 COMMENT 'Количество членов семьи с ОСМС',
    `SUM_COUNT_CREDIT` Int64 COMMENT 'Количество кредитов в семье',
    `SUM_IS_VILLAGE_IIN` Int64 COMMENT 'Семья проживает в городе или в селе',
    `cat_family` UInt64 COMMENT 'Количество членов семьи ',
    `GKB_PAYMENT_DAYS_OVERDUE_90_1000` UInt64 COMMENT 'Сумма просрочки',
    `SUM_DETI5` Int64 COMMENT 'Дети нуждающиеся в садике',
    `SUM_CNTDETI5` Int64 COMMENT 'Количество детей до 6 лет',
    `SUM_DETI15` Int64 COMMENT 'Количество детей нуждающиеся в школьном образований',
    `SUM_CNTDETI15` Int64 COMMENT 'Количество детей до 16 лет',
    `OLDSUM` Int64 COMMENT 'Количество членов семьи до 18 лет'
) ENGINE = MergeTree()
Order by
    cat_family;

insert into
    SOC_KARTA.SK_FAMILY_VITRINA_BEFORE_MAIN
SELECT
    ID_SK_FAMILY_QUALITY2,
    SUM(CNT_DETI18) as SUM_CNT_DETI18,
    MAX(INCOME_OOP_1_IIN) as AVG_INCOME_OOP_1_IIN,
    SUM(CNT_EMPLOYABLE_IIN2) as SUM_CNT_EMPLOYABLE_IIN2,
    sum(CNT_EMPLOYABLE_IIN3) as SUM_CNT_EMPLOYABLE_IIN3,
    if(b.CNT_NEDV_IIN is null,0,b.CNT_NEDV_IIN) as SUM_CNT_NEDV_IIN,
    AVG(if(b.M2 is null,0,b.M2)) as AVG_SQ_MET_IIN,
    SUM(CNT_DV_IIN) as SUM_CNT_DV_IIN,
    SUM(CNT_DV_COM_IIN) as SUM_CNT_DV_COM_IIN,
    SUM(CNT_GRST_IIN) as SUM_CNT_GRST_IIN,
    SUM(ZEM_NEDV_DESC2) as SUM_ZEM_NEDV_DESC2,
    SUM(ZEM_NEDV_DESC1) as SUM_ZEM_NEDV_DESC1,
    SUM(CNT_NEDV_COM_IIN) as SUM_CNT_NEDV_COM_IIN,
    SUM(INCOME_LPH_IIN) as SUM_INCOME_LPH_IIN,
    SUM(CNT_IP_IIN) as SUM_CNT_IP_IIN,
    SUM(CNT_UL_IIN) as SUM_CNT_UL_IIN,
    SUM(DETIDO23) as SUM_DETIDO23,
    SUM(INVALID1or2Group) as SUM_INVALID1or2Group,
    SUM(INVALIDsDETSTVAorREBENOKinvalid) as SUM_INVALIDsDETSTVAorREBENOKinvalid,
    SUM(INVALID3GROUP) as SUM_INVALID3GROUP,
    SUM(HRONIC) as SUM_HRONIC,
    SUM(if(DUCHET == 'D-UCHET', 1, 0)) as SUM_DUCHET,
    SUM(NEED_EDU_IIN) as SUM_NEED_EDU_IIN,
    SUM(NEED_MED_IIN2) as SUM_NEED_MED_IIN2,
    if(
        SUM(EDU_HIGHSCHOOL) >= 1,
        1,
        0
    ) as SUM_EDU_HIGHSCHOOL,
    if(
        SUM(EDU_SRED) >= 1,
        1,
        0
    ) as SUM_EDU_SRED,
    if(
        SUM(EDU_SCHOOL) >= 1,
        1,
        0
    ) as SUM_EDU_SCHOOL,
    SUM(OSMS) as SUM_OSMS,
    sum(COUNT_CREDIT) as SUM_COUNT_CREDIT,
    SUM(IS_VILLAGE_IIN) as SUM_IS_VILLAGE_IIN,
    count(distinct(IIN)) as cat_family,
    sum(
        if(
            PERSON_AGE >= 18,
            if(
                GKB_PAYMENT_DAYS_OVERDUE <= 20,
                if(GKB_DEBT_PASTDUE_VALUE < 110, 1, 0),
                0
            ),
            0
        )
    ) as GKB_PAYMENT_DAYS_OVERDUE_90_1000,
    SUM(DETI5) as SUM_DETI5,
    SUM(CNTDETI5) as SUM_CNTDETI5,
    SUM(DETI15) as SUM_DETI15,
    SUM(CNTDETI15) as SUM_CNTDETI15,
    SUM(if(PERSON_AGE >= 18, 1, 0)) as OLDSUM
FROM
    SOC_KARTA.SK_FAMILY_QUALITY_IIN3 a
    left join (select SK_FAMILY_ID as SK_FAMILY_ID, sum(CNT_NEDV_IIN) as CNT_NEDV_IIN , sum(M2) as M2  from SOC_KARTA.NEDV_ZHIL_FAMILY 
group by SK_FAMILY_ID ) b on a.ID_SK_FAMILY_QUALITY2 = toString(b.SK_FAMILY_ID)
where
    ID_SK_FAMILY_QUALITY != 0
group by
    ID_SK_FAMILY_QUALITY2,SUM_CNT_NEDV_IIN;

-----====SOC_KARTA.SK_FAMILY_VITRINA_MAIN
drop table SOC_KARTA.SK_FAMILY_VITRINA_MAIN;

CREATE TABLE SOC_KARTA.SK_FAMILY_VITRINA_MAIN (
    `ID_SK_FAMILY_QUALITY2` String COMMENT 'ID Семьи',
    `numcat` UInt8 COMMENT 'Столбец для внутренного рассчета нумерация групп благополучия',
    `cat_family` String COMMENT 'Количество людей в семье',
    `type1` String COMMENT 'Группа благополучия',
    `type2` String COMMENT 'Подгруппа благополучия',
    `type3` String COMMENT 'Показатели благополучия',
    `ball` Float64 COMMENT 'Баллы за показатель',
    `number1` UInt8 COMMENT 'Столбец для внутренного подсчета'
) ENGINE = MergeTree()
ORDER by
    ID_SK_FAMILY_QUALITY2;

insert into
    SOC_KARTA.SK_FAMILY_VITRINA_MAIN
select
    ID_SK_FAMILY_QUALITY2,
    numcat,
    cat_family,
    if(
        numcat in range(1, 4, 1),
        'Экономические условия (базовая оценка семьи)',
        if(
            numcat in range(4, 11, 1),
            'Дополнительные показатели благополучия семьи',
            if(
                numcat in range(11, 17, 1),
                'Риски',
                if(
                    numcat in range(17, 19, 1),
                    'Косвенные показатели в семье',
                    if(
                        numcat in range(19, 23, 1),
                        'Другие социальные рисковые показатели',
                        if(
                            numcat >= 25
                            or numcat = 27,
                            'Косвенные показатели в семье',
                            'Риски'
                        )
                    )
                )
            )
        )
    ) as type1,
    (
        case
            numcat
            when 1 then 'Уровень среднедушевого дохода (СДД) в семье'
            when 2 then 'Информация о занятости членов семьи'
            when 3 then 'Наличие в собственности семьи недвижимого имущества'
            when 4 then 'Наличие в собственности семьи движимого имущества'
            when 5 then 'Наличие в собственности семьи коммерческого движимого имущества'
            when 6 then 'Наличие сельскохозяйственной техники'
            when 7 then 'Наличие в собственности земельного участка'
            when 8 then 'Наличие в собственности семьи коммерческого недвижимого имущества'
            when 9 then 'Наличие у семьи личного подсобного хозяйства'
            when 10 then 'Учредитель в ЮЛ и ИП'
            when 11 then 'Присутствие в семье четырех и более несовершеннолетних детей и учащейся молодежи до 23 лет'
            when 12 then 'Наличие в семье лица с инвалидностью (1 и 2 группы)'
            when 13 then 'Наличие в семье лица с инвалидностью с детства или ребенка - инвалида'
            when 14 then 'Наличие в семье лица с инвалидностью (3 группы)'
            when 15 then 'Информация о наличии у членов семьи хронических заболеваний'
            when 16 then 'Информация о наличии у членов семьи Д - учета'
            when 17 then 'Информация о прикреплении детей в семье к дошкольным и школьным образовательным учреждениям'
            when 18 then 'Информация о прикреплении членов семьи медицинским учреждениям'
            when 19 then 'Информация об среднем образовании взрослых членов семьи'
            when 20 then 'Информация об профессиональном образовании взрослых членов семьи'
            when 21 then 'Информация об высшем образовании взрослых членов семьи'
            when 22 then 'Информация о участии в ОСМС'
            when 23 then 'Закредитованность семьи'
            when 24 then 'Задолженность семьи по кредитам'
            when 25 then 'Информация о прикреплении детей в семье к дошкольным учреждениям'
            when 26 then 'Информация о прикреплении детей в семье к школьным образовательным учреждениям'
            when 27 then 'Информация об образовании взрослых членов семьи'
            else 'netu'
        end
    ) as type2,
    if(
        numcat == 1,
        (
            case
                when AVG_INCOME_OOP_1_IIN < 1 then 'Семья без доходов'
                when AVG_INCOME_OOP_1_IIN > 0
                and AVG_INCOME_OOP_1_IIN < 25213 then 'Уровень среднедушевого дохода в семье – от 0 до 70% от ПМ'
                when AVG_INCOME_OOP_1_IIN > 25212
                and AVG_INCOME_OOP_1_IIN < 36019 then 'Уровень среднедушевого дохода в семье – от черты бедности до 1 прожиточного минимума'
                when AVG_INCOME_OOP_1_IIN > 36018
                and AVG_INCOME_OOP_1_IIN < 72037 then 'Уровень среднедушевого дохода в семье – от 1 до 2 прожиточных минимумов'
                when AVG_INCOME_OOP_1_IIN > 72036
                and AVG_INCOME_OOP_1_IIN < 108055 then 'Уровень среднедушевого дохода в семье – от 2 до 3 прожиточных минимумов'
                when AVG_INCOME_OOP_1_IIN > 108054
                and AVG_INCOME_OOP_1_IIN < 144073 then 'Уровень среднедушевого дохода в семье – от 3 до 4 прожиточных минимумов'
                when AVG_INCOME_OOP_1_IIN > 144072 then 'Уровень среднедушевого дохода в семье – от 4 и более прожиточных минимумов'
                else 'netu'
            end
        ),
        if(
            numcat == 2,
            (
                case
                    when SUM_CNT_EMPLOYABLE_IIN2 > 0
                    and SUM_CNT_EMPLOYABLE_IIN3 < 1 then 'Семьи с трудоспособными и не работающими членами семьи'
                    when SUM_CNT_EMPLOYABLE_IIN3 == 1 then 'В семье только 1 член семьи работает'
                    when SUM_CNT_EMPLOYABLE_IIN3 > 1 then 'В семье двое и более членов семьи работают'
                    else 'Семьи без работающих членов семьи'
                end
            ),
            if(
                numcat == 3,
                (
                    case
                        when SUM_CNT_NEDV_IIN < 1 then 'Семьи без жилья'
                        when SUM_CNT_NEDV_IIN == 1
                        and AVG_SQ_MET_IIN > 18 then 'В семье только одно жилье, менее 18 кв. метров на 1 члена семьи'
                        when SUM_CNT_NEDV_IIN == 1
                        and AVG_SQ_MET_IIN < 19 then 'В семье только одно жилье, более 18 кв. метров на 1 члена семьи'
                        when SUM_CNT_NEDV_IIN == 2 then 'В семье есть 2 единицы жилья'
                        when SUM_CNT_NEDV_IIN > 2
                        and SUM_CNT_NEDV_IIN < 6 then 'В семье есть от 3 до 5 единиц жилья'
                        when SUM_CNT_NEDV_IIN > 5
                        and SUM_CNT_NEDV_IIN < 11 then 'В семье есть от 6 до 10 единиц жилья'
                        when SUM_CNT_NEDV_IIN > 10 then 'В семье есть более 11 единиц жилья'
                        else 'netu'
                    end
                ),
                if(
                    numcat == 4,
                    (
                        case
                            when SUM_CNT_DV_IIN < 1 then 'В семье нет автотраспорта'
                            when SUM_CNT_DV_IIN == 1 then 'В семье 1 (один) автотраспорт'
                            when SUM_CNT_DV_IIN == 2 then 'В семье 2 (два) автотраспорта'
                            when SUM_CNT_DV_IIN > 2 then 'В семье 3 (три) и более автотранспорта'
                            else 'netu'
                        end
                    ),
                    if(
                        numcat == 5,
                        (
                            case
                                when SUM_CNT_DV_COM_IIN > 0 then 'В семье есть коммерческий траспорт (автобус, грузовой автотранспорт)'
                                else 'В семье нет коммерческого траспорта'
                            end
                        ),
                        if(
                            numcat == 6,
                            (
                                case
                                    when SUM_CNT_GRST_IIN < 1 then 'В семье отсутствует сельхотехника'
                                    when SUM_CNT_GRST_IIN == 1 then 'В семье есть 1 сельхозтехника'
                                    when SUM_CNT_GRST_IIN > 1 then 'В семье есть 2 и более сельхозтехники'
                                    else 'netu'
                                end
                            ),
                            if(
                                numcat == 7,
                                (
                                    case
                                        when SUM_ZEM_NEDV_DESC2 < 1
                                        and SUM_ZEM_NEDV_DESC1 < 1 then 'Семьи без земельного участка'
                                        when SUM_ZEM_NEDV_DESC1 > 0
                                        and SUM_ZEM_NEDV_DESC2 < 1 then 'В семье есть земельный участок под индивидуальное жилищное строительство'
                                        when SUM_ZEM_NEDV_DESC2 > 0
                                        and SUM_ZEM_NEDV_DESC1 < 1 then 'В семье есть земельный участок под сельхозземельные участки (более 1 Га)'
                                        when SUM_ZEM_NEDV_DESC1 > 0
                                        and SUM_ZEM_NEDV_DESC2 > 0 then 'В семье есть земельный участок под индивидуальное жилищное строительство и земельный участок под сельхозземельные участки (более 1 Га)'
                                        else 'netu'
                                    end
                                ),
                                if(
                                    numcat == 8,
                                    (
                                        case
                                            when SUM_CNT_NEDV_COM_IIN < 1 then 'В семье нет коммерческой недвижимости'
                                            when SUM_CNT_NEDV_COM_IIN > 0 then 'В семье есть коммерческая недвижимость'
                                            else 'netu'
                                        end
                                    ),
                                    if(
                                        numcat == 9,
                                        (
                                            case
                                                when SUM_INCOME_LPH_IIN < 1 then 'В семье нет личного подсобного хозяйства'
                                                when SUM_INCOME_LPH_IIN > 0
                                                and SUM_INCOME_LPH_IIN < 500000 then 'В семье есть личное подсобное хозяйство с доходом до 500 тыс. тенге'
                                                when SUM_INCOME_LPH_IIN > 499999
                                                and SUM_INCOME_LPH_IIN < 1000000 then 'В семье есть личное подсобное хозяйство с доходом от 500 до 1 000 тыс. тенге'
                                                when SUM_INCOME_LPH_IIN > 999999
                                                and SUM_INCOME_LPH_IIN < 2000000 then 'В семье есть личное подсобное хозяйство с доходом от 1 000 до 2 000 тыс. тенге'
                                                when SUM_INCOME_LPH_IIN > 1999999 then 'В семье есть личное подсобное хозяйство с доходом более 2 000 тыс. тенге'
                                                else 'netu'
                                            end
                                        ),
                                        if(
                                            numcat == 10,
                                            (
                                                case
                                                    when SUM_CNT_IP_IIN < 1
                                                    and SUM_CNT_UL_IIN < 1 then 'В семье нет учредителей ТОО и ИП'
                                                    when SUM_CNT_IP_IIN > 0
                                                    and SUM_CNT_UL_IIN < 1 then 'В семье есть ИП'
                                                    when SUM_CNT_IP_IIN < 1
                                                    and SUM_CNT_UL_IIN > 0 then 'В семье есть учредители ТОО'
                                                    when SUM_CNT_IP_IIN > 0
                                                    and SUM_CNT_UL_IIN > 0 then 'В семье есть учредители ТОО и ИП'
                                                    else 'netu'
                                                end
                                            ),
                                            if(
                                                numcat == 11,
                                                (
                                                    case
                                                        when SUM_DETIDO23 = 1 then 'В семье есть один несовершеннолетнии ребенок или учащейся молодеж до 23 лет'
                                                        when SUM_DETIDO23 = 2 then 'В семье есть два несовершеннолетних детей и учащейся молодежи до 23 лет'
                                                        when SUM_DETIDO23 = 3 then 'В семье есть три несовершеннолетних детей и учащейся молодежи до 23 лет'
                                                        when SUM_DETIDO23 > 3 then 'В семье есть от 4 - х и более несовершеннолетних детей и учащейся молодежи до 23 лет'
                                                        when SUM_DETIDO23 < 1 then 'В семье нет  несовершеннолетних детей и учащейся молодежи до 23 лет'
                                                        else 'netu'
                                                    end
                                                ),
                                                if(
                                                    numcat == 12,
                                                    (
                                                        case
                                                            when SUM_INVALID1or2Group < 1 then 'В семье отсутсвуют лица с инвалидностью (1 и 2 группы)'
                                                            when SUM_INVALID1or2Group > 0 then 'В семье есть лица с инвалидностью (1 или 2 группы)'
                                                            else 'netu'
                                                        end
                                                    ),
                                                    if(
                                                        numcat == 13,
                                                        (
                                                            case
                                                                when SUM_INVALIDsDETSTVAorREBENOKinvalid < 1 then 'В семье нет лиц с инвалидностью с детства или ребенка-инвалида'
                                                                when SUM_INVALIDsDETSTVAorREBENOKinvalid > 0 then 'В семье есть лица с инвалидностью с детства или ребенка-инвалида'
                                                                else 'netu'
                                                            end
                                                        ),
                                                        if(
                                                            numcat == 14,
                                                            (
                                                                case
                                                                    when SUM_INVALID3GROUP < 1 then 'В семье нет лиц с инвалидностью (3 группы)'
                                                                    when SUM_INVALID3GROUP > 0 then 'В семье есть лица с инвалидностью (3 группы)'
                                                                    else 'netu'
                                                                end
                                                            ),
                                                            if(
                                                                numcat == 15,
                                                                (
                                                                    case
                                                                        when SUM_HRONIC < 1 then 'В семье нет хронических заболеваний'
                                                                        when SUM_HRONIC > 0 then 'В семье есть члены с хроническими заболеваниями'
                                                                        else 'netu'
                                                                    end
                                                                ),
                                                                if(
                                                                    numcat == 16,
                                                                    (
                                                                        case
                                                                            when SUM_DUCHET < 1 then 'В семье нет членов состоящие на Д-учете'
                                                                            when SUM_DUCHET > 0 then 'В семье есть члены состоящие на Д-учете'
                                                                            else 'netu'
                                                                        end
                                                                    ),
                                                                    if(
                                                                        numcat == 17,
                                                                        (
                                                                            case
                                                                                when SUM_NEED_EDU_IIN <> SUM_CNT_DETI18
                                                                                and SUM_NEED_EDU_IIN > 0
                                                                                and SUM_CNT_DETI18 > 0 then 'В семье есть дети до 18 лет не прикрепленные к школе, садику'
                                                                                when SUM_NEED_EDU_IIN = SUM_CNT_DETI18
                                                                                and SUM_NEED_EDU_IIN > 0
                                                                                and SUM_CNT_DETI18 > 0 then 'В семье есть дети до 18 лет прикрепленные к школе садику'
                                                                                else 'В семье нет детей до 18 лет'
                                                                            end
                                                                        ),
                                                                        if(
                                                                            numcat == 18,
                                                                            (
                                                                                case
                                                                                    when SUM_NEED_MED_IIN2 < 1 then 'Семьи с не прикрепленными членами семьи к медицинскому учреждению'
                                                                                    when SUM_NEED_MED_IIN2 > 0 then 'Семьи с прикрепленными членами семьи к медицинскому учреждению'
                                                                                    else 'netu'
                                                                                end
                                                                            ),
                                                                            if(
                                                                                numcat == 19,
                                                                                (
                                                                                    case
                                                                                        when SUM_EDU_SCHOOL < 1 then 'Семьи, в которых у взрослых членов семьи нет среднего образования'
                                                                                        when SUM_EDU_SCHOOL > 0 then 'Семьи, в которых у взрослых членов семьи есть среднее образование'
                                                                                        else 'netu'
                                                                                    end
                                                                                ),
                                                                                if(
                                                                                    numcat == 20,
                                                                                    (
                                                                                        case
                                                                                            when SUM_EDU_SRED < 1 then 'Семьи, в которых у взрослых членов семьи нет профессионального и среднеспециального образование'
                                                                                            when SUM_EDU_SRED > 0 then 'Семьи, в которых у взрослых членов семьи есть профессиональное и среднеспециальное образование'
                                                                                            else 'netu'
                                                                                        end
                                                                                    ),
                                                                                    if(
                                                                                        numcat == 21,
                                                                                        (
                                                                                            case
                                                                                                when SUM_EDU_HIGHSCHOOL < 1 then 'Семьи, в которых у взрослых членов семьи нет высшего образования'
                                                                                                when SUM_EDU_HIGHSCHOOL > 0 then 'Семьи, в которых у взрослых членов семьи есть высшее образование'
                                                                                                else 'netu'
                                                                                            end
                                                                                        ),
                                                                                        if(
                                                                                            numcat == 22,
                                                                                            (
                                                                                                case
                                                                                                    when SUM_OSMS < 1 then 'Семьи в которых нет участников ОСМС'
                                                                                                    when SUM_OSMS > 0 then 'Семьи в которых есть участники ОСМС'
                                                                                                    else 'netu'
                                                                                                end
                                                                                            ),
                                                                                            if(
                                                                                                numcat == 23,
                                                                                                (
                                                                                                    case
                                                                                                        when SUM_COUNT_CREDIT < 1 then 'Семья без кредитов'
                                                                                                        when SUM_COUNT_CREDIT > 0 then 'Семья есть кредиты'
                                                                                                        else 'netu'
                                                                                                    end
                                                                                                ),
                                                                                                if(
                                                                                                    numcat == 24,
                                                                                                    (
                                                                                                        case
                                                                                                            when GKB_PAYMENT_DAYS_OVERDUE_90_1000 > 1 then 'Все совершеннолетние члены семьи имеет просроченную задолженность по кредитам больше 90 дней и свыше 1000 тенге'
                                                                                                            when GKB_PAYMENT_DAYS_OVERDUE_90_1000 = 1 then 'Один совершеннолетнии член семьи имеет просроченную задолженность по кредитам больше 90 дней и свыше 1000 тенге'
                                                                                                            when GKB_PAYMENT_DAYS_OVERDUE_90_1000 < 1 then 'Семья не имеет задолженности по кредитам больше 90 дней и свыше 1000 тенге'
                                                                                                            else 'netu'
                                                                                                        end
                                                                                                    ),
                                                                                                    if(
                                                                                                        numcat == 25,
                                                                                                        (
                                                                                                            case
                                                                                                                when SUM_DETI5 > 0
                                                                                                                and SUM_CNTDETI5 > 0 then 'В семье есть дети от 2 до 6 лет включительно не прикрепленые к садику'
                                                                                                                when SUM_DETI5 < 1
                                                                                                                and SUM_CNTDETI5 > 0 then 'В семье есть дети от 2 до 6 лет включительно прикрепленые к садику'
                                                                                                                when SUM_CNTDETI5 < 1 then 'В семье нет детей от 2 до 6 лет включительно'
                                                                                                                else 'netu'
                                                                                                            end
                                                                                                        ),
                                                                                                        if(
                                                                                                            numcat == 26,
                                                                                                            (
                                                                                                                case
                                                                                                                    when SUM_DETI15 > 0
                                                                                                                    and SUM_CNTDETI15 > 0 then 'В семье есть дети от 6 до 15 лет включительно не прикрепленые к школе'
                                                                                                                    when SUM_DETI15 < 1
                                                                                                                    and SUM_CNTDETI15 > 0 then 'В семье есть дети от 6 до 15 лет включительно прикрепленые к школе'
                                                                                                                    when SUM_CNTDETI15 < 1 then 'В семье нет детей от 6 до 15 лет включительно'
                                                                                                                    else 'netu'
                                                                                                                end
                                                                                                            ),
                                                                                                            if(
                                                                                                                numcat == 27,
                                                                                                                (
                                                                                                                    case
                                                                                                                        when SUM_EDU_HIGHSCHOOL > 0 then 'В семье есть высшее образование'
                                                                                                                        when SUM_EDU_SRED > 0 then 'В семье есть профессиональное или среднеспециальное образование'
                                                                                                                        when SUM_EDU_SCHOOL > 0 then 'В семье есть среднее образование'
                                                                                                                        else 'В семьи нет образования'
                                                                                                                    end
                                                                                                                ),
                                                                                                                'netu'
                                                                                                            )
                                                                                                        )
                                                                                                    )
                                                                                                )
                                                                                            )
                                                                                        )
                                                                                    )
                                                                                )
                                                                            )
                                                                        )
                                                                    )
                                                                )
                                                            )
                                                        )
                                                    )
                                                )
                                            )
                                        )
                                    )
                                )
                            )
                        )
                    )
                )
            )
        )
    ) as type3,
    if(
        numcat == 1,
        (
            case
                when type3 == 'Семья без доходов' then 0.0
                when type3 == 'Уровень среднедушевого дохода в семье – от 0 до 70% от ПМ' then 1.0
                when type3 == 'Уровень среднедушевого дохода в семье – от черты бедности до 1 прожиточного минимума' then 2.0
                when type3 == 'Уровень среднедушевого дохода в семье – от 1 до 2 прожиточных минимумов' then 5.0
                when type3 == 'Уровень среднедушевого дохода в семье – от 2 до 3 прожиточных минимумов' then 6.0
                when type3 == 'Уровень среднедушевого дохода в семье – от 3 до 4 прожиточных минимумов' then 14.0
                when type3 == 'Уровень среднедушевого дохода в семье – от 4 и более прожиточных минимумов' then 17.0
                else 0.0
            end
        ),
        if(
            numcat == 2,
            (
                case
                    when type3 == 'Семьи с трудоспособными и не работающими членами семьи' then 0.0
                    when type3 == 'В семье только 1 член семьи работает' then 2.0
                    when type3 == 'В семье двое и более членов семьи работают' then 4.0
                    else 0.0
                end
            ),
            if(
                numcat == 3,
                (
                    case
                        when type3 == 'Семьи без жилья' then 0.0
                        when type3 == 'В семье только одно жилье, менее 18 кв. метров на 1 члена семьи' then if(SUM_IS_VILLAGE_IIN > 0, 1.0, 2.0)
                        when type3 == 'В семье только одно жилье, более 18 кв. метров на 1 члена семьи' then if(SUM_IS_VILLAGE_IIN > 0, 3.0, 4.0)
                        when type3 == 'В семье есть 2 единицы жилья' then 5.0
                        when type3 == 'В семье есть от 3 до 5 единиц жилья' then 7.0
                        when type3 == 'В семье есть от 6 до 10 единиц жилья' then 9.0
                        when type3 == 'В семье есть более 11 единиц жилья' then 12.0
                        else 0.0
                    end
                ),
                if(
                    numcat == 4,
                    (
                        case
                            when type3 == 'В семье нет автотраспорта' then 0.0
                            when type3 == 'В семье 1 (один) автотраспорт' then 0.5
                            when type3 == 'В семье 2 (два) автотраспорта' then 5.0
                            when type3 == 'В семье 3 (три) и более автотранспорта' then 9.0
                            else 0.0
                        end
                    ),
                    if(
                        numcat == 5,
                        (
                            case
                                when type3 == 'В семье есть коммерческий траспорт (автобус, грузовой автотранспорт)' then if(SUM_CNT_DV_IIN > 0, 0.0, 9.0)
                                else 0.0
                            end
                        ),
                        if(
                            numcat == 6,
                            (
                                case
                                    when type3 == 'В семье отсутствует сельхотехника' then 0.0
                                    when type3 == 'В семье есть 1 сельхозтехника' then 1.0
                                    when type3 == 'В семье есть 2 и более сельхозтехники' then 9.0
                                    else 0.0
                                end
                            ),
                            if(
                                numcat == 7,
                                (
                                    case
                                        when type3 == 'Семьи без земельного участка' then 0.0
                                        when type3 == 'В семье есть земельный участок под индивидуальное жилищное строительство' then 1.0
                                        when type3 == 'В семье есть земельный участок под сельхозземельные участки (более 1 Га)' then 2.0
                                        when type3 == 'В семье есть земельный участок под индивидуальное жилищное строительство и земельный участок под сельхозземельные участки (более 1 Га)' then 1.0
                                        else 0.0
                                    end
                                ),
                                if(
                                    numcat == 8,
                                    (
                                        case
                                            when type3 == 'В семье нет коммерческой недвижимости' then 0.0
                                            when type3 == 'В семье есть коммерческая недвижимость' then 1.0
                                            else 0.0
                                        end
                                    ),
                                    if(
                                        numcat == 9,
                                        (
                                            case
                                                when type3 == 'В семье нет личного подсобного хозяйства' then 0.0
                                                when type3 == 'В семье есть личное подсобное хозяйство с доходом до 500 тыс. тенге' then 1.0
                                                when type3 == 'В семье есть личное подсобное хозяйство с доходом от 500 до 1 000 тыс. тенге' then 2.0
                                                when type3 == 'В семье есть личное подсобное хозяйство с доходом от 1 000 до 2 000 тыс. тенге' then 6.0
                                                when type3 == 'В семье есть личное подсобное хозяйство с доходом более 2 000 тыс. тенге' then 8.0
                                                else 0.0
                                            end
                                        ),
                                        if(
                                            numcat == 10,
                                            (
                                                case
                                                    when type3 == 'В семье нет учредителей ТОО и ИП' then 0.0
                                                    when type3 == 'В семье есть ИП' then 1.0
                                                    when type3 == 'В семье есть учредители ТОО' then 3.0
                                                    when type3 == 'В семье есть учредители ТОО и ИП' then 3.0
                                                    else 0.0
                                                end
                                            ),
                                            if(
                                                numcat == 11,
                                                (
                                                    case
                                                        when type3 == 'В семье нет  несовершеннолетних детей и учащейся молодежи до 23 лет' then 0.0
                                                        when type3 == 'В семье есть от 4-х и более несовершеннолетних детей и учащейся молодежи до 23 лет' then -1.0
                                                        when type3 == 'В семье есть один несовершеннолетнии ребенок или учащейся молодеж до 23 лет' then 0.0
                                                        when type3 == 'В семье есть два несовершеннолетних детей и учащейся молодежи до 23 лет' then 0.0
                                                        when type3 == 'В семье есть три несовершеннолетних детей и учащейся молодежи до 23 лет' then 0.0
                                                        else 0.0
                                                    end
                                                ),
                                                if(
                                                    numcat == 12,
                                                    (
                                                        case
                                                            when type3 == 'В семье отсутсвуют лица с инвалидностью (1 и 2 группы)' then 0.0
                                                            when type3 == 'В семье есть лица с инвалидностью (1 или 2 группы)' then -1.0
                                                            else 0.0
                                                        end
                                                    ),
                                                    if(
                                                        numcat == 13,
                                                        (
                                                            case
                                                                when type3 == 'В семье нет лиц с инвалидностью с детства или ребенка-инвалида' then 0.0
                                                                when type3 == 'В семье есть лица с инвалидностью с детства или ребенка-инвалида' then -1.0
                                                                else 0.0
                                                            end
                                                        ),
                                                        if(
                                                            numcat == 14,
                                                            (
                                                                case
                                                                    when type3 == 'В семье нет лиц с инвалидностью (3 группы)' then 0.0
                                                                    when type3 == 'В семье есть лица с инвалидностью (3 группы)' then -0.5
                                                                    else 0.0
                                                                end
                                                            ),
                                                            if(
                                                                numcat == 15,
                                                                (
                                                                    case
                                                                        when type3 == 'В семье нет хронических заболеваний' then 0.0
                                                                        when type3 == 'В семье есть члены с хроническими заболеваниями' then -0.5
                                                                        else 0.0
                                                                    end
                                                                ),
                                                                if(
                                                                    numcat == 16,
                                                                    (
                                                                        case
                                                                            when type3 == 'В семье нет членов состоящие на Д-учете' then 0.0
                                                                            when type3 == 'В семье есть члены состоящие на Д-учете' then -0.5
                                                                            else 0.0
                                                                        end
                                                                    ),
                                                                    if(
                                                                        numcat == 17,
                                                                        (
                                                                            case
                                                                                when type3 == 'В семье есть дети до 18 лет не прикрепленные к школе, садику' then -1.0
                                                                                when type3 == 'В семье есть дети до 18 лет прикрепленные к школе садику' then 0.0
                                                                                when type3 == 'В семье нет детей до 18 лет' then 0.0
                                                                                else 0.0
                                                                            end
                                                                        ),
                                                                        if(
                                                                            numcat == 18,
                                                                            (
                                                                                case
                                                                                    when type3 == 'Семьи с не прикрепленными членами семьи к медицинскому учреждению' then -1.0
                                                                                    when type3 == 'Семьи с прикрепленными членами семьи к медицинскому учреждению' then 1.0
                                                                                    else 0.0
                                                                                end
                                                                            ),
                                                                            if(
                                                                                numcat == 19,
                                                                                (
                                                                                    case
                                                                                        when type3 == 'Семьи, в которых у взрослых членов семьи нет среднего образования' then 0.0
                                                                                        when type3 == 'Семьи, в которых у взрослых членов семьи есть среднее образование' then 0.5
                                                                                        else 0.0
                                                                                    end
                                                                                ),
                                                                                if(
                                                                                    numcat == 20,
                                                                                    (
                                                                                        case
                                                                                            when type3 == 'Семьи, в которых у взрослых членов семьи нет профессионального и среднеспециального образование' then 0.0
                                                                                            when type3 == 'Семьи, в которых у взрослых членов семьи есть профессиональное и среднеспециальное образование' then 1.0
                                                                                            else 0.0
                                                                                        end
                                                                                    ),
                                                                                    if(
                                                                                        numcat == 21,
                                                                                        (
                                                                                            case
                                                                                                when type3 == 'Семьи, в которых у взрослых членов семьи нет высшего образования' then 0.0
                                                                                                when type3 == 'Семьи, в которых у взрослых членов семьи есть высшее образование' then 1.5
                                                                                                else 0.0
                                                                                            end
                                                                                        ),
                                                                                        if(
                                                                                            numcat == 22,
                                                                                            (
                                                                                                case
                                                                                                    when type3 == 'Семьи в которых нет участников ОСМС' then -0.5
                                                                                                    when type3 == 'Семьи в которых есть участники ОСМС' then 0.5
                                                                                                    else 0.0
                                                                                                end
                                                                                            ),
                                                                                            if(
                                                                                                numcat == 23,
                                                                                                (
                                                                                                    case
                                                                                                        when type3 == 'Семья без кредитов' then 0.0
                                                                                                        when type3 == 'Семья есть кредиты' then -1.0
                                                                                                        else 0.0
                                                                                                    end
                                                                                                ),
                                                                                                if(
                                                                                                    numcat == 24,
                                                                                                    (
                                                                                                        case
                                                                                                            when type3 == 'Все совершеннолетние члены семьи имеет просроченную задолженность по кредитам больше 90 дней и свыше 1000 тенге' then 0.0
                                                                                                            when type3 == 'Один совершеннолетнии член семьи имеет просроченную задолженность по кредитам больше 90 дней и свыше 1000 тенге' then 0.0
                                                                                                            when type3 == 'Семья не имеет задолженности по кредитам больше 90 дней и свыше 1000 тенге' then 0.0
                                                                                                            else 0.0
                                                                                                        end
                                                                                                    ),
                                                                                                    if(
                                                                                                        numcat == 25,
                                                                                                        (
                                                                                                            case
                                                                                                                when type3 == 'В семье есть дети от 1 до 5 лет включительно не прикрепленые к садику' then 0.0
                                                                                                                when type3 == 'В семье есть дети от 1 до 5 лет включительно прикрепленые к садику' then 0.0
                                                                                                                when type3 == 'В семье нет детей от 1 до 5 лет включительно' then 0.0
                                                                                                                else 0.0
                                                                                                            end
                                                                                                        ),
                                                                                                        if(
                                                                                                            numcat == 26,
                                                                                                            (
                                                                                                                case
                                                                                                                    when type3 == 'В семье есть дети от 6 до 15 лет включительно не прикрепленые к школе' then 0.0
                                                                                                                    when type3 == 'В семье есть дети от 6 до 15 лет включительно прикрепленые к школе' then 0.0
                                                                                                                    when type3 == 'В семье нет детей от 6 до 15 лет включительно' then 0.0
                                                                                                                    else 0.0
                                                                                                                end
                                                                                                            ),
                                                                                                            if(
                                                                                                                numcat == 27,
                                                                                                                (
                                                                                                                    case
                                                                                                                        when type3 == 'В семье есть высшее образование' then 0.0
                                                                                                                        when type3 == 'В семье есть профессиональное или среднеспециальное образование' then 0.0
                                                                                                                        when type3 == 'В семье есть среднее образование' then 0.0
                                                                                                                        else 0.0
                                                                                                                    end
                                                                                                                ),
                                                                                                                0.0
                                                                                                            )
                                                                                                        )
                                                                                                    )
                                                                                                )
                                                                                            )
                                                                                        )
                                                                                    )
                                                                                )
                                                                            )
                                                                        )
                                                                    )
                                                                )
                                                            )
                                                        )
                                                    )
                                                )
                                            )
                                        )
                                    )
                                )
                            )
                        )
                    )
                )
            )
        )
    ) as ball,
    if(
        numcat == 1,
        (
            case
                when type3 == 'Семья без доходов' then 1
                when type3 == 'Уровень среднедушевого дохода в семье – от 0 до 70% от ПМ' then 2
                when type3 == 'Уровень среднедушевого дохода в семье – от черты бедности до 1 прожиточного минимума' then 3
                when type3 == 'Уровень среднедушевого дохода в семье – от 1 до 2 прожиточных минимумов' then 4
                when type3 == 'Уровень среднедушевого дохода в семье – от 2 до 3 прожиточных минимумов' then 5
                when type3 == 'Уровень среднедушевого дохода в семье – от 3 до 4 прожиточных минимумов' then 6
                when type3 == 'Уровень среднедушевого дохода в семье – от 4 и более прожиточных минимумов' then 7
                else 0
            end
        ),
        if(
            numcat == 2,
            (
                case
                    when type3 == 'Семьи с трудоспособными и не работающими членами семьи' then 8
                    when type3 == 'В семье только 1 член семьи работает' then 9
                    when type3 == 'В семье двое и более членов семьи работают' then 10
                    else 0
                end
            ),
            if(
                numcat == 3,
                (
                    case
                        when type3 == 'Семьи без жилья' then 11
                        when type3 == 'В семье только одно жилье, менее 18 кв. метров на 1 члена семьи' then 12
                        when type3 == 'В семье только одно жилье, более 18 кв. метров на 1 члена семьи' then 13
                        when type3 == 'В семье есть 2 единицы жилья' then 14
                        when type3 == 'В семье есть от 3 до 5 единиц жилья' then 15
                        when type3 == 'В семье есть от 6 до 10 единиц жилья' then 16
                        when type3 == 'В семье есть более 11 единиц жилья' then 17
                        else 0
                    end
                ),
                if(
                    numcat == 4,
                    (
                        case
                            when type3 == 'В семье нет автотраспорта' then 18
                            when type3 == 'В семье 1 (один) автотраспорт' then 19
                            when type3 == 'В семье 2 (два) автотраспорта' then 20
                            when type3 == 'В семье 3 (три) и более автотранспорта' then 21
                            else 0
                        end
                    ),
                    if(
                        numcat == 5,
                        (
                            case
                                when type3 == 'В семье есть коммерческий траспорт (автобус, грузовой автотранспорт)' then 22
                                else 23
                            end
                        ),
                        if(
                            numcat == 6,
                            (
                                case
                                    when type3 == 'В семье отсутствует сельхотехника' then 24
                                    when type3 == 'В семье есть 1 сельхозтехника' then 25
                                    when type3 == 'В семье есть 2 и более сельхозтехники' then 26
                                    else 0
                                end
                            ),
                            if(
                                numcat == 7,
                                (
                                    case
                                        when type3 == 'Семьи без земельного участка' then 27
                                        when type3 == 'В семье есть земельный участок под индивидуальное жилищное строительство' then 28
                                        when type3 == 'В семье есть земельный участок под сельхозземельные участки (более 1 Га)' then 29
                                        when type3 == 'В семье есть земельный участок под индивидуальное жилищное строительство и земельный участок под сельхозземельные участки (более 1 Га)' then 30
                                        else 0
                                    end
                                ),
                                if(
                                    numcat == 8,
                                    (
                                        case
                                            when type3 == 'В семье нет коммерческой недвижимости' then 31
                                            when type3 == 'В семье есть коммерческая недвижимость' then 32
                                            else 0
                                        end
                                    ),
                                    if(
                                        numcat == 9,
                                        (
                                            case
                                                when type3 == 'В семье нет личного подсобного хозяйства' then 33
                                                when type3 == 'В семье есть личное подсобное хозяйство с доходом до 500 тыс. тенге' then 34
                                                when type3 == 'В семье есть личное подсобное хозяйство с доходом от 500 до 1 000 тыс. тенге' then 35
                                                when type3 == 'В семье есть личное подсобное хозяйство с доходом от 1 000 до 2 000 тыс. тенге' then 36
                                                when type3 == 'В семье есть личное подсобное хозяйство с доходом более 2 000 тыс. тенге' then 37
                                                else 0
                                            end
                                        ),
                                        if(
                                            numcat == 10,
                                            (
                                                case
                                                    when type3 == 'В семье нет учредителей ТОО и ИП' then 38
                                                    when type3 == 'В семье есть ИП' then 39
                                                    when type3 == 'В семье есть учредители ТОО' then 40
                                                    when type3 == 'В семье есть учредители ТОО и ИП' then 41
                                                    else 0
                                                end
                                            ),
                                            if(
                                                numcat == 11,
                                                (
                                                    case
                                                        when type3 == 'В семье нет  несовершеннолетних детей и учащейся молодежи до 23 лет' then 42
                                                        when type3 == 'В семье есть от 4-х и более несовершеннолетних детей и учащейся молодежи до 23 лет' then 43
                                                        when type3 == 'В семье есть один несовершеннолетнии ребенок или учащейся молодеж до 23 лет' then 44
                                                        when type3 == 'В семье есть два несовершеннолетних детей и учащейся молодежи до 23 лет' then 45
                                                        when type3 == 'В семье есть три несовершеннолетних детей и учащейся молодежи до 23 лет' then 46
                                                        else 0
                                                    end
                                                ),
                                                if(
                                                    numcat == 12,
                                                    (
                                                        case
                                                            when type3 == 'В семье отсутсвуют лица с инвалидностью (1 и 2 группы)' then 47
                                                            when type3 == 'В семье есть лица с инвалидностью (1 или 2 группы)' then 48
                                                            else 0
                                                        end
                                                    ),
                                                    if(
                                                        numcat == 13,
                                                        (
                                                            case
                                                                when type3 == 'В семье нет лиц с инвалидностью с детства или ребенка-инвалида' then 49
                                                                when type3 == 'В семье есть лица с инвалидностью с детства или ребенка-инвалида' then 50
                                                                else 0
                                                            end
                                                        ),
                                                        if(
                                                            numcat == 14,
                                                            (
                                                                case
                                                                    when type3 == 'В семье нет лиц с инвалидностью (3 группы)' then 51
                                                                    when type3 == 'В семье есть лица с инвалидностью (3 группы)' then 52
                                                                    else 0
                                                                end
                                                            ),
                                                            if(
                                                                numcat == 15,
                                                                (
                                                                    case
                                                                        when type3 == 'В семье нет хронических заболеваний' then 53
                                                                        when type3 == 'В семье есть члены с хроническими заболеваниями' then 54
                                                                        else 0
                                                                    end
                                                                ),
                                                                if(
                                                                    numcat == 16,
                                                                    (
                                                                        case
                                                                            when type3 == 'В семье нет членов состоящие на Д-учете' then 55
                                                                            when type3 == 'В семье есть члены состоящие на Д-учете' then 56
                                                                            else 0
                                                                        end
                                                                    ),
                                                                    if(
                                                                        numcat == 17,
                                                                        (
                                                                            case
                                                                                when type3 == 'В семье есть дети до 18 лет не прикрепленные к школе, садику' then 57
                                                                                when type3 == 'В семье есть дети до 18 лет прикрепленные к школе садику' then 58
                                                                                when type3 == 'В семье нет детей до 18 лет' then 56
                                                                                else 0
                                                                            end
                                                                        ),
                                                                        if(
                                                                            numcat == 18,
                                                                            (
                                                                                case
                                                                                    when type3 == 'Семьи с не прикрепленными членами семьи к медицинскому учреждению' then 59
                                                                                    when type3 == 'Семьи с прикрепленными членами семьи к медицинскому учреждению' then 60
                                                                                    else 0
                                                                                end
                                                                            ),
                                                                            if(
                                                                                numcat == 19,
                                                                                (
                                                                                    case
                                                                                        when type3 == 'Семьи, в которых у взрослых членов семьи нет среднего образования' then 61
                                                                                        when type3 == 'Семьи, в которых у взрослых членов семьи есть среднее образование' then 62
                                                                                        else 0
                                                                                    end
                                                                                ),
                                                                                if(
                                                                                    numcat == 20,
                                                                                    (
                                                                                        case
                                                                                            when type3 == 'Семьи, в которых у взрослых членов семьи нет профессионального и среднеспециального образование' then 63
                                                                                            when type3 == 'Семьи, в которых у взрослых членов семьи есть профессиональное и среднеспециальное образование' then 64
                                                                                            else 0
                                                                                        end
                                                                                    ),
                                                                                    if(
                                                                                        numcat == 21,
                                                                                        (
                                                                                            case
                                                                                                when type3 == 'Семьи, в которых у взрослых членов семьи нет высшего образования' then 65
                                                                                                when type3 == 'Семьи, в которых у взрослых членов семьи есть высшее образование' then 66
                                                                                                else 0
                                                                                            end
                                                                                        ),
                                                                                        if(
                                                                                            numcat == 22,
                                                                                            (
                                                                                                case
                                                                                                    when type3 == 'Семьи в которых нет участников ОСМС' then 67
                                                                                                    when type3 == 'Семьи в которых есть участники ОСМС' then 68
                                                                                                    else 0
                                                                                                end
                                                                                            ),
                                                                                            if(
                                                                                                numcat == 23,
                                                                                                (
                                                                                                    case
                                                                                                        when type3 == 'Семья без кредитов' then 69
                                                                                                        when type3 == 'Семья есть кредиты' then 70
                                                                                                        else 0
                                                                                                    end
                                                                                                ),
                                                                                                if(
                                                                                                    numcat == 24,
                                                                                                    (
                                                                                                        case
                                                                                                            when type3 == 'Все совершеннолетние члены семьи имеет просроченную задолженность по кредитам больше 90 дней и свыше 1000 тенге' then 71
                                                                                                            when type3 == 'Один совершеннолетнии член семьи имеет просроченную задолженность по кредитам больше 90 дней и свыше 1000 тенге' then 72
                                                                                                            when type3 == 'Семья не имеет задолженности по кредитам больше 90 дней и свыше 1000 тенге' then 73
                                                                                                            else 0
                                                                                                        end
                                                                                                    ),
                                                                                                    if(
                                                                                                        numcat == 25,
                                                                                                        (
                                                                                                            case
                                                                                                                when type3 == 'В семье есть дети от 1 до 5 лет включительно не прикрепленые к садику' then 74
                                                                                                                when type3 == 'В семье есть дети от 1 до 5 лет включительно прикрепленые к садику' then 75
                                                                                                                when type3 == 'В семье нет детей от 1 до 5 лет включительно' then 76
                                                                                                                else 0
                                                                                                            end
                                                                                                        ),
                                                                                                        if(
                                                                                                            numcat == 26,
                                                                                                            (
                                                                                                                case
                                                                                                                    when type3 == 'В семье есть дети от 6 до 15 лет включительно не прикрепленые к школе' then 77
                                                                                                                    when type3 == 'В семье есть дети от 6 до 15 лет включительно прикрепленые к школе' then 78
                                                                                                                    when type3 == 'В семье нет детей от 6 до 15 лет включительно' then 79
                                                                                                                    else 0
                                                                                                                end
                                                                                                            ),
                                                                                                            if(
                                                                                                                numcat == 27,
                                                                                                                (
                                                                                                                    case
                                                                                                                        when type3 == 'В семье есть высшее образование' then 80
                                                                                                                        when type3 == 'В семье есть профессиональное или среднеспециальное образование' then 81
                                                                                                                        when type3 == 'В семье есть среднее образование' then 82
                                                                                                                        else 0
                                                                                                                    end
                                                                                                                ),
                                                                                                                0
                                                                                                            )
                                                                                                        )
                                                                                                    )
                                                                                                )
                                                                                            )
                                                                                        )
                                                                                    )
                                                                                )
                                                                            )
                                                                        )
                                                                    )
                                                                )
                                                            )
                                                        )
                                                    )
                                                )
                                            )
                                        )
                                    )
                                )
                            )
                        )
                    )
                )
            )
        )
    ) as number1
from
    SOC_KARTA.SK_FAMILY_VITRINA_BEFORE_MAIN as set1 array
    JOIN range(1, 28, 1) AS numcat;

---====== SOC_KARTA.SEGMENTATION_ALL
drop table SOC_KARTA.SEGMENTATION_ALL;

CREATE TABLE SOC_KARTA.SEGMENTATION_ALL (
    `ID_SK_FAMILY_QUALITY2` String COMMENT 'ID Семьи',
    `numcat` UInt8 COMMENT 'Столбец для внутренного рассчета нумерация групп благополучия',
    `cat_family` String COMMENT 'Количество людей в семье',
    `type1` String COMMENT 'Группа благополучия',
    `type2` String COMMENT 'Подгруппа благополучия',
    `type3` String COMMENT 'Показатели благополучия',
    `ball` Float64 COMMENT 'Баллы за показатель',
    `number1` UInt8 COMMENT 'Столбец для внутренного рассчета',
    `count_iin` UInt8 COMMENT 'Количество людей в семье',
    `FULL_KATO_NAME` String COMMENT 'Полный КАТО человека',
    `KATO_2` String COMMENT 'КАТО региона',
    `KATO_2_NAME` String COMMENT 'Регионы',
    `KATO_4` String COMMENT 'КАТО районы',
    `KATO_4_NAME` String COMMENT 'Районы'
) ENGINE = MergeTree()
ORDER by
    ID_SK_FAMILY_QUALITY2;

insert into
    SOC_KARTA.SEGMENTATION_ALL
SELECT
    set1.ID_SK_FAMILY_QUALITY2,
    set1.numcat,
    cat_family,
    type1,
    type2,
    type3,
    ball,
    number1,
    count_iin,
    FULL_KATO_NAME,
    KATO_2,
    KATO_2_NAME,
    KATO_4,
    KATO_4_NAME
FROM
    SOC_KARTA.SK_FAMILY_VITRINA_MAIN as set1
    left join (
        SELECT
            ID_SK_FAMILY_QUALITY2,
            count_iin,
            FULL_KATO_NAME,
            KATO_2,
            KATO_2_NAME,
            KATO_4,
            KATO_4_NAME
        FROM
            SOC_KARTA.SK_FAMILY_VITRINA_KATO
    ) as set3 on set1.ID_SK_FAMILY_QUALITY2 == set3.ID_SK_FAMILY_QUALITY2;

--==========================SOC_KARTA.SEGMENTATION_FINAL
drop TABLE SOC_KARTA.SEGMENTATION_FINAL;

CREATE TABLE SOC_KARTA.SEGMENTATION_FINAL (
    `ID_SK_FAMILY_QUALITY2` String COMMENT 'ID Семьи',
    `numcat` UInt8 COMMENT 'Столбец для внутренного рассчета нумерация групп благополучия',
    `cat_family` String COMMENT 'Количество людей в семье',
    `type1` String COMMENT 'Группа благополучия',
    `type2` String COMMENT 'Подгруппа благополучия',
    `type3` String COMMENT 'Показатели благополучия',
    `ball` Float64 COMMENT 'Баллы за показатель',
    `count_iin` UInt64 COMMENT 'Количество людей в семье',
    `FULL_KATO_NAME` String COMMENT 'Полный КАТО человека',
    `KATO_2` String COMMENT 'КАТО региона',
    `KATO_2_NAME` String COMMENT 'Регионы',
    `KATO_4` String COMMENT 'КАТО районы',
    `KATO_4_NAME` String COMMENT 'Районы',
    `REGCOUNT` UInt64 COMMENT 'Количество по показателям в регионе',
    `Filtr` String COMMENT 'Столбец для фильтра количество или доля',
    `FAMILY_CAT` String COMMENT 'Категория благополучия семьи'
) ENGINE = MergeTree()
ORDER BY
    ID_SK_FAMILY_QUALITY2 SETTINGS parts_to_throw_insert = 50000,
    index_granularity = 8192;

insert into
    SOC_KARTA.SEGMENTATION_FINAL
select
    ID_SK_FAMILY_QUALITY2,
    numcat,
    cat_family,
    type1,
    type2,
    type3,
    set1.ball,
    count_iin,
    FULL_KATO_NAME,
    KATO_2,
    KATO_2_NAME,
    KATO_4,
    KATO_4_NAME,
    REGCOUNT,
    if(numcat1 == 1, 'Количество', 'Доля') as Filtr,
    FAMILY_CAT
from
    (
        SELECT
            set1.ID_SK_FAMILY_QUALITY2 as ID_SK_FAMILY_QUALITY2,
            numcat,
            cat_family,
            type1,
            type2,
            type3,
            set1.ball,
            count_iin,
            set1.FULL_KATO_NAME as FULL_KATO_NAME,
            set1.KATO_2 as KATO_2,
            set1.KATO_2_NAME as KATO_2_NAME,
            set1.KATO_4 as KATO_4,
            set1.KATO_4_NAME as KATO_4_NAME,
            REGCOUNT,
            --    if(numcat1 == 1, 'Количество', 'Доля') as Filtr,
            set2.FAMILY_CAT as FAMILY_CAT
        FROM
            SOC_KARTA.SEGMENTATION_ALL as set1
            left join (
                SELECT
                    ID_SK_FAMILY_QUALITY2,
                    SUM(ball) as ball,
                    (
                        case
                            when ball < 5 then 'E'
                            when ball > 4
                            and ball < 9 then 'D'
                            when ball > 8
                            and ball < 12 then 'C'
                            when ball > 11
                            and ball < 16 then 'B'
                            when ball > 15 then 'A'
                            else 'error'
                        end
                    ) as FAMILY_CAT
                FROM
                    SOC_KARTA.SEGMENTATION_ALL
                group by
                    ID_SK_FAMILY_QUALITY2
            ) as set2 on set1.ID_SK_FAMILY_QUALITY2 == set2.ID_SK_FAMILY_QUALITY2
            left join(
                SELECT
                    KATO_4,
                    COUNT(DISTINCT(ID_SK_FAMILY_QUALITY2)) as REGCOUNT
                FROM
                    SOC_KARTA.SEGMENTATION_ALL
                group by
                    KATO_4
            ) as set3 on set1.KATO_4 = set3.KATO_4
    ) array
    JOIN range(1, 3, 1) AS numcat1;

--==========================
-- SOC_KARTA.SEGMENTATION_GROUP definition
drop table SOC_KARTA.SEGMENTATION_GROUP;

CREATE TABLE SOC_KARTA.SEGMENTATION_GROUP (
    `cat_family` String COMMENT 'Количество людей в семье',
    `type1` String COMMENT 'Группа благополучия',
    `type2` String COMMENT 'Подгруппа благополучия',
    `type3` String COMMENT 'Показатели благополучия',
    `ball` Float64 COMMENT 'Баллы за показатель',
    `count_iin` UInt64 COMMENT 'Количество людей в семье',
    `FULL_KATO_NAME` String COMMENT 'Полный КАТО человека',
    `KATO_2` String COMMENT 'КАТО региона',
    `KATO_2_NAME` String COMMENT 'Регионы',
    `KATO_4` String COMMENT 'КАТО районы',
    `KATO_4_NAME` String COMMENT 'Районы',
    `Filtr` String COMMENT 'Столбец для фильтра количество или доля',
    `FAMILY_CAT` String COMMENT 'Категория благополучия семьи',
    `VALUE1` Float64 COMMENT 'Максимальное количество в регионе',
    `VALUE2` Float64 COMMENT 'Количество или доля по показателю',
    `count` Float64 COMMENT 'Количество семей по выбранному показателю'
) ENGINE = MergeTree()
order by
    FULL_KATO_NAME;

insert into
    SOC_KARTA.SEGMENTATION_GROUP
select
    cat_family,
    type1,
    type2,
    type3,
    ball,
    count_iin,
    FULL_KATO_NAME,
    KATO_2,
    KATO_2_NAME,
    KATO_4,
    KATO_4_NAME,
    Filtr,
    FAMILY_CAT,
    toFloat64(MAX(REGCOUNT)) as VALUE1,
    if(
        Filtr == 'Количество',
        toFloat64(COUNT(DISTINCT(ID_SK_FAMILY_QUALITY2))),
        COUNT(DISTINCT(ID_SK_FAMILY_QUALITY2)) / MAX(maxcount) * 100
    ) as VALUE2,
    toFloat64(COUNT(DISTINCT(ID_SK_FAMILY_QUALITY2))) as count1
from
    (
        SELECT
            ID_SK_FAMILY_QUALITY2,
            numcat,
            cat_family,
            type1,
            type2,
            type3,
            ball,
            count_iin,
            FULL_KATO_NAME,
            KATO_2,
            KATO_2_NAME,
            KATO_4,
            KATO_4_NAME,
            REGCOUNT,
            Filtr,
            FAMILY_CAT,
            (
                SELECT
                    count(DISTINCT(ID_SK_FAMILY_QUALITY2)) as maxcount
                FROM
                    SOC_KARTA.SEGMENTATION_FINAL
            ) as maxcount
        FROM
            SOC_KARTA.SEGMENTATION_FINAL
    )
group by
    cat_family,
    type1,
    type2,
    type3,
    ball,
    count_iin,
    FULL_KATO_NAME,
    KATO_2,
    KATO_2_NAME,
    KATO_4,
    KATO_4_NAME,
    Filtr,
    FAMILY_CAT;

------------------------------------------
drop TABLE SOC_KARTA.REITING;

CREATE TABLE SOC_KARTA.REITING (
    `Rating` String COMMENT 'Рейтинг',
    `type2` String COMMENT 'Подгруппа благополучия',
    `type3` String COMMENT 'Показатели благополучия',
    `value_reiting` UInt64 COMMENT 'Значение рейтинга',
    `proc` Float64 COMMENT 'Процент рейтинга по показателям'
) ENGINE = MergeTree()
order by
    Rating;

insert into
    SOC_KARTA.REITING
select
    Rating,
    type2,
    type3,
    count(distinct(ID_SK_FAMILY_QUALITY2)) as value_reiting,
    value_reiting / max(allc) as proc
from
    (
        select
            *
        from
            (
                SELECT
                    ID_SK_FAMILY_QUALITY2,
                    type2,
                    type3,
                    FAMILY_CAT,
                    Rating
                FROM
                    SOC_KARTA.SEGMENTATION_FINAL as set1
                    left join (
                        SELECT
                            ID_SK_FAMILY_QUALITY2,
                            Rating
                        FROM
                            SOC_KARTA.FamilySegmentation_Revenue
                    ) as set2 on toString(set1.ID_SK_FAMILY_QUALITY2) = toString(set2.ID_SK_FAMILY_QUALITY2)
            ) as set1
            left join (
                select
                    Rating,
                    count(distinct(ID_SK_FAMILY_QUALITY2)) as allc
                from
                    (
                        SELECT
                            ID_SK_FAMILY_QUALITY2,
                            type2,
                            type3,
                            FAMILY_CAT,
                            Rating
                        FROM
                            SOC_KARTA.SEGMENTATION_FINAL as set1
                            left join (
                                SELECT
                                    ID_SK_FAMILY_QUALITY2,
                                    Rating
                                FROM
                                    SOC_KARTA.FamilySegmentation_Revenue
                            ) as set2 on toString(set1.ID_SK_FAMILY_QUALITY2) = toString(set2.ID_SK_FAMILY_QUALITY2)
                        where
                            numcat = 1
                    )
                group by
                    Rating
            ) as set2 on toString(set1.Rating) = toString(set2.Rating)
    )
group by
    Rating,
    type2,
    type3;

-------
drop TABLE SOC_KARTA.SEGMENTATION_GROUP;

CREATE TABLE SOC_KARTA.SEGMENTATION_GROUP (
    `Rating` String COMMENT 'Рейтинг',
    `cat_family` String COMMENT 'Количество людей в семье',
    `type1` String COMMENT 'Группа благополучия',
    `type2` String COMMENT 'Подгруппа благополучия',
    `type3` String COMMENT 'Показатели благополучия',
    `ball` Float64 COMMENT 'Баллы за показатель',
    `count_iin` UInt64 COMMENT 'Количество людей в семье',
    `FULL_KATO_NAME` String COMMENT 'Полный КАТО человека',
    `KATO_2` String COMMENT 'КАТО региона',
    `KATO_2_NAME` String COMMENT 'Регионы',
    `KATO_4` String COMMENT 'КАТО районы',
    `KATO_4_NAME` String COMMENT 'Районы',
    `Filtr` String COMMENT 'Столбец для фильтра количество или доля',
    `FAMILY_CAT` String COMMENT 'Категория благополучия семьи',
    `VALUE1` Float64 COMMENT 'Максимальное количество в регионе',
    `VALUE2` Float64 COMMENT 'Количество или доля по показателю',
    `count` Float64 COMMENT 'Количество семей по выбранному показателю',
    `value_reiting` UInt64 COMMENT 'Значение рейтинга',
    `proc` Float64 COMMENT 'Процент рейтинга по показателям'
) ENGINE = MergeTree()
order by
    FULL_KATO_NAME;

insert into
    SOC_KARTA.SEGMENTATION_GROUP
select
    Rating,
    cat_family,
    type1,
    type2,
    type3,
    ball,
    count_iin,
    FULL_KATO_NAME,
    KATO_2,
    KATO_2_NAME,
    KATO_4,
    KATO_4_NAME,
    Filtr,
    FAMILY_CAT,
    VALUE1,
    VALUE2,
    count1,
    value_reiting,
    proc
from
    (
        select
            Rating,
            cat_family,
            type1,
            type2,
            type3,
            ball,
            count_iin,
            FULL_KATO_NAME,
            KATO_2,
            KATO_2_NAME,
            KATO_4,
            KATO_4_NAME,
            Filtr,
            FAMILY_CAT,
            toFloat64(MAX(REGCOUNT)) as VALUE1,
            if(
                Filtr == 'Количество',
                toFloat64(COUNT(DISTINCT(ID_SK_FAMILY_QUALITY2))),
                COUNT(DISTINCT(ID_SK_FAMILY_QUALITY2)) / MAX(maxcount) * 100
            ) as VALUE2,
            toFloat64(COUNT(DISTINCT(ID_SK_FAMILY_QUALITY2))) as count1
        from
            (
                SELECT
                    ID_SK_FAMILY_QUALITY2,
                    numcat,
                    cat_family,
                    type1,
                    type2,
                    type3,
                    ball,
                    count_iin,
                    FULL_KATO_NAME,
                    KATO_2,
                    KATO_2_NAME,
                    KATO_4,
                    KATO_4_NAME,
                    REGCOUNT,
                    Filtr,
                    FAMILY_CAT,
                    Rating,
                    (
                        SELECT
                            count(DISTINCT(ID_SK_FAMILY_QUALITY2)) as maxcount
                        FROM
                            SOC_KARTA.SEGMENTATION_FINAL
                    ) as maxcount
                FROM
                    SOC_KARTA.SEGMENTATION_FINAL as set1
                    left join (
                        SELECT
                            ID_SK_FAMILY_QUALITY2,
                            Rating
                        FROM
                            SOC_KARTA.FamilySegmentation_Revenue
                    ) as set2 on toString(set1.ID_SK_FAMILY_QUALITY2) = toString(set2.ID_SK_FAMILY_QUALITY2)
            )
        group by
            Rating,
            cat_family,
            type1,
            type2,
            type3,
            ball,
            count_iin,
            FULL_KATO_NAME,
            KATO_2,
            KATO_2_NAME,
            KATO_4,
            KATO_4_NAME,
            Filtr,
            FAMILY_CAT
    ) as set1
    left join (
        SELECT
            Rating,
            type2,
            type3,
            value_reiting,
            proc
        FROM
            SOC_KARTA.REITING
    ) as set2 on toString(set1.Rating) = toString(set2.Rating)
    and set1.type2 = set2.type2
    and set1.type3 = set2.type3;

--------------SOC_KARTA.SEGMENTATION_GROUP2
drop TABLE SOC_KARTA.SEGMENTATION_GROUP2;

CREATE TABLE SOC_KARTA.SEGMENTATION_GROUP2 (
    `ID_SK_FAMILY_QUALITY2` String COMMENT 'ID Семьи',
    `FULL_KATO_NAME` String COMMENT 'Полный КАТО человека',
    `KATO_2_NAME` String COMMENT 'Регионы',
    `KATO_2` String COMMENT 'КАТО региона',
    `KATO_4_NAME` String COMMENT 'Районы',
    `KATO_4` String COMMENT 'КАТО районы',
    `FAMILY_CAT` String COMMENT 'Категория благополучия семьи',
    `filtr1` String COMMENT 'Уровень среднедушевого дохода (СДД) в семье',
    `filtr2` String COMMENT 'Информация о занятости членов семьи',
    `filtr3` String COMMENT 'Наличие в собственности семьи недвижимого имущества',
    `filtr4` String COMMENT 'Наличие в собственности семьи движимого имущества',
    `filtr5` String COMMENT 'Наличие в собственности семьи коммерческого движимого имущества',
    `filtr6` String COMMENT 'Наличие сельскохозяйственной техники',
    `filtr7` String COMMENT 'Наличие в собственности земельного участка',
    `filtr8` String COMMENT 'Наличие в собственности семьи коммерческого недвижимого имущества',
    `filtr9` String COMMENT 'Наличие у семьи личного подсобного хозяйства',
    `filtr10` String COMMENT 'Учредитель в ЮЛ и ИП'
) ENGINE = MergeTree()
ORDER BY
    FULL_KATO_NAME;

insert into
    SOC_KARTA.SEGMENTATION_GROUP2
select
    ID_SK_FAMILY_QUALITY2,
    groupArray(FULL_KATO_NAME) [1],
    groupArray(KATO_2_NAME) [1],
    groupArray(KATO_2) [1],
    groupArray(KATO_4_NAME) [1],
    groupArray(KATO_4) [1],
    groupArray(FAMILY_CAT) [1],
    groupArray(filtr1) [indexOf(groupArray(numcat),1)],
    groupArray(filtr2) [indexOf(groupArray(numcat),2)],
    groupArray(filtr3) [indexOf(groupArray(numcat),3)],
    groupArray(filtr4) [indexOf(groupArray(numcat),4)],
    groupArray(filtr5) [indexOf(groupArray(numcat),5)],
    groupArray(filtr6) [indexOf(groupArray(numcat),6)],
    groupArray(filtr7) [indexOf(groupArray(numcat),7)],
    groupArray(filtr8) [indexOf(groupArray(numcat),8)],
    groupArray(filtr9) [indexOf(groupArray(numcat),9)],
    groupArray(filtr10) [indexOf(groupArray(numcat),10)]
from
    (
        select
            ID_SK_FAMILY_QUALITY2,
            KATO_2_NAME,
            KATO_2,
            KATO_4_NAME,
            KATO_4,
            FULL_KATO_NAME,
            FAMILY_CAT,
            numcat,
            if(numcat == 1, type3, '') as filtr1,
            if(numcat == 2, type3, '') as filtr2,
            if(numcat == 3, type3, '') as filtr3,
            if(numcat == 4, type3, '') as filtr4,
            if(numcat == 5, type3, '') as filtr5,
            if(numcat == 6, type3, '') as filtr6,
            if(numcat == 7, type3, '') as filtr7,
            if(numcat == 8, type3, '') as filtr8,
            if(numcat == 9, type3, '') as filtr9,
            if(numcat == 10, type3, '') as filtr10
        from
            SOC_KARTA.SEGMENTATION_FINAL sf
        where
            Filtr == 'Количество'
    )
group by
    ID_SK_FAMILY_QUALITY2;

--SOC_KARTA.SEGMENTATION_GROUP3
drop TABLE SOC_KARTA.SEGMENTATION_GROUP3;

CREATE TABLE SOC_KARTA.SEGMENTATION_GROUP3 (
    `ID_SK_FAMILY_QUALITY2` String COMMENT 'ID Семьи',
    `filtr11` String COMMENT 'Присутствие в семье четырех и более несовершеннолетних детей и учащейся молодежи до 23 лет',
    `filtr12` String COMMENT 'Наличие в семье лица с инвалидностью (1 и 2 группы)',
    `filtr13` String COMMENT 'Наличие в семье лица с инвалидностью с детства или ребенка - инвалида',
    `filtr14` String COMMENT 'Наличие в семье лица с инвалидностью (3 группы)',
    `filtr15` String COMMENT 'Информация о наличии у членов семьи хронических заболеваний',
    `filtr16` String COMMENT 'Информация о наличии у членов семьи Д - учета',
    `filtr17` String COMMENT 'Информация о прикреплении детей в семье к дошкольным и школьным образовательным учреждениям',
    `filtr18` String COMMENT 'Информация о прикреплении членов семьи медицинским учреждениям',
    `filtr19` String COMMENT 'Информация об среднем образовании взрослых членов семьи',
    `filtr20` String COMMENT 'Информация об профессиональном образовании взрослых членов семьи',
    `filtr21` String COMMENT 'Информация об высшем образовании взрослых членов семьи',
    `filtr22` String COMMENT 'Информация о участии в ОСМС',
    `filtr23` String COMMENT 'Закредитованность семьи',
    `filtr24` String COMMENT 'Задолженность семьи по кредитам',
    `filtr25` String COMMENT 'Информация о прикреплении детей в семье к дошкольным учреждениям',
    `filtr26` String COMMENT 'Информация о прикреплении детей в семье к школьным образовательным учреждениям',
    `filtr27` String COMMENT 'Информация об образовании взрослых членов семьи',
    `count_iin` UInt64
) ENGINE = MergeTree()
ORDER BY
    ID_SK_FAMILY_QUALITY2;

insert into
    SOC_KARTA.SEGMENTATION_GROUP3
select
    ID_SK_FAMILY_QUALITY2,
    groupArray(filtr11) [indexOf(groupArray(numcat),11)],
    groupArray(filtr12) [indexOf(groupArray(numcat),12)],
    groupArray(filtr13) [indexOf(groupArray(numcat),13)],
    groupArray(filtr14) [indexOf(groupArray(numcat),14)],
    groupArray(filtr15) [indexOf(groupArray(numcat),15)],
    groupArray(filtr16) [indexOf(groupArray(numcat),16)],
    groupArray(filtr17) [indexOf(groupArray(numcat),17)],
    groupArray(filtr18) [indexOf(groupArray(numcat),18)],
    groupArray(filtr19) [indexOf(groupArray(numcat),19)],
    groupArray(filtr20) [indexOf(groupArray(numcat),20)],
    groupArray(filtr21) [indexOf(groupArray(numcat),21)],
    groupArray(filtr22) [indexOf(groupArray(numcat),22)],
    groupArray(filtr23) [indexOf(groupArray(numcat),23)],
    groupArray(filtr24) [indexOf(groupArray(numcat),24)],
    groupArray(filtr25) [indexOf(groupArray(numcat),25)],
    groupArray(filtr26) [indexOf(groupArray(numcat),26)],
    groupArray(filtr27) [indexOf(groupArray(numcat),27)],
    groupArray(count_iin) [1]
from
    (
        select
            ID_SK_FAMILY_QUALITY2,
            numcat,
            if(numcat == 11, type3, '') as filtr11,
            if(numcat == 12, type3, '') as filtr12,
            if(numcat == 13, type3, '') as filtr13,
            if(numcat == 14, type3, '') as filtr14,
            if(numcat == 15, type3, '') as filtr15,
            if(numcat == 16, type3, '') as filtr16,
            if(numcat == 17, type3, '') as filtr17,
            if(numcat == 18, type3, '') as filtr18,
            if(numcat == 19, type3, '') as filtr19,
            if(numcat == 20, type3, '') as filtr20,
            if(numcat == 21, type3, '') as filtr21,
            if(numcat == 22, type3, '') as filtr22,
            if(numcat == 23, type3, '') as filtr23,
            if(numcat == 24, type3, '') as filtr24,
            if(numcat == 25, type3, '') as filtr25,
            if(numcat == 26, type3, '') as filtr26,
            if(numcat == 27, type3, '') as filtr27,
            count_iin
        from
            SOC_KARTA.SEGMENTATION_FINAL sf
        where
            Filtr == 'Количество'
    )
group by
    ID_SK_FAMILY_QUALITY2;

----
-- SOCdrop KARTA.SEGMENTATION_GROUP_FINAL definition
drop TABLE SOC_KARTA.SEGMENTATION_GROUP_FINAL;

CREATE TABLE SOC_KARTA.SEGMENTATION_GROUP_FINAL (
    `FULL_KATO_NAME` String COMMENT 'Полный КАТО человека',
    `KATO_2_NAME` String COMMENT 'Регионы',
    `KATO_2` String COMMENT 'КАТО региона',
    `KATO_4_NAME` String COMMENT 'Районы',
    `KATO_4` String COMMENT 'КАТО районы',
    `FAMILY_CAT` String COMMENT 'Категория благополучия семьи',
    `filtr1` String COMMENT 'Уровень среднедушевого дохода (СДД) в семье',
    `filtr2` String COMMENT 'Информация о занятости членов семьи',
    `filtr3` String COMMENT 'Наличие в собственности семьи недвижимого имущества',
    `filtr4` String COMMENT 'Наличие в собственности семьи движимого имущества',
    `filtr5` String COMMENT 'Наличие в собственности семьи коммерческого движимого имущества',
    `filtr6` String COMMENT 'Наличие сельскохозяйственной техники',
    `filtr7` String COMMENT 'Наличие в собственности земельного участка',
    `filtr8` String COMMENT 'Наличие в собственности семьи коммерческого недвижимого имущества',
    `filtr9` String COMMENT 'Наличие у семьи личного подсобного хозяйства',
    `filtr10` String COMMENT 'Учредитель в ЮЛ и ИП',
    `filtr11` String COMMENT 'Присутствие в семье четырех и более несовершеннолетних детей и учащейся молодежи до 23 лет',
    `filtr12` String COMMENT 'Наличие в семье лица с инвалидностью (1 и 2 группы)',
    `filtr13` String COMMENT 'Наличие в семье лица с инвалидностью с детства или ребенка - инвалида',
    `filtr14` String COMMENT 'Наличие в семье лица с инвалидностью (3 группы)',
    `filtr15` String COMMENT 'Информация о наличии у членов семьи хронических заболеваний',
    `filtr16` String COMMENT 'Информация о наличии у членов семьи Д - учета',
    `filtr17` String COMMENT 'Информация о прикреплении детей в семье к дошкольным и школьным образовательным учреждениям',
    `filtr18` String COMMENT 'Информация о прикреплении членов семьи медицинским учреждениям',
    `filtr19` String COMMENT 'Информация об среднем образовании взрослых членов семьи',
    `filtr20` String COMMENT 'Информация об профессиональном образовании взрослых членов семьи',
    `filtr21` String COMMENT 'Информация об высшем образовании взрослых членов семьи',
    `filtr22` String COMMENT 'Информация о участии в ОСМС',
    `filtr23` String COMMENT 'Закредитованность семьи',
    `filtr24` String COMMENT 'Задолженность семьи по кредитам',
    `filtr25` String COMMENT 'Информация о прикреплении детей в семье к дошкольным учреждениям',
    `filtr26` String COMMENT 'Информация о прикреплении детей в семье к школьным образовательным учреждениям',
    `filtr27` String COMMENT 'Информация об образовании взрослых членов семьи',
    `VALUE` UInt64
) ENGINE = MergeTree()
ORder By
    FULL_KATO_NAME;

insert into
    SOC_KARTA.SEGMENTATION_GROUP_FINAL
select
    FULL_KATO_NAME,
    KATO_2_NAME,
    KATO_2,
    KATO_4_NAME,
    KATO_4,
    FAMILY_CAT,
    filtr1,
    filtr2,
    filtr3,
    filtr4,
    filtr5,
    filtr6,
    filtr7,
    filtr8,
    filtr9,
    filtr10,
    filtr11,
    filtr12,
    filtr13,
    filtr14,
    filtr15,
    filtr16,
    filtr17,
    filtr18,
    filtr19,
    filtr20,
    filtr21,
    filtr22,
    filtr23,
    filtr24,
    filtr25,
    filtr26,
    filtr27,
    COUNT(DISTINCT(ID_SK_FAMILY_QUALITY2)) as VALUE
from
    (
        select
            *
        from
            SOC_KARTA.SEGMENTATION_GROUP2 sg
            left join SOC_KARTA.SEGMENTATION_GROUP3 sg3 on sg.ID_SK_FAMILY_QUALITY2 == sg3.ID_SK_FAMILY_QUALITY2
    )
group by
    FULL_KATO_NAME,
    KATO_2_NAME,
    KATO_2,
    KATO_4_NAME,
    KATO_4,
    FAMILY_CAT,
    filtr1,
    filtr2,
    filtr3,
    filtr4,
    filtr5,
    filtr6,
    filtr7,
    filtr8,
    filtr9,
    filtr10,
    filtr11,
    filtr12,
    filtr13,
    filtr14,
    filtr15,
    filtr16,
    filtr17,
    filtr18,
    filtr19,
    filtr20,
    filtr21,
    filtr22,
    filtr23,
    filtr24,
    filtr25,
    filtr26,
    filtr27;

-- SOC_KARTA.SEGMENTATION_GROUP_FINAL2 definition
drop TABLE SOC_KARTA.SEGMENTATION_GROUP_FINAL2;

CREATE TABLE SOC_KARTA.SEGMENTATION_GROUP_FINAL2 (
    `FULL_KATO_NAME` String COMMENT 'Полный КАТО человека',
    `KATO_2_NAME` String COMMENT 'Регионы',
    `KATO_2` String COMMENT 'КАТО региона',
    `KATO_4_NAME` String COMMENT 'Районы',
    `KATO_4` String COMMENT 'КАТО районы',
    `FAMILY_CAT` String COMMENT 'Категория благополучия семьи',
    `filtr1` String COMMENT 'Уровень среднедушевого дохода (СДД) в семье',
    `filtr2` String COMMENT 'Информация о занятости членов семьи',
    `filtr3` String COMMENT 'Наличие в собственности семьи недвижимого имущества',
    `filtr4` String COMMENT 'Наличие в собственности семьи движимого имущества',
    `filtr5` String COMMENT 'Наличие в собственности семьи коммерческого движимого имущества',
    `filtr6` String COMMENT 'Наличие сельскохозяйственной техники',
    `filtr7` String COMMENT 'Наличие в собственности земельного участка',
    `filtr8` String COMMENT 'Наличие в собственности семьи коммерческого недвижимого имущества',
    `filtr9` String COMMENT 'Наличие у семьи личного подсобного хозяйства',
    `filtr10` String COMMENT 'Учредитель в ЮЛ и ИП',
    `filtr11` String COMMENT 'Присутствие в семье четырех и более несовершеннолетних детей и учащейся молодежи до 23 лет',
    `filtr12` String COMMENT 'Наличие в семье лица с инвалидностью (1 и 2 группы)',
    `filtr13` String COMMENT 'Наличие в семье лица с инвалидностью с детства или ребенка - инвалида',
    `filtr14` String COMMENT 'Наличие в семье лица с инвалидностью (3 группы)',
    `filtr15` String COMMENT 'Информация о наличии у членов семьи хронических заболеваний',
    `filtr16` String COMMENT 'Информация о наличии у членов семьи Д - учета',
    `filtr17` String COMMENT 'Информация о прикреплении детей в семье к дошкольным и школьным образовательным учреждениям',
    `filtr18` String COMMENT 'Информация о прикреплении членов семьи медицинским учреждениям',
    `filtr19` String COMMENT 'Информация об среднем образовании взрослых членов семьи',
    `filtr20` String COMMENT 'Информация об профессиональном образовании взрослых членов семьи',
    `filtr21` String COMMENT 'Информация об высшем образовании взрослых членов семьи',
    `filtr22` String COMMENT 'Информация о участии в ОСМС',
    `filtr23` String COMMENT 'Закредитованность семьи',
    `filtr24` String COMMENT 'Задолженность семьи по кредитам',
    `filtr25` String COMMENT 'Информация о прикреплении детей в семье к дошкольным учреждениям',
    `filtr26` String COMMENT 'Информация о прикреплении детей в семье к школьным образовательным учреждениям',
    `filtr27` String COMMENT 'Информация об образовании взрослых членов семьи',
    `VALUE` UInt64,
    `maxcount` UInt64,
    `REGCOUNT` UInt64 COMMENT 'Количество по показателям в регионе',
    `Filtr` String COMMENT 'Столбец для фильтра количество или доля',
    `VALUE1` Float64 COMMENT 'Максимальное количество в регионе',
    `VALUE2` Float64
) ENGINE = MergeTree()
order by
    FULL_KATO_NAME;

insert into
    SOC_KARTA.SEGMENTATION_GROUP_FINAL2
SELECT
    FULL_KATO_NAME,
    KATO_2_NAME,
    KATO_2,
    KATO_4_NAME,
    KATO_4,
    FAMILY_CAT,
    filtr1,
    filtr2,
    filtr3,
    filtr4,
    filtr5,
    filtr6,
    filtr7,
    filtr8,
    filtr9,
    filtr10,
    filtr11,
    filtr12,
    filtr13,
    filtr14,
    filtr15,
    filtr16,
    filtr17,
    filtr18,
    filtr19,
    filtr20,
    filtr21,
    filtr22,
    filtr23,
    filtr24,
    filtr25,
    filtr26,
    filtr27,
    VALUE,
    (
        select
            SUM(VALUE)
        FROM
            SOC_KARTA.SEGMENTATION_GROUP_FINAL
    ) as maxcount,
    REGCOUNT,
    if(numcat1 == 1, 'Количество', 'Доля') as Filtr,
    if(
        Filtr == 'Количество',
        toFloat64(VALUE),
        VALUE / REGCOUNT * 100
    ) as VALUE1,
    if(
        Filtr == 'Количество',
        toFloat64(VALUE),
        VALUE / maxcount * 100
    ) as VALUE2
FROM
    SOC_KARTA.SEGMENTATION_GROUP_FINAL as set1
    left join (
        select
            KATO_2_NAME,
            KATO_2,
            KATO_4_NAME,
            KATO_4,
            SUM(VALUE) as REGCOUNT
        FROM
            SOC_KARTA.SEGMENTATION_GROUP_FINAL
        group by
            KATO_2_NAME,
            KATO_2,
            KATO_4_NAME,
            KATO_4
    ) as set2 on set1.KATO_2_NAME == set2.KATO_2_NAME
    and set1.KATO_2 == set2.KATO_2
    and set1.KATO_4_NAME == set2.KATO_4_NAME
    and set1.KATO_4 == set2.KATO_4 array
    JOIN range(1, 3, 1) AS numcat1;

--.......
-- SOC_KARTA.SEGMENTATION_ASSOGIN definition
drop TABLE SOC_KARTA.SEGMENTATION_ASSOGIN;

CREATE TABLE SOC_KARTA.SEGMENTATION_ASSOGIN (
    `ID_SK_FAMILY_QUALITY2` String COMMENT 'ID Семьи',
    `FULL_KATO_NAME` String COMMENT 'Полный КАТО человека',
    `KATO_2_NAME` String COMMENT 'Регионы',
    `KATO_2` String COMMENT 'КАТО региона',
    `KATO_4_NAME` String COMMENT 'Районы',
    `KATO_4` String COMMENT 'КАТО районы',
    `FAMILY_CAT` String COMMENT 'Категория благополучия семьи',
    `filtr1` String COMMENT 'Уровень среднедушевого дохода (СДД) в семье',
    `filtr2` String COMMENT 'Информация о занятости членов семьи',
    `filtr3` String COMMENT 'Наличие в собственности семьи недвижимого имущества',
    `filtr4` String COMMENT 'Наличие в собственности семьи движимого имущества',
    `filtr5` String COMMENT 'Наличие в собственности семьи коммерческого движимого имущества',
    `filtr6` String COMMENT 'Наличие сельскохозяйственной техники',
    `filtr7` String COMMENT 'Наличие в собственности земельного участка',
    `filtr8` String COMMENT 'Наличие в собственности семьи коммерческого недвижимого имущества',
    `filtr9` String COMMENT 'Наличие у семьи личного подсобного хозяйства',
    `filtr10` String COMMENT 'Учредитель в ЮЛ и ИП',
    `sg3.ID_SK_FAMILY_QUALITY2` String,
    `filtr11` String COMMENT 'Присутствие в семье четырех и более несовершеннолетних детей и учащейся молодежи до 23 лет',
    `filtr12` String COMMENT 'Наличие в семье лица с инвалидностью (1 и 2 группы)',
    `filtr13` String COMMENT 'Наличие в семье лица с инвалидностью с детства или ребенка - инвалида',
    `filtr14` String COMMENT 'Наличие в семье лица с инвалидностью (3 группы)',
    `filtr15` String COMMENT 'Информация о наличии у членов семьи хронических заболеваний',
    `filtr16` String COMMENT 'Информация о наличии у членов семьи Д - учета',
    `filtr17` String COMMENT 'Информация о прикреплении детей в семье к дошкольным и школьным образовательным учреждениям',
    `filtr18` String COMMENT 'Информация о прикреплении членов семьи медицинским учреждениям',
    `filtr19` String COMMENT 'Информация об среднем образовании взрослых членов семьи',
    `filtr20` String COMMENT 'Информация об профессиональном образовании взрослых членов семьи',
    `filtr21` String COMMENT 'Информация об высшем образовании взрослых членов семьи',
    `filtr22` String COMMENT 'Информация о участии в ОСМС',
    `filtr23` String COMMENT 'Закредитованность семьи',
    `filtr24` String COMMENT 'Задолженность семьи по кредитам',
    `filtr25` String COMMENT 'Информация о прикреплении детей в семье к дошкольным учреждениям',
    `filtr26` String COMMENT 'Информация о прикреплении детей в семье к школьным образовательным учреждениям',
    `filtr27` String COMMENT 'Информация об образовании взрослых членов семьи',
    `count_iin` UInt64
) ENGINE = MergeTree()
order by
    ID_SK_FAMILY_QUALITY2;

insert into
    SOC_KARTA.SEGMENTATION_ASSOGIN
select
    *
from
    SOC_KARTA.SEGMENTATION_GROUP2 sg
    left join SOC_KARTA.SEGMENTATION_GROUP3 sg3 on sg.ID_SK_FAMILY_QUALITY2 == sg3.ID_SK_FAMILY_QUALITY2;

-----------
-- SOC_KARTA.SEGMENTATION_GROUP_FINAL3 definition
DROP TABLE SOC_KARTA.SEGMENTATION_GROUP_FINAL3;

CREATE TABLE SOC_KARTA.SEGMENTATION_GROUP_FINAL3 (
    `ID_SK_FAMILY_QUALITY2` String COMMENT 'ID Семьи',
    `FULL_KATO_NAME` String COMMENT 'Полный КАТО человека',
    `KATO_2_NAME` String COMMENT 'Регионы',
    `KATO_2` String COMMENT 'КАТО региона',
    `KATO_4_NAME` String COMMENT 'Районы',
    `KATO_4` String COMMENT 'КАТО районы',
    `FAMILY_CAT` String COMMENT 'Категория благополучия семьи',
    `filtr1` String COMMENT 'Уровень среднедушевого дохода (СДД) в семье',
    `filtr2` String COMMENT 'Информация о занятости членов семьи',
    `filtr3` String COMMENT 'Наличие в собственности семьи недвижимого имущества',
    `filtr4` String COMMENT 'Наличие в собственности семьи движимого имущества',
    `filtr5` String COMMENT 'Наличие в собственности семьи коммерческого движимого имущества',
    `filtr6` String COMMENT 'Наличие сельскохозяйственной техники',
    `filtr7` String COMMENT 'Наличие в собственности земельного участка',
    `filtr8` String COMMENT 'Наличие в собственности семьи коммерческого недвижимого имущества',
    `filtr9` String COMMENT 'Наличие у семьи личного подсобного хозяйства',
    `filtr10` String COMMENT 'Учредитель в ЮЛ и ИП',
    `ID_SK_FAMILY_QUALITY22` String,
    `filtr11` String COMMENT 'Присутствие в семье четырех и более несовершеннолетних детей и учащейся молодежи до 23 лет',
    `filtr12` String COMMENT 'Наличие в семье лица с инвалидностью (1 и 2 группы)',
    `filtr13` String COMMENT 'Наличие в семье лица с инвалидностью с детства или ребенка - инвалида',
    `filtr14` String COMMENT 'Наличие в семье лица с инвалидностью (3 группы)',
    `filtr15` String COMMENT 'Информация о наличии у членов семьи хронических заболеваний',
    `filtr16` String COMMENT 'Информация о наличии у членов семьи Д - учета',
    `filtr17` String COMMENT 'Информация о прикреплении детей в семье к дошкольным и школьным образовательным учреждениям',
    `filtr18` String COMMENT 'Информация о прикреплении членов семьи медицинским учреждениям',
    `filtr19` String COMMENT 'Информация об среднем образовании взрослых членов семьи',
    `filtr20` String COMMENT 'Информация об профессиональном образовании взрослых членов семьи',
    `filtr21` String COMMENT 'Информация об высшем образовании взрослых членов семьи',
    `filtr22` String COMMENT 'Информация о участии в ОСМС',
    `filtr23` String COMMENT 'Закредитованность семьи',
    `filtr24` String COMMENT 'Задолженность семьи по кредитам',
    `filtr25` String COMMENT 'Информация о прикреплении детей в семье к дошкольным учреждениям',
    `filtr26` String COMMENT 'Информация о прикреплении детей в семье к школьным образовательным учреждениям',
    `filtr27` String COMMENT 'Информация об образовании взрослых членов семьи',
    `filtr28` String COMMENT 'Получатели АСП',
    `filtr29` String COMMENT 'Плательщики ЕСП',
    `filtr30` String COMMENT 'плательщик ОПВ',
    `count_iin` UInt64 COMMENT 'Количество людей в семье',
    `KATO_42` String COMMENT 'КАТО 4 районы',
    `value_rayon` UInt64 COMMENT 'Значение по Району',
    `max_value` UInt64 COMMENT 'Максимальное значение по региону ',
    `Rating` String COMMENT 'Рейтинг'
) ENGINE = MergeTree()
ORDER BY
    ID_SK_FAMILY_QUALITY2 SETTINGS index_granularity = 8192;

----TRUNCATE TABLE SOC_KARTA.SEGMENTATION_GROUP_FINAL3
INSERT INTO
    SOC_KARTA.SEGMENTATION_GROUP_FINAL3
SELECT
    set1.ID_SK_FAMILY_QUALITY2,
    FULL_KATO_NAME,
    KATO_2_NAME,
    KATO_2,
    KATO_4_NAME,
    set1.KATO_4,
    FAMILY_CAT,
    filtr1,
    filtr2,
    filtr3,
    filtr4,
    filtr5,
    filtr6,
    filtr7,
    filtr8,
    filtr9,
    filtr10,
    set2.ID_SK_FAMILY_QUALITY2,
    filtr11,
    filtr12,
    filtr13,
    filtr14,
    filtr15,
    filtr16,
    filtr17,
    filtr18,
    filtr19,
    filtr20,
    filtr21,
    filtr22,
    filtr23,
    filtr24,
    filtr25,
    filtr26,
    filtr27,
    filtr28,
    filtr29,
    filtr30,
    count_iin,
    set3.KATO_4,
    value_rayon,
    (
        select
            count(distinct(ID_SK_FAMILY_QUALITY2)) as max_value
        from
            SOC_KARTA.SEGMENTATION_GROUP2
    ) as max_value,
    Rating
FROM
    SOC_KARTA.SEGMENTATION_GROUP2 as set1
    left join SOC_KARTA.SEGMENTATION_GROUP3 as set2 on set1.ID_SK_FAMILY_QUALITY2 = set2.ID_SK_FAMILY_QUALITY2
    left join (
        select
            KATO_4,
            count(distinct(ID_SK_FAMILY_QUALITY2)) as value_rayon
        from
            SOC_KARTA.SEGMENTATION_GROUP2
        where
            KATO_4 <> 'Без прописки'
            and KATO_4 is not null
        group by
            KATO_4
    ) as set3 on set1.KATO_4 = set3.KATO_4
    left join (
        SELECT
            toString(ID_SK_FAMILY_QUALITY) as ID_SK_FAMILY_QUALITY2,
            if(
                sum(INCOME_ASP_IIN) > 0,
                'Получатели АСП',
                'Не являются получателями АСП'
            ) as filtr28,
            if(
                sum(INCOME_ESP_IIN) > 0,
                'Плательщики ЕСП',
                'Не являются плательщиками ЕСП'
            ) as filtr29,
            if(
                sum(if(INCOME_OOP_3_IIN > 0, 1, 0)) = 1,
                'Семьи, в которых есть один плательщик ОПВ ',
                if(
                    sum(if(INCOME_OOP_3_IIN > 0, 1, 0)) > 1,
                    'Семьи, в которых есть два и более плательщиков ОПВ',
                    'Семьи, в которых нет плательщиков ОПВ'
                )
            ) as filtr30
        FROM
            SOC_KARTA.SK_FAMILY_QUALITY_IIN
        group by
            ID_SK_FAMILY_QUALITY
    ) as set4 on set1.ID_SK_FAMILY_QUALITY2 = set4.ID_SK_FAMILY_QUALITY2
    left join SOC_KARTA.FamilySegmentation_Revenue as set5 on set1.ID_SK_FAMILY_QUALITY2 = toString(set5.ID_SK_FAMILY_QUALITY2);

---------------------------
drop table SOC_KARTA.FAMILY_APPEALS3;

CREATE TABLE SOC_KARTA.FAMILY_APPEALS3 (
    `IIN` String COMMENT 'ИИН человека',
    `FAMILY_ID` Int64 COMMENT 'ID семьи',
    `FAMILY_CAT` String COMMENT 'Категория благополучия семьи',
    `REG_DATE` DateTime COMMENT 'Дата регистрация обращения',
    `APPEAL_ID` String COMMENT 'ID обращения',
    `APPEAL_TYPE` Nullable(String) COMMENT 'Тип Обращения',
    `ORG_TYPE` String COMMENT 'Тип гос органа',
    `ORG_NAME` String COMMENT 'Название гос органа',
    `ISSUE` String COMMENT 'Решение по обращению',
    `SUBISSUE` String COMMENT 'Доп инфо по решению',
    `DECISION` String COMMENT 'Нужно давать ответ или нет по обрашению',
    `ID_SK_FAMILY_QUALITY2` String COMMENT 'ID Семьи',
    `FULL_KATO_NAME` String COMMENT 'Полный КАТО человека',
    `KATO_2_NAME` String COMMENT 'Регионы',
    `KATO_2` String COMMENT 'КАТО региона',
    `KATO_4_NAME` String COMMENT 'Районы',
    `KATO_4` String COMMENT 'КАТО районы',
    `filtr1` String COMMENT 'Уровень среднедушевого дохода (СДД) в семье',
    `filtr2` String COMMENT 'Информация о занятости членов семьи',
    `filtr3` String COMMENT 'Наличие в собственности семьи недвижимого имущества',
    `filtr4` String COMMENT 'Наличие в собственности семьи движимого имущества',
    `filtr5` String COMMENT 'Наличие в собственности семьи коммерческого движимого имущества',
    `filtr6` String COMMENT 'Наличие сельскохозяйственной техники',
    `filtr7` String COMMENT 'Наличие в собственности земельного участка',
    `filtr8` String COMMENT 'Наличие в собственности семьи коммерческого недвижимого имущества',
    `filtr9` String COMMENT 'Наличие у семьи личного подсобного хозяйства',
    `filtr10` String COMMENT 'Учредитель в ЮЛ и ИП',
    `filtr11` String COMMENT 'Присутствие в семье четырех и более несовершеннолетних детей и учащейся молодежи до 23 лет',
    `filtr12` String COMMENT 'Наличие в семье лица с инвалидностью (1 и 2 группы)',
    `filtr13` String COMMENT 'Наличие в семье лица с инвалидностью с детства или ребенка - инвалида',
    `filtr14` String COMMENT 'Наличие в семье лица с инвалидностью (3 группы)',
    `filtr15` String COMMENT 'Информация о наличии у членов семьи хронических заболеваний',
    `filtr16` String COMMENT 'Информация о наличии у членов семьи Д - учета',
    `filtr17` String COMMENT 'Информация о прикреплении детей в семье к дошкольным и школьным образовательным учреждениям',
    `filtr18` String COMMENT 'Информация о прикреплении членов семьи медицинским учреждениям',
    `filtr19` String COMMENT 'Информация об среднем образовании взрослых членов семьи',
    `filtr20` String COMMENT 'Информация об профессиональном образовании взрослых членов семьи',
    `filtr21` String COMMENT 'Информация об высшем образовании взрослых членов семьи',
    `filtr22` String COMMENT 'Информация о участии в ОСМС',
    `filtr23` String COMMENT 'Закредитованность семьи',
    `filtr24` String COMMENT 'Задолженность семьи по кредитам',
    `filtr25` String COMMENT 'Информация о прикреплении детей в семье к дошкольным учреждениям',
    `filtr26` String COMMENT 'Информация о прикреплении детей в семье к школьным образовательным учреждениям',
    `filtr27` String COMMENT 'Информация об образовании взрослых членов семьи',
    `count_iin` UInt64
) ENGINE = MergeTree()
ORDER BY
    FAMILY_ID;

-----TRUNCATE TABLE SOC_KARTA.FAMILY_APPEALS3;
INSERT INTO
    SOC_KARTA.FAMILY_APPEALS3
SELECT
    IIN,
    FAMILY_ID,
    FAMILY_CAT,
    REG_DATE,
    APPEAL_ID,
    APPEAL_TYPE,
    ORG_TYPE,
    ORG_NAME,
    ISSUE,
    SUBISSUE,
    DECISION,
    ID_SK_FAMILY_QUALITY2,
    FULL_KATO_NAME,
    KATO_2_NAME,
    KATO_2,
    KATO_4_NAME,
    KATO_4,
    filtr1,
    filtr2,
    filtr3,
    filtr4,
    filtr5,
    filtr6,
    filtr7,
    filtr8,
    filtr9,
    filtr10,
    filtr11,
    filtr12,
    filtr13,
    filtr14,
    filtr15,
    filtr16,
    filtr17,
    filtr18,
    filtr19,
    filtr20,
    filtr21,
    filtr22,
    filtr23,
    filtr24,
    filtr25,
    filtr26,
    filtr27,
    count_iin
FROM
    SOC_KARTA.FAMILY_APPEALS as set1
    left join SOC_KARTA.SEGMENTATION_ASSOGIN as set2 on toString(set1.FAMILY_ID) = set2.ID_SK_FAMILY_QUALITY2;

drop TABLE SOC_KARTA.SK_FAMILY_VITRINA;

CREATE TABLE SOC_KARTA.SK_FAMILY_VITRINA (
    `ID_SK_FAMILY_QUALITY2` Nullable(String) COMMENT 'ID Семьи',
    `count_iin` UInt64 COMMENT 'Количество людей в семье',
    `one` UInt64 COMMENT 'Есть ли главная женщина в семье MEMBER_TYPE =1 ',
    `two` UInt64 COMMENT 'Есть ли в семье MEMBER_TYPE =2 ',
    `coun` UInt64 COMMENT 'Есть ли в семье MEMBER_TYPE кроме 1 и 2 ',
    `FULL_KATO_NAME` String COMMENT 'Полный КАТО человека',
    `KATO_2` String COMMENT 'КАТО региона',
    `KATO_2_NAME` String COMMENT 'Регионы',
    `KATO_4` String COMMENT 'КАТО районы',
    `KATO_4_NAME` String COMMENT 'Районы',
    `KATO_6` String COMMENT 'КАТО Населенных пунктов',
    `KATO` String COMMENT 'КАТО 6 знаков',
    `count_18` UInt64,
    `count_18_less` UInt64,
    `IS__VILLAGE_IIN` Int64,
    `FAMILY_TYPE` Int8 COMMENT 'Тип семьи',
    `SUM_CNT_DETI18` UInt64,
    `AVG_INCOME_OOP_1_IIN` Float64,
    `SUM_CNT_EMPLOYABLE_IIN2` UInt64,
    `SUM_CNT_EMPLOYABLE_IIN3` UInt64,
    `SUM_CNT_NEDV_IIN` Int64,
    `AVG_SQ_MET_IIN` Float64,
    `SUM_SQ_MET_IIN` Float64,
    `SUM_CNT_DV_IIN` Int64,
    `SUM_CNT_DV_COM_IIN` Int64,
    `SUM_CNT_GRST_IIN` Int64,
    `SUM_ZEM_NEDV_DESC2` UInt64,
    `SUM_ZEM_NEDV_DESC1` UInt64,
    `SUM_CNT_NEDV_COM_IIN` Int64,
    `SUM_INCOME_LPH_IIN` Int64,
    `SUM_CNT_IP_IIN` Int64,
    `SUM_CNT_UL_IIN` Int64,
    `SUM_DETIDO23` UInt64,
    `SUM_INVALID1or2Group` UInt64,
    `SUM_INVALID1or2Group_18` UInt64,
    `SUM_INVALID1or2Group_18_less` UInt64,
    `SUM_INVALIDsDETSTVAorREBENOKinvalid` UInt64,
    `SUM_INVALID3GROUP` UInt64,
    `SUM_INVALID3GROUP_18` UInt64,
    `SUM_INVALID3GROUP_18_less` UInt64,
    `SUM_HRONIC` UInt64,
    `SUM_HRONIC_18` UInt64,
    `SUM_HRONIC_18_less` UInt64,
    `SUM_DUCHET` UInt64,
    `SUM_DUCHET_18` UInt64,
    `SUM_DUCHET_18_less` UInt64,
    `SUM_NEED_EDU_IIN` Int64,
    `SUM_NEED_MED_IIN2` UInt64,
    `SUM_NEED_MED_IIN2_18` UInt64,
    `SUM_NEED_MED_IIN2_18_less` UInt64,
    `SUM_EDU_SCHOOL` UInt64,
    `SUM_EDU_SRED` UInt64,
    `SUM_EDU_HIGHSCHOOL` UInt64,
    `SUM_OSMS` Int64,
    `SUM_COUNT_CREDIT` Int64,
    `SUM_COUNT_CREDIT_18` UInt64,
    `SUM_COUNT_CREDIT_18_less` UInt64,
    `GKB_PAYMENT_DAYS_OVERDUE_90_1000` UInt64
) ENGINE = MergeTree()
order by
    count_iin;

insert into
    SOC_KARTA.SK_FAMILY_VITRINA
SELECT
    ID_SK_FAMILY_QUALITY2,
    count_iin,
    one,
    two,
    coun,
    FULL_KATO_NAME,
    KATO_2,
    KATO_2_NAME,
    KATO_4,
    KATO_4_NAME,
    KATO_6,
    KATO,
    count_18,
    count_18_less,
    IS__VILLAGE_IIN,
    FAMILY_TYPE,
    SUM_CNT_DETI18,
    AVG_INCOME_OOP_1_IIN,
    SUM_CNT_EMPLOYABLE_IIN2,
    SUM_CNT_EMPLOYABLE_IIN3,
    SUM_CNT_NEDV_IIN,
    AVG_SQ_MET_IIN,
    SUM_SQ_MET_IIN,
    SUM_CNT_DV_IIN,
    SUM_CNT_DV_COM_IIN,
    SUM_CNT_GRST_IIN,
    SUM_ZEM_NEDV_DESC2,
    SUM_ZEM_NEDV_DESC1,
    SUM_CNT_NEDV_COM_IIN,
    SUM_INCOME_LPH_IIN,
    SUM_CNT_IP_IIN,
    SUM_CNT_UL_IIN,
    SUM_DETIDO23,
    SUM_INVALID1or2Group,
    SUM_INVALID1or2Group_18,
    SUM_INVALID1or2Group_18_less,
    SUM_INVALIDsDETSTVAorREBENOKinvalid,
    SUM_INVALID3GROUP,
    SUM_INVALID3GROUP_18,
    SUM_INVALID3GROUP_18_less,
    SUM_HRONIC,
    SUM_HRONIC_18,
    SUM_HRONIC_18_less,
    SUM_DUCHET,
    SUM_DUCHET_18,
    SUM_DUCHET_18_less,
    SUM_NEED_EDU_IIN,
    SUM_NEED_MED_IIN2,
    SUM_NEED_MED_IIN2_18,
    SUM_NEED_MED_IIN2_18_less,
    SUM_EDU_SCHOOL,
    SUM_EDU_SRED,
    SUM_EDU_HIGHSCHOOL,
    SUM_OSMS,
    SUM_COUNT_CREDIT,
    SUM_COUNT_CREDIT_18,
    SUM_COUNT_CREDIT_18_less,
    GKB_PAYMENT_DAYS_OVERDUE_90_1000
FROM
    (
        SELECT
            ID_SK_FAMILY_QUALITY2,
            count(distinct(IIN)) as count_iin,
            indexOf(groupArray(MEMBER_TYPE), 1) as one,
            indexOf(groupArray(MEMBER_TYPE), 2) as two,
            indexOf(groupArray(MEMBER_TYPE), 3) as three,
            indexOf(groupArray(MEMBER_TYPE), 4) as four,
            indexOf(groupArray(MEMBER_TYPE), 5) as five,
            indexOf(groupArray(MEMBER_TYPE), 13) as thirteen,
            if(
                one > 0,
                one,
                if(
                    two > 0,
                    two,
                    if(
                        three > 0,
                        three,
                        if(four > 0, four, if(five > 0, five, thirteen))
                    )
                )
            ) as coun,
            count(if(PERSON_AGE >= 18, 1, 0)) as count_18,
            count(if(PERSON_AGE < 18, 1, 0)) as count_18_less,
            SUM(IS_VILLAGE_IIN) as IS__VILLAGE_IIN,
            MAX(FAMILY_TYPE) as FAMILY_TYPE,
            SUM(CNT_DETI18) as SUM_CNT_DETI18,
            AVG(INCOME_OOP_1_IIN) as AVG_INCOME_OOP_1_IIN,
            SUM(CNT_EMPLOYABLE_IIN2) as SUM_CNT_EMPLOYABLE_IIN2,
            sum(CNT_EMPLOYABLE_IIN3) as SUM_CNT_EMPLOYABLE_IIN3,
            SUM(CNT_NEDV_IIN) as SUM_CNT_NEDV_IIN,
            AVG(SQ_MET_IIN) as AVG_SQ_MET_IIN,
            SUM(SQ_MET_IIN) as SUM_SQ_MET_IIN,
            SUM(CNT_DV_IIN) as SUM_CNT_DV_IIN,
            SUM(CNT_DV_COM_IIN) as SUM_CNT_DV_COM_IIN,
            SUM(CNT_GRST_IIN) as SUM_CNT_GRST_IIN,
            SUM(ZEM_NEDV_DESC2) as SUM_ZEM_NEDV_DESC2,
            SUM(ZEM_NEDV_DESC1) as SUM_ZEM_NEDV_DESC1,
            SUM(CNT_NEDV_COM_IIN) as SUM_CNT_NEDV_COM_IIN,
            SUM(INCOME_LPH_IIN) as SUM_INCOME_LPH_IIN,
            SUM(CNT_IP_IIN) as SUM_CNT_IP_IIN,
            SUM(CNT_UL_IIN) as SUM_CNT_UL_IIN,
            SUM(DETIDO23) as SUM_DETIDO23,
            SUM(INVALID1or2Group) as SUM_INVALID1or2Group,
            SUM(
                IF(
                    INVALID1or2Group = 1
                    AND PERSON_AGE >= 18,
                    1,
                    0
                )
            ) as SUM_INVALID1or2Group_18,
            SUM(
                IF(
                    INVALID1or2Group = 1
                    AND PERSON_AGE < 18,
                    1,
                    0
                )
            ) as SUM_INVALID1or2Group_18_less,
            SUM(INVALIDsDETSTVAorREBENOKinvalid) as SUM_INVALIDsDETSTVAorREBENOKinvalid,
            SUM(INVALID3GROUP) as SUM_INVALID3GROUP,
            SUM(
                IF(
                    INVALID3GROUP = 1
                    AND PERSON_AGE >= 18,
                    1,
                    0
                )
            ) as SUM_INVALID3GROUP_18,
            SUM(
                IF(
                    INVALID3GROUP = 1
                    AND PERSON_AGE < 18,
                    1,
                    0
                )
            ) as SUM_INVALID3GROUP_18_less,
            SUM(HRONIC) as SUM_HRONIC,
            SUM(
                IF(
                    HRONIC = 1
                    AND PERSON_AGE >= 18,
                    1,
                    0
                )
            ) as SUM_HRONIC_18,
            SUM(
                IF(
                    HRONIC = 1
                    AND PERSON_AGE < 18,
                    1,
                    0
                )
            ) as SUM_HRONIC_18_less,
            SUM(if(DUCHET == 'D-UCHET', 1, 0)) as SUM_DUCHET,
            SUM(
                if(
                    DUCHET == 'D-UCHET'
                    AND PERSON_AGE >= 18,
                    1,
                    0
                )
            ) as SUM_DUCHET_18,
            SUM(
                if(
                    DUCHET == 'D-UCHET'
                    AND PERSON_AGE < 18,
                    1,
                    0
                )
            ) as SUM_DUCHET_18_less,
            SUM(NEED_EDU_IIN) as SUM_NEED_EDU_IIN,
            SUM(NEED_MED_IIN2) as SUM_NEED_MED_IIN2,
            sum(
                if(
                    NEED_MED_IIN2 = 1
                    AND PERSON_AGE >= 18,
                    1,
                    0
                )
            ) as SUM_NEED_MED_IIN2_18,
            sum(
                if(
                    NEED_MED_IIN2 = 1
                    AND PERSON_AGE < 18,
                    1,
                    0
                )
            ) as SUM_NEED_MED_IIN2_18_less,
            SUM(EDU_SCHOOL) as SUM_EDU_SCHOOL,
            SUM(EDU_SRED) as SUM_EDU_SRED,
            SUM(EDU_HIGHSCHOOL) as SUM_EDU_HIGHSCHOOL,
            SUM(OSMS) as SUM_OSMS,
            sum(COUNT_CREDIT) as SUM_COUNT_CREDIT,
            sum(
                if(
                    COUNT_CREDIT = 1
                    AND PERSON_AGE >= 18,
                    1,
                    0
                )
            ) as SUM_COUNT_CREDIT_18,
            sum(
                if(
                    COUNT_CREDIT = 1
                    AND PERSON_AGE < 18,
                    1,
                    0
                )
            ) as SUM_COUNT_CREDIT_18_less,
            sum(
                if(
                    PERSON_AGE >= 18,
                    if(
                        GKB_PAYMENT_DAYS_OVERDUE <= 20,--
                        if(GKB_DEBT_PASTDUE_VALUE < 110, 1, 0),
                        0
                    ),
                    0
                )
            ) as GKB_PAYMENT_DAYS_OVERDUE_90_1000
        FROM
            SOC_KARTA.SK_FAMILY_QUALITY_IIN3
        where
            ID_SK_FAMILY_QUALITY != 0
        group by
            ID_SK_FAMILY_QUALITY2
    ) as set1
    left join SOC_KARTA.SK_FAMILY_VITRINA_KATO as set2 on set1.ID_SK_FAMILY_QUALITY2 = set2.ID_SK_FAMILY_QUALITY2;

---------------------------
-- SOC_KARTA.SEGMENTATION_GROUP_FINAL4 definition
drop table SOC_KARTA.SEGMENTATION_GROUP_FINAL4;

CREATE TABLE SOC_KARTA.SEGMENTATION_GROUP_FINAL4 (
    `ID_SK_FAMILY_QUALITY2` String COMMENT 'ID Семьи',
    `FULL_KATO_NAME` String COMMENT 'Полный КАТО человека',
    `KATO_2_NAME` String COMMENT 'Регионы',
    `KATO_2` String COMMENT 'КАТО региона',
    `KATO_4_NAME` String COMMENT 'Районы',
    `KATO_4` String COMMENT 'КАТО районы',
    `FAMILY_CAT` String COMMENT 'Категория благополучия семьи',
    `filtr1` String COMMENT 'Уровень среднедушевого дохода (СДД) в семье',
    `filtr2` String COMMENT 'Информация о занятости членов семьи',
    `filtr3` String COMMENT 'Наличие в собственности семьи недвижимого имущества',
    `filtr4` String COMMENT 'Наличие в собственности семьи движимого имущества',
    `filtr5` String COMMENT 'Наличие в собственности семьи коммерческого движимого имущества',
    `filtr6` String COMMENT 'Наличие сельскохозяйственной техники',
    `filtr7` String COMMENT 'Наличие в собственности земельного участка',
    `filtr8` String COMMENT 'Наличие в собственности семьи коммерческого недвижимого имущества',
    `filtr9` String COMMENT 'Наличие у семьи личного подсобного хозяйства',
    `filtr10` String COMMENT 'Учредитель в ЮЛ и ИП',
    `ID_SK_FAMILY_QUALITY22` String,
    `filtr11` String COMMENT 'Присутствие в семье четырех и более несовершеннолетних детей и учащейся молодежи до 23 лет',
    `filtr12` String COMMENT 'Наличие в семье лица с инвалидностью (1 и 2 группы)',
    `filtr13` String COMMENT 'Наличие в семье лица с инвалидностью с детства или ребенка - инвалида',
    `filtr14` String COMMENT 'Наличие в семье лица с инвалидностью (3 группы)',
    `filtr15` String COMMENT 'Информация о наличии у членов семьи хронических заболеваний',
    `filtr16` String COMMENT 'Информация о наличии у членов семьи Д - учета',
    `filtr17` String COMMENT 'Информация о прикреплении детей в семье к дошкольным и школьным образовательным учреждениям',
    `filtr18` String COMMENT 'Информация о прикреплении членов семьи медицинским учреждениям',
    `filtr19` String COMMENT 'Информация об среднем образовании взрослых членов семьи',
    `filtr20` String COMMENT 'Информация об профессиональном образовании взрослых членов семьи',
    `filtr21` String COMMENT 'Информация об высшем образовании взрослых членов семьи',
    `filtr22` String COMMENT 'Информация о участии в ОСМС',
    `filtr23` String COMMENT 'Закредитованность семьи',
    `filtr24` String COMMENT 'Задолженность семьи по кредитам',
    `filtr25` String COMMENT 'Информация о прикреплении детей в семье к дошкольным учреждениям',
    `filtr26` String COMMENT 'Информация о прикреплении детей в семье к школьным образовательным учреждениям',
    `filtr27` String COMMENT 'Информация об образовании взрослых членов семьи',
    `filtr28` String COMMENT 'Получатели АСП',
    `filtr29` String COMMENT 'Плательщики ЕСП',
    `filtr30` String COMMENT 'плательщик ОПВ',
    `filtr31` String COMMENT 'Проживает город/село',
    `filtr32` String COMMENT 'Наличие социально значимых заболевании',
    `filtr33` String COMMENT 'Подавал ли кто-то из семьи обращение',
    `filtr34` String COMMENT 'от 8 (вкл) до 14 лет (вкл) не ходящие в школу',
    `count_iin` UInt64 COMMENT 'Количество людей в семье',
    `KATO_42` String COMMENT 'КАТО 4 районы',
    `value_rayon` UInt64 COMMENT 'Значение по Району',
    `max_value` UInt64 COMMENT 'Максимальное значение по региону ',
    `Rating` String COMMENT 'Рейтинг',
    `max_reg` UInt64 COMMENT 'Максимальное значение по региону '
) ENGINE = MergeTree()
ORDER BY
    ID_SK_FAMILY_QUALITY2 SETTINGS parts_to_throw_insert = 50000,
    index_granularity = 8192;

INSERT INTO
    SOC_KARTA.SEGMENTATION_GROUP_FINAL4
SELECT
    sgf.ID_SK_FAMILY_QUALITY2,
    FULL_KATO_NAME,
    sgf.KATO_2_NAME,
    KATO_2,
    KATO_4_NAME,
    toInt32OrZero(KATO_4) AS KATO_4,
    (
        case
            when FAMILY_CAT = 'A' then 'А - благополучный уровень семьи (средний и выше)'
            when FAMILY_CAT = 'B' then 'В - удовлетворительный уровень семьи (ниже среднего)'
            when FAMILY_CAT = 'C' then 'С - Неблагополучный уровень семьи (необходим мониторинг) '
            when FAMILY_CAT = 'D' then 'D - Кризисный уровень семьи (требуется помощь)'
            when FAMILY_CAT = 'E' then 'E - Экстренный уровень семьи (требуется срочная помощь)'
        end
    ) FAMILY_CAT,
    REPLACE(
        filtr1,
        'Уровень среднедушевого дохода в семье – ',
        ''
    ) AS filtr1,
    filtr2,
    filtr3,
    filtr4,
    filtr5,
    filtr6,
    (
        case
            when filtr7 = 'В семье есть земельный участок под сельхозземельные участки (более 1 Га)' then 'В семье есть сельхозземельные участки (более 1 Га)'
            when filtr7 = 'В семье есть земельный участок под индивидуальное жилищное строительство' then 'В семье есть земельный участок под ИЖС'
            when filtr7 = 'В семье есть земельный участок под индивидуальное жилищное строительство и земельный участок под сельхозземельные участки (более 1 Га)' then 'В семье есть земельный участок под ИЖС и сельхозземельные участки (более 1 Га)'
            else filtr7
        end
    ) as filtr7,
    filtr8,
    filtr9,
    filtr10,
    ID_SK_FAMILY_QUALITY22,
    (
        case
            when filtr11 = 'В семье есть три несовершеннолетних детей и учащейся молодежи до 23 лет' then 'В семье есть трое несовершеннолетних детей и учащейся молодежи до 23 лет'
            when filtr11 = 'В семье есть один несовершеннолетнии ребенок или учащейся молодеж до 23 лет' then 'В семье есть один несовершеннолетний ребенок или учащейся молодеж до 23 лет'
            else filtr11
        end
    ) as filtr11,
    (
        case
            when filtr12 = 'В семье отсутсвуют лица с инвалидностью (1 и 2 группы)' then 'В семье нет лиц с инвалидностью (1 и 2 группы)'
            else filtr12
        end
    ) as filtr12,
    filtr13,
    filtr14,
    filtr15,
    filtr16,
    filtr17,
    filtr18,
    filtr19,
    (
        case
            when filtr20 = 'Семьи, в которых у взрослых членов семьи нет профессионального и среднеспециального образование' then 'В семье нет профессионального и среднеспециального образование'
            when filtr20 = 'Семьи, в которых у взрослых членов семьи есть профессиональное и среднеспециальное образование' then 'В семье есть профессиональное и среднеспециальное образование'
            else filtr20
        end
    ) as filtr20,
    filtr21,
    filtr22,
    filtr23,
    (
        case
            when filtr24 = 'Все совершеннолетние члены семьи имеет просроченную задолженность по кредитам больше 90 дней и свыше 1000 тенге' then 'Члены семьи имеют просроченную задолженность по кредитам'
            when filtr24 = 'Один совершеннолетнии член семьи имеет просроченную задолженность по кредитам больше 90 дней и свыше 1000 тенге' then 'Один член семьи имеет просроченную задолженность по кредитам'
            when filtr24 = 'Семья не имеет задолженности по кредитам больше 90 дней и свыше 1000 тенге' then 'Семья не имеет задолженности по кредитам'
            else filtr24
        end
    ) as filtr24,
    filtr25,
    filtr26,
    filtr27,
    filtr28,
    filtr29,
    filtr30,
    filtr31,
    filtr32,
    IF(
        fa.FAMILY_ID = 0,
        'Нет обращения',
        'Есть обращение'
    ) filtr33,
    filtr34,
    count_iin,
    KATO_42,
    value_rayon,
    max_value,
    Rating,
    max_reg
FROM
    SOC_KARTA.SEGMENTATION_GROUP_FINAL3 AS sgf
    LEFT JOIN (
        SELECT
            DISTINCT FAMILY_ID
        FROM
            SOC_KARTA.FAMILY_APPEALS3
    ) AS fa ON sgf.ID_SK_FAMILY_QUALITY2 = toString(fa.FAMILY_ID)
    LEFT JOIN (
        select
            ID_SK_FAMILY_QUALITY,
            (
                case
                    when SUM(DETI8) > 0
                    and SUM(CNTDETI8) > 0 then 'В семье есть дети от 8 до 14 лет включительно не прикрепленые к школе'
                    when SUM(DETI8) < 1
                    and SUM(CNTDETI8) > 0 then 'В семье есть дети от 8 до 14 лет включительно прикрепленые к школе'
                    when SUM(CNTDETI8) < 1 then 'В семье нет детей от 8 до 14 лет включительно'
                    else 'netu'
                end
            ) as filtr34,
            if(
                MAX(IS_VILLAGE_IIN) > 0,
                'Семья живет в селе',
                'Семья живет в городе'
            ) as filtr31,
            if(
                sum(SSD_PATIENTS) > 0,
                'В Семье есть социально значимые заболевания',
                'В Семье нет социально значимых заболевании'
            ) as filtr32
        from
            (
                SELECT
                    ID_SK_FAMILY_QUALITY,
                    IS_VILLAGE_IIN,
                    SSD_PATIENTS,
                    if(
                        BIRTH_DATE_IIN is null,
                        -1,
                        toInt64(
                            datediff(
                                'day',
                                toDateOrZero(BIRTH_DATE_IIN),
                                toDate('2022-06-01 00:00:00')
                            ) / 365
                        )
                    ) as raz,
                    if(
                        raz < 15
                        and raz > 7
                        and IS_SCHOOL_PRIKR_IIN == 0,
                        1,
                        0
                    ) as DETI8,
                    if(
                        raz < 15
                        and raz > 7,
                        1,
                        0
                    ) as CNTDETI8
                FROM
                    SOC_KARTA.SK_FAMILY_QUALITY_IIN
            )
        group by
            ID_SK_FAMILY_QUALITY
    ) as set6 on sgf.ID_SK_FAMILY_QUALITY2 = toString(set6.ID_SK_FAMILY_QUALITY)
    left join (
        SELECT
            KATO_2_NAME,
            count(distinct(ID_SK_FAMILY_QUALITY2)) max_reg
        FROM
            SOC_KARTA.SK_FAMILY_VITRINA_KATO
        group by
            KATO_2_NAME
    ) as set7 on sgf.KATO_2_NAME = set7.KATO_2_NAME;
