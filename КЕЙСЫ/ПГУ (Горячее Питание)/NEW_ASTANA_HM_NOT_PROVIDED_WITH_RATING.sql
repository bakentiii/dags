SELECT COUNT(DISTINCT e2.ID),
s.ID                                    as school_id,
       s.RU_NAME                               AS ORGANIZATION_NAME,
       AREA.RNAME                              AS OBLAST,
       REGION.RNAME                            AS REGION,
       LOCALITY.RNAME                          AS NASELENNI_PUNKT,
       e2.CLASS_NUM_ID , 
      e2.IS_ORPHAN ,
      efp.HOTMEAL_PROVIDE_ID,
      s2.ID,s2.IIN,
      e2.ID,
      sf.INCOME_ASP  as ASP,
      sf.SDD  as SDD,
      sfm.SK_FAMILY_ID as  SK_FAMILY_ID
        FROM MON_NOBD.EDUCATION e2
                 INNER JOIN MON_NOBD.STUDENT_ATTR sa ON sa.STUDENT_ID = e2.STUDENT_ID
                 INNER JOIN MON_NOBD.EDU_FOODPROVIDE efp ON e2.ID = efp.EDUCATION_ID
                 LEFT JOIN MON_NOBD.SCHOOL s on s.ID = e2.SCHOOL_ID
                INNER JOIN MON_NOBD.STUDENT s2  on s2.ID = e2.STUDENT_ID
         INNER JOIN (SELECT ATTR.SCHOOL_ID, ATTR.DATE_CLOSE, ATTR.REGION_ID, ATTR.TYPE_SCHOOL_ID, ATTR.ID
                     FROM MON_NOBD.SCHOOL_ATTR ATTR
                     WHERE ATTR.EDU_PERIOD_ID = 0 and ATTR.DEP_ID IN ('1', '2') and ATTR.KFS_ID=3 and ATTR.HAS_BUFFET is not NULL)SAA ON SAA.SCHOOL_ID = e2.SCHOOL_ID
        -- INNER JOIN MON_NOBD.EDU_FOODPROVIDE FP ON FP.EDUCATION_ID = e2.id
         INNER JOIN MON_NOBD.D_REGION LOCALITY ON SAA.REGION_ID = LOCALITY.ID
         LEFT JOIN MON_NOBD.D_REGION AREA ON LOCALITY.AREA_ID = AREA.ID
         LEFT JOIN MON_NOBD.D_REGION REGION ON LOCALITY.DISTRICT_ID = REGION.ID
         LEFT JOIN MON_NOBD.SCHOOL_SPEC ss on ss.SCHOOL_ATTR_ID= SAA.ID
         LEFT JOIN SK_FAMILY.SK_FAMILY_MEMBER sfm ON sfm.IIN = s2.IIN 
         LEFT JOIN SK_FAMILY.SK_FAMILY_QUALITY sf on sfm.SK_FAMILY_ID = sf.ID 
        WHERE s.ID = e2.SCHOOL_ID
          AND e2.EDU_PERIOD_ID = 0
          AND e2.EDU_STATUS IN (0,4)
          AND e2.CLASS_NUM_ID IN ('7', '8', '9', '10', '11', '12', '13', '14', '15')
          and e2.LEARN_EVENING_ID in ('6')
         AND efp.BUFFET_PROVIDE_ID =1--.hotMeal_provide_id is NULL 
 and REGION.CODE like '71%'
  and LOCALITY.CODE like '71%'
  and AREA.CODE like '71%'
  and SAA.ID IN (
    SELECT SS.SCHOOL_ATTR_ID
    FROM MON_NOBD.SCHOOL_SPEC SS
             LEFT JOIN MON_NOBD.D_SCHOOLSPEC_TYPE SST ON SS.SPEC_TYPE_ID = SST.ID
    WHERE SST.CODE LIKE '02.1.%'
       OR SST.CODE LIKE '02.2.%'
       OR SST.CODE LIKE '02.3.%'
       OR SST.CODE LIKE '02.4.%'
       OR SST.CODE LIKE '02.5.%'
       OR SST.CODE LIKE '07.%'
       OR SST.CODE IN ('02.6.1', '02.6.2', '02.6.3', '02.6.4'))-- and s.id=19602
         AND s.DATE_CLOSE1 IS NULL
         AND SDD <= 40567 and SDD != 0 and SDD is not null
  group by s.ID, s.RU_NAME,s.KK_NAME,AREA.RNAME, REGION.RNAME, LOCALITY.RNAME,e2.IS_ORPHAN,e2.CLASS_NUM_ID,efp.HOTMEAL_PROVIDE_ID,s2.ID,s2.IIN,e2.ID,sf.INCOME_ASP,sf.SDD,SK_FAMILY_ID  --,ATTR.has_buffet-





-------FOR RATING
select 
a.*,
sg.FAMILY_CAT as FAMILY_CAT,
sg.Rating as Rating
from {rsult of previous script} a
left join
(
  select distinct SK_FAMILY_ID from
  (select sfm.SK_FAMILY_ID  from PROACTIVE.PROACTIVE_COUNT pc
  left join SK_FAMILY.SK_FAMILY_MEMBER sfm on toString(sfm.IIN)=pc.IIN
  where NAME_RU='Назначение государственной адресной социальной помощи'
  union all
  select DISTINCT SK_FAMILY_ID from PROACTIVE.MTSZN_STATUS ms)
) 
b on b.SK_FAMILY_ID=toInt64(a.SK_FAMILY_ID)
left join SK_FAMILY.SEGMENTATION_GROUP_FINAL4 sg on sg.ID_SK_FAMILY_QUALITY2=toString(a.SK_FAMILY_ID)
left join SK_FAMILY.SK_FAMILY_QUALITY q on q.ID=toInt64(a.SK_FAMILY_ID)
left join 
(
  SELECT DISTINCT e2.STUDENT_ID, s.IIN as IIN 
        FROM MON_NOBD.STUDENT_ATTR sa
        left join MON_NOBD.STUDENT s on sa.STUDENT_ID = s.ID 
                 INNER JOIN MON_NOBD.EDUCATION e2  ON e2.STUDENT_ID=sa.STUDENT_ID 
        WHERE --s.ID = e2.SCHOOL_ID
        -- e1.class_num_id = e2.class_num_id
           e2.EDU_PERIOD_ID = 0
          AND e2.EDU_STATUS IN (0, 4)
          AND e2.CLASS_NUM_ID IN ('3', '4', '5', '6', '7', '8', '9', '10', '11', '12', '13', '14', '15')
          and e2.LEARN_EVENING_ID in ('6')
          AND sa.IS_ORPHAN  IN ('1')
 ) s on s.IIN=a.IIN
