--DELTA
SELECT if(max(DATA_REG)is null,'1900-01-01 00:00:00',max(DATA_REG)) FROM DM_ZEROS.DM_MU_RN_PROPERTY_IIN_RNN where DATA_REG not in('2099-12-28 15:40:00','2096-05-12 12:20:00','2093-04-12 10:20:00')

--main script
SELECT 
obj.c_obj_id AS UH_OH,
obj.cm2970_1 AS FUNCTION_NAZNACHENIYA,
obj.cm532 as TYPE_OF_PROPERTY,
oobj.cm1211_1 as DESC_TYPE_OF_PROPERTY,
fos.cm1275_1 AS FORM_SOB,
l.cm1527 as IIN,
rz2.cm1560 as DATA_REG,
rz2.cm7041_1 as PRAV_DOK,
rz2.cm911 AS SUMMA_SDELKI,
vp.cm1199_1 as VID_PRAVA,
a.cm3520_1 as ADRESS,
a.cm13773 as AR_CODE,
strn.cm2694 PLOSH_OB,
strn.cm2695 PLOSH_ZHILAYA,
rz1.cm889 as PLOSH_ZEM,
rz1.cm890 as PLOSH_ZHIL_NEW,
ate.cm8521 as KATO,
oobj.cm13062 as RAZDELENIYE_IMUSH
from RN3.ct159 obj
left join RN3.ct191 oobj on oobj.c_obj_id = obj.cm532
left join RN3.ct182 a on a.c_obj_id= obj.cm533 and a.c_tr_id = 0
left join RN3.ct183 ate on ate.c_obj_id=a.cm3545 and ate.c_tr_id = 0
left join RN3.CT308 rz1 on rz1.cm894 = obj.c_obj_id and rz1.C_TR_ID = 0
left join RN3.CT719 zrz on zrz.cm2860 = rz1.c_obj_id and zrz.C_TR_ID = 0
left join RN3.CT310 rz2 on rz2.c_obj_id = zrz.cm2861  and rz2.C_TR_ID = 0
left join RN3.CT256 vp on vp.c_obj_id = rz2.cm927 and vp.C_TR_ID = 0
left join RN3.CT242 l on l.c_obj_id = rz2.cm929 and l.C_TR_ID = 0
LEFT JOIN RN3.CT284 fos on rz2.cm928=fos.c_obj_id and fos.C_TR_ID = 0
left join rn3.ct759 zontp on zontp.cm3000 = obj.c_obj_id and zontp.c_tr_id = 0
left join rn3.ct819 at on at.cm3331 = obj.c_obj_id
left join rn3.ct519 tp on tp.c_obj_id = zontp.cm3002  and tp.c_tr_id = 0
left join (
SELECT cm2694,cm2695,cm3082, c_tr_id from rn3.ct640 where c_obj_id in (
SELECT max(c_obj_id) as c_obj_id FROM rn3.ct640 group by cm3082)
) strn on strn.cm3082 = tp.c_obj_id and strn.c_tr_id = 0
where obj.cm5292 = 14162629 AND rz1.cm5294 = 14162629 AND rz2.cm1560 is not NULL AND rz2.cm5293 = 14162629
and rz2.cm1560>TO_DATE(?, 'YYYY-MM-DD HH24:MI:SS') and rz2.cm1560 < trunc(sysdate +1)