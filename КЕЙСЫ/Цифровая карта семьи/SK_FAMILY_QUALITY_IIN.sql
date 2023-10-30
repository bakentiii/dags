--SK_FAMILY_QUALITY_IIN
select distinct
cm.IIN as IIN, 
sfm.BIRTH_DATE as BIRTH_DATE_IIN,
fcr.FAMILY_ID AS ID_SK_FAMILY_QUALITY,
if(e.INCOME_ESP_IIN is null,0,e.INCOME_ESP_IIN) AS INCOME_ESP_IIN,
IF(IIN IN (SELECT * FROM DICTIONARY.STUDENTS_IIN),1,0) AS IS_STUDENT_IIN,
ROUND(IF(ifNull(opv_3.OPV_ZP_3,0)=0,ifNull(soc_3.SOC_ZP_3,0),opv_3.OPV_ZP_3)) AS INCOME_OOP_3_IIN,
ROUND(IF(ifNull(opv12.OPV_ZP_12,0)=0,ifNull(soc_12.SOC_ZP_12,0),opv12.OPV_ZP_12)) AS INCOME_OOP_12_IIN,
ifNull(pos.cnt,0) AS INCOME_CBD_IIN,
ifNull(nz.CNT_NEDV_IIN,0) as CNT_NEDV_IIN,
ifNull(nn.CNT_NEDV_COM_IIN,0) as CNT_NEDV_COM_IIN,
ifNull(nedp.CNT_NEDV_PROCHEE_IIN,0) as CNT_NEDV_PROCHEE_IIN,
if(fcr.RPN_ATTACH = 1,0,1) as NEED_MED_IIN,
ifNull(zem.NEDV_ZU_IIN,0) as CNT_NEDV_ZU_IIN,
aut.cnt_dv_iin as CNT_DV_IIN,
aut.cnt_dv_com_iin as CNT_DV_COM_IIN,
if(ibd.IP in (select IIN from SK_FAMILY.FAMILY_CASE_RPN),1,0) as CNT_IP_IIN,
ifNull(msh.cnt_grst_iin,0) as CNT_GRST_IIN,
edu.NEED_EDU_IIN as NEED_EDU_IIN,
if(INCOME_OOP_3_IIN>0,1,0) as CNT_EMPLOYABLE_IIN,
tr.IS_TRUDOSPOSOB_IIN as IS_TRUDOSPOSOB_IIN,
st.TRUD as TRUD_STAT_32,
bi.IS_PREGNANT_IIN as IS_PREGNANT_IIN,
bi.IS_INVALID_IIN as IS_INVALID_IIN,
if(u.UCHR_IIN in (select IIN from SK_FAMILY.FAMILY_CASE_RPN),1,0) as CNT_UL_IIN,
if(sad.IIN = '',0,1) as IS_DET_SAD_IIN,
if(wko.IIN = '',0,1) as IS_SCHOOL_PRIKR_IIN,
if(so.IIN ='',0,1) as IS_VILLAGE_IIN,
ifNull(zem.PLOSH_ZEM,0) as PLOSH_ZEM,
if(CNT_NEDV_IIN = 0,1,0) as NEED_NEDV_IIN,
if(CNT_NEDV_IIN<1,0.0,nz.M2) as SQ_MET_IIN,
if(CNT_NEDV_COM_IIN<1,0.0,nn.M2_COM) as SQ_MET_COM_IIN,
if(CNT_NEDV_PROCHEE_IIN<1,0.0,nedp.M2_PROCHEE) as SQ_MET_PROCHEE,
IF(IS_TRUDOSPOSOB_IIN = 1 AND INCOME_OOP_12_IIN=0 AND CNT_IP_IIN=0 AND CNT_UL_IIN=0 AND INCOME_ESP_IIN=0 AND IS_STUDENT_IIN=0 AND IS_INVALID_IIN=0 AND IS_PREGNANT_IIN =0 ,1,0) as NEED_EMP_IIN,
multiIf(PLOSH_ZEM = 0,0,PLOSH_ZEM<1,1,2)as ZEM_NEDV_DESC,
ifNull(lph.CNT_LPH_IIN,0) as CNT_LPH_IIN,
ifNull(lph.INCOME_LPH_IIN,0) as INCOME_LPH_IIN,
ifNull(asp.CALC_SUM,0) as INCOME_ASP_IIN,
if(upper(os.IIN) in (select IIN from SK_FAMILY.FAMILY_CASE_RPN),1,0) as OSMS,
INCOME_ASP_IIN + ifNull(e.AMOUNT,0) + INCOME_OOP_3_IIN + INCOME_CBD_IIN AS TOTAL_INCOME_IIN,
fcr.FAMILY_TYPE as FAMILY_TYPE,
fcr.MEMBER_TYPE as MEMBER_TYPE,
if(ssd.IIN is null,0,1) as SSD_PATIENTS,
ifNull(gkb.COUNT_CREDIT,0) as GKB_COUNT_CREDIT,
ifNull(gkb.AMOUNT,0) as GKB_AMOUNT,
ifNull(gkb.DEBT_VALUE,0) as GKB_DEBT_VALUE,
ifNull(gkb.DEBT_PASTDUE_VALUE,0) as GKB_DEBT_PASTDUE_VALUE,
ifNull(gkb.PAYMENT_DAYS_OVERDUE,0) as GKB_PAYMENT_DAYS_OVERDUE
from SK_FAMILY.FAMILY_CASE_RPN cm 
left join SK_FAMILY.SK_FAMILY_MEMBER sfm on cm.IIN=sfm.IIN
left join DM_ZEROS.DM_MU_GBL_PERSON_LIVE_KZ_CITZN_sample_test  ctz on cm.IIN=ctz.IIN 
left join SK_FAMILY.OPV_ZP_3 opv_3 on cm.IIN = opv_3.RNN
left join SK_FAMILY.OPV_ZP_12 opv12  on cm.IIN = opv12.RNN 
left join SK_FAMILY.SOC_ZP_12 soc_12 on cm.IIN = soc_12.RNN 
left join SK_FAMILY.SOC_ZP_3 soc_3 on cm.IIN = soc_3.RNN 
left join SK_FAMILY.SOC_VIPLATI pos on cm.IIN = pos.RNN 
left join SK_FAMILY.NEDV_ZHIL nz on cm.IIN =nz.IIN 
left join SK_FAMILY.NEDV_NEZHIL nn on cm.IIN =nn.IIN 
left join SK_FAMILY.NEDV_ZEM zem on cm.IIN =zem.IIN
left join MCRIAP_RPEP_EGOV1_LK.Count_TS_FL aut on  cm.IIN = aut.IIN
left join (select distinct IP from SK_FAMILY.IP_BIN_DEISTVUIUSHIE) ibd on cm.IIN = ibd.IP 
left join DM_ZEROS.MSH_ZEROS msh on cm.IIN = msh.IIN
left join SK_FAMILY.NEED_EDU_IIN edu on cm.IIN = edu.IIN 
left join SK_FAMILY.BEREM_INVALIDY bi on cm.IIN = bi.IIN
--left join SK_FAMILY.TRUDOSPOSBNIE tr on cm.IIN = tr.IIN
left join SK_FAMILY.IS_TRUDOSPOSOB_IIN tr on cm.IIN = tr.IIN
left join SK_FAMILY.STATUS32 st on cm.IIN = st.IIN
left join SK_FAMILY.ESP e on cm.IIN = e.IIN 
left join SK_FAMILY.FAMILY_CASE_RPN fcr on cm.IIN = fcr.IIN 
left join SK_FAMILY.UCHREDITELI u on cm.IIN = u.UCHR_IIN 
left join SK_FAMILY.VILLAGE_IIN so on cm.IIN = so.IIN
left join (SELECT * from SK_FAMILY.MTZSN_FAMILTY_ASP where counter=2) asp on cm.IIN = asp.IIN 
left join (select distinct upper(IIN) as IIN from SK_FAMILY.OSMS) os on cm.IIN = os.IIN
left join SK_FAMILY.STUDENT_SADIK sad on cm.IIN = sad.IIN
left join SK_FAMILY.STUDENT_WKOLA wko on cm.IIN = wko.IIN
left join (select OWNER , sum(KOLVO) as CNT_LPH_IIN, sum(SUMMA) as INCOME_LPH_IIN from MSH_ISZH.LPH_CASE lc group by OWNER ) lph on cm.IIN =lph.OWNER
left join (select distinct IIN from SK_FAMILY.SSD_PATIENTS) as ssd ON cm.IIN = ssd.IIN
left join SK_FAMILY.GKB gkb on cm.IIN = gkb.HASH_IIN
left join SK_FAMILY.NEDV_PROCHEE nedp on cm.IIN = nedp.IIN
