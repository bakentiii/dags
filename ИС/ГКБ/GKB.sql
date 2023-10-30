--DELTA
SELECT max(REP_DATE) from SK_FAMILY.GKB_HIST

--SCRIPT
SELECT
  debtor_info_id
, hash_iin
, count_credit
, amount
, debt_value
, debt_pastdue_value
, payment_days_overdue
, rep_date
FROM v_sdu.for_sdu_monthly_debts
where rep_date > ?

--ПОКАЗАТЕЛЬ ДЛЯ ЦКС
SELECT 
HASH_IIN,
((arrayReverseSort(x -> x.1, groupArray((toString(REP_DATE),COUNT_CREDIT,DEBT_VALUE, AMOUNT, DEBT_PASTDUE_VALUE, PAYMENT_DAYS_OVERDUE))) as x).1)[1] as DT_,
(x.2)[1] as COUNT_CREDIT_,
(x.3)[1] as DEBT_VALUE_,
(x.4)[1] as AMOUNT_,
(x.5)[1] as DEBT_PASTDUE_VALUE_,
(x.6)[1] as PAYMENT_DAYS_OVERDUE_
from SK_FAMILY.GKB_HIST gh 
where   REP_DATE >= toDateTime('2022-04-01 00:00:00') group by HASH_IIN 