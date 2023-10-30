select DESC_TYPE_OF_PROPERTY
,FORM_SOB
,DATA_REG 
,COALESCE(PRAV_DOK, '') as PRAV_DOK
,SUBSTRING(arrayElement(splitByChar(',', PRAV_DOK),1),1,position(arrayElement(splitByChar(',', PRAV_DOK),1),'№')-1) as TIP_DOGOVORA
,SUBSTRING(arrayElement(splitByChar(',', PRAV_DOK),2),1,position(arrayElement(splitByChar(',', PRAV_DOK),2),'л')-1) as NOTARIUS
,SUMMA_SDELKI 
,VID_PRAVA 
,COALESCE(ADRESS, '') as ADDRESS
--,arrayElement(splitByChar(',', address),2) as RAYON
,arrayElement(splitByChar(',',assumeNotNull(SUBSTRING(ADRESS,position(ADDRESS,'обл.')))),1) as OBLAST
,arrayElement(splitByChar(',',assumeNotNull(multiIf(ADRESS like '% г.%',SUBSTRING(ADRESS,position(ADDRESS,' г.')),
ADRESS like '% с.о%', SUBSTRING(ADRESS,position(ADDRESS,' с.о')), 
SUBSTRING(ADRESS,position(ADDRESS,'г.'))
))),1) as GOROD
,arrayElement(splitByChar(',',assumeNotNull(multiIf(ADRESS like '% р-н%',SUBSTRING(ADRESS,position(ADDRESS,' р-н')),
ADRESS like '% с.%', SUBSTRING(ADRESS,position(ADDRESS,' с.')), 
SUBSTRING(ADRESS,position(ADDRESS,' р-н'))
))),1) as RAYON
--,multiIf(address like '% æ.ì.%',arrayElement(splitByChar(',', address),4),
--address like '% ìêð. Àëü-Ôàðàáè%',arrayElement(splitByChar(',', address),4),
--arrayElement(splitByChar(',', address),3)
--)as ULICA
--,arrayElement(splitByChar(',', address),3) as ULICA
--,SUBSTRING(arrayElement(splitByChar(',', ADDRESS),3),1,position(ADDRESS,'ä.')-1) as ULICA
,arrayElement(splitByChar(',',assumeNotNull(multiIf(ADRESS like '% ул.%',SUBSTRING(ADRESS,position(ADDRESS,' ул.')),
ADRESS like '%мкр.%',SUBSTRING(ADRESS,position(ADDRESS,' мкр.')),
ADRESS like '%пр.%',SUBSTRING(ADRESS,position(ADDRESS,' пр.')),
ADRESS like '%кв-л%',SUBSTRING(ADRESS,position(ADDRESS,' кв-л')),
ADRESS like '%тр-т%',SUBSTRING(ADRESS,position(ADDRESS,' тр-т')),
ADRESS like '%пер.%',SUBSTRING(ADRESS,position(ADDRESS,' пер.')),
ADRESS like '%ж.м.%',SUBSTRING(ADRESS,position(ADDRESS,' ж.м.')),
ADRESS like '%шос.%',SUBSTRING(ADRESS,position(ADDRESS,' шос.')),
ADRESS like '%наб.%',SUBSTRING(ADRESS,position(ADDRESS,' наб.')),
ADRESS like '%туп.%',SUBSTRING(ADRESS,position(ADDRESS,' туп.')),
ADRESS like '%пр-д.%',SUBSTRING(ADRESS,position(ADDRESS,' пр-д.')),
ADRESS like '%уч.кв.%',SUBSTRING(ADRESS,position(ADDRESS,' уч.кв.')),
ADRESS like '%раз.%',SUBSTRING(ADRESS,position(ADDRESS,' раз.')),
ADRESS like '%м.%',SUBSTRING(ADRESS,position(ADDRESS,' м.')),
ADRESS like '%пл.%',SUBSTRING(ADRESS,position(ADDRESS,' пл.')),
ADRESS like '%ж.р.%',SUBSTRING(ADRESS,position(ADDRESS,' ж.р.')),
SUBSTRING(ADRESS,position(ADDRESS,' ул.'))
))),1) as ULICA
--,arrayElement(splitByChar(',',SUBSTRING(ADDRESS,position(ADDRESS,'óë.'))),1) as ULICA
--,multiIf(address like '%ìêð%', arrayElement(splitByChar(',', address),5),
--address like '%Þãî-Âîñòîê%',arrayElement(splitByChar(',', address),5),
--address like '%Æ?í³áåê Õàíäàð%',arrayElement(splitByChar(',', address),5),
--address like '% æ.ì.%',arrayElement(splitByChar(',', address),5),
--arrayElement(splitByChar(',', address),4)
--) as DOM
--,SUBSTRING(arrayElement(splitByChar(',', ADDRESS),4),1) as DOM
,arrayElement(splitByChar(',',assumeNotNull(SUBSTRING(ADRESS,position(ADDRESS,' д.'),9))),1) as DOM
,arrayElement(splitByChar(',',assumeNotNull(SUBSTRING(ADRESS,position(ADDRESS,' кв.')))),1) as KVARTIRA
,AR_CODE 
,MAX(PLOSH_OB) AS PLOSH_OB 
,KATO 
,ROUND(SUMMA_SDELKI/PLOSH_OB,2) as CENA_1_KV
,multiIf(
CENA_1_KV  <= 50000, '0-50k',
CENA_1_KV  > 50000 AND CENA_1_KV <= 100000, '50k-100k',
CENA_1_KV  > 100000 AND CENA_1_KV <= 150000, '100k-150k',
CENA_1_KV  > 150000 AND CENA_1_KV <= 200000, '150k-200k',
CENA_1_KV  > 200000 AND CENA_1_KV <= 250000, '200k-250k',
CENA_1_KV  > 250000 AND CENA_1_KV <= 300000, '250k-300k',
CENA_1_KV  > 300000 AND CENA_1_KV <= 350000, '300k-350k',
CENA_1_KV  > 350000 AND CENA_1_KV <= 400000, '350k-400k',
CENA_1_KV  > 400000 AND CENA_1_KV <= 450000, '400k-450k',
CENA_1_KV  > 450000 AND CENA_1_KV <= 500000, '450k-500k',
CENA_1_KV  > 500000 AND CENA_1_KV <= 550000, '500k-550k',
CENA_1_KV  > 550000 AND CENA_1_KV <= 600000, '550k-600k',
CENA_1_KV  > 600000 AND CENA_1_KV <= 650000, '600k-650k',
CENA_1_KV  > 650000 AND CENA_1_KV <= 700000, '650k-700k',
CENA_1_KV  > 700000 AND CENA_1_KV <= 750000, '700k-750k',
CENA_1_KV  > 750000 AND CENA_1_KV <= 800000, '750k-800k',
CENA_1_KV  > 800000 AND CENA_1_KV <= 850000, '800k-850k',
CENA_1_KV  > 850000 AND CENA_1_KV <= 900000, '850k-900k',
CENA_1_KV  > 900000 AND CENA_1_KV <= 950000, '900k-950k',
CENA_1_KV  > 950000 AND CENA_1_KV <= 1000000, '950k-1M',
'> 1M'
)as KATEGORIYA_CEN
,multiIf(
KATEGORIYA_CEN  = '0-50k',1,
KATEGORIYA_CEN  = '50k-100k',2,
KATEGORIYA_CEN  = '100k-150k',3,
KATEGORIYA_CEN  = '150k-200k',4,
KATEGORIYA_CEN  = '200k-250k',5,
KATEGORIYA_CEN  = '250k-300k',6,
KATEGORIYA_CEN  = '300k-350k',7,
KATEGORIYA_CEN  = '350k-400k',8,
KATEGORIYA_CEN  = '400k-450k',9,
KATEGORIYA_CEN  = '450k-500k',10,
KATEGORIYA_CEN  = '500k-550k',11,
KATEGORIYA_CEN  = '550k-600k',12,
KATEGORIYA_CEN  = '600k-650k',13,
KATEGORIYA_CEN  = '650k-700k',14,
KATEGORIYA_CEN  = '700k-750k',15,
KATEGORIYA_CEN  = '750k-800k',16,
KATEGORIYA_CEN  = '800k-850k',17,
KATEGORIYA_CEN  = '850k-900k',18,
KATEGORIYA_CEN  = '900k-950k',19,
KATEGORIYA_CEN  = '950k-1M',20,
21
) as ORDER_KATEGORIYA_CEN
from DM_ZEROS.DM_MU_RN_PROPERTY_IIN_RNN where SUMMA_SDELKI is not null and SUMMA_SDELKI <> 0 and DATA_REG >= '2019-01-01 00:00:00' AND (DESC_TYPE_OF_PROPERTY = 'Частный дом' OR DESC_TYPE_OF_PROPERTY ='Квартира') and NOTARIUS LIKE '%нотариус%' AND SUMMA_SDELKI < 5.0E8
GROUP BY 
DESC_TYPE_OF_PROPERTY
,FORM_SOB
,DATA_REG 
,PRAV_DOK 
,TIP_DOGOVORA
,NOTARIUS
,SUMMA_SDELKI 
,VID_PRAVA 
,COALESCE(ADRESS, '') as address
,OBLAST
,GOROD
,RAYON
,ULICA
,DOM
,KVARTIRA
,AR_CODE 
,KATO
having PLOSH_OB <> 0 and PLOSH_OB is not null 
