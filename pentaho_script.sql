--Скрипт для поиска айди трансформаций по названию таблицы оутпут
--Можно использовать полное название таблицы без схемы но с оператором = вместо like
select rsa.id_transformation, rsa.code, rsa.value_str, rt.id_directory from public.r_step_attribute rsa
left join public.r_transformation rt
on rsa.id_transformation = rt.id_transformation
where upper(value_str) like '%DELEVERYINVOICEDETAILS%'
group by rsa.id_transformation, rsa.code, rsa.value_str, rt.id_directory
order by rsa.id_transformation, rsa.code, rsa.value_str, rt.id_directory


--Скрипт для поиска директории трансформаций по их айдишникам
select 
 rt.id_transformation
, rd5.directory_name
, rd4.directory_name
, rd3.directory_name
, rd2.directory_name
, rd1.directory_name
, rd.directory_name
, rt."NAME" as NAMES
, rt.id_directory as LOC
from public.r_transformation rt
left join public.r_directory rd
on rt.id_directory  = rd.id_directory 
left join public.r_directory rd1
on rd.id_directory_parent  = rd1.id_directory
left join public.r_directory rd2
on rd1.id_directory_parent  = rd2.id_directory
left join public.r_directory rd3
on rd2.id_directory_parent  = rd3.id_directory
left join public.r_directory rd4
on rd3.id_directory_parent  = rd4.id_directory
left join public.r_directory rd5
on rd4.id_directory_parent  = rd5.id_directory
where rt.id_transformation in('3129')