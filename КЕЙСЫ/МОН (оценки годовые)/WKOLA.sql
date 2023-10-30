SELECT
  st.iin AS 'ИИН',
  st.lastname AS 'Фамилия',
  st.firstName AS 'Имя',
  COALESCE(st.middleName, '') AS 'Отчество',
  grade.rname AS 'Параллель',
  e.class_letter AS 'Литера',
  lang.rname AS 'Язык обучения',
  subj_math.point_num AS 'Математика',
  subj_phys.point_num AS 'Физика',
  subj_chem.point_num AS 'Химия',
  subj_biol.point_num AS 'Биология',
  subj_geogr.point_num AS 'География',
  subj_compsci.point_num AS 'Информатика',
  subj_natsci.point_num AS 'Естествознание',
  s.ru_name as 'Наименование организации образования',
  s.bin AS 'БИН',
  area.rname AS 'Область',
  region.rname AS 'Район',
  locality.rname AS 'Населенный пункт',
  areaType.rname AS 'Территориальная принадлежность',
  s.id AS 'ID_school',  
  ( SELECT STRING_AGG(sst.rname, ', ') WITHIN GROUP (ORDER BY sst.id) FROM nedb.school_spec LEFT JOIN nedb.d_schoolSpec_type sst ON sst.id = nedb.school_spec.spec_type_id WHERE sa.id = nedb.school_spec.school_attr_id ) AS 'Виды организации образования',
  st.id student_id, 
  e.edu_id education_id 
FROM nedb.school s
INNER JOIN nedb.school_attr sa ON s.id = sa.school_id AND sa.edu_period_id = 0
INNER JOIN nedb.education e ON e.school_id = s.id AND e.edu_period_id = 0
INNER JOIN nedb.student_attr sta ON sta.student_id = e.student_id AND sta.edu_period_id = 0
INNER JOIN nedb.student st ON st.id = e.student_id
-- като
INNER JOIN nedb.d_region locality ON sa.region_id = locality.id 
LEFT JOIN nedb.d_region area ON area.id = locality.area_id
LEFT JOIN nedb.d_region region ON region.id = locality.district_id
-- показатели организации
LEFT JOIN nedb.d_kfs kfs ON kfs.id = sa.kfs_id
LEFT JOIN nedb.d_dep dep ON dep.id = sa.dep_id
LEFT JOIN nedb.d_areaType areaType ON areaType.id = sa.areaType_id
-- показатели учащихся
LEFT JOIN nedb.d_class_num2 grade ON e.class_num_id = grade.id
LEFT JOIN nedb.d_lang lang ON lang.id = e.lang_id
-- оценки по предметам
LEFT JOIN nedb.edu_natMat subj_math ON subj_math.education_id = e.id AND subj_math.natMat_subj_id = 3
LEFT JOIN nedb.edu_natMat subj_phys ON subj_phys.education_id = e.id AND subj_phys.natMat_subj_id = 4
LEFT JOIN nedb.edu_natMat subj_chem ON subj_chem.education_id = e.id AND subj_chem.natMat_subj_id = 5
LEFT JOIN nedb.edu_natMat subj_biol ON subj_biol.education_id = e.id AND subj_biol.natMat_subj_id = 6
LEFT JOIN nedb.edu_natMat subj_geogr ON subj_geogr.education_id = e.id AND subj_geogr.natMat_subj_id = 7
LEFT JOIN nedb.edu_natMat subj_compsci ON subj_compsci.education_id = e.id AND subj_compsci.natMat_subj_id = 8
LEFT JOIN nedb.edu_natMat subj_natsci ON subj_natsci.education_id = e.id AND subj_natsci.natMat_subj_id = 9
-- условия
WHERE EXISTS ( SELECT spec_type_id FROM nedb.school_spec LEFT JOIN nedb.d_schoolSpec_type sst ON sst.id = nedb.school_spec.spec_type_id WHERE sa.id = nedb.school_spec.school_attr_id AND ( sst.code LIKE '02.1.%' OR sst.code LIKE '02.2.%' OR sst.code LIKE '02.3.%' OR sst.code LIKE '02.4.%' OR sst.code LIKE '02.5.%' OR sst.code LIKE '07.%' OR sst.code IN ('02.6.1', '02.6.2', '02.6.3', '02.6.4', '08.3', '08.4', '08.5', '08.6', '09.3', '09.4') ) ) 
AND sa.date_close IS NULL -- школа не закрыта
-- условия контингент
AND e.edu_status IN (0, 4) -- статус по контингенту (0 - действующий, 4 - на выбытии)
AND e.class_num_id IN ('3', '4', '5', '6', '7', '8', '9', '10', '11', '12', '13', '14', '15') -- параллель 1-13 классы
-- сортировка
ORDER BY area.rname, region.rname, locality.rname, s.ru_name