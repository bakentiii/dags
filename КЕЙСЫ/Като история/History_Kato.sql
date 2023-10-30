SELECT 
    a.ID AS ID,

    a.CODE_KATO AS CODE_KATO,

    a.MNE_KATO_FULL AS MNE_KATO_FULL,

    MNE_KATO_REGION,

    MNE_KATO_REGION_NAME,

    MNE_KATO_RAYON,

    MNE_KATO_RAYON_NAME
--
--    a.MNE_KATO_LVL_5 AS MNE_KATO_LVL_5,
--
--    a.MNE_KATO_NAME_LVL_5 AS MNE_KATO_NAME_LVL_5,
--
--    a.MNE_KATO_LVL_6 AS MNE_KATO_LVL_6,
--
--    a.MNE_KATO_NAME_LVL_6 AS MNE_KATO_NAME_LVL_6
FROM 
(
    SELECT DISTINCT 
        a.ID AS ID,

        a.CODE_KATO AS CODE_KATO,
-- if((b.MNE_KATO_FULL = '') AND (c.MNE_KATO_FULL = '') AND (d.MNE_KATO_FULL = '') AND (e.MNE_KATO_FULL = '') AND (f.MNE_KATO_FULL != ''),
-- f.MNE_KATO_FULL,
-- if((b.MNE_KATO_FULL = '') AND (c.MNE_KATO_FULL = '') AND (d.MNE_KATO_FULL = '') AND (e.MNE_KATO_FULL != ''),
-- e.MNE_KATO_FULL,
-- if((b.MNE_KATO_FULL = '') AND (c.MNE_KATO_FULL = '') AND (d.MNE_KATO_FULL != ''),
-- d.MNE_KATO_FULL,
-- if((b.MNE_KATO_FULL = '') AND (c.MNE_KATO_FULL != ''),
-- c.MNE_KATO_FULL,
-- b.MNE_KATO_FULL)))) AS MNE_KATO_FULL,
-- 
        if((e.MNE_KATO_FULL = '') AND (f.MNE_KATO_FULL != ''),
 f.MNE_KATO_FULL,
 e.MNE_KATO_FULL) AS MNE_KATO_FULL

--        MNE_KATO_LVL_6,
--
--        MNE_KATO_NAME_LVL_6,
--
--        MNE_KATO_LVL_5,
--
--        MNE_KATO_NAME_LVL_5
    FROM 
    (
        SELECT 
            ID,

            CODE_KATO
        FROM DWH_DIC.MNE_META_KATO
    ) AS a
    LEFT JOIN 
--    (
--        SELECT 
--            ID,
--
--            CODE_KATO AS MNE_KATO_FULL,
--
--            CODE_KATO AS MNE_KATO_LVL_6,
--
--            NAME_RU AS MNE_KATO_NAME_LVL_6,
--
--            max_date
--        FROM 
--        (
--            SELECT 
--                ID,
--
--                max(BEG_DATE) AS max_date
--            FROM DWH_DIC.MNE_META_KATO
--            WHERE LVL IN (6)
--            GROUP BY ID
--        ) AS a
--        LEFT JOIN DWH_DIC.MNE_META_KATO AS b ON (a.ID = b.ID) AND (a.max_date = b.BEG_DATE)
--    ) AS b ON a.ID = b.ID
--    LEFT JOIN 
--    (
--        SELECT 
--            ID,
--
--            CODE_KATO AS MNE_KATO_FULL,
--
--            substring(CODE_KATO,
-- 1,
-- 7) AS MNE_KATO_LVL_5,
--
--            NAME_RU AS MNE_KATO_NAME_LVL_5,
--
--            max_date
--        FROM 
--        (
--            SELECT 
--                ID,
--
--                max(BEG_DATE) AS max_date
--            FROM DWH_DIC.MNE_META_KATO
--            WHERE LVL IN (5)
--            GROUP BY ID
--        ) AS a
--        LEFT JOIN DWH_DIC.MNE_META_KATO AS b ON (a.ID = b.ID) AND (a.max_date = b.BEG_DATE)
--    ) AS c ON a.ID = c.ID
--    LEFT JOIN 
--    (
--        SELECT 
--            ID,
--
--            CODE_KATO AS MNE_KATO_FULL,
--
--            substring(CODE_KATO,
-- 1,
-- 6) AS MNE_KATO_LVL_4,
--
--            NAME_RU AS MNE_KATO_NAME_LVL_4,
--
--            max_date
--        FROM 
--        (
--            SELECT 
--                ID,
--
--                max(BEG_DATE) AS max_date
--            FROM DWH_DIC.MNE_META_KATO
--            WHERE LVL IN (4)
--            GROUP BY ID
--        ) AS a
--        LEFT JOIN DWH_DIC.MNE_META_KATO AS b ON (a.ID = b.ID) AND (a.max_date = b.BEG_DATE)
--    ) AS d ON a.ID = d.ID
--    LEFT JOIN 
    (
        SELECT 
            ID,

            CODE_KATO AS MNE_KATO_FULL,

            substring(CODE_KATO,
 1,
 4) AS MNE_KATO_RAYON,

            NAME_RU AS MNE_KATO_RAYON_NAME,

            max_date
        FROM 
        (
            SELECT 
                ID,

                max(BEG_DATE) AS max_date
            FROM DWH_DIC.MNE_META_KATO
            WHERE LVL IN (3)
            GROUP BY ID
        ) AS a
        LEFT JOIN DWH_DIC.MNE_META_KATO AS b ON (a.ID = b.ID) AND (a.max_date = b.BEG_DATE)
    ) AS e ON a.ID = e.ID
    LEFT JOIN 
    (
        SELECT 
            ID,

            CODE_KATO AS MNE_KATO_FULL,

            substring(CODE_KATO,
 1,
 2) AS MNE_KATO_REGION,

            NAME_RU AS MNE_KATO_REGION_NAME,

            max_date
        FROM 
        (
            SELECT 
                ID,

                max(BEG_DATE) AS max_date
            FROM DWH_DIC.MNE_META_KATO
            WHERE LVL IN (2)
            GROUP BY ID
        ) AS a
        LEFT JOIN DWH_DIC.MNE_META_KATO AS b ON (a.ID = b.ID) AND (a.max_date = b.BEG_DATE)
    ) AS f ON a.ID = f.ID
    WHERE MNE_KATO_FULL != ''
) AS a
--LEFT JOIN 
--(
--    SELECT 
--        ID,
--
--        CODE_KATO AS MNE_KATO_FULL,
--
----        substring(CODE_KATO,
---- 1,
---- 6) AS MNE_KATO_SEL_OKR,
----
----        NAME_RU AS MNE_KATO_SEL_OKR_NAME,
--
--        max_date
--    FROM 
--    (
--        SELECT 
--            ID,
--
--            max(BEG_DATE) AS max_date
--        FROM DWH_DIC.MNE_META_KATO
--        WHERE LVL IN (4)
--        GROUP BY ID
--    ) AS a
--    LEFT JOIN DWH_DIC.MNE_META_KATO AS b ON (a.ID = b.ID) AND (a.max_date = b.BEG_DATE)
--) AS so ON substring(a.MNE_KATO_FULL,
-- 1,
-- 6) = MNE_KATO_SEL_OKR
LEFT JOIN 
(
    SELECT 
        ID,

        CODE_KATO AS MNE_KATO_FULL,

        substring(CODE_KATO,
 1,
 4) AS MNE_KATO_RAYON,

        NAME_RU AS MNE_KATO_RAYON_NAME,

        max_date
    FROM 
    (
        SELECT 
            ID,

            max(BEG_DATE) AS max_date
        FROM DWH_DIC.MNE_META_KATO
        WHERE LVL IN (3)
        GROUP BY ID
    ) AS a
    LEFT JOIN DWH_DIC.MNE_META_KATO AS b ON (a.ID = b.ID) AND (a.max_date = b.BEG_DATE)
) AS rayon ON substring(a.MNE_KATO_FULL,
 1,
 4) = MNE_KATO_RAYON
LEFT JOIN 
(
    SELECT 
        ID,

        CODE_KATO AS MNE_KATO_FULL,

        substring(CODE_KATO,
 1,
 2) AS MNE_KATO_REGION,

        NAME_RU AS MNE_KATO_REGION_NAME,

        max_date
    FROM 
    (
        SELECT 
            ID,

            max(BEG_DATE) AS max_date
        FROM DWH_DIC.MNE_META_KATO
        WHERE LVL IN (2)
        GROUP BY ID
    ) AS a
    LEFT JOIN DWH_DIC.MNE_META_KATO AS b ON (a.ID = b.ID) AND (a.max_date = b.BEG_DATE)
) AS f ON substring(a.MNE_KATO_FULL,
 1,
 2) = MNE_KATO_REGION