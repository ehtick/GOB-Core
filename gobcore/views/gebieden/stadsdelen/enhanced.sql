    SELECT
        stadsdelen.identificatie
    ,   stadsdelen.code
    ,   stadsdelen.naam
    ,   stadsdelen.begin_geldigheid
    ,   stadsdelen.eind_geldigheid
    ,   stadsdelen.documentdatum
    ,   stadsdelen.documentnummer
    ,   json_build_object(
             'identificatie', gemeentes.identificatie,
             'naam', gemeentes.naam
        ) AS _ref_ligt_in_gemeente_brk_gme
    ,   stadsdelen.geometrie
    FROM
        gebieden_stadsdelen AS stadsdelen
        LEFT JOIN
            (SELECT
                identificatie
            ,   naam
            ,   eind_geldigheid
            FROM brk_gemeentes
            WHERE eind_geldigheid IS NULL
            ) AS gemeentes
            ON stadsdelen.ligt_in_gemeente->>'id' = gemeentes.identificatie
    WHERE stadsdelen.eind_geldigheid IS NULL
    ORDER BY stadsdelen.code