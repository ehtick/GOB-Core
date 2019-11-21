    SELECT
        wijken.identificatie
    ,   wijken.code
    ,   wijken.naam
    ,   wijken.begin_geldigheid
    ,   wijken.eind_geldigheid
    ,   wijken.documentdatum
    ,   wijken.documentnummer
    ,   wijken.cbs_code
    ,   json_build_object(
             'identificatie', ggwgebieden.identificatie,
             'code', ggwgebieden.code,
             'naam', ggwgebieden.naam
        ) AS _ref_ligt_in_ggwgebied_gbd_ggw
    ,   json_build_object(
             'identificatie', stadsdelen.identificatie,
             'code', stadsdelen.code,
             'naam', stadsdelen.naam
        ) AS _ref_ligt_in_stadsdeel_gbd_sdl
    ,   json_build_object(
             'identificatie', gemeentes.identificatie,
             'naam', gemeentes.naam
        ) AS _ref_ligt_in_gemeente_brk_gme
    ,   wijken.geometrie
    FROM
        gebieden_wijken AS wijken
        LEFT JOIN
            (SELECT
                identificatie
            ,   code
            ,   naam
            ,   eind_geldigheid
            ,   geometrie
            FROM gebieden_ggwgebieden
            WHERE eind_geldigheid IS NULL
            ) AS ggwgebieden
            ON ST_WITHIN(wijken.geometrie, ST_BUFFER(ggwgebieden.geometrie, 0.1))
        LEFT JOIN
            (SELECT
                identificatie
            ,   code
            ,   naam
            ,   eind_geldigheid
            ,   ligt_in_gemeente
            FROM gebieden_stadsdelen
            WHERE eind_geldigheid IS NULL
            ) AS stadsdelen
            ON wijken.ligt_in_stadsdeel->>'id' = stadsdelen.identificatie
        LEFT JOIN
            (SELECT
                identificatie
            ,   naam
            ,   eind_geldigheid
            FROM brk_gemeentes
            WHERE eind_geldigheid IS NULL
            ) AS gemeentes
            ON stadsdelen.ligt_in_gemeente->>'id' = gemeentes.identificatie
    WHERE wijken.eind_geldigheid IS NULL
    ORDER BY wijken.code