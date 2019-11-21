    SELECT
        buurten.identificatie
    ,   buurten.code
    ,   buurten.naam
    ,   buurten.begin_geldigheid
    ,   buurten.eind_geldigheid
    ,   buurten.documentdatum
    ,   buurten.documentnummer
    ,   buurten.cbs_code
    ,   json_build_object(
             'identificatie', wijken.identificatie,
             'code', wijken.code,
             'naam', wijken.naam
        ) AS _ref_ligt_in_wijk_gbd_wijk
    ,   json_build_object(
             'identificatie', ggwgebieden.identificatie,
             'code', ggwgebieden.code,
             'naam', ggwgebieden.naam
        ) AS _ref_ligt_in_ggwgebied_gbd_ggw
    ,   json_build_object(
             'identificatie', ggpgebieden.identificatie,
             'code', ggpgebieden.code,
             'naam', ggpgebieden.naam
        ) AS _ref_ligt_in_ggpgebied_gebieden_ggpgebieden
    ,   json_build_object(
             'identificatie', stadsdelen.identificatie,
             'code', stadsdelen.code,
             'naam', stadsdelen.naam
        ) AS _ref_ligt_in_stadsdeel_gbd_sdl
    ,   json_build_object(
             'identificatie', gemeentes.identificatie,
             'naam', gemeentes.naam
        ) AS _ref_ligt_in_gemeente_brk_gme
    ,   buurten.geometrie
    FROM
        gebieden_buurten AS buurten
        LEFT JOIN
            (SELECT
                identificatie
            ,   code
            ,   naam
            ,   eind_geldigheid
            ,   ligt_in_stadsdeel
            FROM gebieden_wijken
            WHERE eind_geldigheid IS NULL
            ) AS wijken
            ON buurten.ligt_in_wijk->>'id' = wijken.identificatie
        LEFT JOIN
            (SELECT
                identificatie
            ,   code
            ,   naam
            ,   value->>'id' AS bestaat_uit_buurt
            ,   eind_geldigheid
            FROM gebieden_ggwgebieden, jsonb_array_elements(gebieden_ggwgebieden.bestaat_uit_buurten)
            WHERE eind_geldigheid IS NULL
            ) AS ggwgebieden
            ON buurten.identificatie = ggwgebieden.bestaat_uit_buurt
        LEFT JOIN
            (SELECT
                identificatie
            ,   code
            ,   naam
            ,   value->>'id' AS bestaat_uit_buurt
            ,   eind_geldigheid
            FROM gebieden_ggpgebieden, jsonb_array_elements(gebieden_ggpgebieden.bestaat_uit_buurten)
            WHERE eind_geldigheid IS NULL
            ) AS ggpgebieden
            ON buurten.identificatie = ggpgebieden.bestaat_uit_buurt
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
    WHERE buurten.eind_geldigheid IS NULL
    ORDER BY buurten.code