    SELECT
        bouwblokken.identificatie
    ,   bouwblokken.code
    ,   bouwblokken.begin_geldigheid
    ,   bouwblokken.eind_geldigheid
    ,   json_build_object(
             'identificatie', buurten.identificatie,
             'code', buurten.code,
             'naam', buurten.naam
        ) AS _ref_ligt_in_buurt_gbd_brt
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
        ) AS _ref_ligt_in_ggpgebied_gbd_ggp
    ,   json_build_object(
             'identificatie', stadsdelen.identificatie,
             'code', stadsdelen.code,
             'naam', stadsdelen.naam
        ) AS _ref_ligt_in_stadsdeel_gbd_sdl
    ,   json_build_object(
             'identificatie', gemeentes.identificatie,
             'naam', gemeentes.naam
        ) AS _ref_ligt_in_gemeente_brk_gme
    ,   bouwblokken.geometrie
    FROM
        gebieden_bouwblokken AS bouwblokken
        LEFT JOIN
            (SELECT
                identificatie
            ,   code
            ,   naam
            ,   eind_geldigheid
            ,   ligt_in_wijk
            FROM gebieden_buurten
            WHERE eind_geldigheid IS NULL
            ) AS buurten
        ON bouwblokken.ligt_in_buurt->>'id' = buurten.identificatie
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
            ON bouwblokken.ligt_in_buurt->>'id' = ggwgebieden.bestaat_uit_buurt
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
            ON bouwblokken.ligt_in_buurt->>'id' = ggpgebieden.bestaat_uit_buurt
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
    WHERE bouwblokken.eind_geldigheid IS NULL
    ORDER BY bouwblokken.code