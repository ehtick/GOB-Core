    SELECT  meetbouten.identificatie
    ,       meetbouten.locatie
    ,       eerste_metingen.datum                                           AS datum
    ,       meest_recente_metingen.hoogte_tov_nap
    ,       meest_recente_metingen.zakkingssnelheid
    ,       meest_recente_metingen.zakking_cumulatief
    ,       status
    ,       meetbouten.geometrie
    ,   json_build_object(
             'identificatie', bouwblokken.identificatie,
             'code', bouwblokken.code
        ) AS _ref_ligt_in_bouwblok_gbd_bbk
    ,   json_build_object(
             'identificatie', buurten.identificatie,
             'code', buurten.code,
             'naam', buurten.naam
        ) AS _ref_ligt_in_buurt_gbd_brt
    ,   json_build_object(
             'identificatie', stadsdelen.identificatie,
             'code', stadsdelen.code,
             'naam', stadsdelen.naam
        ) AS _ref_ligt_in_stadsdeel_gdb_sdl
    ,       nabij_nummeraanduiding AS _ref_nabij_nummeraanduiding_bag_nag
            FROM              meetbouten_meetbouten                         AS meetbouten
            LEFT JOIN (
              SELECT identificatie, code FROM gebieden_bouwblokken
              WHERE eind_geldigheid IS NULL) AS bouwblokken on meetbouten.ligt_in_bouwblok->>'id' = bouwblokken.identificatie
            LEFT JOIN (
              SELECT identificatie, code, naam FROM gebieden_buurten
              WHERE eind_geldigheid IS NULL) AS buurten on meetbouten.ligt_in_buurt->>'id' = buurten.identificatie
            LEFT JOIN (
              SELECT identificatie, code, naam FROM gebieden_stadsdelen
              WHERE eind_geldigheid IS NULL) AS stadsdelen on meetbouten.ligt_in_stadsdeel->>'id' = stadsdelen.identificatie
            LEFT JOIN (
              SELECT DISTINCT ON (hoort_bij_meetbout->>'id') * from meetbouten_metingen
              ORDER BY hoort_bij_meetbout->>'id', datum ASC
            ) AS eerste_metingen ON meetbouten._id = eerste_metingen.hoort_bij_meetbout->>'id'
            LEFT JOIN (
              SELECT DISTINCT ON (hoort_bij_meetbout->>'id') * from meetbouten_metingen
              ORDER BY hoort_bij_meetbout->>'id', datum DESC
            ) AS meest_recente_metingen ON meetbouten._id = meest_recente_metingen.hoort_bij_meetbout->>'id'
    WHERE meetbouten.publiceerbaar = TRUE