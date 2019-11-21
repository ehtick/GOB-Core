    SELECT metingen.identificatie
    ,       datum
    ,       type_meting
    ,       hoogte_tov_nap
    ,       zakking
    ,       zakkingssnelheid
    ,       zakking_cumulatief
    ,       is_gemeten_door
    ,       (hoeveelste_meting - 1)                                   AS hoeveelste_meting
    ,       CASE WHEN aantal_dagen::TEXT = '0' THEN ''::TEXT
                 ELSE aantal_dagen::TEXT
            END                                                       AS aantal_dagen
    ,       wijze_van_inwinnen
    ,       hoort_bij_meetbout                                        AS _ref_hoort_bij_meetbout_mbn_mbt
    ,       refereert_aan_referentiepunten                            AS _mref_refereert_aan_referentiepunten_mbn_rpt
    ,   json_build_object(
             'identificatie', stadsdelen.identificatie,
             'code', stadsdelen.code,
             'naam', stadsdelen.naam
        ) AS _ref_ligt_in_stadsdeel_gbd_sdl
    FROM  meetbouten_metingen                                         AS metingen
    LEFT JOIN (
            SELECT _id, ligt_in_stadsdeel->>'id' AS ligt_in_stadsdeel FROM meetbouten_meetbouten
    ) AS meetbouten ON metingen.hoort_bij_meetbout->>'id' = meetbouten._id
    LEFT JOIN (
            SELECT identificatie, code, naam FROM gebieden_stadsdelen
            WHERE eind_geldigheid IS NULL) AS stadsdelen on meetbouten.ligt_in_stadsdeel = stadsdelen.identificatie
    WHERE metingen.publiceerbaar = TRUE