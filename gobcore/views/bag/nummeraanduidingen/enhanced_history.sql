    SELECT
      nag.identificatie,
      nag.volgnummer,
      nag.registratiedatum,
      nag.aanduiding_in_onderzoek,
      nag.geconstateerd,
      nag.huisnummer,
      nag.huisletter,
      nag.huisnummertoevoeging,
      nag.postcode,
      json_build_object(
        'identificatie', ligt_aan_openbareruimte.identificatie,
        'volgnummer', ligt_aan_openbareruimte.volgnummer,
        'naam', ligt_aan_openbareruimte.naam
      ) AS _ref_ligt_aan_openbareruimte_bag_ore,
      json_build_object(
        'identificatie', ligt_in_woonplaats.identificatie,
        'volgnummer', ligt_in_woonplaats.volgnummer,
        'naam', ligt_in_woonplaats.naam
      ) AS _ref_ligt_in_woonplaats_bag_wps,
      nag.begin_geldigheid,
      nag.eind_geldigheid,
      nag.type_adresseerbaar_object,
      nag.type_adres,
      nag.documentdatum,
      nag.documentnummer,
      nag.status,
      json_build_object(
        'identificatie', adresseert_verblijfsobject.identificatie,
        'volgnummer', adresseert_verblijfsobject.volgnummer
      ) AS _ref_adresseert_verblijfsobject_bag_vot,
      json_build_object(
        'identificatie', adresseert_ligplaats.identificatie,
        'volgnummer', adresseert_ligplaats.volgnummer
      ) AS _ref_adresseert_ligplaats_bag_lps,
      json_build_object(
        'identificatie', adresseert_standplaats.identificatie,
        'volgnummer', adresseert_standplaats.volgnummer
      ) AS _ref_adresseert_standplaats_bag_sps
    FROM
      bag_nummeraanduidingen AS nag
    -- SELECT ligt_aan_openbareruimte
    LEFT JOIN
      bag_openbareruimtes AS ligt_aan_openbareruimte
    ON
      nag.ligt_aan_openbareruimte->>'id' = ligt_aan_openbareruimte._id AND
      nag.ligt_aan_openbareruimte->>'volgnummer' = ligt_aan_openbareruimte.volgnummer
    -- SELECT ligt_in_woonplaats
    LEFT JOIN
      bag_woonplaatsen AS ligt_in_woonplaats
    ON
      nag.ligt_in_woonplaats->>'id' = ligt_in_woonplaats._id AND
      nag.ligt_in_woonplaats->>'volgnummer' = ligt_in_woonplaats.volgnummer
    -- SELECT adresseert_verblijfsobject
    LEFT JOIN
      bag_verblijfsobjecten AS adresseert_verblijfsobject
    ON
      nag.adresseert_verblijfsobject->>'id' = adresseert_verblijfsobject._id AND
      nag.adresseert_verblijfsobject->>'volgnummer' = adresseert_verblijfsobject.volgnummer
    -- SELECT adresseert_ligplaats
    LEFT JOIN
      bag_ligplaatsen AS adresseert_ligplaats
    ON
      nag.adresseert_ligplaats->>'id' = adresseert_ligplaats._id AND
      nag.adresseert_ligplaats->>'volgnummer' = adresseert_ligplaats.volgnummer
    -- SELECT adresseert_standplaats
    LEFT JOIN
      bag_standplaatsen AS adresseert_standplaats
    ON
      nag.adresseert_standplaats->>'id' = adresseert_standplaats._id AND
      nag.adresseert_standplaats->>'volgnummer' = adresseert_standplaats.volgnummer
    WHERE nag._date_deleted IS NULL
    ORDER BY  nag.identificatie, nag.volgnummer