    SELECT
      nag.identificatie,
      nag.aanduiding_in_onderzoek,
      nag.geconstateerd,
      nag.huisnummer,
      nag.huisletter,
      nag.huisnummertoevoeging,
      nag.postcode,
      json_build_object(
        'identificatie', ligt_aan_openbareruimte.identificatie,
        'naam', ligt_aan_openbareruimte.naam
      ) AS _ref_ligt_aan_openbareruimte_bag_ore,
      json_build_object(
        'identificatie', ligt_in_woonplaats.identificatie,
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
        'identificatie', adresseert_verblijfsobject.identificatie
      ) AS _ref_adresseert_verblijfsobject_bag_vot,
      json_build_object(
        'identificatie', adresseert_ligplaats.identificatie
      ) AS _ref_adresseert_ligplaats_bag_lps,
      json_build_object(
        'identificatie', adresseert_standplaats.identificatie
      ) AS _ref_adresseert_standplaats_bag_sps
    FROM
      bag_nummeraanduidingen AS nag
    -- SELECT ligt_aan_openbareruimte
    LEFT JOIN
      bag_openbareruimtes AS ligt_aan_openbareruimte
    ON
      nag.ligt_aan_openbareruimte->>'id' = ligt_aan_openbareruimte._id AND
      (nag.ligt_aan_openbareruimte->>'volgnummer')::int = ligt_aan_openbareruimte.volgnummer
    -- SELECT ligt_in_woonplaats
    LEFT JOIN
      bag_woonplaatsen AS ligt_in_woonplaats
    ON
      nag.ligt_in_woonplaats->>'id' = ligt_in_woonplaats._id AND
      (nag.ligt_in_woonplaats->>'volgnummer')::int = ligt_in_woonplaats.volgnummer
    -- SELECT adresseert_verblijfsobject
    LEFT JOIN (
          SELECT
              dst_id,
              dst_volgnummer,
              src_id,
              max(src_volgnummer) as src_volgnummer
          FROM (
                   SELECT *
                   FROM mv_bag_vot_bag_nag_heeft_hoofdadres
                   UNION
                   SELECT *
                   FROM mv_bag_vot_bag_nag_heeft_nevenadres
               ) q GROUP BY dst_id, dst_volgnummer, src_id
      ) vot_adressen ON vot_adressen.dst_id = nag._id AND vot_adressen.dst_volgnummer = nag.volgnummer
    LEFT JOIN bag_verblijfsobjecten adresseert_verblijfsobject ON vot_adressen.src_id = adresseert_verblijfsobject._id and vot_adressen.src_volgnummer = adresseert_verblijfsobject.volgnummer
    -- SELECT adresseert_ligplaats
    LEFT JOIN (
          SELECT
              dst_id,
              dst_volgnummer,
              src_id,
              max(src_volgnummer) as src_volgnummer
          FROM (
                   SELECT *
                   FROM mv_bag_lps_bag_nag_heeft_hoofdadres
                   UNION
                   SELECT *
                   FROM mv_bag_lps_bag_nag_heeft_nevenadres
               ) q GROUP BY dst_id, dst_volgnummer, src_id
      ) lps_adressen ON lps_adressen.dst_id = nag._id AND lps_adressen.dst_volgnummer = nag.volgnummer
    LEFT JOIN bag_ligplaatsen adresseert_ligplaats ON lps_adressen.src_id = adresseert_ligplaats._id and lps_adressen.src_volgnummer = adresseert_ligplaats.volgnummer
    -- SELECT adresseert_standplaats
    LEFT JOIN (
          SELECT
              dst_id,
              dst_volgnummer,
              src_id,
              max(src_volgnummer) as src_volgnummer
          FROM (
                   SELECT *
                   FROM mv_bag_sps_bag_nag_heeft_hoofdadres
                   UNION
                   SELECT *
                   FROM mv_bag_sps_bag_nag_heeft_nevenadres
               ) q GROUP BY dst_id, dst_volgnummer, src_id
      ) sps_adressen ON sps_adressen.dst_id = nag._id AND sps_adressen.dst_volgnummer = nag.volgnummer
    LEFT JOIN bag_standplaatsen adresseert_standplaats ON sps_adressen.src_id = adresseert_standplaats._id and sps_adressen.src_volgnummer = adresseert_standplaats.volgnummer
    WHERE
      (nag._expiration_date > current_date OR nag._expiration_date IS NULL)
      AND nag._date_deleted IS NULL