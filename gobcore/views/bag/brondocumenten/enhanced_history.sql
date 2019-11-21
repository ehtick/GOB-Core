    SELECT
      bag.dossier,
      dsr.heeft_brondocument AS documentnummer,
      date(dct.registratiedatum) AS registratiedatum,
      dct.bronleverancier->>'omschrijving' AS bronleverancier,
      dct.type_dossier->>'omschrijving' AS type_dossier,
      dct.type_brondocument->>'omschrijving' AS type_brondocument 
    FROM (
      SELECT heeft_dossier->>'id' AS dossier FROM bag_ligplaatsen
      UNION
      SELECT heeft_dossier->>'id' AS dossier FROM bag_nummeraanduidingen
      UNION
      SELECT heeft_dossier->>'id' AS dossier FROM bag_openbareruimtes
      UNION
      SELECT heeft_dossier->>'id' AS dossier FROM bag_panden
      UNION
      SELECT heeft_dossier->>'id' AS dossier FROM bag_standplaatsen
      UNION
      SELECT heeft_dossier->>'id' AS dossier FROM bag_verblijfsobjecten
      UNION
      SELECT heeft_dossier->>'id' AS dossier FROM bag_woonplaatsen
    ) bag
    LEFT JOIN (
     SELECT dossier, value->>'id' AS heeft_brondocument FROM bag_dossiers, jsonb_array_elements(bag_dossiers.heeft_brondocumenten)
    ) dsr ON dsr.dossier = bag.dossier
    LEFT JOIN bag_brondocumenten dct ON dsr.heeft_brondocument = dct.documentnummer
    WHERE dsr.heeft_brondocument IS NOT NULL
    ORDER BY bag.dossier, dsr.heeft_brondocument