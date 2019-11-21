    SELECT 
      bpg.identificatie,
      kot.identificatie AS kadastraal_object,
      kot.aangeduid_door_kadastralegemeentecode->'broninfo'->>'omschrijving' AS aangeduid_door_kadastralegemeentecode,
      kot.aangeduid_door_kadastralesectie->>'bronwaarde' AS aangeduid_door_kadastralesectie,
      kot.perceelnummer,
      kot.indexletter,
      kot.indexnummer
    FROM
      wkpb_beperkingen bpg
    LEFT JOIN (
      SELECT 
        *
      FROM 
        rel_wkpb_bpg_brk_kot_belast_kadastrale_objecten rel
      LEFT JOIN
        brk_kadastraleobjecten kot1
      ON 
        rel.dst_id = kot1._id 
      AND
        rel.dst_volgnummer = kot1.volgnummer
      WHERE rel.dst_id IS NOT NULL
    ) kot 
    ON
      kot.src_id = bpg._id
    WHERE bpg._expiration_date IS NULL or bpg._expiration_date > NOW()
    ORDER BY
      bpg.identificatie