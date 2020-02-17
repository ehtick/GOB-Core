SELECT bdt.documentnummer
	 , dsr.dossier
	 , bpg.identificatie
     , bpg.persoonsgegevens_afschermen
     , bpg.orgaan
     , bpg.aard
FROM
	wkpb_brondocumenten bdt
INNER JOIN mv_wkpb_dsr_wkpb_bdt_heeft_brondocumenten dsr_bdt
  ON bdt."_source_id" = dsr_bdt.dst_id
INNER JOIN wkpb_dossiers dsr
  ON dsr_bdt.src_id  = dsr."_source_id"
  AND dsr."_date_deleted" IS NULL
INNER JOIN mv_wkpb_bpg_wkpb_dsr_heeft_dossier bpg_dsr
  ON dsr."_source_id" = bpg_dsr.dst_id
INNER JOIN wkpb_beperkingen bpg
  ON bpg.identificatie = bpg_dsr.src_id
  AND bpg."_date_deleted" IS NULL
WHERE
	bpg."status" -> 'code' = '3'
AND bpg.aard -> 'code' <> '3'
AND (bpg._expiration_date IS NULL
     OR bpg._expiration_date > NOW())
ORDER BY
    bpg.identificatie