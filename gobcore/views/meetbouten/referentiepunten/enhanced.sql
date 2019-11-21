    SELECT identificatie
    ,      geometrie
    ,      hoogte_tov_nap
    ,      datum
    ,      CONCAT_WS(' ', nabij_nummeraanduiding->>'bronwaarde', locatie)       AS locatie
    FROM  meetbouten_referentiepunten
    WHERE publiceerbaar = TRUE