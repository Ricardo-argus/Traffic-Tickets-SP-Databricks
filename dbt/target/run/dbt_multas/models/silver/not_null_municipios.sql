
  
  
  
  create or replace view `workspace`.`multas_analytics`.`not_null_municipios`
  
  as (
    -- Analyse if there's NOT null districts 

WITH districts AS (
    SELECT p.ID_MUNICIPIO
    FROM multas_analytics.multas_pagas p
    JOIN multas_analytics.multas_vencidas v 
      ON p.ID_MUNICIPIO = v.ID_MUNICIPIO
    WHERE p.ID_MUNICIPIO IS NOT NULL
)
SELECT count(*) as qtd_municipios_validos
FROM districts
  )
