
  
  
  
  create or replace view `workspace`.`multas_analytics`.`quantity_municipios`
  
  as (
    -- Analyse if there's NOT null districts 

WITH districts AS (
    SELECT p.ID_MUNICIPIO
    FROM multas_analytics.multas_pagas p
    JOIN multas_analytics.multas_vencidas v 
      ON p.ID_MUNICIPIO = v.ID_MUNICIPIO
)
SELECT count(*) as qtd_municipios_validos
FROM districts
  )
