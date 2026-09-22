-- Analyse if there's NOT null districts 

WITH districts AS(
    SELECT count(P.ID_MUNICIPIO) FROM multas_analytics.multas_pagas p 
    JOIN multas_analytics.multas_vencidas v on p.ID_MUNICIPIO = v.ID_MUNICIPIO
)
SELECT * FROM districts WHERE P.ID_MUNICIPIO IS NOT NULL