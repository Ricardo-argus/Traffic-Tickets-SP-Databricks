WITH percentage_per_municipio AS(
SELECT
    d.MUNICIPIO,
    SUM(gm.QUANTIDADE) AS qtd_multas_vencidas,
    ROUND(
        100 * SUM(gm.QUANTIDADE)
        / SUM(SUM(gm.QUANTIDADE)) OVER (),
        2
    ) AS porcentagem
FROM multas_analytics.gold_multas gm
LEFT JOIN multas_analytics.municipios d
ON gm.ID_MUNICIPIO = d.ID_MUNICIPIO
WHERE gm.STATUS = 'PAGA'
GROUP BY d.MUNICIPIO
)
SELECT * FROM percentage_per_municipio
ORDER BY porcentagem DESC;