WITH vehicle_type AS (
    SELECT
        g.QUANTIDADE,
        t.TIPO_VEICULO
    FROM {{ source('gold', 'gold_multas') }} g
    LEFT JOIN {{ source('gold', 'tipos_veiculo') }} t
        ON t.ID_TIPO_VEICULO = g.ID_TIPO_VEICULO
    WHERE t.TIPO_VEICULO != 'Nao Identificado'
    AND g.STATUS = 'PAGA'
)
SELECT 
    TIPO_VEICULO, 
    {{ sum_column('QUANTIDADE') }} AS QUANTIDADE_TOTAL
FROM vehicle_type
GROUP BY TIPO_VEICULO
ORDER BY QUANTIDADE_TOTAL DESC;
