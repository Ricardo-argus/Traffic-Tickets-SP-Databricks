WITH vehicle_type AS (
    SELECT *
    FROM {{ source('gold', 'gold_multas') }}
    WHERE TIPO_VEICULO != 'Nao Identificado'
)
SELECT 
    TIPO_VEICULO, 
    {{ sum_column('QUANTIDADE') }} AS QUANTIDADE_TOTAL
FROM vehicle_type
GROUP BY TIPO_VEICULO
ORDER BY QUANTIDADE_TOTAL DESC;
