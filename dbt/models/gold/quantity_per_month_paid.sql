--Collect the total amount of fines paid per month
WITH quantity_per_month_paid AS (
  SELECT 
  * 
  FROM {{ source('gold', 'gold_multas') }}
  WHERE STATUS = 'PAGA'
)
SELECT 
STATUS as Status_multa, 
{{ sum_column('QUANTIDADE') }} AS QUANTIDADE_TOTAL, 
DATA 
FROM quantity_per_month_paid
GROUP BY STATUS, DATA
ORDER BY  DATA ASC