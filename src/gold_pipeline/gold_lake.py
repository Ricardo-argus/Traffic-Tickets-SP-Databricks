# Gold layer: metrics over gold_multas and parquet export.

import io
import sys
sys.path.insert(0, "/Workspace/Users/ricardo.shs615@gmail.com/Traffic-Tickets-SP-Databricks/src")
import pandas as pd
from pyspark.sql import SparkSession

from common.config import (
    PARQUET_SEVERITY_MUNICIPALITIES,
    TBL_GOLD_FINES,
    TBL_METRICS_SEVERITY,
    TBL_METRICS_MUNICIPALITIES,
    TBL_METRICS_PAID_BY_VEHICLE,
    TBL_METRICS_VEHICLE,
)
from common.io_delta import write_metric_from_sql
from common.logging_utils import setup_logging
from common.spark_utils import get_spark_session

log = setup_logging("gold_lake")

# METRICS BY MUNICIPALITY
SQL_METRICS_MUNICIPALITIES = """
SELECT
    SUM(g.quantidade) AS total_multas,
    CASE 
        WHEN m.MUNICIPIO IS NULL THEN 'OUTROS'
        ELSE m.MUNICIPIO
    END AS municipio,
    CASE 
        WHEN SUM(g.quantidade) > 250 THEN 'ALTO'
        WHEN SUM(g.quantidade) > 100 THEN 'MEDIO'
        ELSE 'BAIXA'
    END AS impacto_regional
FROM gold g
LEFT JOIN multas_analytics.municipios m 
    ON m.ID_MUNICIPIO = g.ID_MUNICIPIO
WHERE g.STATUS = 'VENCIDA'
GROUP BY m.MUNICIPIO
ORDER BY total_multas DESC
"""

# METRICS BY VEHICLE TYPE
SQL_METRICS_VEHICLE = """
SELECT 
    g.tipo_veiculo,
    SUM(g.quantidade) AS total_multas
FROM gold g
WHERE g.STATUS = 'VENCIDA'
GROUP BY g.tipo_veiculo
ORDER BY total_multas DESC
"""

# METRICS BY SEVERITY (MONTH/YEAR)
SQL_METRICS_SEVERITY = """
SELECT 
    CASE WHEN ic.gravidade IS NULL THEN 'DESCONHECIDA' ELSE ic.gravidade END AS gravidade,
    SUM(g.quantidade) AS total_multas,
    EXTRACT(MONTH FROM g.DATA) AS mes,
    EXTRACT(YEAR FROM g.DATA) AS ano
FROM gold g
LEFT JOIN multas_analytics.infracoes_codes ic 
    ON CONCAT(CAST(ic.`Código_da_infração` AS STRING), '-', CAST(ic.`Desdob.` AS STRING)) = g.codigo_infracao
WHERE g.STATUS = 'VENCIDA'
GROUP BY ic.gravidade, mes, ano
ORDER BY total_multas DESC
"""

# Metric for the quantity of paid fines per date and vehicle type since 2025
SQL_METRICS_PAID_BY_VEHICLE = """
WITH paid_per_vehicle AS (
    SELECT
        DATA,
        SUM(QUANTIDADE) AS quantidade_multas_pagas,
        TIPO_VEICULO
    FROM gold
    WHERE STATUS = 'PAGA'
    GROUP BY DATA, TIPO_VEICULO
)

SELECT
quantidade_multas_pagas, 
CASE
    WHEN quantidade_multas_pagas > 50000 THEN 'HIGH QUANTITY'
    WHEN quantidade_multas_pagas > 20000 THEN 'MEDIUM QUANTITY'
ELSE 'LOW QUANTITY'
END AS Status_quantidade, 
TIPO_VEICULO, 
DATA 
FROM paid_per_vehicle
WHERE to_date(DATA) >= '2025-10-30'
AND DATA <= '2026-09-01'
ORDER BY quantidade_multas_pagas DESC;"""

# SEVERE INFRACTIONS BY MUNICIPALITY metrics (parquet export)
SQL_SEVERITY_BY_MUNICIPALITY = """
SELECT 
    g.ID_MUNICIPIO,
    m.MUNICIPIO,
    SUM(g.quantidade) AS total_multas
FROM multas_analytics.gold_multas g 
LEFT JOIN multas_analytics.infracoes_codes ic 
    ON CONCAT(CAST(ic.`Código_da_infração` AS STRING), '-', CAST(ic.`Desdob.` AS STRING)) = g.codigo_infracao
LEFT JOIN multas_analytics.municipios m 
    ON m.ID_MUNICIPIO = g.ID_MUNICIPIO
WHERE g.STATUS = 'VENCIDA'
AND (
    try_cast(regexp_extract(ic.gravidade, '^ *([0-9]+)', 1) AS INT) > 5
    OR ic.gravidade LIKE '%Gravíss%'
)
GROUP BY g.ID_MUNICIPIO, m.MUNICIPIO
ORDER BY total_multas DESC"""

# Exports one of the metrics to parquet
def export_severity_by_municipality(results) -> None:
    df = pd.DataFrame([r.asDict() for r in results])
    try:
        df.to_parquet(PARQUET_SEVERITY_MUNICIPALITIES, index=False)
    except PermissionError:
        buf = io.BytesIO()
        df.to_parquet(buf, index=False)
        with open(PARQUET_SEVERITY_MUNICIPALITIES, "wb") as f:
            f.write(buf.getbuffer())

#Runs the function that creates the Delta metrics using the gold layer
def main(spark: SparkSession = None) -> None:
    spark = spark or get_spark_session()

    # Read the fines table
    gold = spark.table(TBL_GOLD_FINES)
    log.info("[gold] gold_multas read: %d rows", gold.count())
    gold.createOrReplaceTempView("gold")

    log.info("[gold] STEP 1: writing multas_metricas_municipios...")
    write_metric_from_sql(spark, SQL_METRICS_MUNICIPALITIES, TBL_METRICS_MUNICIPALITIES)

    log.info("[gold] STEP 2: writing multas_metricas_veiculo...")
    write_metric_from_sql(spark, SQL_METRICS_VEHICLE, TBL_METRICS_VEHICLE)

    log.info("[gold] STEP 3: writing multas_metricas_gravidade...")
    write_metric_from_sql(spark, SQL_METRICS_SEVERITY, TBL_METRICS_SEVERITY)

    log.info("[gold] STEP 4: writing quantidade_multas_pagas_por_veiculo...")
    write_metric_from_sql(spark, SQL_METRICS_PAID_BY_VEHICLE, TBL_METRICS_PAID_BY_VEHICLE)

    results = spark.sql(SQL_SEVERITY_BY_MUNICIPALITY).collect()
    log.info("[gold] STEP 5: exporting parquet with %d municipalities...", len(results))
    export_severity_by_municipality(results)
    log.info("[gold] GOLD PIPELINE FINISHED")

if __name__ == "__main__":
    main()
