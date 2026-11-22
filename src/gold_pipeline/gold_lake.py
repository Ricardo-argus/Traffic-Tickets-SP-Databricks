#Camada gold: métricas sobre gold_multas e exportação parquet.

import io
import sys
sys.path.insert(0, "/Workspace/Users/ricardo.shs615@gmail.com/Traffic-Tickets-SP-Databricks/src")
import pandas as pd
from pyspark.sql import SparkSession

from common.config import (
    PARQUET_GRAVIDADE_MUNICIPIOS,
    TBL_GOLD_MULTAS,
    TBL_METRICAS_GRAVIDADE,
    TBL_METRICAS_MUNICIPIOS,
    TBL_METRICAS_QTDE_PAGAS_VEICULO,
    TBL_METRICAS_VEICULO,
)
from common.io_delta import gravar_metrica_sql
from common.logging_utils import setup_logging
from common.spark_utils import iniciar_sessao

log = setup_logging("gold_lake")

# MÉTRICAS POR MUNICÍPIO
SQL_METRICAS_MUNICIPIOS = """
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

# MÉTRICAS POR TIPO DE VEÍCULO
SQL_METRICAS_VEICULO = """
SELECT 
    g.tipo_veiculo,
    SUM(g.quantidade) AS total_multas
FROM gold g
WHERE g.STATUS = 'VENCIDA'
GROUP BY g.tipo_veiculo
ORDER BY total_multas DESC
"""

# MÉTRICAS POR GRAVIDADE (MES/ANO)
SQL_METRICAS_GRAVIDADE = """
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

# Criar Métrica para quantidade de multas por data e tipo de veiculo desde 2025
SQL_QTDE_PAGAS_VEICULO = """
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

# MÉTRICAS DE INFRAÇÕES GRAVES POR MUNICÍPIO (exportação parquet)
SQL_GRAVIDADE_POR_MUNICIPIO = """
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

# Exportação de uma das métricas em parquet
def exportar_parquet_gravidade(resultados) -> None:
    df = pd.DataFrame([r.asDict() for r in resultados])
    try:
        df.to_parquet(PARQUET_GRAVIDADE_MUNICIPIOS, index=False)
    except PermissionError:
        buf = io.BytesIO()
        df.to_parquet(buf, index=False)
        with open(PARQUET_GRAVIDADE_MUNICIPIOS, "wb") as f:
            f.write(buf.getbuffer())

#Executa função para criação de métricas Delta utilizando camada_gold
def main(spark: SparkSession = None) -> None:
    spark = spark or iniciar_sessao()

    # FAZER LEITURA DAS TABELAS DE MULTAS
    gold = spark.table(TBL_GOLD_MULTAS)
    log.info("[gold] leitura gold_multas: %d linhas", gold.count())
    gold.createOrReplaceTempView("gold")

    log.info("[gold] ETAPA 1: gravando multas_metricas_municipios...")
    gravar_metrica_sql(spark, SQL_METRICAS_MUNICIPIOS, TBL_METRICAS_MUNICIPIOS)

    log.info("[gold] ETAPA 2: gravando multas_metricas_veiculo...")
    gravar_metrica_sql(spark, SQL_METRICAS_VEICULO, TBL_METRICAS_VEICULO)

    log.info("[gold] ETAPA 3: gravando multas_metricas_gravidade...")
    gravar_metrica_sql(spark, SQL_METRICAS_GRAVIDADE, TBL_METRICAS_GRAVIDADE)

    log.info("[gold] ETAPA 4: gravando quantidade_multas_pagas_por_veiculo...")
    gravar_metrica_sql(spark, SQL_QTDE_PAGAS_VEICULO, TBL_METRICAS_QTDE_PAGAS_VEICULO)

    resultados = spark.sql(SQL_GRAVIDADE_POR_MUNICIPIO).collect()
    log.info("[gold] ETAPA 5: exportando parquet com %d municípios...", len(resultados))
    exportar_parquet_gravidade(resultados)
    log.info("[gold] PIPELINE GOLD CONCLUIDO")


if __name__ == "__main__":
    main()
