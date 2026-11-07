import sys
import pandas as pd
sys.path.insert(0, "/Workspace/Users/ricardo.shs615@gmail.com/Traffic-Tickets-SP-Databricks/src")
from pyspark.sql.functions import col
from pyspark.sql import SparkSession
from bronze_pipeline.bronze_lake import iniciar_sessao

spark = iniciar_sessao()

# FAZER LEITURA DAS TABELAS DE MULTAS
gold = spark.read.table("multas_analytics.gold_multas")
gold.createOrReplaceTempView("gold")

# MÉTRICAS POR MUNICÍPIO
spark.sql("""
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
""").write.mode("overwrite").saveAsTable("multas_metricas_municipios")

# MÉTRICAS POR TIPO DE VEÍCULO
spark.sql("""
SELECT 
    g.tipo_veiculo,
    SUM(g.quantidade) AS total_multas
FROM gold g
WHERE g.STATUS = 'VENCIDA'
GROUP BY g.tipo_veiculo
ORDER BY total_multas DESC
""").write.mode("overwrite").saveAsTable("multas_metricas_veiculo")

# MÉTRICAS POR GRAVIDADE (MES/ANO)
spark.sql("""
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
""").write.mode("overwrite").saveAsTable("multas_metricas_gravidade")

#CRIAR ARQUIVO PARQUET COM METRICAS SOBRE GOLD_MULTAS & INFRACOES GRAVES POR MUNICIPIO
resultados = spark.sql("""
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
ORDER BY total_multas DESC""").collect()

pd.DataFrame([r.asDict() for r in resultados]).to_parquet(
    "/Workspace/Users/ricardo.shs615@gmail.com/Traffic-Tickets-SP-Databricks/data/multas_gravidade_per_municipality.parquet",
    index=False
)