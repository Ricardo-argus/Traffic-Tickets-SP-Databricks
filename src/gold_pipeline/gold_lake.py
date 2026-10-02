import sys
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
    ic.gravidade,
    SUM(g.quantidade) AS total_multas,
    EXTRACT(MONTH FROM g.DATA) AS mes,
    EXTRACT(YEAR FROM g.DATA) AS ano
FROM gold g
LEFT JOIN multas_analytics.infracoes_codes ic 
    ON CAST(ic.`Código_da_infração` AS STRING) = g.codigo_infracao
WHERE g.STATUS = 'VENCIDA'
GROUP BY ic.gravidade, mes, ano
ORDER BY total_multas DESC
""").write.mode("overwrite").saveAsTable("multas_metricas_gravidade")

#CRIAR ARQUIVO PARQUET COM METRICAS SOBRE GOLD_MULTAS & INFRACOES

#PARQUET FILE OBSERVANDO AS MULTAS COM GRAVIDADE > 5 ATRASADAS, DE ACORDO COM MUNICIPIOS, QUANTIDADE E INDICANDO A PORCENTAGEM TOTAL RELATIVO AO TOTAL DE MULTAS