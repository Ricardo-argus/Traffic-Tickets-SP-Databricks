## Refino dos dados

from pyspark.sql.functions import col
from pyspark.sql import SparkSession
from bronze_lake import iniciar_sessao

spark = iniciar_sessao()

# FAZER LEITURA DAS TABELAS DE MULTAS
gold = spark.read.table("gold_multas")

# INSERIR SPARK.SQL PARA ARMAZENAR METRICAS
gold.createOrReplaceTempView("gold_multas")

spark.sql("""
SELECT
    DISTINCT(m.MUNICIPIO),
    SUM(g.quantidade) as total_multas,
    CASE
    WHEN m.MUNICIPIO IS NULL THEN 'OUTROS'
    ELSE m.MUNICIPIO
    END as municipio,
    CASE 
    WHEN total_multas > 250 THEN 'ALTO'
    WHEN total_multas > 100 THEN 'MEDIO'
    ELSE 'BAIXA'
    END as IMPACTO_Regional
FROM gold_multas g
LEFT JOIN multas_analytics.municipios m ON m.ID_MUNICIPIO = g.ID_MUNICIPIO
WHERE g.Status = 'VENCIDA'
GROUP BY m.MUNICIPIO
ORDER BY total_multas DESC
""").cache().write.mode("overwrite").saveAsTable("multas_metricas")

# CRIAR GRAFICOS QUE SERAO SALVOS EM CHARTS , AVALIANDO SEGUINTES INFORMACOES

# QUANTIDADE DE MULTAS VENCIDAS POR TIPO DE VEICULO 
gold.createOrReplaceTempView("gold_multas")

spark.sql("""
SELECT 
    DISTINCT(tipo_veiculo),
    SUM(g.quantidade) as total_multas
FROM gold_multas g
WHERE g.Status = 'VENCIDA'
GROUP BY tipo_veiculo
ORDER BY total_multas DESC
""").cache().write.mode("overwrite").saveAsTable("multas_metricas_veiculo")

# QUANTIDADE DE MULTAS VENCIDAS POR GRAVIDADE (MES/ANO)
gold.createOrReplaceTempView("gold_multas")

spark.sql("""
SELECT 
    DISTINCT(ic.gravidade),
    SUM(g.quantidade) as total_multas,
    EXTRACT(MONTH FROM g.DATA) AS MÊS,
    EXTRACT(YEAR FROM g.DATA) AS ANO
FROM gold_multas g
LEFT JOIN multas_analytics.infracoes_codes ic ON ic.Código_da_infração = g.codigo_infracao
WHERE g.Status = 'VENCIDA'
GROUP BY gravidade, MÊS, ANO
ORDER BY total_multas DESC
""").cache().write.mode("overwrite").saveAsTable("multas_metricas_gravidade")