## Refino dos dados

from pyspark.sql.functions import col
from pyspark.sql import SparkSession
from bronze_lake import iniciar_sessao

spark = iniciar_sessao():

# FAZER LEITURA DAS TABELAS DE MULTAS
gold = spark.read.table("gold_multas")

# INSERIR SPARK.SQL PARA ARMAZENAR METRICAS
gold.createOrReplaceTempView("gold_multas")

spark.sql("""
SELECT 
    SUM(quantidade) as total_multas,
    count(distinct id_municipio) as total_municipios
FROM gold_multas
""").cache().write.mode("overwrite").saveAsTable("multas_metricas")

# CRIAR GRAFICOS QUE SERAO SALVOS EM CHARTS , AVALIANDO SEGUINTES INFORMACOES
# QUANTIDADE DE MULTAS VENCIDAS POR TIPO DE VEICULO 
# Multas por município, por tipo de infração, por gravidade, por órgão responsável.

