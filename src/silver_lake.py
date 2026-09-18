## Refino dos dados

from pyspark.sql.functions import col
from pyspark.sql import SparkSession
from bronze_lake.function import iniciar_sessao

spark = iniciar_sessao()


#Ler tabelas gravadas no Delta Lake
silver_pagas = spark.table("multas_analytics.multas_pagas")

silver_vencidas = spark.table("multas_analytics.multas_vencidas")

# AJUSTAR COLNAMES
silver_vencidas = silver_vencidas.withcolumnrenamed("QTDE", "QUANTIDADE")

#TRATAR NULOS
silver_pagas = silver_pagas.fillna({"QUANTIDADE": 1})


# VERIFICAR DUPLICIDADES

# TRATAR ERROS DE FORMATO

# TRATAR ERROS DE CONVERSÃO DE TIPO

# TRATAR ERROS DE CONVERSÃO DE DATA

# CRIAR TABELA INTERMEDIARIA COM ID_MUNICIPIO + MUNICIPIOS RELACIONADOS

# INSERIR SPARK.SQL 

# UTILIZAR SPARKDATAFRAMES

# CRIAR TABELAS GOLD_PAGAS / GOLD_VENCIDAS

