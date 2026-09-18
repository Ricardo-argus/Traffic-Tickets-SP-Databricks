## Refino dos dados

from pyspark.sql.functions import col
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from bronze_lake.function import iniciar_sessao

spark = iniciar_sessao()


#Ler tabelas gravadas no Delta Lake
silver_pagas = spark.table("multas_analytics.multas_pagas")

silver_vencidas = spark.table("multas_analytics.multas_vencidas")

# AJUSTAR COLNAMES
silver_vencidas = silver_vencidas.withcolumnrenamed("QTDE", "QUANTIDADE")

#TRATAR NULOS
silver_pagas = silver_pagas.fillna({"QUANTIDADE": 1})

# CRIAR COLUNA DATA
#Pagas
#JUNTAR COLUNA MES + ANO E CONVERTER PARA DATE

#Vencidas



#REORGANIZAR ORDEM DAS COLUNAS

#Pagas
cols_pagas = silver_pagas.columns

new_order_pagas = ["ID_MULTA"] + [c for c in cols_pagas if c != "ID_MULTA"]

silver_pagas = silver_pagas.select(*new_order_pagas)

#Vencidas
cols_vencidas = silver_vencidas.columns

new_order_vencidas = ["ID_MULTA"] + [c for c in cols_vencidas if c != "ID_MULTA"]

silver_vencidas = silver_vencidas.select(*new_order_vencidas)


# CRIAR TABELAS PAGAS / VENCIDAS (GOLD)


