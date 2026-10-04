import re
from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

def iniciar_sessao():
    return SparkSession.builder.appName("TrafficTicketsSP").getOrCreate()

spark = iniciar_sessao()

# Name columns for Multas_pagasy 
schema_pagas = StructType([
    StructField("ID_MUNICIPIO", IntegerType(), True),
    StructField("MUNICIPIO", StringType(), True),
    StructField("CODIGO_INFRACAO", StringType(), True),
    StructField("DESCRICAO_INFRACAO", StringType(), True),
    StructField("UF_PLACA_VEICULO", StringType(), True),
    StructField("TIPO_VEICULO", StringType(), True),
    StructField("CATEGORIA_VEICULO", StringType(), True),
    StructField("QUANTIDADE", IntegerType(), True),
    StructField("MES", IntegerType(), True),
    StructField("ANO", IntegerType(), True),
])

#Collect Multas CSVs Data
multas_pagas = spark.read.csv("/Workspace/Users/ricardo.shs615@gmail.com/Traffic-Tickets-SP-Databricks/data/multas_pagas_*.csv", header=True, schema=schema_pagas)

multas_vencidas = spark.read.csv("/Workspace/Users/ricardo.shs615@gmail.com/Traffic-Tickets-SP-Databricks/data/multas_vencidas_*.csv", header=True, inferSchema=True)

# Infracoes CSV ingestion
Infra_Codes = spark.read.csv("/Workspace/Users/ricardo.shs615@gmail.com/Traffic-Tickets-SP-Databricks/data/tabela-codigo-infracoes-renainf.csv", header=True, inferSchema=True)

# Sanitize column names for Delta compatibility
for col_name in Infra_Codes.columns:
    new_name = re.sub(r"[ ,;{}()\n\t=]", "_", col_name)
    if new_name != col_name:
        Infra_Codes = Infra_Codes.withColumnRenamed(col_name, new_name)
            
# Convert Data to Delta Tables in DB
if spark.catalog.tableExists("multas_analytics.multas_pagas"):
    multas_pagas.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("multas_analytics.multas_pagas")
else:
    multas_pagas.write.format("delta").mode("overwrite").saveAsTable("multas_analytics.multas_pagas")

if spark.catalog.tableExists("multas_analytics.multas_vencidas"):
    multas_vencidas.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("multas_analytics.multas_vencidas")
else:
    multas_vencidas.write.format("delta").mode("overwrite").saveAsTable("multas_analytics.multas_vencidas")

if spark.catalog.tableExists("multas_analytics.infracoes_codes"):
    Infra_Codes.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("multas_analytics.infracoes_codes")
else:
    Infra_Codes.write.format("delta").mode("overwrite").saveAsTable("multas_analytics.infracoes_codes")