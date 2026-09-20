## Refino dos dados

import sys
sys.path.insert(0, "/Workspace/Users/ricardo.shs615@gmail.com/Traffic-Tickets-SP/src")
from pyspark.sql.functions import col
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from bronze_pipeline.bronze_lake import iniciar_sessao

spark = iniciar_sessao()

#Ler tabelas gravadas no Delta Lake
silver_pagas = spark.table("multas_analytics.multas_pagas")

silver_vencidas = spark.table("multas_analytics.multas_vencidas")

# AJUSTAR COLNAMES
silver_vencidas = silver_vencidas.withColumnRenamed("QTDE", "QUANTIDADE")
silver_vencidas = silver_vencidas.withColumnRenamed("NOME_MUNICIPIO", "MUNICIPIO")

#TRATAR NULOS
silver_pagas = silver_pagas.fillna({"QUANTIDADE": 1})

#JUNTAR COLUNA MES + ANO E CONVERTER PARA DATE
silver_pagas = silver_pagas.withColumn(
    "DATA", F.to_date(F.concat_ws("-", col("ANO"), F.lpad(col("MES"), 2, "0")), "yyyy-MM")
)
silver_vencidas = silver_vencidas.withColumn(
    "DATA", F.to_date(F.concat_ws("-", col("ANO"), F.lpad(col("MES"), 2, "0")), "yyyy-MM")
)

#CRIAR NOVA TABELA INTERMEDIARIA MUNICIPIOS
municipios_pagas = silver_pagas.select("ID_MUNICIPIO", "MUNICIPIO")
municipios_vencidas = silver_vencidas.select("ID_MUNICIPIO", "MUNICIPIO")

#UNIR AS DUAS TABELAS
municipios = municipios_pagas.union(municipios_vencidas).distinct()

# Criar Tabela no Delta Lake
if spark.catalog.tableExists("multas_analytics.municipios"):
    municipios.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("multas_analytics.municipios")
else:
    municipios.write.format("delta").mode("overwrite").saveAsTable("multas_analytics.municipios")

#REORGANIZAR ORDEM DAS COLUNAS

#Pagas
cols_pagas = silver_pagas.columns

new_order_pagas = ["ID_MULTA"] + [c for c in cols_pagas if c != "ID_MULTA"]

silver_pagas = silver_pagas.select(*new_order_pagas)

#Vencidas
cols_vencidas = silver_vencidas.columns

new_order_vencidas = ["ID_MULTA"] + [c for c in cols_vencidas if c != "ID_MULTA"]

silver_vencidas = silver_vencidas.select(*new_order_vencidas)

#Adicionar Coluna Status
silver_pagas = silver_pagas.withColumn("STATUS", F.lit("PAGA"))
silver_vencidas = silver_vencidas.withColumn("STATUS", F.lit("VENCIDA"))

# Criar duas tabelas Silver no Delta Lake
if spark.catalog.tableExists("multas_analytics.silver_pagas"):
    silver_pagas.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("multas_analytics.silver_pagas")
else:
    silver_pagas.write.format("delta").mode("overwrite").saveAsTable("multas_analytics.silver_pagas")
    
if spark.catalog.tableExists("multas_analytics.silver_vencidas"):
    silver_vencidas.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("multas_analytics.silver_vencidas")
else:
    silver_vencidas.write.format("delta").mode("overwrite").saveAsTable("multas_analytics.silver_vencidas")

# CRIAR TABELA AUXILIAR QUE AVALIA POPULACAO ESTIMADA DE CADA UM DOS MUNICIPIOS PRESENTES NA TABELA MUNICIPIOS (SEADE/DADOS ABERTOS)

# VERIFICAR QUANTIDADE MUNICIPIOS QUE ESTAO NA BASE DE DADOS EM COMPARACAO COM OS MUNICIPIOS EXISTENTES NO ESTADO
# BAIXAR LISTA DE MUNICIPIOS NA SEADE E COMPARAR COM OS DA  BASE DE DADOS MULTAS ANALYTICS

# CRIAR TABELA GOLD JUNTANDO AS DUAS MULTAS
gold_multas = silver_pagas.select(
    "ID_MULTA", "ID_MUNICIPIO", "CODIGO_INFRACAO", "UF_PLACA_VEICULO",
    "TIPO_VEICULO", "CATEGORIA_VEICULO", "QUANTIDADE", "DATA", "STATUS"
).union(
    silver_vencidas.select(
        "ID_MULTA", "ID_MUNICIPIO", "CODIGO_INFRACAO", "UF_PLACA_VEICULO",
        "TIPO_VEICULO", "CATEGORIA_VEICULO", "QUANTIDADE", "DATA", "STATUS"
    )
)

#Identify Non Identified Vehicles 
gold_multas = gold_multas.Fillna({"CATEGORIA_VEICULO", "Nao Identificado"})

gold_multas = gold_multas.Fillna({"TIPO_VEICULO", "Nao Identificado"})
           
# Criar Tabela GOLD no Delta Lake
if spark.catalog.tableExists("multas_analytics.gold_multas"):
    gold_multas.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("multas_analytics.gold_multas")
else:
    gold_multas.write.format("delta").mode("overwrite").saveAsTable("multas_analytics.gold_multas")