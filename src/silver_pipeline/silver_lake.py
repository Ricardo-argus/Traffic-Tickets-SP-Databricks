## Refino dos dados

import sys
sys.path.insert(0, "/Workspace/Users/ricardo.shs615@gmail.com/Traffic-Tickets-SP-Databricks/src")
from pyspark.sql.functions import col
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.functions import row_number
from bronze_pipeline.bronze_lake import iniciar_sessao
import logging
import traceback

# rastreia cada etapa; em caso de erro, loga o traceback completo 
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
log = logging.getLogger("silver_lake")

spark = iniciar_sessao()

#Ler tabelas gravadas no Delta Lake
silver_pagas = spark.table("multas_analytics.multas_pagas")

silver_vencidas = spark.table("multas_analytics.multas_vencidas")

# AJUSTAR COLNAMES
silver_vencidas = silver_vencidas.withColumnRenamed("QTDE", "QUANTIDADE")
silver_vencidas = silver_vencidas.withColumnRenamed("NOME_MUNICIPIO", "MUNICIPIO")

# Remover municipios nulos não informados
log.info("[silver] ETAPA 1: leitura bronze — pagas=%d linhas, vencidas=%d linhas",
         silver_pagas.count(), silver_vencidas.count())

n_pagas_antes, n_vencidas_antes = silver_pagas.count(), silver_vencidas.count()

silver_pagas = silver_pagas.filter(F.col("MUNICIPIO").isNotNull())

silver_vencidas = silver_vencidas.filter(F.col("MUNICIPIO").isNotNull())

log.info("[silver] filtro MUNICIPIO nulo: pagas %d->%d (%d removidas), vencidas %d->%d (%d removidas)",
         n_pagas_antes, silver_pagas.count(), n_pagas_antes - silver_pagas.count(),
         n_vencidas_antes, silver_vencidas.count(), n_vencidas_antes - silver_vencidas.count())

log.info("[silver] ETAPA 2: tratamento de nulos (QUANTIDADE)")

#TRATAR NULOS
silver_pagas = silver_pagas.fillna({"QUANTIDADE": 1})

# Vencidas: em vez de fillna(1), cria uma coluna ajustada
silver_vencidas = silver_vencidas.withColumn(
    "QUANTIDADE_AJUSTADA",
    F.when(F.col("QUANTIDADE").isNotNull(), F.col("QUANTIDADE"))
     .otherwise(F.lit(1))
)

# Remove duplicatas de multas vencidas em snapshots mensais, mantendo apenas o primeiro registro por multa.
dedup_window = Window.partitionBy(
    "ID_MUNICIPIO", "MUNICIPIO", "CODIGO_INFRACAO",
    "UF_PLACA_VEICULO", "TIPO_VEICULO", "CATEGORIA_VEICULO"
).orderBy("ANO", "MES")

n_antes_dedup = silver_vencidas.count()
silver_vencidas = (
    silver_vencidas
    .withColumn("_snapshot_rank", F.row_number().over(dedup_window))
    .filter(F.col("_snapshot_rank") == 1)
    .drop("_snapshot_rank")
)
log.info("[silver] ETAPA 3: dedup snapshots vencidas: %d -> %d linhas (%d duplicatas removidas)",
         n_antes_dedup, silver_vencidas.count(), n_antes_dedup - silver_vencidas.count())

# agrupar e somar quantidades totais
silver_pagas = (
    silver_pagas.groupBy(
        "ID_MUNICIPIO", "MUNICIPIO", "CODIGO_INFRACAO",
        "UF_PLACA_VEICULO", "TIPO_VEICULO", "CATEGORIA_VEICULO",
        "MES", "ANO"
    )
    .agg(F.sum(F.coalesce(col("QUANTIDADE"), F.lit(1))).alias("QUANTIDADE"))
)
log.info("[silver] ETAPA 4: agregação pagas concluída: %d linhas no grain", silver_pagas.count())

# Vencidas: groupBy apos deduplicacao — cada multa aparece apenas uma vez
silver_vencidas = (
    silver_vencidas.groupBy(
        "ID_MUNICIPIO", "MUNICIPIO", "CODIGO_INFRACAO",
        "UF_PLACA_VEICULO", "TIPO_VEICULO", "CATEGORIA_VEICULO",
        "MES", "ANO"
    )
    .agg(F.sum("QUANTIDADE_AJUSTADA").alias("QUANTIDADE"))
)
log.info("[silver] agregação vencidas concluída: %d linhas no grain", silver_vencidas.count())

# starting with ID
windowSpec = Window.orderBy(monotonically_increasing_id())

silver_pagas = silver_pagas.withColumn("ID_MULTA", row_number(). over(windowSpec))
silver_vencidas = silver_vencidas.withColumn("ID_MULTA", row_number(). over(windowSpec))

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
log.info("[silver] dimensão municipios: %d municípios distintos", municipios.count())

# Criar Tabela no Delta Lake
if spark.catalog.tableExists("multas_analytics.municipios"):
    municipios.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("multas_analytics.municipios")
else:
    municipios.write.format("delta").mode("overwrite").saveAsTable("multas_analytics.municipios")
log.info("[silver] tabela municipios gravada no Delta Lake")

#REORGANIZAR ORDEM DAS COLUNAS

#Pagas
cols_pagas = silver_pagas.columns

new_order_pagas = cols_pagas

silver_pagas = silver_pagas.select(*new_order_pagas)

#Vencidas
cols_vencidas = silver_vencidas.columns

new_order_vencidas = cols_vencidas

silver_vencidas = silver_vencidas.select(*new_order_vencidas)

#Adicionar Coluna Status
silver_pagas = silver_pagas.withColumn("STATUS", F.lit("PAGA"))
silver_vencidas = silver_vencidas.withColumn("STATUS", F.lit("VENCIDA"))

# Criar duas tabelas Silver no Delta Lake
log.info("[silver] ETAPA 5: gravando tabelas silver no Delta Lake...")
try:
    if spark.catalog.tableExists("multas_analytics.silver_pagas"):
        silver_pagas.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("multas_analytics.silver_pagas")
    else:
        silver_pagas.write.format("delta").mode("overwrite").saveAsTable("multas_analytics.silver_pagas")
    log.info("[silver] silver_pagas gravada: %d linhas", silver_pagas.count())
    
    if spark.catalog.tableExists("multas_analytics.silver_vencidas"):
        silver_vencidas.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("multas_analytics.silver_vencidas")
    else:
        silver_vencidas.write.format("delta").mode("overwrite").saveAsTable("multas_analytics.silver_vencidas")
    log.info("[silver] silver_vencidas gravada: %d linhas", silver_vencidas.count())
    log.info("[silver] PIPELINE SILVER CONCLUIDO")
except Exception:
    log.error("[silver] FALHOU na gravação das tabelas silver:\n%s", traceback.format_exc())
    raise

# CRIAR TABELA GOLD JUNTANDO AS DUAS MULTAS
gold_multas = silver_pagas.select(
    "ID_MUNICIPIO", "CODIGO_INFRACAO", "UF_PLACA_VEICULO",
    "TIPO_VEICULO", "CATEGORIA_VEICULO", "QUANTIDADE", "DATA", "STATUS"
).union(
    silver_vencidas.select(
        "ID_MUNICIPIO", "CODIGO_INFRACAO", "UF_PLACA_VEICULO",
        "TIPO_VEICULO", "CATEGORIA_VEICULO", "QUANTIDADE", "DATA", "STATUS"
    )
)

#Identify Non Identified Vehicles 
gold_multas = gold_multas.fillna({"CATEGORIA_VEICULO": "Nao Identificado"})

gold_multas = gold_multas.fillna({"TIPO_VEICULO": "Nao Identificado"})
           
# Criar Tabela GOLD no Delta Lake
log.info("[silver] ETAPA 6: gravando gold_multas (%d linhas)...", gold_multas.count())
try:
    if spark.catalog.tableExists("multas_analytics.gold_multas"):
        gold_multas.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("multas_analytics.gold_multas")
    else:
        gold_multas.write.format("delta").mode("overwrite").saveAsTable("multas_analytics.gold_multas")
    log.info("[silver] PIPELINE SILVER+GOLD CONCLUIDO")
except Exception:
    log.error("[silver] FALHOU na gravação da gold_multas:\n%s", traceback.format_exc())
    raise