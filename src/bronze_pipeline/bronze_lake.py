import re
import sys
sys.path.insert(0, "/Workspace/Users/ricardo.shs615@gmail.com/Traffic-Tickets-SP-Databricks/src")
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

from common.config import (
    CSV_INFRACOES,
    CSV_PAGAS,
    CSV_VENCIDAS,
    TBL_BRONZE_INFRACOES,
    TBL_BRONZE_PAGAS,
    TBL_BRONZE_VENCIDAS,
)
from common.io_delta import gravar_delta
from common.logging_utils import setup_logging
from common.spark_utils import iniciar_sessao

log = setup_logging("bronze_lake")

# Name columns for Multas_pagasy
SCHEMA_PAGAS = StructType([
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

# Sanitize column names for Delta compatibility.
def sanitizar_colunas(df: DataFrame) -> DataFrame:
    for col_name in df.columns:
        new_name = re.sub(r"[ ,;{}()\n\t=]", "_", col_name)
        if new_name != col_name:
            df = df.withColumnRenamed(col_name, new_name)
    return df


def ler_csv(spark: SparkSession, caminho: str, schema: StructType = None) -> DataFrame:
    if schema is not None:
        return spark.read.csv(caminho, header=True, schema=schema)
    return spark.read.csv(caminho, header=True, inferSchema=True)


def main(spark: SparkSession = None) -> None:
    spark = spark or iniciar_sessao()

    # Collect Multas CSVs Data
    multas_pagas = ler_csv(spark, CSV_PAGAS, schema=SCHEMA_PAGAS)
    multas_vencidas = ler_csv(spark, CSV_VENCIDAS)

    # Infracoes CSV ingestion
    Infra_Codes = sanitizar_colunas(ler_csv(spark, CSV_INFRACOES))

    log.info("[bronze] ETAPA 1: leitura dos CSVs — pagas=%d linhas, vencidas=%d linhas, infracoes_codes=%d linhas",
             multas_pagas.count(), multas_vencidas.count(), Infra_Codes.count())

    # Convert Data to Delta Tables in DB
    log.info("[bronze] ETAPA 2: gravando tabelas no Delta Lake...")
    gravar_delta(spark, multas_pagas, TBL_BRONZE_PAGAS)
    gravar_delta(spark, multas_vencidas, TBL_BRONZE_VENCIDAS)
    gravar_delta(spark, Infra_Codes, TBL_BRONZE_INFRACOES)
    log.info("[bronze] PIPELINE BRONZE CONCLUIDO")


if __name__ == "__main__":
    main()
