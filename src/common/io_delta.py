# Leitura e gravação de tabelas Delta.

import logging
import traceback
from pyspark.sql import DataFrame, SparkSession
from common.logging_utils import setup_logging

log = setup_logging("io_delta")

# Grava `df` como tabela Delta em modo overwrite.
def gravar_delta(spark: SparkSession, df: DataFrame, tabela: str) -> int:

    n = df.count()
    try:
        writer = df.write.format("delta").mode("overwrite")
        if spark.catalog.tableExists(tabela):
            writer = writer.option("overwriteSchema", "true")
        writer.saveAsTable(tabela)
    except Exception:
        log.error("[io_delta] FALHOU ao gravar %s:\n%s", tabela, traceback.format_exc())
        raise
    log.info("[io_delta] %s gravada: %d linhas", tabela, n)
    return n

# Executa uma query SQL e grava o resultado como tabela Delta."""
def gravar_metrica_sql(spark: SparkSession, sql: str, tabela: str) -> int:
    return gravar_delta(spark, spark.sql(sql), tabela)
