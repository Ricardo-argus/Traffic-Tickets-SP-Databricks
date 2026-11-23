# Reading and writing Delta tables.

import logging
import traceback
from pyspark.sql import DataFrame, SparkSession
from common.logging_utils import setup_logging

log = setup_logging("io_delta")

# Writes `df` as a Delta table in overwrite mode.
def write_delta_table(spark: SparkSession, df: DataFrame, table: str) -> int:

    n = df.count()
    try:
        writer = df.write.format("delta").mode("overwrite")
        if spark.catalog.tableExists(table):
            writer = writer.option("overwriteSchema", "true")
        writer.saveAsTable(table)
    except Exception:
        log.error("[io_delta] FAILED to write %s:\n%s", table, traceback.format_exc())
        raise
    log.info("[io_delta] %s written: %d rows", table, n)
    return n

# Runs a SQL query and writes its result as a Delta table.
def write_metric_from_sql(spark: SparkSession, sql: str, table: str) -> int:
    return write_delta_table(spark, spark.sql(sql), table)
