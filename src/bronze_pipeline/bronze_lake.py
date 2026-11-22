import re
import sys
sys.path.insert(0, "/Workspace/Users/ricardo.shs615@gmail.com/Traffic-Tickets-SP-Databricks/src")
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

from common.config import (
    CSV_INFRACTIONS,
    CSV_PAID,
    CSV_OVERDUE,
    TBL_BRONZE_INFRACTIONS,
    TBL_BRONZE_PAID,
    TBL_BRONZE_OVERDUE,
)
from common.io_delta import write_delta_table
from common.logging_utils import setup_logging
from common.spark_utils import get_spark_session

log = setup_logging("bronze_lake")

# Column schema for the paid fines source (multas_pagas)
SCHEMA_PAID = StructType([
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
def sanitize_column_names(df: DataFrame) -> DataFrame:
    for col_name in df.columns:
        new_name = re.sub(r"[ ,;{}()\n\t=]", "_", col_name)
        if new_name != col_name:
            df = df.withColumnRenamed(col_name, new_name)
    return df


def read_csv_source(spark: SparkSession, path: str, schema: StructType = None) -> DataFrame:
    if schema is not None:
        return spark.read.csv(path, header=True, schema=schema)
    return spark.read.csv(path, header=True, inferSchema=True)


def main(spark: SparkSession = None) -> None:
    spark = spark or get_spark_session()

    # Collect fines CSV data
    fines_paid = read_csv_source(spark, CSV_PAID, schema=SCHEMA_PAID)
    fines_overdue = read_csv_source(spark, CSV_OVERDUE)

    # Infraction codes CSV ingestion
    infraction_codes = sanitize_column_names(read_csv_source(spark, CSV_INFRACTIONS))

    log.info("[bronze] STEP 1: CSV reads — paid=%d rows, overdue=%d rows, infraction_codes=%d rows",
             fines_paid.count(), fines_overdue.count(), infraction_codes.count())

    # Convert data to Delta tables
    log.info("[bronze] STEP 2: writing Delta tables...")
    write_delta_table(spark, fines_paid, TBL_BRONZE_PAID)
    write_delta_table(spark, fines_overdue, TBL_BRONZE_OVERDUE)
    write_delta_table(spark, infraction_codes, TBL_BRONZE_INFRACTIONS)
    log.info("[bronze] BRONZE PIPELINE FINISHED")


if __name__ == "__main__":
    main()
