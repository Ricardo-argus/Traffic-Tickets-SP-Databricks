import os
import re
import sys
from pathlib import Path
try:
    SRC = str(Path(__file__).resolve().parent.parent)
except NameError:
    _repo = Path.cwd().parents[1]
    _env = _repo / ".env"
    if _env.is_file():
        for _line in _env.read_text().splitlines():
            _k, _, _v = _line.partition("=")
            if _k.strip().startswith("MULTAS_"):
                os.environ.setdefault(_k.strip(), _v.strip())
    SRC = os.getenv("MULTAS_SRC", str(_repo / "src"))
sys.path.insert(0, SRC)
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


# Source registry: logical name -> {path, schema, table, sanitize}
SOURCES = {
    "paid": {"path": CSV_PAID, "schema": SCHEMA_PAID, "table": TBL_BRONZE_PAID},
    "overdue": {"path": CSV_OVERDUE, "schema": None, "table": TBL_BRONZE_OVERDUE},
    "infraction_codes": {
        "path": CSV_INFRACTIONS,
        "schema": None,
        "table": TBL_BRONZE_INFRACTIONS,
        "sanitize": True,
    },
}

# Reads one registered source; sanitizes column names when the source requires it.
def read_source(spark: SparkSession, source: dict) -> DataFrame:
    df = read_csv_source(spark, source["path"], schema=source["schema"])
    if source.get("sanitize"):
        df = sanitize_column_names(df)
    return df


def main(spark: SparkSession = None) -> None:
    spark = spark or get_spark_session()

    # Collect fines CSV data
    frames = {name: read_source(spark, source) for name, source in SOURCES.items()}
    log.info("[bronze] STEP 1: CSV reads — %s",
             ", ".join(f"{name}={df.count()} rows" for name, df in frames.items()))

    # Convert data to Delta tables
    log.info("[bronze] STEP 2: writing Delta tables...")
    for name, source in SOURCES.items():
        write_delta_table(spark, frames[name], source["table"])
    log.info("[bronze] BRONZE PIPELINE FINISHED")


if __name__ == "__main__":
    main()
