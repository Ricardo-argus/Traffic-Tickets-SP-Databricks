# Data refinement (silver layer)
import os
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
from pyspark.sql import functions as F
from pyspark.sql.functions import col, monotonically_increasing_id, row_number
from pyspark.sql.window import Window

from common.config import (
    TBL_BRONZE_PAID,
    TBL_BRONZE_OVERDUE,
    TBL_MUNICIPALITIES,
    TBL_VEHICLE_TYPES,
    TBL_VEHICLE_CATEGORIES,
    TBL_SILVER_PAID,
    TBL_SILVER_OVERDUE,
    TBL_GOLD_FINES,
)
from common.io_delta import write_delta_table
from common.logging_utils import setup_logging
from common.spark_utils import get_spark_session

log = setup_logging("silver_lake")

# Columns that identify a fine (without MES/ANO) and the full silver grain
FINE_KEY_COLUMNS = [
    "ID_MUNICIPIO", "MUNICIPIO", "CODIGO_INFRACAO",
    "UF_PLACA_VEICULO", "TIPO_VEICULO", "CATEGORIA_VEICULO",
]
SILVER_GRAIN = FINE_KEY_COLUMNS + ["MES", "ANO"]

# Column mapping that standardizes the overdue source to the paid layout
RENAME_MAP = {
    "QTDE": "QUANTIDADE",
    "NOME_MUNICIPIO": "MUNICIPIO",
}

# Value used to fill vehicle columns that were not reported
UNIDENTIFIED_VEHICLE = "Nao Identificado"

def rename_overdue_columns(df: DataFrame) -> DataFrame:
    for old_name, new_name in RENAME_MAP.items():
        df = df.withColumnRenamed(old_name, new_name)
    return df


def filter_null_municipalities(df: DataFrame) -> DataFrame:
    return df.filter(F.col("MUNICIPIO").isNotNull())


def fill_paid_quantities(df: DataFrame) -> DataFrame:
    return df.fillna({"QUANTIDADE": 1})


def add_overdue_adjusted_quantity(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "QUANTIDADE_AJUSTADA",
        F.when(F.col("QUANTIDADE").isNotNull(), F.col("QUANTIDADE"))
         .otherwise(F.lit(1)),
    )

#Removes duplicates of overdue fines across cumulative monthly snapshots
def dedupe_monthly_snapshots(df: DataFrame) -> DataFrame:
    dedup_window = Window.partitionBy(*FINE_KEY_COLUMNS).orderBy("ANO", "MES")
    return (
        df.withColumn("_snapshot_rank", F.row_number().over(dedup_window))
          .filter(F.col("_snapshot_rank") == 1)
          .drop("_snapshot_rank")
    )

# Groups by the silver grain and sums the quantities.
def aggregate_to_grain(df: DataFrame, quantity_column) -> DataFrame:
    return (
        df.groupBy(*SILVER_GRAIN)
          .agg(F.sum(quantity_column).alias("QUANTIDADE"))
    )

#Adds a sequential ID_MULTA (row_number over a monotonic id)
def add_fine_id(df: DataFrame) -> DataFrame:
    window_spec = Window.orderBy(monotonically_increasing_id())
    return df.withColumn("ID_MULTA", row_number().over(window_spec))

#Combines the MES + ANO columns and converts to DATE (yyyy-MM)
def add_fine_date(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "DATA",
        F.to_date(F.concat_ws("-", col("ANO"), F.lpad(col("MES"), 2, "0")), "yyyy-MM"),
    )

# Builds the intermediate municipalities table: distinct union of the two lakes.
def build_municipality_dim(paid: DataFrame, overdue: DataFrame) -> DataFrame:
    return (
        paid.select("ID_MUNICIPIO", "MUNICIPIO")
            .union(overdue.select("ID_MUNICIPIO", "MUNICIPIO"))
            .distinct()
    )

# Builds the vehicle type dimension: every distinct TIPO_VEICULO 
def build_vehicle_type_dim(paid: DataFrame, overdue: DataFrame) -> DataFrame:
    types = (
        paid.select(F.coalesce(col("TIPO_VEICULO"), F.lit(UNIDENTIFIED_VEHICLE)).alias("TIPO_VEICULO"))
            .unionByName(
                overdue.select(F.coalesce(col("TIPO_VEICULO"), F.lit(UNIDENTIFIED_VEHICLE)).alias("TIPO_VEICULO"))
            )
            .distinct()
    )
    id_window = Window.orderBy("TIPO_VEICULO")
    return types.withColumn("ID_TIPO_VEICULO", row_number().over(id_window))

# Builds the vehicle category dimension:
def build_vehicle_category_dim(paid: DataFrame, overdue: DataFrame) -> DataFrame:
    categories = (
        paid.select(F.coalesce(col("CATEGORIA_VEICULO"), F.lit(UNIDENTIFIED_VEHICLE)).alias("CATEGORIA_VEICULO"))
            .unionByName(
                overdue.select(F.coalesce(col("CATEGORIA_VEICULO"), F.lit(UNIDENTIFIED_VEHICLE)).alias("CATEGORIA_VEICULO"))
            )
            .distinct()
    )
    id_window = Window.orderBy("CATEGORIA_VEICULO")
    return categories.withColumn("ID_CATEGORIA_VEICULO", row_number().over(id_window))


def add_status(df: DataFrame, status: str) -> DataFrame:
    return df.withColumn("STATUS", F.lit(status))

#Build the gold table by unioning the two fine sources, swapping the vehicle
def build_gold_fines(paid: DataFrame, overdue: DataFrame, vehicle_types: DataFrame, vehicle_categories: DataFrame) -> DataFrame:
    gold_columns = [
        "ID_MUNICIPIO", "CODIGO_INFRACAO", "UF_PLACA_VEICULO",
        "TIPO_VEICULO", "CATEGORIA_VEICULO", "QUANTIDADE", "DATA", "STATUS",
    ]
    fines = (
        paid.select(*gold_columns)
            .union(overdue.select(*gold_columns))
            .fillna({
                "TIPO_VEICULO": UNIDENTIFIED_VEHICLE,
                "CATEGORIA_VEICULO": UNIDENTIFIED_VEHICLE,
            })
    )
    # Column order: each surrogate key sits exactly where the description used to be.
    gold_order = [
        "ID_MUNICIPIO", "CODIGO_INFRACAO", "UF_PLACA_VEICULO",
        "ID_TIPO_VEICULO", "ID_CATEGORIA_VEICULO", "QUANTIDADE", "DATA", "STATUS",
    ]
    return (
        fines.join(vehicle_types, on="TIPO_VEICULO", how="left")
            .join(vehicle_categories, on="CATEGORIA_VEICULO", how="left")
            .select(*gold_order)
    )


def main(spark: SparkSession = None) -> None:
    spark = spark or get_spark_session()

    # Read the tables stored in Delta Lake
    silver_paid = spark.table(TBL_BRONZE_PAID)
    silver_overdue = rename_overdue_columns(spark.table(TBL_BRONZE_OVERDUE))

    paid_before, overdue_before = silver_paid.count(), silver_overdue.count()
    log.info("[silver] STEP 1: bronze read — paid=%d rows, overdue=%d rows",
             paid_before, overdue_before)

    # Remove unreported null municipalities
    silver_paid = filter_null_municipalities(silver_paid)
    silver_overdue = filter_null_municipalities(silver_overdue)
    log.info("[silver] null MUNICIPIO filter: paid %d->%d (%d removed), overdue %d->%d (%d removed)",
             paid_before, silver_paid.count(), paid_before - silver_paid.count(),
             overdue_before, silver_overdue.count(), overdue_before - silver_overdue.count())

    # Handle nulls
    log.info("[silver] STEP 2: null handling (QUANTIDADE)")
    silver_paid = fill_paid_quantities(silver_paid)
    silver_overdue = add_overdue_adjusted_quantity(silver_overdue)

    # Dedup of cumulative overdue snapshots
    before_dedup = silver_overdue.count()
    silver_overdue = dedupe_monthly_snapshots(silver_overdue)
    log.info("[silver] STEP 3: overdue snapshot dedup: %d -> %d rows (%d duplicates removed)",
             before_dedup, silver_overdue.count(), before_dedup - silver_overdue.count())

    # Group and sum total quantities
    silver_paid = aggregate_to_grain(silver_paid, F.coalesce(col("QUANTIDADE"), F.lit(1)))
    log.info("[silver] STEP 4: paid aggregation done: %d rows at grain", silver_paid.count())

    # Overdue: groupBy after dedup — each fine appears only once
    silver_overdue = aggregate_to_grain(silver_overdue, "QUANTIDADE_AJUSTADA")
    log.info("[silver] overdue aggregation done: %d rows at grain", silver_overdue.count())

    silver_paid = add_fine_id(silver_paid)
    silver_overdue = add_fine_id(silver_overdue)

    silver_paid = add_fine_date(silver_paid)
    silver_overdue = add_fine_date(silver_overdue)

    # Build the intermediate municipalities table + write to Delta Lake
    municipalities = build_municipality_dim(silver_paid, silver_overdue)
    write_delta_table(spark, municipalities, TBL_MUNICIPALITIES)
    log.info("[silver] municipalities dimension written to Delta Lake")

    # Build the vehicle type dimension + write to Delta Lake
    vehicle_types = build_vehicle_type_dim(silver_paid, silver_overdue)
    write_delta_table(spark, vehicle_types, TBL_VEHICLE_TYPES)
    log.info("[silver] vehicle type dimension written to Delta Lake")

    # Build the vehicle category dimension + write to Delta Lake
    vehicle_categories = build_vehicle_category_dim(silver_paid, silver_overdue)
    write_delta_table(spark, vehicle_categories, TBL_VEHICLE_CATEGORIES)
    log.info("[silver] vehicle category dimension written to Delta Lake")

    # Add the STATUS column
    silver_paid = add_status(silver_paid, "PAGA")
    silver_overdue = add_status(silver_overdue, "VENCIDA")

    # Write the two silver tables to Delta Lake
    log.info("[silver] STEP 5: writing silver tables to Delta Lake...")
    write_delta_table(spark, silver_paid, TBL_SILVER_PAID)
    write_delta_table(spark, silver_overdue, TBL_SILVER_OVERDUE)
    log.info("[silver] SILVER PIPELINE FINISHED")

    # Build the gold table by unioning the two fine sources
    gold_fines = build_gold_fines(silver_paid, silver_overdue, vehicle_types, vehicle_categories)
    write_delta_table(spark, gold_fines, TBL_GOLD_FINES)
    log.info("[silver] SILVER+GOLD PIPELINE FINISHED")


if __name__ == "__main__":
    main()
