"""Unit tests for the silver layer pure transformations."""

from conftest import FINE_COLUMNS
from silver_pipeline.silver_lake import (
    FINE_KEY_COLUMNS,
    SILVER_GRAIN,
    add_fine_date,
    add_fine_id,
    add_overdue_adjusted_quantity,
    add_status,
    aggregate_to_grain,
    build_gold_fines,
    build_municipality_dim,
    dedupe_monthly_snapshots,
    fill_paid_quantities,
    filter_null_municipalities,
    rename_overdue_columns,
)


def make_fines(spark, rows):
    """Toy fines DataFrame at the bronze grain plus QUANTIDADE."""
    return spark.createDataFrame(rows, FINE_COLUMNS + ", QUANTIDADE INT")


def test_grain_definition():
    assert FINE_KEY_COLUMNS == [
        "ID_MUNICIPIO", "MUNICIPIO", "CODIGO_INFRACAO",
        "UF_PLACA_VEICULO", "TIPO_VEICULO", "CATEGORIA_VEICULO",
    ]
    assert SILVER_GRAIN == FINE_KEY_COLUMNS + ["MES", "ANO"]


def test_rename_overdue_columns(spark):
    df = spark.createDataFrame([(1, 5, "SAO PAULO")], "ID_MUNICIPIO INT, QTDE INT, NOME_MUNICIPIO STRING")
    assert rename_overdue_columns(df).columns == ["ID_MUNICIPIO", "QUANTIDADE", "MUNICIPIO"]


def test_filter_null_municipalities(spark):
    df = spark.createDataFrame([(1, "SAO PAULO"), (2, None)], "ID_MUNICIPIO INT, MUNICIPIO STRING")
    kept = filter_null_municipalities(df).collect()
    assert [r.ID_MUNICIPIO for r in kept] == [1]


def test_fill_paid_quantities(spark):
    df = spark.createDataFrame([(None,), (7,)], "QUANTIDADE INT")
    values = sorted(r.QUANTIDADE for r in fill_paid_quantities(df).collect())
    assert values == [1, 7]


def test_add_overdue_adjusted_quantity_keeps_original(spark):
    df = spark.createDataFrame([(None,), (7,)], "QUANTIDADE INT")
    rows = add_overdue_adjusted_quantity(df).orderBy("QUANTIDADE_AJUSTADA").collect()
    assert [(r.QUANTIDADE, r.QUANTIDADE_AJUSTADA) for r in rows] == [(None, 1), (7, 7)]


def test_dedupe_monthly_snapshots_keeps_earliest(spark):
    df = make_fines(spark, [
        (1, "SAO PAULO", "5010-0", "SP", "AUTOMOVEL", "PARTICULAR", 12, 2025, 2),  # first snapshot: kept
        (1, "SAO PAULO", "5010-0", "SP", "AUTOMOVEL", "PARTICULAR", 1, 2026, 2),   # later duplicate: removed
        (2, "CAMPINAS", "7455-1", "SP", "CAMINHAO", None, 3, 2026, 4),              # different fine: kept
    ])
    rows = dedupe_monthly_snapshots(df).orderBy("ID_MUNICIPIO").select("ID_MUNICIPIO", "MES", "ANO").collect()
    assert [(r.ID_MUNICIPIO, r.MES, r.ANO) for r in rows] == [(1, 12, 2025), (2, 3, 2026)]


def test_aggregate_to_grain_sums_by_name(spark):
    df = (
        make_fines(spark, [
            (1, "SAO PAULO", "5010-0", "SP", "AUTOMOVEL", "PARTICULAR", 1, 2025, 3),
            (1, "SAO PAULO", "5010-0", "SP", "AUTOMOVEL", "PARTICULAR", 1, 2025, 4),
            (1, "SAO PAULO", "5010-0", "SP", "AUTOMOVEL", "PARTICULAR", 2, 2025, 9),
        ])
        .withColumnRenamed("QUANTIDADE", "QUANTIDADE_AJUSTADA")
    )
    rows = aggregate_to_grain(df, "QUANTIDADE_AJUSTADA").orderBy("MES").select("MES", "QUANTIDADE").collect()
    assert [(r.MES, r.QUANTIDADE) for r in rows] == [(1, 7), (2, 9)]


def test_aggregate_to_grain_accepts_expression(spark):
    from pyspark.sql import functions as F
    from pyspark.sql.functions import col

    df = make_fines(spark, [
        (1, "SAO PAULO", "5010-0", "SP", "AUTOMOVEL", "PARTICULAR", 1, 2025, 3),
        (1, "SAO PAULO", "5010-0", "SP", "AUTOMOVEL", "PARTICULAR", 1, 2025, None),  # null counts as 1 (coalesce)
    ])
    rows = aggregate_to_grain(df, F.coalesce(col("QUANTIDADE"), F.lit(1))).collect()
    assert [(r.MES, r.QUANTIDADE) for r in rows] == [(1, 4)]


def test_add_fine_id_is_unique_sequential(spark):
    df = make_fines(spark, [(i, "SAO PAULO", "5010-0", "SP", "AUTOMOVEL", "PARTICULAR", 1, 2025, 1) for i in range(1, 6)])
    ids = sorted(r.ID_MULTA for r in add_fine_id(df).collect())
    assert ids == [1, 2, 3, 4, 5]


def test_add_fine_date_zero_pads_month(spark):
    df = spark.createDataFrame([(1, 2025), (12, 2025)], "MES INT, ANO INT")
    rows = add_fine_date(df).orderBy("MES").collect()
    assert [str(r.DATA) for r in rows] == ["2025-01-01", "2025-12-01"]


def test_build_municipality_dim_distinct_union(spark):
    paid = spark.createDataFrame([(1, "SAO PAULO"), (2, "CAMPINAS")], "ID_MUNICIPIO INT, MUNICIPIO STRING")
    overdue = spark.createDataFrame([(2, "CAMPINAS")], "ID_MUNICIPIO INT, MUNICIPIO STRING")
    rows = sorted((r.ID_MUNICIPIO, r.MUNICIPIO) for r in build_municipality_dim(paid, overdue).collect())
    assert rows == [(1, "SAO PAULO"), (2, "CAMPINAS")]


def test_add_status(spark):
    df = spark.createDataFrame([(1,)], "ID_MUNICIPIO INT")
    assert [r.STATUS for r in add_status(df, "PAGA").collect()] == ["PAGA"]


def test_build_gold_fines_unions_and_fills_unidentified(spark):
    gold_cols = (
        "ID_MUNICIPIO INT, CODIGO_INFRACAO STRING, UF_PLACA_VEICULO STRING, "
        "TIPO_VEICULO STRING, CATEGORIA_VEICULO STRING, QUANTIDADE INT, DATA STRING, STATUS STRING"
    )
    paid = spark.createDataFrame([(1, "5010-0", "SP", "AUTOMOVEL", "PARTICULAR", 2, "2025-12-01", "PAGA")], gold_cols)
    overdue = spark.createDataFrame([(2, "7455-1", "SP", None, None, 3, "2026-03-01", "VENCIDA")], gold_cols)
    rows = build_gold_fines(paid, overdue).orderBy("ID_MUNICIPIO").collect()
    assert [(r.TIPO_VEICULO, r.CATEGORIA_VEICULO, r.STATUS) for r in rows] == [
        ("AUTOMOVEL", "PARTICULAR", "PAGA"),
        ("Nao Identificado", "Nao Identificado", "VENCIDA"),
    ]
