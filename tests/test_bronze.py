"""Unit tests for the bronze layer: column sanitization, strict schema, CSV read."""

from bronze_pipeline.bronze_lake import (
    SCHEMA_PAID,
    SOURCES,
    read_csv_source,
    read_source,
    sanitize_column_names,
)
from common.config import CSV_INFRACTIONS

def test_sanitize_replaces_special_characters(spark):
    df = spark.createDataFrame([("x",)], "`sobe; a (teste)=1` STRING")
    assert sanitize_column_names(df).columns == ["sobe__a__teste__1"]


def test_sanitize_keeps_clean_names(spark):
    df = spark.createDataFrame([("x",)], "codigo_infracao STRING")
    assert sanitize_column_names(df).columns == ["codigo_infracao"]


def test_sanitize_real_infraction_headers(spark):
    # Header names that actually come from the RENAINF reference CSV
    df = spark.createDataFrame([("x", "y")], "`Código da Infração` STRING, `Amparo Legal (CTB)` STRING")
    assert sanitize_column_names(df).columns == ["Código_da_Infração", "Amparo_Legal__CTB_"]


def test_schema_paid_matches_source_columns():
    assert SCHEMA_PAID.names == [
        "ID_MUNICIPIO", "MUNICIPIO", "CODIGO_INFRACAO", "DESCRICAO_INFRACAO",
        "UF_PLACA_VEICULO", "TIPO_VEICULO", "CATEGORIA_VEICULO",
        "QUANTIDADE", "MES", "ANO",
    ]

def test_read_csv_source_reads_reference_file(spark):
    # Light integration: reads the real RENAINF reference CSV shipped in the repo (read-only)
    df = read_csv_source(spark, CSV_INFRACTIONS)
    assert df.count() == 258
    assert len(df.columns) == 7


def test_sources_registry_and_read_source(spark):
    # Registry: 3 sources, only the RENAINF one requires sanitization
    assert set(SOURCES) == {"paid", "overdue", "infraction_codes"}
    assert SOURCES["paid"]["schema"] is SCHEMA_PAID
    assert SOURCES["infraction_codes"]["sanitize"] is True
    assert "sanitize" not in SOURCES["paid"]

    # read_source applies the registry: sanitized headers on the RENAINF file
    df = read_source(spark, SOURCES["infraction_codes"])
    assert df.count() == 258
    assert all(" " not in c and "(" not in c for c in df.columns)
