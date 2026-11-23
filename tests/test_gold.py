"""Unit tests for the gold layer: SQL business rules and the parquet export.

The two SQL metrics that only reference the `gold` temp view (by vehicle and
paid-by-vehicle) are executed behaviorally against toy DataFrames; the SQL
that joins real Delta tables (municipalities, infraction codes) is pinned by
asserting its business-rule literals.
"""

from types import SimpleNamespace

import pandas as pd

from gold_pipeline.gold_lake import (
    METRICS,
    SQL_METRICS_MUNICIPALITIES,
    SQL_METRICS_PAID_BY_VEHICLE,
    SQL_METRICS_SEVERITY,
    SQL_SEVERITY_BY_MUNICIPALITY,
    SQL_METRICS_VEHICLE,
    export_severity_by_municipality,
)
from common.config import (
    TBL_METRICS_MUNICIPALITIES,
    TBL_METRICS_PAID_BY_VEHICLE,
    TBL_METRICS_SEVERITY,
    TBL_METRICS_VEHICLE,
)

GOLD_COLUMNS = (
    "ID_MUNICIPIO INT, CODIGO_INFRACAO STRING, UF_PLACA_VEICULO STRING, "
    "TIPO_VEICULO STRING, CATEGORIA_VEICULO STRING, QUANTIDADE INT, "
    "DATA STRING, STATUS STRING"
)


def _gold_view(spark, rows):
    spark.createDataFrame(rows, GOLD_COLUMNS).createOrReplaceTempView("gold")


def test_metrics_registry_covers_all_metric_tables():
    assert set(METRICS) == {
        TBL_METRICS_MUNICIPALITIES,
        TBL_METRICS_VEHICLE,
        TBL_METRICS_SEVERITY,
        TBL_METRICS_PAID_BY_VEHICLE,
    }
    assert METRICS[TBL_METRICS_VEHICLE]["sql"] is SQL_METRICS_VEHICLE
    assert METRICS[TBL_METRICS_MUNICIPALITIES]["sql"] is SQL_METRICS_MUNICIPALITIES
    assert all(metric["description"] for metric in METRICS.values())


def test_metrics_vehicle_sql_counts_only_overdue(spark):
    _gold_view(spark, [
        (1, "5010-0", "SP", "AUTOMOVEL", "PARTICULAR", 10, "2025-11-01", "VENCIDA"),
        (1, "5010-0", "SP", "AUTOMOVEL", "PARTICULAR", 5, "2025-12-01", "VENCIDA"),
        (1, "5010-0", "SP", "MOTOCICLETO", "PARTICULAR", 3, "2025-11-01", "VENCIDA"),
        (1, "5010-0", "SP", "AUTOMOVEL", "PARTICULAR", 100, "2025-11-01", "PAGA"),  # ignored: paid
    ])
    rows = spark.sql(SQL_METRICS_VEHICLE).collect()
    assert sorted((r.tipo_veiculo, r.total_multas) for r in rows) == [
        ("AUTOMOVEL", 15),
        ("MOTOCICLETO", 3),
    ]
    spark.catalog.dropTempView("gold")


def test_metrics_paid_by_vehicle_thresholds_and_window(spark):
    _gold_view(spark, [
        # 60.000 on the same date/type -> HIGH QUANTITY (> 50.000)
        (1, "5010-0", "SP", "AUTOMOVEL", "PARTICULAR", 30000, "2025-11-05", "PAGA"),
        (1, "5010-0", "SP", "AUTOMOVEL", "PARTICULAR", 30000, "2025-11-05", "PAGA"),
        # 20.001 -> MEDIUM QUANTITY
        (1, "7455-1", "SP", "MOTOCICLETO", "PARTICULAR", 20001, "2025-12-10", "PAGA"),
        # exactly 50.000 -> MEDIUM (threshold is strictly > 50.000)
        (1, "7455-1", "SP", "CAMINHAO", "PARTICULAR", 50000, "2026-01-15", "PAGA"),
        # 19.999 and exactly 20.000 -> LOW QUANTITY
        (1, "7455-1", "SP", "ONIBUS", "PARTICULAR", 19999, "2026-02-20", "PAGA"),
        (1, "7455-1", "SP", "BARCA", "PARTICULAR", 20000, "2026-03-15", "PAGA"),
        # outside the date window -> excluded
        (1, "7455-1", "SP", "AUTOMOVEL", "PARTICULAR", 99999, "2025-10-29", "PAGA"),
        (1, "7455-1", "SP", "AUTOMOVEL", "PARTICULAR", 99999, "2026-09-02", "PAGA"),
        # overdue -> excluded
        (1, "5010-0", "SP", "AUTOMOVEL", "PARTICULAR", 99999, "2025-11-05", "VENCIDA"),
    ])
    rows = spark.sql(SQL_METRICS_PAID_BY_VEHICLE).collect()
    status = {(str(r.DATA), r.TIPO_VEICULO): r.Status_quantidade for r in rows}
    assert status == {
        ("2025-11-05", "AUTOMOVEL"): "HIGH QUANTITY",
        ("2025-12-10", "MOTOCICLETO"): "MEDIUM QUANTITY",
        ("2026-01-15", "CAMINHAO"): "MEDIUM QUANTITY",
        ("2026-02-20", "ONIBUS"): "LOW QUANTITY",
        ("2026-03-15", "BARCA"): "LOW QUANTITY",
    }
    spark.catalog.dropTempView("gold")


def test_municipality_sql_pins_business_rules():
    for rule in ("> 250", "'ALTO'", "> 100", "'MEDIO'", "'BAIXA'", "'OUTROS'", "'VENCIDA'"):
        assert rule in SQL_METRICS_MUNICIPALITIES


def test_severity_sql_pins_business_rules():
    # Severe = leading digit > 5 OR 'Gravíss' text, overdue fines only
    for rule in ("> 5", "Gravíss", "'VENCIDA'", "try_cast"):
        assert rule in SQL_SEVERITY_BY_MUNICIPALITY


def test_metrics_severity_sql_pins_business_rules():
    for rule in ("'DESCONHECIDA'", "EXTRACT(MONTH", "EXTRACT(YEAR", "'VENCIDA'"):
        assert rule in SQL_METRICS_SEVERITY


def test_export_severity_by_municipality_writes_parquet(tmp_path, monkeypatch):
    import gold_pipeline.gold_lake as gl

    out = tmp_path / "severity_by_municipality.parquet"
    monkeypatch.setattr(gl, "PARQUET_SEVERITY_MUNICIPALITIES", str(out))

    rows = [
        SimpleNamespace(asDict=lambda a=a, b=b, c=c: {"ID_MUNICIPIO": a, "MUNICIPIO": b, "total_multas": c})
        for a, b, c in [(1, "SAO PAULO", 25325), (2, "CAMPINAS", 40)]
    ]
    export_severity_by_municipality(rows)

    df = pd.read_parquet(out)
    assert list(df["MUNICIPIO"]) == ["SAO PAULO", "CAMPINAS"]
    assert int(df.loc[0, "total_multas"]) == 25325
