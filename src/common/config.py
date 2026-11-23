"""Shared constants for the pipeline: catalog, paths and table names.

Single place to change when the catalog or the data directory moves.
Every value below can be overridden by a MULTAS_* environment variable
(or by the repo's .env file) without editing code.
"""

import os
from pathlib import Path

CATALOG = os.getenv("MULTAS_CATALOG", "multas_analytics")

# -----------------------------------------------------------------------------
# File paths (source CSVs and exported parquet) — the repo's data/ by default
# -----------------------------------------------------------------------------
DATA_DIR = os.getenv("MULTAS_DATA_DIR", str(Path(__file__).resolve().parents[2] / "data"))
CSV_PAID = f"{DATA_DIR}/multas_pagas_*.csv"
CSV_OVERDUE = f"{DATA_DIR}/multas_vencidas_*.csv"
CSV_INFRACTIONS = f"{DATA_DIR}/tabela-codigo-infracoes-renainf.csv"
PARQUET_SEVERITY_MUNICIPALITIES = f"{DATA_DIR}/multas_gravidade_per_municipality.parquet"

# -----------------------------------------------------------------------------
# Delta tables — 2-part names (schema.table):
# schema `multas_analytics` in the `workspace` catalog, exactly as the current scripts use
# -----------------------------------------------------------------------------
TBL_BRONZE_PAID = f"{CATALOG}.multas_pagas"
TBL_BRONZE_OVERDUE = f"{CATALOG}.multas_vencidas"
TBL_BRONZE_INFRACTIONS = f"{CATALOG}.infracoes_codes"

TBL_MUNICIPALITIES = f"{CATALOG}.municipios"
TBL_VEHICLE_TYPES = f"{CATALOG}.tipos_veiculo"
TBL_SILVER_PAID = f"{CATALOG}.silver_pagas"
TBL_SILVER_OVERDUE = f"{CATALOG}.silver_vencidas"
TBL_GOLD_FINES = f"{CATALOG}.gold_multas"

# Gold metrics — currently written unqualified (they use the session's default catalog)
TBL_METRICS_MUNICIPALITIES = "multas_metricas_municipios"
TBL_METRICS_VEHICLE = "multas_metricas_veiculo"
TBL_METRICS_SEVERITY = "multas_metricas_gravidade"
TBL_METRICS_PAID_BY_VEHICLE = "quantidade_multas_pagas_por_veiculo"
