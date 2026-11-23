"""Pytest configuration for the Traffic-Tickets-SP pipeline tests.

Locates the pipeline ``src/`` directory — via the ``MULTAS_SRC`` environment
variable, the folder next to ``tests/``, or the workspace default — and adds it
to ``sys.path`` so the lake packages import cleanly. Also provides a shared
SparkSession fixture for the transformation tests.
"""

import os
import sys
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

# Locate src/: MULTAS_SRC env var > folder next to tests/ > any ancestor of cwd
# (or the user home) that contains the Traffic-Tickets-SP-Databricks repo.
def _src_candidates():
    yield os.getenv("MULTAS_SRC")
    yield str(Path(__file__).resolve().parent.parent / "src")
    for base in (Path.cwd(), *Path.cwd().parents, Path.home()):
        yield str(base / "Traffic-Tickets-SP-Databricks" / "src")

SRC = next((p for p in _src_candidates() if p and Path(p).is_dir()), None)
if SRC is None:
    raise RuntimeError(
        "Could not locate the pipeline 'src/' directory; "
        "set the MULTAS_SRC environment variable to its absolute path"
    )
if SRC not in sys.path:
    sys.path.insert(0, SRC)

# Toy schema matching the fine key columns consumed by the silver layer
FINE_COLUMNS = (
    "ID_MUNICIPIO INT, MUNICIPIO STRING, CODIGO_INFRACAO STRING, "
    "UF_PLACA_VEICULO STRING, TIPO_VEICULO STRING, CATEGORIA_VEICULO STRING, "
    "MES INT, ANO INT"
)


@pytest.fixture(scope="session")
def spark():
    """Shared SparkSession for the whole test run (no stop: reused across suites)."""
    return SparkSession.builder.appName("TrafficTicketsSP-tests").getOrCreate()
