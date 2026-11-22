"""Pytest configuration for the Traffic-Tickets-SP pipeline tests.

Adds ``src/`` to ``sys.path`` so the lake packages import cleanly and provides
a shared SparkSession fixture for the transformation tests.
"""

import sys

import pytest
from pyspark.sql import SparkSession

SRC = "/Workspace/Users/ricardo.shs615@gmail.com/Traffic-Tickets-SP-Databricks/src"
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
