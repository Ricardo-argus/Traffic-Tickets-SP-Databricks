"""Utilitários de SparkSession compartilhados entre os três lakes."""

from pyspark.sql import SparkSession


def iniciar_sessao() -> SparkSession:
    """Retorna a SparkSession compartilhada do pipeline (cria se ainda não existir)."""
    return SparkSession.builder.appName("TrafficTicketsSP").getOrCreate()
