"""Shared SparkSession utilities for the three lakes."""

from pyspark.sql import SparkSession


def get_spark_session() -> SparkSession:
    """Return the pipeline's shared SparkSession (creates it if it doesn't exist)."""
    return SparkSession.builder.appName("TrafficTicketsSP").getOrCreate()
