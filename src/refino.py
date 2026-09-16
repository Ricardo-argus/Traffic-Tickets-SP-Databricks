# Refino dos dados

from pyspark.sql.functions import col
from pyspark.sql import SparkSession

def iniciar_sessao():
    return SparkSession.builder.getOrCreate()

def aplicar_schema(df):
    return df.withColumn('InvoiceNo', col('InvoiceNo').cast('string')) \
             .withColumn('StockCode', col('StockCode').cast('string')) \
             .withColumn('Quantity', col('Quantity').cast('int')) \
             .withColumn('unitPrice', col('unitPrice').cast('double'))

def remover_duplicados(df):
    return df.dropDuplicates()

def tratar_nulos(df):
    return df.fillna({"Quantity": 0, "unitPrice": 0.0})

def valores_negativos(df):
    return df.filter((col("Quantity") > 0) & (col("unitPrice") > 0))

def executar_refino():
    spark = iniciar_sessao()

    # Garante que o schema existe
    spark.sql("CREATE SCHEMA IF NOT EXISTS ricardo_catalog.silver")

    # Define catálogo e schema
    spark.sql("USE CATALOG ricardo_catalog")
    spark.sql("USE SCHEMA silver")

    # Lê da Bronze
    df = spark.table("ricardo_catalog.bronze.ecommerce_data")

    # Refino
    df_limpo = aplicar_schema(df)
    df_limpo = remover_duplicados(df_limpo)
    df_limpo = tratar_nulos(df_limpo)
    df_limpo = valores_negativos(df_limpo)

    # Salva na Silver (com caminho completo para evitar ambiguidades)
    df_limpo.coalesce(1) \
    .write.mode("overwrite") \
    .option("header", True) \
    .csv("/Volumes/ricardo_catalog/silver/silver_volume/ecommerce_data_silver_csv")