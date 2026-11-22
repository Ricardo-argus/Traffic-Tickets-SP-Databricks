## Refino dos dados
import sys
sys.path.insert(0, "/Workspace/Users/ricardo.shs615@gmail.com/Traffic-Tickets-SP-Databricks/src")
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, monotonically_increasing_id, row_number
from pyspark.sql.window import Window

from common.config import (
    TBL_BRONZE_PAGAS,
    TBL_BRONZE_VENCIDAS,
    TBL_MUNICIPIOS,
    TBL_SILVER_PAGAS,
    TBL_SILVER_VENCIDAS,
    TBL_GOLD_MULTAS,
)
from common.io_delta import gravar_delta
from common.logging_utils import setup_logging
from common.spark_utils import iniciar_sessao

log = setup_logging("silver_lake")

# Colunas que identificam uma multa (sem MES/ANO) e o grain completo da silver
COLUNAS_MULTA = [
    "ID_MUNICIPIO", "MUNICIPIO", "CODIGO_INFRACAO",
    "UF_PLACA_VEICULO", "TIPO_VEICULO", "CATEGORIA_VEICULO",
]
GRAIN_SILVER = COLUNAS_MULTA + ["MES", "ANO"]

def padronizar_colunas_vencidas(df: DataFrame) -> DataFrame:
    return (
        df.withColumnRenamed("QTDE", "QUANTIDADE")
          .withColumnRenamed("NOME_MUNICIPIO", "MUNICIPIO")
    )


def filtrar_municipios_nulos(df: DataFrame) -> DataFrame:
    return df.filter(F.col("MUNICIPIO").isNotNull())


def tratar_quantidade_pagas(df: DataFrame) -> DataFrame:
    return df.fillna({"QUANTIDADE": 1})


def criar_quantidade_ajustada_vencidas(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "QUANTIDADE_AJUSTADA",
        F.when(F.col("QUANTIDADE").isNotNull(), F.col("QUANTIDADE"))
         .otherwise(F.lit(1)),
    )

#Remove duplicatas de multas vencidas em snapshots mensais cumulativos
def deduplicar_snapshots(df: DataFrame) -> DataFrame:
    dedup_window = Window.partitionBy(*COLUNAS_MULTA).orderBy("ANO", "MES")
    return (
        df.withColumn("_snapshot_rank", F.row_number().over(dedup_window))
          .filter(F.col("_snapshot_rank") == 1)
          .drop("_snapshot_rank")
    )

# Agrupa no grain da silver e soma as quantidades.
def agregar_por_grain(df: DataFrame, coluna_quantidade) -> DataFrame:
    return (
        df.groupBy(*GRAIN_SILVER)
          .agg(F.sum(coluna_quantidade).alias("QUANTIDADE"))
    )

#Adiciona ID_MULTA sequencial (row_number sobre id monotônico)
def gerar_id_multa(df: DataFrame) -> DataFrame:
    window_spec = Window.orderBy(monotonically_increasing_id())
    return df.withColumn("ID_MULTA", row_number().over(window_spec))

#JUNTA COLUNA MES + ANO E CONVERTER PARA DATE (yyyy-MM)
def gerar_coluna_data(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "DATA",
        F.to_date(F.concat_ws("-", col("ANO"), F.lpad(col("MES"), 2, "0")), "yyyy-MM"),
    )

"""CRIA NOVA TABELA INTERMEDIARIA MUNICIPIOS: união distinta dos dois lakes."""
def criar_dimensao_municipios(pagas: DataFrame, vencidas: DataFrame) -> DataFrame:
    return (
        pagas.select("ID_MUNICIPIO", "MUNICIPIO")
             .union(vencidas.select("ID_MUNICIPIO", "MUNICIPIO"))
             .distinct()
    )


def adicionar_status(df: DataFrame, status: str) -> DataFrame:
    return df.withColumn("STATUS", F.lit(status))


def construir_gold(pagas: DataFrame, vencidas: DataFrame) -> DataFrame:
    """CRIAR TABELA GOLD JUNTANDO AS DUAS MULTAS.

    Identify Non Identified Vehicles: TIPO/CATEGORIA nulos → 'Nao Identificado'.
    """
    colunas_gold = [
        "ID_MUNICIPIO", "CODIGO_INFRACAO", "UF_PLACA_VEICULO",
        "TIPO_VEICULO", "CATEGORIA_VEICULO", "QUANTIDADE", "DATA", "STATUS",
    ]
    gold = pagas.select(*colunas_gold).union(vencidas.select(*colunas_gold))
    gold = gold.fillna({"CATEGORIA_VEICULO": "Nao Identificado"})
    return gold.fillna({"TIPO_VEICULO": "Nao Identificado"})


def main(spark: SparkSession = None) -> None:
    """Executa o pipeline silver: bronze → silver → gold_multas."""
    spark = spark or iniciar_sessao()

    # Ler tabelas gravadas no Delta Lake
    silver_pagas = spark.table(TBL_BRONZE_PAGAS)
    silver_vencidas = padronizar_colunas_vencidas(spark.table(TBL_BRONZE_VENCIDAS))

    n_pagas_antes, n_vencidas_antes = silver_pagas.count(), silver_vencidas.count()
    log.info("[silver] ETAPA 1: leitura bronze — pagas=%d linhas, vencidas=%d linhas",
             n_pagas_antes, n_vencidas_antes)

    # Remover municipios nulos não informados
    silver_pagas = filtrar_municipios_nulos(silver_pagas)
    silver_vencidas = filtrar_municipios_nulos(silver_vencidas)
    log.info("[silver] filtro MUNICIPIO nulo: pagas %d->%d (%d removidas), vencidas %d->%d (%d removidas)",
             n_pagas_antes, silver_pagas.count(), n_pagas_antes - silver_pagas.count(),
             n_vencidas_antes, silver_vencidas.count(), n_vencidas_antes - silver_vencidas.count())

    # TRATAR NULOS
    log.info("[silver] ETAPA 2: tratamento de nulos (QUANTIDADE)")
    silver_pagas = tratar_quantidade_pagas(silver_pagas)
    silver_vencidas = criar_quantidade_ajustada_vencidas(silver_vencidas)

    # Dedup de snapshots cumulativos das vencidas
    n_antes_dedup = silver_vencidas.count()
    silver_vencidas = deduplicar_snapshots(silver_vencidas)
    log.info("[silver] ETAPA 3: dedup snapshots vencidas: %d -> %d linhas (%d duplicatas removidas)",
             n_antes_dedup, silver_vencidas.count(), n_antes_dedup - silver_vencidas.count())

    # agrupar e somar quantidades totais
    silver_pagas = agregar_por_grain(silver_pagas, F.coalesce(col("QUANTIDADE"), F.lit(1)))
    log.info("[silver] ETAPA 4: agregação pagas concluída: %d linhas no grain", silver_pagas.count())

    # Vencidas: groupBy apos deduplicacao — cada multa aparece apenas uma vez
    silver_vencidas = agregar_por_grain(silver_vencidas, "QUANTIDADE_AJUSTADA")
    log.info("[silver] agregação vencidas concluída: %d linhas no grain", silver_vencidas.count())

    silver_pagas = gerar_id_multa(silver_pagas)
    silver_vencidas = gerar_id_multa(silver_vencidas)

    silver_pagas = gerar_coluna_data(silver_pagas)
    silver_vencidas = gerar_coluna_data(silver_vencidas)

    # CRIAR NOVA TABELA INTERMEDIARIA MUNICIPIOS + gravar no Delta Lake
    municipios = criar_dimensao_municipios(silver_pagas, silver_vencidas)
    gravar_delta(spark, municipios, TBL_MUNICIPIOS)
    log.info("[silver] dimensão municipios gravada no Delta Lake")

    # Adicionar Coluna Status
    silver_pagas = adicionar_status(silver_pagas, "PAGA")
    silver_vencidas = adicionar_status(silver_vencidas, "VENCIDA")

    # Criar duas tabelas Silver no Delta Lake
    log.info("[silver] ETAPA 5: gravando tabelas silver no Delta Lake...")
    gravar_delta(spark, silver_pagas, TBL_SILVER_PAGAS)
    gravar_delta(spark, silver_vencidas, TBL_SILVER_VENCIDAS)
    log.info("[silver] PIPELINE SILVER CONCLUIDO")

    # CRIAR TABELA GOLD JUNTANDO AS DUAS MULTAS
    gold_multas = construir_gold(silver_pagas, silver_vencidas)
    gravar_delta(spark, gold_multas, TBL_GOLD_MULTAS)
    log.info("[silver] PIPELINE SILVER+GOLD CONCLUIDO")


if __name__ == "__main__":
    main()
