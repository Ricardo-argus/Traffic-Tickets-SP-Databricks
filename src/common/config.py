"""Constantes compartilhadas do pipeline: catálogo, caminhos e nomes de tabelas.

Único lugar para mudar quando o catálogo ou o diretório de dados mudar.
"""

CATALOG = "multas_analytics"

# -----------------------------------------------------------------------------
# Caminhos de arquivos (CSVs de origem e parquet exportado)
# -----------------------------------------------------------------------------
DATA_DIR = "/Workspace/Users/ricardo.shs615@gmail.com/Traffic-Tickets-SP-Databricks/data"
CSV_PAGAS = f"{DATA_DIR}/multas_pagas_*.csv"
CSV_VENCIDAS = f"{DATA_DIR}/multas_vencidas_*.csv"
CSV_INFRACOES = f"{DATA_DIR}/tabela-codigo-infracoes-renainf.csv"
PARQUET_GRAVIDADE_MUNICIPIOS = f"{DATA_DIR}/multas_gravidade_per_municipality.parquet"

# -----------------------------------------------------------------------------
# Tabelas Delta — nomes de 2 partes (schema.table):
# schema `multas_analytics` no catálogo `workspace`, exatamente como nos scripts atuais
# -----------------------------------------------------------------------------
TBL_BRONZE_PAGAS = f"{CATALOG}.multas_pagas"
TBL_BRONZE_VENCIDAS = f"{CATALOG}.multas_vencidas"
TBL_BRONZE_INFRACOES = f"{CATALOG}.infracoes_codes"

TBL_MUNICIPIOS = f"{CATALOG}.municipios"
TBL_SILVER_PAGAS = f"{CATALOG}.silver_pagas"
TBL_SILVER_VENCIDAS = f"{CATALOG}.silver_vencidas"
TBL_GOLD_MULTAS = f"{CATALOG}.gold_multas"

# Métricas gold — gravadas hoje sem qualificar o catálogo (usam o default da sessão)
TBL_METRICAS_MUNICIPIOS = "multas_metricas_municipios"
TBL_METRICAS_VEICULO = "multas_metricas_veiculo"
TBL_METRICAS_GRAVIDADE = "multas_metricas_gravidade"
TBL_METRICAS_QTDE_PAGAS_VEICULO = "quantidade_multas_pagas_por_veiculo"
