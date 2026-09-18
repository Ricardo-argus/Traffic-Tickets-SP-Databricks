## Refino dos dados

from pyspark.sql.functions import col
from pyspark.sql import SparkSession

def iniciar_sessao():

# FAZER LEITURA DAS TABELAS DE MULTAS

# CRIAR colunas NOVAS para multas_pagas e multas_vencidas LEVANTAM INFORMACOES AGREGADAS IMPORTANTES 
#EX: GRAVIDADE, orgao competente, infrator (colunas presentes na tabela de infracoes)



# INSERIR SPARK.SQL PARA ARMAZENAR METRICAS


# CRIAR GRAFICOS QUE SERAO SALVOS EM CHARTS , AVALIANDO SEGUINTES INFORMACOES
# QUANTIDADE DE MULTAS VENCIDAS POR TIPO DE VEICULO 
# Multas por município, por tipo de infração, por gravidade, por órgão responsável.

# CRIAR UMA TABELA COM O JOIN DAS DUAS TABELAS (VENCIDAS E PAGAS) PARA QUE SE POSSA FAZER COMPARACOES SOBRE TOTAL DE MULTAS APLICADAS POR MUNICIPIO, CODIGOS MAIS APLICADOS ETC 
