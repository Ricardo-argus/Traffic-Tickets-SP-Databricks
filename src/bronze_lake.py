
import re
from pyspark.sql import SparkSession

def iniciar_sessao():
    return SparkSession.builder.appName("TrafficTicketsSP").getOrCreate()

#Collect Multas CSVs Data
multas_pagas = spark.read.csv("/Workspace/Users/ricardo.shs615@gmail.com/Traffic-Tickets-SP/data/multas_pagas_*.csv", header=True, inferSchema=True)

multas_vencidas = spark.read.csv("/Workspace/Users/ricardo.shs615@gmail.com/Traffic-Tickets-SP/data/multas_vencidas_*.csv", header=True, inferSchema=True)

# Save as Tables to query
multas_pagas.write.format("delta").mode("overwrite").saveAsTable("multas_pagas")
multas_vencidas.write.format("delta").mode("overwrite").saveAsTable("multas_vencidas")

# Infracoes CSV ingestion
Infra_Codes = spark.read.csv("/Workspace/Users/ricardo.shs615@gmail.com/Traffic-Tickets-SP/data/tabela-codigo-infracoes-renainf.csv", header=True, inferSchema=True)

# Sanitize column names for Delta compatibility
for col_name in Infra_Codes.columns:
    new_name = re.sub(r"[ ,;{}()\n\t=]", "_", col_name)
    if new_name != col_name:
        Infra_Codes = Infra_Codes.withColumnRenamed(col_name, new_name)

# Create Table for Infra_Codes
Infra_Codes.write.format("delta").mode("overwrite").saveAsTable("multas_analytics.infracoes_codes")