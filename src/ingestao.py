
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("TrafficTicketsSP").getOrCreate()


df_pagas = spark.read.csv("/Workspace/Users/ricardo.shs615@gmail.com/Traffic-Tickets-SP/data/multas_pagas_*.csv", header=True, inferSchema=True)

df_vencidas = spark.read.csv("/Workspace/Users/ricardo.shs615@gmail.com/Traffic-Tickets-SP/data/multas_vencidas_*.csv", header=True, inferSchema=True)

# Save as Tables to query
df_pagas.write.format("delta").mode("overwrite").saveAsTable("multas_pagas")
df_vencidas.write.format("delta").mode("overwrite").saveAsTable("multas_vencidas")
