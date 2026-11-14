
from pyspark.sql import SparkSession
from pyspark.sql.functions import count
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    LongType,
    DoubleType
)

# 1. Avvio della sessione Spark
spark = SparkSession.builder \
    .appName("MegaShop Real Time Streaming") \
    .getOrCreate()

print("Spark Streaming avviato!")


# 2. Definizione dello schema dei dati
schema = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("customer_id", LongType(), True),
    StructField("product_id", LongType(), True),
    StructField("region_id", LongType(), True),
    StructField("quantity", LongType(), True),
    StructField("amount", DoubleType(), True),
    StructField("payment_type", StringType(), True),
    StructField("ts", StringType(), True),
    StructField("year", IntegerType(), True),
    StructField("month", IntegerType(), True)
])


# 3. Lettura dei file JSON in streaming
streaming_df = spark.readStream \
    .format("json") \
    .schema(schema) \
    .option("maxFilesPerTrigger", 1) \
    .load("data_local/json/")


# 4. Conteggio delle transazioni per regione
transazioni_per_regione = streaming_df.groupBy("region_id") \
    .agg(count("*").alias("numero_transazioni"))


# 5. Visualizzazione dei risultati nel terminale (Console Sink)
query = transazioni_per_regione.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", False) \
    .start()

# 6. Mantenimento dello streaming attivo
query.awaitTermination()