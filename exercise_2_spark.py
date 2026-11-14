from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("MegaShop ETL") \
    .getOrCreate()

print("Spark avviato correttamente!")
print("Versione Spark:", spark.version)

percorso = "data_local/parquet/"

transazioni = spark.read.parquet(percorso + "transactions_batch_*.parquet")

prodotti = spark.read.parquet(percorso + "products.parquet")

regioni = spark.read.parquet(percorso + "regions.parquet")

print("Transazioni:", transazioni.count())
print("Prodotti:", prodotti.count())
print("Regioni:", regioni.count())

transazioni_prodotti = transazioni.join(
    prodotti,
    on="product_id",
    how="inner"
)

transazioni_complete = transazioni_prodotti.join(
    regioni,
    on="region_id",
    how="inner"
)

df_finale = transazioni_complete.select(
    "transaction_id",
    "region_name",
    "category",
    "amount",
    "year"
)

df_finale.printSchema()

print("Numero righe:", df_finale.count())

df_finale.show(5)

df_finale.write \
    .mode("overwrite") \
    .partitionBy("year") \
    .parquet("data_local/processed_sales")