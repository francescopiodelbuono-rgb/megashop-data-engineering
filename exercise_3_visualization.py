
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as spark_sum
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter

# 1. Avvio Spark
spark = SparkSession.builder \
    .appName("MegaShop Visualizzazione") \
    .getOrCreate()

# 2. Lettura dei dati elaborati nell'esercizio 2
df_finale = spark.read.parquet("data_local/processed_sales")

# 3. Calcolo del fatturato totale per categoria
fatturato_categoria = df_finale.groupBy("category") \
    .agg(spark_sum("amount").alias("fatturato"))

# 4. Conversione in Pandas
df_grafico = fatturato_categoria.toPandas()

# 5. Ordinamento delle categorie
df_grafico = df_grafico.sort_values("category")

# 6. Creazione del grafico
plt.figure(figsize=(12, 7))

plt.bar(
    df_grafico["category"],
    df_grafico["fatturato"],
    width=0.45
)

# 7. Personalizzazione degli assi
plt.ylim(0, 12_000_000)

# Mostra i valori dell'asse verticale in milioni di euro
plt.gca().yaxis.set_major_formatter(
    FuncFormatter(lambda valore, posizione: f"{valore / 1_000_000:.0f}")
)

plt.yticks(range(0, 12_000_001, 2_000_000))

plt.xlabel("Categoria")
plt.ylabel("Fatturato (milioni di €)")
plt.title("Fatturato totale per categoria")

# 8. Miglioramento della visualizzazione
plt.grid(axis="y", alpha=0.3)
plt.tight_layout()

# 9. Salvataggio del grafico
plt.savefig("fatturato_per_categoria.png", dpi=300)

# 10. Visualizzazione
plt.show()

# 11. Chiusura Spark
spark.stop()
