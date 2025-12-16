from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Read Bronze") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Lire la table Delta Bronze
df = spark.read.format("delta").load("data/bronze")

print("\n Aperçu des données Bronze :")
df.show(20, truncate=False)

print(f"\n Nombre total de records : {df.count()}")

print("\n Répartition par bâtiment :")
df.groupBy("building").count().show()

print("\n Répartition par type de capteur :")
df.groupBy("type").count().show()

print("\n  Alertes HIGH :")
df.filter(df.alert_level == "HIGH").show()