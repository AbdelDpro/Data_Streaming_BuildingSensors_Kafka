from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, when, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

print("=" * 60)
print("DÉMARRAGE DU PIPELINE DE STREAMING")
print("=" * 60)

# ÉTAPE 1 : Configuration Spark avec Delta Lake
print("\n Configuration de Spark avec Delta Lake...")

spark = SparkSession.builder \
    .appName("IoT Building Sensors - Bronze Layer") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
print("Spark initialisé avec succès")

# ÉTAPE 2 : Définir le schéma EXACT de vos données JSON
print("\n Définition du schéma des données...")

schema = StructType([
    StructField("timestamp", StringType(), True),
    StructField("device_id", StringType(), True),
    StructField("building", StringType(), True),
    StructField("floor", IntegerType(), True),
    StructField("type", StringType(), True),
    StructField("value", DoubleType(), True),
    StructField("unit", StringType(), True)
])

print("Schéma défini (capteurs IoT)")

# ÉTAPE 3 : Lire le flux de données JSON
print("\n Lecture du flux JSON depuis data/input/...")

df_stream = spark.readStream \
    .schema(schema) \
    .option("maxFilesPerTrigger", 1) \
    .json("data/input")

print("Flux de lecture configuré")

# ÉTAPE 4 : Transformations (nettoyage, filtrage, projection)
print("\nApplication des transformations...")

df_cleaned = df_stream \
    .filter(col("device_id").isNotNull()) \
    .filter(col("value").isNotNull()) \
    .filter(col("timestamp").isNotNull()) \
    .filter(col("value") >= 0) \
    .withColumn("ingestion_time", current_timestamp()) \
    .withColumn("timestamp_parsed", to_timestamp(col("timestamp"))) \
    .withColumn("alert_level",
        when((col("type") == "temperature") & (col("value") > 28), "HIGH")
        .when((col("type") == "co2") & (col("value") > 1000), "HIGH")
        .when((col("type") == "humidity") & (col("value") < 30), "MEDIUM")
        .when((col("type") == "humidity") & (col("value") > 70), "MEDIUM")
        .otherwise("LOW")
    ) \
    .withColumn("building_floor", 
        col("building").cast("string") + "-" + col("floor").cast("string")
    ) \
    .select(
        "device_id",
        "timestamp_parsed",
        "building",
        "floor",
        "building_floor",
        "type",
        "value",
        "unit",
        "alert_level",
        "ingestion_time"
    )

print("Transformations configurées :")
print("   - Nettoyage : Filtrage des valeurs nulles et négatives")
print("   - Filtrage : Valeurs cohérentes (value >= 0)")
print("   - Enrichissement :")
print("      • Niveau d'alerte (température > 28°C, CO2 > 1000ppm)")
print("      • Identifiant bâtiment-étage (ex: A-2)")
print("      • Timestamp d'ingestion")

# ÉTAPE 5 : Écrire dans Delta Lake (Bronze Layer)
print("\n Configuration de l'écriture dans Delta Lake (Bronze)...")

query = df_cleaned.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "checkpoints/bronze") \
    .option("path", "data/bronze") \
    .trigger(processingTime="10 seconds") \
    .start()

print("Pipeline démarré avec succès !")
print("\n" + "=" * 60)
print("INFORMATIONS DU PIPELINE")
print("=" * 60)
print(f"Source        : data/input/ (JSON)")
print(f"Destination   : data/bronze/ (Delta Lake)")
print(f"Checkpoint    : checkpoints/bronze/")
print(f"⏱Fréquence     : Toutes les 10 secondes")
print(f"Types capteurs : température, CO2, humidité")
print(f"Bâtiments     : A, B (multi-étages)")
print(f"Spark UI      : http://localhost:4040")
print("=" * 60)
print("\nPour arrêter le pipeline : Ctrl+C\n")

# ÉTAPE 6 : Attendre la fin du streaming
try:
    query.awaitTermination()
except KeyboardInterrupt:
    print("\n\n Arrêt du pipeline demandé...")
    query.stop()
    spark.stop()
    print("Pipeline arrêté proprement")