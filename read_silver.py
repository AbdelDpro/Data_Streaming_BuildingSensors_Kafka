from pyspark.sql import SparkSession
import os

# Configuration
DELTA_LAKE_PATH = '/home/abdeldpro/cours/Esther_brief/Analyse_flux_data_kafka/data/silver/iot_data'

# 1. Initialisation de la session Spark avec les packages Delta
spark = SparkSession.builder \
    .appName("DeltaReader") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# 2. Lecture des données dans le format 'delta'
try:
    print(f"Tentative de lecture de la table Delta à : {DELTA_LAKE_PATH}")
    
    df_silver = spark.read \
        .format("delta") \
        .load(DELTA_LAKE_PATH)

    # 3. Affichage du schéma et des premières lignes
    print("\n--- Schéma des Données Silver ---")
    df_silver.printSchema()

    print("\n--- 10 Premières Lignes des Données Silver ---")
    df_silver.show(10, truncate=False)

    print(f"\nNombre total d'enregistrements dans la table Delta : {df_silver.count()}")

except Exception as e:
    print(f"\n!!! ERREUR : Impossible de lire la table Delta.")
    print("Vérifiez si le chemin est correct et si le script de streaming a déjà écrit des données.")
    print(f"Détails : {e}")

finally:
    spark.stop()