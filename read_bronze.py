from pyspark.sql import SparkSession

# On configure SparkSession (la "centrale de commande") pour qu'elle sache lire le format Delta
spark = SparkSession.builder \
    .appName("Read Bronze") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Chargement des données brutes depuis le dossier bronze
df = spark.read.format("delta").load("data/bronze")

# J'affiche les 20 premières lignes pour vérifier la structure
print("\n Aperçu des données Bronze :")
df.show(20, truncate=False)

# Affichage de la volumétrie
print(f"\n Nombre total de records : {df.count()}")

# Analyse par catégories (Bâtiment et Capteur)
print("\n Répartition par bâtiment :")
df.groupBy("building").count().show()

print("\n Répartition par type de capteur :")
df.groupBy("type").count().show()

# J'isole les données critiques
print("\n  Alertes HIGH :")
df.filter(df.alert_level == "HIGH").show()