from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, current_timestamp, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, IntegerType

KAFKA_BROKER = 'localhost:9092' 
KAFKA_TOPIC = 'iot_sensors_raw'
DELTA_LAKE_PATH = '/home/abdeldpro/cours/Esther_brief/Analyse_flux_data_kafka/data/silver/iot_data'

# J'initialise ma session Spark avec les dépendances Kafka et Delta
spark = SparkSession.builder \
    .appName("KafkaToDeltaStreaming") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,io.delta:delta-core_2.12:2.4.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Je définis la structure attendue de mes données pour le parsing
schema = StructType([
    StructField("timestamp", StringType(), True), 
    StructField("device_id", StringType(), True),
    StructField("building", StringType(), True),
    StructField("floor", IntegerType(), True),
    StructField("type", StringType(), True),
    StructField("value", DoubleType(), True),
    StructField("unit", StringType(), True),
])

# Je configure la lecture du flux depuis le broker Kafka
df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "earliest") \
    .load()

# J'extrais, je transforme et je nettoie les données
df_processed = df_raw \
    .selectExpr("CAST(value AS STRING) as json_value", "timestamp as kafka_ingest_time") \
    .withColumn("data", from_json(col("json_value"), schema)) \
    .select(

        col("data.device_id").alias("sensor_id"),
        to_timestamp(col("data.timestamp"), "yyyy-MM-dd'T'HH:mm:ss'Z'").alias("event_time"),
        col("data.building").alias("building"),
        col("data.floor").alias("floor"),
        col("data.type").alias("sensor_type"), 
        col("data.value").alias("sensor_value"),
        col("data.unit").alias("unit"),

        col("kafka_ingest_time"),
        current_timestamp().alias("processing_time")
    ) \
    .filter(col("sensor_value").isNotNull())

# Je lance l'écriture continue vers la destination Delta Lake
query = df_processed.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", DELTA_LAKE_PATH + "/_checkpoint") \
    .trigger(processingTime='5 seconds') \
    .start(DELTA_LAKE_PATH)

print(f"Le pipeline Spark Structured Streaming est démarré. Les données nettoyées seront écrites dans la couche Silver à : {DELTA_LAKE_PATH}")

query.awaitTermination()