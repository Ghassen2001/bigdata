from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType

KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
KAFKA_TOPIC = "csv-data-topic"
OUTPUT_PATH = "/opt/airflow/data/processed_data.json"

# Exemple de schéma générique (à adapter selon vos données réelles)
schema = StructType([])  # On va inférer le schéma dynamiquement plus tard

spark = SparkSession.builder \
    .appName("KafkaSparkProcessing") \
    .getOrCreate()

# Lecture des données depuis Kafka
df = spark.read \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "earliest") \
    .load()

# Les messages sont dans la colonne 'value' (binaire)
df = df.selectExpr("CAST(value AS STRING) as json_str")

# Inférer le schéma à partir d'un échantillon
data_sample = df.limit(100).toPandas()["json_str"].apply(lambda x: eval(x))
if not data_sample.empty:
    from pyspark.sql.types import StructField
    fields = [StructField(str(k), StringType(), True) for k in data_sample.iloc[0].keys()]
    schema = StructType(fields)

# Appliquer le schéma
df = df.withColumn("data", from_json(col("json_str"), schema)).select("data.*")

# Exemple de traitement Spark (à adapter selon vos besoins)
df_clean = df.dropna(how="all")

# Sauvegarde du résultat
if df_clean.count() > 0:
    df_clean.write.mode("overwrite").json(OUTPUT_PATH)
    print(f"✅ Données traitées sauvegardées dans {OUTPUT_PATH}")
else:
    print("Aucune donnée à sauvegarder.")

spark.stop()
