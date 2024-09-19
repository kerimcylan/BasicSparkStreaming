from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, DoubleType, IntegerType

# Initialize Spark session
spark = SparkSession.builder \
    .appName("KafkaSparkStreaming") \
    .master("local[*]") \
    .config("spark.cassandra.connection.host", "cassandra") \
    .getOrCreate()

# Schema Defining
schema = StructType() \
    .add("id", StringType()) \
    .add("vendor_id", StringType()) \
    .add("pickup_datetime", StringType()) \
    .add("passenger_count", StringType()) \
    .add("pickup_longitude", StringType()) \
    .add("pickup_latitude", StringType()) \
    .add("dropoff_longitude", StringType()) \
    .add("dropoff_latitude", StringType()) \
    .add("store_and_fwd_flag", StringType())

# Reading data from Kafka
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "nyc_taxi_rides") \
    .option("startingOffsets", "earliest") \
    .load()

parsed_df = df.selectExpr("CAST(value AS STRING) as json_value") \
              .select(from_json(col("json_value"), schema).alias("data"))


taxi_df = parsed_df.select(
    col("data.id").alias("id"),
    col("data.vendor_id").alias("vendor_id"),
    col("data.pickup_datetime").alias("pickup_datetime"),
    col("data.passenger_count").cast(IntegerType()).alias("passenger_count"),
    col("data.pickup_longitude").cast(DoubleType()).alias("pickup_longitude"),
    col("data.pickup_latitude").cast(DoubleType()).alias("pickup_latitude"),
    col("data.dropoff_longitude").cast(DoubleType()).alias("dropoff_longitude"),
    col("data.dropoff_latitude").cast(DoubleType()).alias("dropoff_latitude"),
    col("data.store_and_fwd_flag").alias("store_and_fwd_flag")
)


query = taxi_df.writeStream \
               .outputMode("append") \
               .format("org.apache.spark.sql.cassandra") \
               .option("keyspace", "taxi") \
               .option("table", "nyc_taxi_rides") \
               .option("checkpointLocation", "/app/checkpoints/nyc_taxi_rides") \
               .start()

query.awaitTermination()
