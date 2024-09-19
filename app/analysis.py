from pyspark.sql import SparkSession
from pyspark.sql.functions import hour, count, col

# Initialize Spark session
spark = SparkSession.builder \
    .appName("HourlyRideDistribution") \
    .master("local[*]") \
    .config("spark.cassandra.connection.host", "cassandra") \
    .getOrCreate()

# Cassandra setups for read the data
taxi_df = spark.read \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="nyc_taxi_rides", keyspace="taxi") \
    .load()


taxi_df = taxi_df.withColumn("pickup_hour", hour(col("pickup_datetime")))

# Basic Analysis Part
hourly_analysis = taxi_df.groupBy("pickup_hour") \
                         .agg(count("*").alias("total_rides")) \
                         .orderBy("pickup_hour")

# Write to the cassandra
hourly_analysis.write \
               .format("org.apache.spark.sql.cassandra") \
               .option("keyspace", "taxi") \
               .option("table", "hourly_ride_distribution") \
               .mode("append") \
               .save()

print("Hourly ride distribution analysis job completed successfully.")
