"""
Spark Data Processing Pipeline
raw → silver (cleaned) → gold (aggregated analytics)
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType
import os

MINIO_ENDPOINT  = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS    = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET    = os.getenv("MINIO_SECRET_KEY", "minioadmin")

RAW_PATH    = "s3a://taxi/raw/"
SILVER_PATH = "s3a://taxi/silver/"
GOLD_PATH   = "s3a://taxi/gold/"

def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("TaxiPipeline")
        .config("spark.hadoop.fs.s3a.endpoint",           MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key",         MINIO_ACCESS)
        .config("spark.hadoop.fs.s3a.secret.key",         MINIO_SECRET)
        .config("spark.hadoop.fs.s3a.path.style.access",  "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )

def process_yellow(spark: SparkSession):
    print("\n=== Yellow Taxi: raw -> silver ===")
    df = spark.read.parquet(RAW_PATH + "yellow_2024_01.parquet",
                            RAW_PATH + "yellow_2024_02.parquet")

    silver = (
        df
        .dropna(subset=["tpep_pickup_datetime", "tpep_dropoff_datetime",
                        "PULocationID", "DOLocationID"])
        .filter(F.col("trip_distance") > 0)
        .filter(F.col("fare_amount")   > 0)
        .filter(F.col("passenger_count").between(1, 6))
        .withColumn("pickup_hour",
                    F.hour("tpep_pickup_datetime"))
        .withColumn("pickup_day_of_week",
                    F.dayofweek("tpep_pickup_datetime"))
        .withColumn("trip_duration_minutes",
                    (F.unix_timestamp("tpep_dropoff_datetime") -
                     F.unix_timestamp("tpep_pickup_datetime")) / 60)
        .withColumn("speed_mph",
                    F.col("trip_distance") /
                    (F.col("trip_duration_minutes") / 60 + 0.001))
        .withColumn("provider", F.lit("yellow"))
        .filter(F.col("trip_duration_minutes").between(1, 300))
        .filter(F.col("speed_mph") < 100)
        .withColumnRenamed("tpep_pickup_datetime",  "pickup_datetime")
        .withColumnRenamed("tpep_dropoff_datetime", "dropoff_datetime")
        .withColumnRenamed("PULocationID",          "pickup_location_id")
        .withColumnRenamed("DOLocationID",          "dropoff_location_id")
        .select("provider", "pickup_datetime", "dropoff_datetime",
                "pickup_location_id", "dropoff_location_id",
                "trip_distance", "fare_amount", "tip_amount",
                "passenger_count", "pickup_hour", "pickup_day_of_week",
                "trip_duration_minutes", "speed_mph")
    )

    silver.write.mode("overwrite").parquet(SILVER_PATH + "yellow/")
    count = silver.count()
    print(f"  Silver yellow rows: {count:,}")
    return silver

def process_fhvhv(spark: SparkSession):
    print("\n=== FHVHV (Uber/Lyft): raw -> silver ===")
    df = spark.read.parquet(RAW_PATH + "fhvhv_2024_01.parquet",
                            RAW_PATH + "fhvhv_2024_02.parquet")

    silver = (
        df
        .dropna(subset=["pickup_datetime", "dropoff_datetime",
                        "PULocationID", "DOLocationID"])
        .filter(F.col("trip_miles") > 0)
        .filter(F.col("base_passenger_fare") > 0)
        .withColumn("provider",
            F.when(F.col("hvfhs_license_num") == "HV0002", "juno")
             .when(F.col("hvfhs_license_num") == "HV0003", "uber")
             .when(F.col("hvfhs_license_num") == "HV0004", "via")
             .when(F.col("hvfhs_license_num") == "HV0005", "lyft")
             .otherwise("other"))
        .withColumn("pickup_hour",        F.hour("pickup_datetime"))
        .withColumn("pickup_day_of_week", F.dayofweek("pickup_datetime"))
        .withColumn("trip_duration_minutes",
                    (F.unix_timestamp("dropoff_datetime") -
                     F.unix_timestamp("pickup_datetime")) / 60)
        .withColumn("speed_mph",
                    F.col("trip_miles") /
                    (F.col("trip_duration_minutes") / 60 + 0.001))
        .filter(F.col("trip_duration_minutes").between(1, 300))
        .filter(F.col("speed_mph") < 100)
        .withColumnRenamed("PULocationID",        "pickup_location_id")
        .withColumnRenamed("DOLocationID",        "dropoff_location_id")
        .withColumnRenamed("trip_miles",          "trip_distance")
        .withColumnRenamed("base_passenger_fare", "fare_amount")
        .withColumn("tip_amount",
                    F.coalesce(F.col("tips").cast(DoubleType()), F.lit(0.0)))
        .withColumn("passenger_count", F.lit(None).cast("integer"))
        .select("provider", "pickup_datetime", "dropoff_datetime",
                "pickup_location_id", "dropoff_location_id",
                "trip_distance", "fare_amount", "tip_amount",
                "passenger_count", "pickup_hour", "pickup_day_of_week",
                "trip_duration_minutes", "speed_mph")
    )

    silver.write.mode("overwrite").parquet(SILVER_PATH + "fhvhv/")
    count = silver.count()
    print(f"  Silver FHVHV rows: {count:,}")
    return silver

def build_gold(spark: SparkSession):
    print("\n=== Building Gold layer ===")

    yellow   = spark.read.parquet(SILVER_PATH + "yellow/")
    fhvhv    = spark.read.parquet(SILVER_PATH + "fhvhv/")
    combined = yellow.unionByName(fhvhv)

    # Gold 1: rides per hour
    rides_per_hour = (
        combined
        .groupBy("provider",
                 F.to_date("pickup_datetime").alias("trip_date"),
                 "pickup_hour")
        .agg(
            F.count("*").alias("ride_count"),
            F.avg("fare_amount").alias("avg_fare"),
            F.avg("trip_distance").alias("avg_distance"),
            F.avg("trip_duration_minutes").alias("avg_duration_min"),
            F.sum("fare_amount").alias("total_revenue"),
        )
        .orderBy("trip_date", "pickup_hour")
    )
    rides_per_hour.write.mode("overwrite").parquet(GOLD_PATH + "rides_per_hour/")
    print(f"  Gold rides_per_hour rows: {rides_per_hour.count():,}")

    # Gold 2: provider summary
    provider_summary = (
        combined
        .groupBy("provider")
        .agg(
            F.count("*").alias("total_rides"),
            F.avg("fare_amount").alias("avg_fare"),
            F.avg("tip_amount").alias("avg_tip"),
            F.avg("trip_distance").alias("avg_distance_miles"),
            F.avg("trip_duration_minutes").alias("avg_duration_min"),
            F.sum("fare_amount").alias("total_revenue"),
            F.avg("speed_mph").alias("avg_speed_mph"),
        )
    )
    provider_summary.write.mode("overwrite").parquet(GOLD_PATH + "provider_summary/")
    print(f"  Gold provider_summary rows: {provider_summary.count():,}")

    # Gold 3: top routes
    top_routes = (
        combined
        .groupBy("pickup_location_id", "dropoff_location_id", "provider")
        .agg(
            F.count("*").alias("ride_count"),
            F.avg("fare_amount").alias("avg_fare"),
            F.avg("trip_duration_minutes").alias("avg_duration_min"),
        )
        .orderBy(F.desc("ride_count"))
        .limit(500)
    )
    top_routes.write.mode("overwrite").parquet(GOLD_PATH + "top_routes/")
    print(f"  Gold top_routes rows: {top_routes.count():,}")

    # Gold 4: day of week demand
    dow_demand = (
        combined
        .groupBy("pickup_day_of_week", "provider")
        .agg(
            F.count("*").alias("ride_count"),
            F.avg("fare_amount").alias("avg_fare"),
        )
    )
    dow_demand.write.mode("overwrite").parquet(GOLD_PATH + "dow_demand/")
    print(f"  Gold dow_demand rows: {dow_demand.count():,}")

def main():
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    process_yellow(spark)
    process_fhvhv(spark)
    build_gold(spark)

    spark.stop()
    print("\nPipeline complete.")

if __name__ == "__main__":
    main()