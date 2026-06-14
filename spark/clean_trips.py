import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

PROJECT = "data-management2-498012"
DEFAULT_INPUT = "gs://data-management2-498012-raw-landing/trips/*/*.csv"
BQ_TABLE = f"{PROJECT}.raw.trips"
TEMP_BUCKET = "data-management2-498012-dataproc-stage"

CANON = ["ride_id", "rideable_type", "started_at", "ended_at",
         "start_station_name", "start_station_id", "end_station_name",
         "end_station_id", "start_lat", "start_lng", "end_lat", "end_lng",
         "member_casual"]

def main():
    input_path = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_INPUT
    spark = SparkSession.builder.appName("clean-citibike-trips").getOrCreate()
    spark.conf.set("temporaryGcsBucket", TEMP_BUCKET)

    df = (spark.read
          .option("header", "true")
          .option("mode", "PERMISSIVE")
          .csv(input_path))

    # tag city from file path (honest, no fake column)
    df = df.withColumn("_path", F.input_file_name())
    df = df.withColumn("city",
                       F.when(F.col("_path").contains("/trips/jc/"), F.lit("jc"))
                        .otherwise(F.lit("nyc")))

    existing = [c for c in CANON if c in df.columns]
    df = df.select(*existing, "city")

    # cast types
    df = (df
          .withColumn("started_at", F.to_timestamp("started_at"))
          .withColumn("ended_at", F.to_timestamp("ended_at"))
          .withColumn("start_lat", F.col("start_lat").cast("double"))
          .withColumn("start_lng", F.col("start_lng").cast("double"))
          .withColumn("end_lat", F.col("end_lat").cast("double"))
          .withColumn("end_lng", F.col("end_lng").cast("double")))

    # derive duration
    df = df.withColumn(
        "trip_duration_sec",
        F.col("ended_at").cast("long") - F.col("started_at").cast("long"))

    # structural / load-time quality filters
    df = df.filter(
        F.col("ride_id").isNotNull() &
        F.col("started_at").isNotNull() &
        F.col("ended_at").isNotNull() &
        F.col("start_station_id").isNotNull() &
        F.col("end_station_id").isNotNull() &
        F.col("start_lat").isNotNull() & F.col("start_lng").isNotNull() &
        (F.col("trip_duration_sec") >= 60) &
        (F.col("trip_duration_sec") <= 60 * 60 * 24))

    # dedup
    df = df.dropDuplicates(["ride_id"])

    # partition + lineage
    df = (df
          .withColumn("start_date", F.to_date("started_at"))
          .withColumn("ingest_ts", F.current_timestamp()))

    # scope guard: last 2 yr only
    df = df.filter((F.col("start_date") >= F.lit("2024-06-01")) &
                   (F.col("start_date") <= F.lit("2026-05-31")))

    (df.write.format("bigquery")
       .option("table", BQ_TABLE)
       .option("temporaryGcsBucket", TEMP_BUCKET)
       .option("partitionField", "start_date")
       .option("partitionType", "DAY")
       .option("clusteredFields", "start_station_id")
       .mode("overwrite")
       .save())

    print("clean_trips: write complete", flush=True)
    spark.stop()

if __name__ == "__main__":
    main()