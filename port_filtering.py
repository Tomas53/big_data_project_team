from pyspark.sql.functions import col, to_timestamp, lag, unix_timestamp
from pyspark.sql.types import DoubleType
from pyspark.sql.window import Window

def load_and_filter_data(spark, csv_path, logger):
    # Read AIS CSV
    df = spark.read.option("header", True).csv(csv_path)
    logger.info(f"[DEBUG] Initial row count: {df.count()}")

    # Rename and cast columns
    df = df.withColumnRenamed("# Timestamp", "timestamp") \
        .withColumn("timestamp", to_timestamp(col("timestamp"), "dd/MM/yyyy HH:mm:ss")) \
        .withColumn("Latitude", col("Latitude").cast(DoubleType())) \
        .withColumn("Longitude", col("Longitude").cast(DoubleType()))

    # Filter out rows with missing or invalid data
    df = df.filter(
        (col("timestamp").isNotNull()) &
        (col("Latitude").isNotNull()) & (col("Latitude") != 0.0) &
        (col("Longitude").isNotNull()) & (col("Longitude") != 0.0) &
        (col("MMSI").isNotNull())
    )
    logger.info(f"[DEBUG] After null/zero filtering: {df.count()}")

    # Window spec per vessel by time
    w = Window.partitionBy("MMSI").orderBy("timestamp")

    # Remove points with duplicate timestamps per vessel
    df = df.withColumn("prev_ts", lag("timestamp").over(w))
    df = df.filter((col("prev_ts").isNull()) | (unix_timestamp("timestamp") != unix_timestamp("prev_ts")))
    logger.info(f"[DEBUG] After duplicate timestamp filtering: {df.count()}")

    # Filter out entries with 0-second time difference (data spam)
    df = df.withColumn("time_diff", unix_timestamp("timestamp") - unix_timestamp("prev_ts"))
    df = df.filter((col("time_diff").isNull()) | (col("time_diff") > 0))
    logger.info(f"[DEBUG] After 0-second time difference filtering: {df.count()}")

    # TODO: Optional - add distance-based speed filtering if needed

    return df
