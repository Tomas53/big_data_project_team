from pyspark.sql.functions import col, to_timestamp
from pyspark.sql.types import DoubleType

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

    logger.info(f"[DEBUG] Filtered row count: {df.count()}")
    return df