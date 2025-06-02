from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp
from pyspark.sql.types import DoubleType
from logger_config import get_logger

# Initialize logger
logger = get_logger("port_detection.log")

# Define the path to your new AIS dataset
CSV_PATH = "./ais_dataset/aisdk-2024-05-04/aisdk-2024-05-04.csv"

def main():
    # Initialize SparkSession
    spark = SparkSession.builder \
        .appName("Port Detection - Data Filtering") \
        .config("spark.python.worker.faulthandler.enabled", "true") \
        .config("spark.sql.execution.pyspark.udf.faulthandler.enabled", "true") \
        .config("spark.sql.ansi.enabled", "false") \
        .getOrCreate()

    # Read the AIS dataset
    df = spark.read.option("header", True).csv(CSV_PATH)
    logger.info(f"Initial row count: {df.count()}")

    # Rename and cast columns
    df = df.withColumnRenamed("# Timestamp", "timestamp") \
        .withColumn("timestamp", to_timestamp(col("timestamp"), "dd/MM/yyyy HH:mm:ss")) \
        .withColumn("Latitude", col("Latitude").cast(DoubleType())) \
        .withColumn("Longitude", col("Longitude").cast(DoubleType()))

    # === DEBUGGING CHECKS START ===
    logger.info(f"[DEBUG] Null timestamps: {df.filter(col('timestamp').isNull()).count()}")
    logger.info(f"[DEBUG] Null latitudes: {df.filter(col('Latitude').isNull()).count()}")
    logger.info(f"[DEBUG] Null longitudes: {df.filter(col('Longitude').isNull()).count()}")
    logger.info(f"[DEBUG] Null MMSI: {df.filter(col('MMSI').isNull()).count()}")
    logger.info(f"[DEBUG] Zero latitude or longitude: {df.filter((col('Latitude') == 0.0) | (col('Longitude') == 0.0)).count()}")
    # === DEBUGGING CHECKS END ===

    # Filter invalid or missing data
    df = df.filter(
        (col("timestamp").isNotNull()) &
        (col("Latitude").isNotNull()) & (col("Latitude") != 0.0) &
        (col("Longitude").isNotNull()) & (col("Longitude") != 0.0) &
        (col("MMSI").isNotNull())
    )
    
    logger.info(f"Filtered row count: {df.count()}")

    # Optional: Save filtered data if needed
    # df.write.mode("overwrite").parquet("filtered_data/")

    spark.stop()

if __name__ == "__main__":
    main()
