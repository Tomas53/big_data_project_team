from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from logger_config import get_logger
from port_filtering import load_and_filter_data  # Import filtering function
from port_detector import detect_ports

# Configs
CSV_PATH = "./ais_dataset/aisdk-2024-05-04/aisdk-2024-05-04.csv"

def main():
    logger = get_logger("port_detection.log")

    spark = SparkSession.builder \
        .appName("Port Detection Pipeline") \
        .config("spark.python.worker.faulthandler.enabled", "true") \
        .config("spark.sql.execution.pyspark.udf.faulthandler.enabled", "true") \
        .config("spark.sql.ansi.enabled", "false") \
        .getOrCreate()

    # Step 1: Load and filter data
    df = load_and_filter_data(spark, CSV_PATH, logger)

    # Step 2: (Next steps go here, e.g., stationary detection or clustering)

    ports_df = detect_ports(df, logger)
    ports_df.show()


    ports_df.orderBy(col("stationary_count").desc()).show(10, truncate=False)

    spark.stop()

if __name__ == "__main__":
    main()
