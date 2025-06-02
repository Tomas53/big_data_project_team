from pyspark.sql.functions import col, floor, count, countDistinct
from pyspark.sql.types import DoubleType

def detect_ports(df, logger, grid_size=0.01, min_stationary_points=50, min_unique_vessels=5):
    """
    Detects potential port areas by identifying grid cells with a high density of stationary vessel positions.

    Args:
        df (DataFrame): AIS DataFrame with at least ['Latitude', 'Longitude', 'SOG', 'MMSI'] columns.
        logger: Logger instance for logging steps.
        grid_size (float): Grid resolution in degrees. Smaller value = finer grid.
        min_stationary_points (int): Minimum number of stationary AIS pings required to consider a grid.
        min_unique_vessels (int): Minimum number of unique vessels seen in the grid to consider it a port.

    Returns:
        DataFrame: Ports DataFrame with columns ['grid_lat', 'grid_lon', 'stationary_count', 'unique_vessels'].
    """
    logger.info("Starting port detection")

    # Ensure SOG is numeric
    df = df.withColumn("SOG", col("SOG").cast(DoubleType()))

    # Step 1: Filter out stationary positions (SOG < 0.5 knots)
    stationary_df = df.filter((col("SOG").isNotNull()) & (col("SOG") < 0.5))
    logger.info(f"Stationary points count: {stationary_df.count()}")

    # Step 2: Assign grid buckets
    stationary_df = stationary_df.withColumn("grid_lat", floor(col("Latitude") / grid_size)) \
                                 .withColumn("grid_lon", floor(col("Longitude") / grid_size))

    # Step 3: Count pings and unique vessels per grid
    grid_stats = stationary_df.groupBy("grid_lat", "grid_lon").agg(
        count("*").alias("stationary_count"),
        countDistinct("MMSI").alias("unique_vessels")
    )

    # Step 4: Filter based on thresholds
    ports_df = grid_stats.filter(
        (col("stationary_count") >= min_stationary_points) &
        (col("unique_vessels") >= min_unique_vessels)
    )

    logger.info(f"Detected {ports_df.count()} candidate ports")
    return ports_df
