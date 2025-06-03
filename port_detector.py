from pyspark.sql.functions import col, floor, count, countDistinct
from pyspark.sql.types import DoubleType

def detect_ports(df, logger, grid_size=0.01, min_stationary_points=50, min_unique_vessels=10):
    """
    Detects ports using a grid-based method with stationary filtering and unique vessel filtering.

    Args:
        df (DataFrame): AIS data with at least ['Latitude', 'Longitude', 'SOG', 'MMSI'].
        logger: Logger instance.
        grid_size (float): Grid cell size in degrees.
        min_stationary_points (int): Minimum stationary points to consider a cell.
        min_unique_vessels (int): Minimum unique MMSI per cell to be a valid port.

    Returns:
        DataFrame: Candidate ports with grid coordinates and vessel counts.
    """
    logger.info("Starting enhanced port detection (grid + vessel filter)")

    df = df.withColumn("SOG", col("SOG").cast(DoubleType()))

    # Filter for stationary vessels
    stationary_df = df.filter((col("SOG").isNotNull()) & (col("SOG") < 0.5))
    logger.info(f"Stationary points count: {stationary_df.count()}")

    # Add grid coordinates
    stationary_df = stationary_df.withColumn("grid_lat", floor(col("Latitude") / grid_size)) \
                                 .withColumn("grid_lon", floor(col("Longitude") / grid_size))

    # Group by grid and count both points and unique vessels
    port_candidates = stationary_df.groupBy("grid_lat", "grid_lon") \
        .agg(
            count("*").alias("stationary_count"),
            countDistinct("MMSI").alias("unique_vessels")
        )

    # Filter cells by both thresholds
    ports_df = port_candidates.filter(
        (col("stationary_count") >= min_stationary_points) &
        (col("unique_vessels") >= min_unique_vessels)
    )

    logger.info(f"Detected {ports_df.count()} port candidates after filtering")
    return ports_df
