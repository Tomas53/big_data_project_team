import folium
from pyspark.sql import DataFrame
import pandas as pd

def visualize_ports(port_df: DataFrame, output_html="ports_map.html", logger=None):
    # Convert grid back to approximate coordinates
    grid_size = 0.01
    pdf = port_df.withColumn("lat", (port_df["grid_lat"] + 0.5) * grid_size) \
                 .withColumn("lon", (port_df["grid_lon"] + 0.5) * grid_size) \
                 .select("lat", "lon", "stationary_count") \
                 .toPandas()

    if logger:
        logger.info(f"Preparing to render {len(pdf)} port candidates")

    # Center around Denmark (latitude ~56, longitude ~10)
    map_center = [56.0, 10.0]
    port_map = folium.Map(location=map_center, zoom_start=6)

    # Add markers
    for _, row in pdf.iterrows():
        folium.CircleMarker(
            location=(row["lat"], row["lon"]),
            radius=4,
            color="blue",
            fill=True,
            fill_opacity=0.6,
            tooltip=f"Count: {row['stationary_count']}"
        ).add_to(port_map)

    # Save the map
    port_map.save(output_html)
    if logger:
        logger.info(f"Map saved to {output_html}")
    else:
        print(f"Map saved to {output_html}")
