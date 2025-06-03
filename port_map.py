import folium
from pyspark.sql import DataFrame
import pandas as pd
from folium.plugins import MarkerCluster
from branca.element import Template, MacroElement


def visualize_ports(port_df: DataFrame, output_html="ports_map.html", logger=None):
    """
    Visualize ports on a map, with size based on unique vessels count.

    Parameters:
    -----------
    port_df : DataFrame
        Spark DataFrame containing port data with at least grid_lat, grid_lon, and unique_vessels.
        If stationary_count is available, it will be shown in tooltips as additional information.
    output_html : str, default="ports_map.html"
        Path to save the output HTML map
    logger : logging.Logger, optional
        Logger for logging messages
    """
    # Check if unique_vessels column exists
    if "unique_vessels" not in port_df.columns:
        error_msg = "Input DataFrame must contain 'unique_vessels' column"
        if logger:
            logger.error(error_msg)
        else:
            print(f"ERROR: {error_msg}")
        raise ValueError(error_msg)

    # Determine if stationary_count is available for tooltip info
    has_stationary_count = "stationary_count" in port_df.columns

    # Convert grid back to approximate coordinates
    grid_size = 0.01
    if has_stationary_count:
        pdf = port_df.withColumn("lat", (port_df["grid_lat"] + 0.5) * grid_size) \
            .withColumn("lon", (port_df["grid_lon"] + 0.5) * grid_size) \
            .select("lat", "lon", "unique_vessels", "stationary_count") \
            .toPandas()
    else:
        pdf = port_df.withColumn("lat", (port_df["grid_lat"] + 0.5) * grid_size) \
            .withColumn("lon", (port_df["grid_lon"] + 0.5) * grid_size) \
            .select("lat", "lon", "unique_vessels") \
            .toPandas()

    # Sort by unique_vessels to identify top ports
    pdf = pdf.sort_values(by="unique_vessels", ascending=False)

    # Identify top 10 ports
    top_10_ports = pdf.head(10).copy()
    other_ports = pdf.iloc[10:].copy()

    if logger:
        logger.info(f"Preparing to render {len(pdf)} port candidates with top 10 highlighted")
        logger.info(f"Top port has {pdf.iloc[0]['unique_vessels']} unique vessels")

    # Center around Denmark (latitude ~56, longitude ~10)
    map_center = [56.0, 10.0]
    port_map = folium.Map(location=map_center, zoom_start=6)

    # Add title to the map
    title_html = '''
        <h3 align="center" style="font-size:16px"><b>Port Locations Map - Unique Vessels</b></h3>
    '''
    port_map.get_root().html.add_child(folium.Element(title_html))

    # Find the maximum unique vessels count for scaling
    max_count = pdf["unique_vessels"].max()

    # Add markers for regular ports
    regular_ports_group = folium.FeatureGroup(name="Regular Ports")
    for _, row in other_ports.iterrows():
        # Scale radius based on unique vessels count (min 3, max 8)
        radius = 3 + (row["unique_vessels"] / max_count) * 5

        # Prepare tooltip text
        if has_stationary_count:
            tooltip_text = (f"Port at ({row['lat']:.4f}, {row['lon']:.4f})<br>" +
                            f"Unique Vessels: {row['unique_vessels']}<br>" +
                            f"Stationary Count: {row['stationary_count']}")
        else:
            tooltip_text = (f"Port at ({row['lat']:.4f}, {row['lon']:.4f})<br>" +
                            f"Unique Vessels: {row['unique_vessels']}")

        folium.CircleMarker(
            location=(row["lat"], row["lon"]),
            radius=radius,
            color="blue",
            fill=True,
            fill_opacity=0.6,
            tooltip=tooltip_text
        ).add_to(regular_ports_group)
    regular_ports_group.add_to(port_map)

    # Add markers for top 10 ports with different style
    top_ports_group = folium.FeatureGroup(name="Top 10 Ports by Unique Vessels")
    for i, row in top_10_ports.iterrows():
        # Larger radius for top ports
        radius = 8 + (row["unique_vessels"] / max_count) * 7

        # Prepare tooltip text
        if has_stationary_count:
            tooltip_text = (f"<b>TOP {i + 1} PORT</b><br>" +
                            f"Location: ({row['lat']:.4f}, {row['lon']:.4f})<br>" +
                            f"Unique Vessels: {row['unique_vessels']}<br>" +
                            f"Stationary Count: {row['stationary_count']}")
        else:
            tooltip_text = (f"<b>TOP {i + 1} PORT</b><br>" +
                            f"Location: ({row['lat']:.4f}, {row['lon']:.4f})<br>" +
                            f"Unique Vessels: {row['unique_vessels']}")

        folium.CircleMarker(
            location=(row["lat"], row["lon"]),
            radius=radius,
            color="red",
            fill=True,
            fill_opacity=0.8,
            tooltip=tooltip_text
        ).add_to(top_ports_group)
    top_ports_group.add_to(port_map)

    # Add layer control
    folium.LayerControl().add_to(port_map)

    # Add a custom legend
    legend_html = '''
    <div style="position: fixed; 
                bottom: 50px; right: 50px; width: 220px; height: 130px; 
                border:2px solid grey; z-index:9999; font-size:14px;
                background-color:white;
                padding: 10px;
                border-radius: 5px;
                ">
    <div style="position: relative; top: 3px; left: 3px; width: 210px;">
    <p style="margin-bottom: 5px;"><b>Legend</b></p>
    <div style="display: flex; align-items: center; margin-bottom: 5px;">
        <div style="width: 15px; height: 15px; border-radius: 50%; background-color: red; margin-right: 5px;"></div>
        <span>Top 10 Ports by Unique Vessels</span>
    </div>
    <div style="display: flex; align-items: center;">
        <div style="width: 15px; height: 15px; border-radius: 50%; background-color: blue; margin-right: 5px;"></div>
        <span>Other Ports</span>
    </div>
    <p style="margin-top: 5px; font-size: 12px;"><i>Circle size indicates number of unique vessels</i></p>
    </div>
    </div>
    '''

    # Add the legend as a child to the map
    port_map.get_root().html.add_child(folium.Element(legend_html))

    # Save the map
    port_map.save(output_html)
    if logger:
        logger.info(f"Map saved to {output_html}")
    else:
        print(f"Map saved to {output_html}")