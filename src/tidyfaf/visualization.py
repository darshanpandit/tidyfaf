import pydeck as pdk
import pandas as pd

class FlowMap:
    def __init__(self, data: pd.DataFrame):
        """
        Initialize the FlowMap with FAF data.
        
        Args:
            data (pd.DataFrame): The FAF data containing origin and destination information.
                                 Expected columns: 'dms_orig', 'dms_dest', 'tons_2020' (or similar flow column)
        """
        self.data = data

    def generate_map(self, output_path="faf_flow_map.html", flow_column="tons_2020", max_flows=1000):
        """
        Generate a Deck.gl ArcLayer map visualizing freight flows.
        
        Args:
            output_path (str): Path to save the HTML map.
            flow_column (str): Column name representing the flow magnitude (e.g., tons, value).
            max_flows (int): Maximum number of top flows to visualize to avoid clutter.
        """
        # Make a copy to avoid modifying original
        df_plot = self.data.copy()

        # Handle GeoDataFrame input from to_gdf()
        if hasattr(df_plot, 'geometry'):
            # Extract coordinates from LineString
            # LineString((orig_lon, orig_lat), (dest_lon, dest_lat))
            def get_coords(geom):
                if hasattr(geom, 'coords'):
                    coords = list(geom.coords)
                    if len(coords) >= 2:
                        return coords[0][0], coords[0][1], coords[1][0], coords[1][1]
                return None, None, None, None

            coords = df_plot['geometry'].apply(get_coords)
            df_plot['orig_lon'] = coords.apply(lambda x: x[0])
            df_plot['orig_lat'] = coords.apply(lambda x: x[1])
            df_plot['dest_lon'] = coords.apply(lambda x: x[2])
            df_plot['dest_lat'] = coords.apply(lambda x: x[3])

        required_cols = ['orig_lat', 'orig_lon', 'dest_lat', 'dest_lon', flow_column]
        if not all(col in df_plot.columns for col in required_cols):
             raise ValueError(
                 f"Data must contain columns: {required_cols}. "
                 "Tip: Use query.to_gdf() before creating FlowMap."
             )

        # Sort by flow and take top N
        df_plot = df_plot.sort_values(by=flow_column, ascending=False).head(max_flows)

        # Normalize flow for width
        max_val = df_plot[flow_column].max()
        df_plot['width'] = df_plot[flow_column] / max_val * 10

        layer = pdk.Layer(
            "ArcLayer",
            df_plot,
            get_source_position=["orig_lon", "orig_lat"],
            get_target_position=["dest_lon", "dest_lat"],
            get_source_color=[0, 255, 0, 160],
            get_target_color=[255, 0, 0, 160],
            get_width="width",
            pickable=True,
            auto_highlight=True,
        )

        view_state = pdk.ViewState(
            latitude=39.8283,
            longitude=-98.5795,
            zoom=3,
            pitch=45,
            bearing=0
        )

        r = pdk.Deck(
            layers=[layer],
            initial_view_state=view_state,
            tooltip={"text": "{dms_orig} to {dms_dest}\nFlow: {" + flow_column + "}"}
        )

        r.to_html(output_path)
        print(f"Map saved to {output_path}")
