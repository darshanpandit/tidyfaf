"""
Centralized data loading utilities.
"""

import pandas as pd
import geopandas as gpd
from pathlib import Path


class DataLoader:
    """
    Centralized data loading with lazy evaluation.

    Parameters
    ----------
    data_dir : Path or str, optional
        Directory containing data files. Defaults to ~/.tidyfaf_data
    """

    def __init__(self, data_dir=None):
        if data_dir is None:
            self.data_dir = Path.home() / ".tidyfaf_data"
        else:
            self.data_dir = Path(data_dir)

    def load_regional(self):
        """
        Load FAF5.7.1.parquet (regional flows, ~2.6M rows).

        Returns
        -------
        pd.DataFrame
            Regional zone-level flows
        """
        filepath = self.data_dir / "FAF5.7.1.parquet"
        if not filepath.exists():
            raise FileNotFoundError(
                f"Regional data not found at {filepath}. "
                "Please run faf.download_and_process() first."
            )
        return pd.read_parquet(filepath)

    def load_state(self):
        """
        Load FAF5.7.1_State.parquet (state flows, ~1.2M rows).

        Returns
        -------
        pd.DataFrame
            State-level flows
        """
        filepath = self.data_dir / "FAF5.7.1_State.parquet"
        if not filepath.exists():
            raise FileNotFoundError(
                f"State data not found at {filepath}. "
                "Please run faf.download_and_process() first."
            )
        return pd.read_parquet(filepath)

    def load_hilo(self):
        """
        Load FAF5.7.1_HiLoForecasts.parquet (forecast scenarios).

        Returns
        -------
        pd.DataFrame
            Regional forecasts with base/high/low scenarios
        """
        filepath = self.data_dir / "FAF5.7.1_HiLoForecasts.parquet"
        if not filepath.exists():
            raise FileNotFoundError(
                f"HiLo forecast data not found at {filepath}. "
                "Please run faf.download_and_process() first."
            )
        return pd.read_parquet(filepath)

    def load_state_hilo(self):
        """
        Load FAF5.7.1_State_HiLoForecasts.parquet (state-level forecasts).

        Returns
        -------
        pd.DataFrame
            State-level forecasts with scenarios
        """
        filepath = self.data_dir / "FAF5.7.1_State_HiLoForecasts.parquet"
        if not filepath.exists():
            raise FileNotFoundError(
                f"State HiLo forecast data not found at {filepath}. "
                "Please run faf.download_and_process() first."
            )
        return pd.read_parquet(filepath)

    def load_network(self):
        """
        Load FAF5_Network_Links.parquet (highway network).

        Returns
        -------
        gpd.GeoDataFrame
            Network links with geometry
        """
        filepath = self.data_dir / "FAF5_Network_Links.parquet"
        if not filepath.exists():
            raise FileNotFoundError(
                f"Network data not found at {filepath}. "
                "Please run faf.download_and_process() first."
            )
        return gpd.read_parquet(filepath)

    def load_zones(self):
        """
        Load FAF5_Zones_Processed.parquet (zone geometries).

        Returns
        -------
        gpd.GeoDataFrame
            FAF zone polygons
        """
        filepath = self.data_dir / "FAF5_Zones_Processed.parquet"
        if not filepath.exists():
            raise FileNotFoundError(
                f"Zone geometries not found at {filepath}. "
                "Please run faf.download_and_process() first."
            )
        return gpd.read_parquet(filepath)
