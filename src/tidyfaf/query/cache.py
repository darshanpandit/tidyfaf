"""
Caching utilities for query results.

Provides a two-level cache:
1. Raw data cache: Stores loaded parquet files (per session)
2. Query result cache: Stores filtered DataFrames (LRU with size limit)
"""

import pandas as pd
import geopandas as gpd
from pathlib import Path
from collections import OrderedDict


class QueryCache:
    """
    Cache manager for query results.

    Implements lazy loading and caching at two levels:
    - Raw data: Full parquet files loaded once per session
    - Filtered results: Query results cached by filter signature

    Parameters
    ----------
    max_results : int, default 100
        Maximum number of filtered results to cache
    data_dir : Path or str, optional
        Directory containing data files. Defaults to ~/.tidyfaf_data
    """

    def __init__(self, max_results=100, data_dir=None):
        self._raw_data_cache = {}  # {data_type: DataFrame}
        self._result_cache = OrderedDict()  # {signature: DataFrame}
        self._max_results = max_results

        if data_dir is None:
            self.data_dir = Path.home() / ".tidyfaf_data"
        else:
            self.data_dir = Path(data_dir)

    def get_raw_data(self, data_type):
        """
        Load raw data file, using cache if available.

        Parameters
        ----------
        data_type : str
            Type of data to load. Options: 'regional', 'state', 'network',
            'hilo', 'state_hilo'

        Returns
        -------
        pd.DataFrame or gpd.GeoDataFrame
            Raw data from parquet file
        """
        if data_type not in self._raw_data_cache:
            self._raw_data_cache[data_type] = self._load_parquet(data_type)
        return self._raw_data_cache[data_type]

    def get_filtered_data(self, data_type, columns=None, filters=None):
        """
        Load data with column projection and predicate pushdown.
        Does NOT cache the result in _raw_data_cache (to save memory).

        Parameters
        ----------
        data_type : str
            Type of data to load
        columns : list, optional
            List of columns to load
        filters : list, optional
            PyArrow-style filters (DNF format)

        Returns
        -------
        pd.DataFrame
            Filtered data
        """
        # If full data is already cached, use it (slicing in memory)
        if data_type in self._raw_data_cache:
            df = self._raw_data_cache[data_type]
            
            # Apply column projection
            if columns:
                # Ensure all requested columns exist
                available_cols = [c for c in columns if c in df.columns]
                df = df[available_cols]
            
            # Note: We cannot easily apply PyArrow filters to a DataFrame here.
            # The caller (BaseQuery) will handle applying filters via _apply_filters.
            # So we just return the column-sliced data.
            return df

        # Otherwise, load from disk with pushdown
        return self._load_parquet(data_type, columns=columns, filters=filters)

    def get_filtered(self, signature):
        """
        Retrieve cached filtered result by signature.

        Parameters
        ----------
        signature : str
            Hash signature of filter state

        Returns
        -------
        pd.DataFrame or None
            Cached result if exists, None otherwise
        """
        if signature in self._result_cache:
            # Move to end (LRU)
            self._result_cache.move_to_end(signature)
            return self._result_cache[signature]
        return None

    def cache_filtered(self, signature, result):
        """
        Cache a filtered result.

        Parameters
        ----------
        signature : str
            Hash signature of filter state
        result : pd.DataFrame
            Filtered DataFrame to cache
        """
        # Evict oldest if at capacity
        if len(self._result_cache) >= self._max_results:
            self._result_cache.popitem(last=False)  # Remove oldest (FIFO)

        self._result_cache[signature] = result

    def clear(self):
        """Clear all cached results (keeps raw data cache)."""
        self._result_cache.clear()

    def clear_all(self):
        """Clear all caches including raw data."""
        self._raw_data_cache.clear()
        self._result_cache.clear()

    def _load_parquet(self, data_type, columns=None, filters=None):
        """
        Load parquet file based on data type.

        Parameters
        ----------
        data_type : str
            Type of data to load
        columns : list, optional
            Columns to read
        filters : list, optional
            PyArrow filters

        Returns
        -------
        pd.DataFrame or gpd.GeoDataFrame
            Loaded data

        Raises
        ------
        FileNotFoundError
            If data file not found
        ValueError
            If invalid data_type specified
        """
        file_map = {
            'regional': 'FAF5.7.1.parquet',
            'state': 'FAF5.7.1_State.parquet',
            'hilo': 'FAF5.7.1_HiLoForecasts.parquet',
            'state_hilo': 'FAF5.7.1_State_HiLoForecasts.parquet',
            'network': 'FAF5_Network_Links.parquet',
            'zones': 'FAF5_Zones_Processed.parquet'
        }

        if data_type not in file_map:
            raise ValueError(
                f"Invalid data_type '{data_type}'. "
                f"Options: {list(file_map.keys())}"
            )

        filepath = self.data_dir / file_map[data_type]

        if not filepath.exists():
            raise FileNotFoundError(
                f"{data_type} data not found at {filepath}. "
                "Please run download_and_process() first."
            )

        print(f"Loading {data_type} data from {filepath.name}...")

        # Load as GeoDataFrame if it contains geometry
        if data_type in ('network', 'zones'):
            # GeoPandas read_parquet handles columns/filters in newer versions,
            # but standard pandas read_parquet is safer for pure attributes.
            # However, network/zones need geometry.
            # filters/columns support depends on geopandas version (>=0.11).
            # We'll assume it's supported or fallback to loading full if error?
            # For now, just pass them.
            return gpd.read_parquet(filepath, columns=columns, filters=filters)
        else:
            return pd.read_parquet(filepath, columns=columns, filters=filters)


# Global singleton cache instance
_global_cache = QueryCache()


def get_cache():
    """Get the global cache instance."""
    return _global_cache


def clear_cache():
    """Clear the global query result cache."""
    _global_cache.clear()


def clear_all_caches():
    """Clear all caches including raw data."""
    _global_cache.clear_all()
