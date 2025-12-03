"""
Abstract base class for query builders.

Implements immutable filter pattern with lazy evaluation and caching.
"""

from abc import ABC, abstractmethod
import hashlib
import json
from .cache import _global_cache


class BaseQuery(ABC):
    """
    Abstract base class for all query builders.

    Implements immutable filter pattern: each filter method returns
    a new query instance with updated filters. Data loads only when
    .get() is called (lazy evaluation).

    Parameters
    ----------
    filters : dict, optional
        Dictionary of filter conditions
    cache : QueryCache, optional
        Cache instance (defaults to global cache)
    metadata : FAFMetadata, optional
        Metadata instance for lookups
    """

    def __init__(self, filters=None, cache=None, metadata=None):
        from tidyfaf.metadata import FAFMetadata

        self._filters = filters or {}
        self._cache = cache or _global_cache
        self._metadata = metadata or FAFMetadata()
        # _data_type should be set as class attribute by subclasses
        # Don't overwrite it here if it exists
        if not hasattr(self.__class__, '_data_type'):
            self._data_type = None

    @abstractmethod
    def _load_data(self, columns=None, filters=None):
        """
        Load raw data from parquet file.

        Must be implemented by subclasses.

        Parameters
        ----------
        columns : list, optional
            Columns to load
        filters : list, optional
            PyArrow filters

        Returns
        -------
        pd.DataFrame or gpd.GeoDataFrame
            Raw data
        """
        pass

    @abstractmethod
    def _apply_filters(self, df):
        """
        Apply self._filters to DataFrame.

        Must be implemented by subclasses.

        Parameters
        ----------
        df : pd.DataFrame
            Raw data to filter

        Returns
        -------
        pd.DataFrame
            Filtered data
        """
        pass

    def _get_needed_columns(self):
        """
        Get list of columns needed for the query.
        Can be overridden by subclasses.
        """
        return None

    def _get_pushdown_filters(self):
        """
        Get list of filters for parquet pushdown.
        Can be overridden by subclasses.
        """
        return None

    def _filter_signature(self):
        """
        Generate hash of current filter state for caching.

        Returns
        -------
        str
            MD5 hash of filter dictionary
        """
        import numpy as np

        def convert_to_python_types(obj):
            """Convert numpy types to Python native types for JSON serialization."""
            if isinstance(obj, (np.integer, np.int64)):
                return int(obj)
            elif isinstance(obj, (np.floating, np.float64)):
                return float(obj)
            elif isinstance(obj, np.ndarray):
                return obj.tolist()
            elif isinstance(obj, list):
                return [convert_to_python_types(item) for item in obj]
            elif isinstance(obj, dict):
                return {k: convert_to_python_types(v) for k, v in obj.items()}
            else:
                return obj

        # Sort filters for consistent hashing
        filter_dict = {k: v for k, v in sorted(self._filters.items())}
        # Convert numpy types to Python native types
        filter_dict = convert_to_python_types(filter_dict)
        
        # Include data_type in signature to prevent collisions
        sig_data = {
            'filters': filter_dict,
            'data_type': getattr(self, '_data_type', 'unknown')
        }
        
        # Convert to JSON string
        filter_json = json.dumps(sig_data, sort_keys=True)
        return hashlib.md5(filter_json.encode()).hexdigest()

    def get(self, format='wide'):
        """
        Execute query and return DataFrame.

        Parameters
        ----------
        format : {'wide', 'long'}, default 'wide'
            Output format. 'wide' returns year columns (tons_2020, tons_2025).
            'long' returns tidy format with year as a dimension.

        Returns
        -------
        pd.DataFrame or gpd.GeoDataFrame
            Query results
        """
        # Include format in cache signature
        sig = self._filter_signature() + f"_{format}"

        # Check cache first
        cached = self._cache.get_filtered(sig)
        if cached is not None:
            return cached.copy()  # Return copy to prevent mutation

        # Determine columns and pushdown filters
        columns = self._get_needed_columns()
        pushdown_filters = self._get_pushdown_filters()

        # Load raw data (potentially partial)
        df = self._load_data(columns=columns, filters=pushdown_filters)

        # Apply filters (memory-side)
        df = self._apply_filters(df)

        # Reshape if needed
        if format == 'long':
            df = self._to_long_format(df)

        # Cache result
        self._cache.cache_filtered(sig, df)

        return df.copy()

    def _to_long_format(self, df):
        """
        Convert wide format (tons_2020, tons_2025, ...) to long format.

        Parameters
        ----------
        df : pd.DataFrame
            Wide format data

        Returns
        -------
        pd.DataFrame
            Long format with year, metric, value columns
        """
        import pandas as pd

        # Identify metadata columns (non-metric columns)
        metric_prefixes = ['tons_', 'value_', 'tmiles_', 'current_value_']
        metric_cols = [col for col in df.columns
                      if any(col.startswith(prefix) for prefix in metric_prefixes)]
        id_cols = [col for col in df.columns if col not in metric_cols]

        # Melt to long format
        df_long = df.melt(
            id_vars=id_cols,
            value_vars=metric_cols,
            var_name='metric_year',
            value_name='value'
        )

        # Split metric_year into metric and year
        df_long['metric'] = df_long['metric_year'].str.split('_').str[0]
        df_long['year'] = df_long['metric_year'].str.split('_').str[1].astype(int)
        df_long = df_long.drop(columns=['metric_year'])

        # Pivot so each metric is a column
        df_long = df_long.pivot_table(
            index=id_cols + ['year'],
            columns='metric',
            values='value'
        ).reset_index()

        return df_long

    def validate(self):
        """
        Validate filter inputs.

        Returns
        -------
        dict
            Validation results with 'valid' bool and 'warnings' list
        """
        warnings = []

        # Check if any filters are set
        if not self._filters:
            warnings.append("No filters applied - query will return all data")

        # Subclasses can override to add specific validations

        return {
            'valid': True,
            'warnings': warnings
        }

    def estimate_size(self):
        """
        Estimate result row count without executing full query.

        Returns
        -------
        int
            Estimated number of rows (approximate)
        """
        # Load raw data (from cache if available)
        df = self._cache.get_raw_data(self._data_type)

        # Apply filters and count
        filtered = self._apply_filters(df)
        return len(filtered)

    def __repr__(self):
        """
        Pretty print current filter state.

        Returns
        -------
        str
            String representation of query
        """
        if not self._filters:
            return f"{self.__class__.__name__}(no filters)"

        filter_strs = []
        for key, val in self._filters.items():
            if isinstance(val, list):
                # Show first 3 items if list is long
                if len(val) > 3:
                    filter_strs.append(f"{key}={val[:3]}...")
                else:
                    filter_strs.append(f"{key}={val}")
            else:
                filter_strs.append(f"{key}={val}")

        return f"{self.__class__.__name__}({', '.join(filter_strs)})"

    def __copy__(self):
        """Create a shallow copy with copied filters dict."""
        return self.__class__(
            filters=self._filters.copy(),
            cache=self._cache,
            metadata=self._metadata
        )