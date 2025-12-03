"""
Query builder for FAF regional zone-level flows.

Supports cross-level queries (e.g., origin states + destination zones).
"""

import pandas as pd
import geopandas as gpd
from .base import BaseQuery
from .validation import resolve_geography, resolve_commodities, resolve_modes, validate_years


class FAFQuery(BaseQuery):
    """
    Query builder for FAF regional zone-level flows.

    Provides chainable methods to filter by origin, destination,
    commodity, mode, year, etc. Supports cross-level queries where
    origins and destinations can be at different geographic levels
    (states vs zones).

    Examples
    --------
    >>> import tidyfaf as faf
    >>> query = (faf.FAFQuery()
    ...     .origin_states(['California', 'Texas'])
    ...     .destination_zones([111])
    ...     .commodities(['Electronics'])
    ...     .years([2020, 2025])
    ... )
    >>> df = query.get()
    """

    _data_type = 'regional'

    def __init__(self, filters=None, cache=None, metadata=None):
        super().__init__(filters, cache, metadata)

    def _load_data(self, columns=None, filters=None):
        """Load regional flows parquet with optional projection and pushdown."""
        return self._cache.get_filtered_data('regional', columns=columns, filters=filters)

    def _get_needed_columns(self):
        """Identify columns needed based on years filter."""
        # Standard metadata columns
        meta_cols = ['dms_orig', 'dms_dest', 'sctg2', 'dms_mode',
                    'trade_type', 'dist_band', 'fr_orig', 'fr_dest',
                    'fr_inmode', 'fr_outmode']
        meta_cols = ['dms_orig', 'dms_dest', 'sctg2', 'dms_mode',
                    'trade_type', 'dist_band', 'fr_orig', 'fr_dest',
                    'fr_inmode', 'fr_outmode']
        
        # Add year-specific metrics
        years = self._filters.get('years', None)
        if years is None:
            # If no year filter, we can't easily know which year columns to pick 
            # without reading metadata. But usually we want all if unspecified.
            # Return None to load all columns.
            return None

        year_cols = []
        for year in years:
            for metric in ['tons', 'value', 'tmiles']:
                year_cols.append(f'{metric}_{year}')
        
        return meta_cols + year_cols

    def _get_pushdown_filters(self):
        """Convert high-level filters to PyArrow DNF filters."""
        filters = []
        
        # Helper to append simple IN filter
        def add_in_filter(col, values):
            if values:
                # PyArrow filter format: [(col, 'in', values), ...]
                # Note: read_parquet filters argument expects a "DNF" list of lists.
                # [[(col, op, val)], [(col, op, val)]] means OR
                # [[(col, op, val), (col, op, val)]] means AND
                # We want AND between different fields.
                # So we build a single list of tuples for AND?
                # Wait, fastparquet/pyarrow 'filters' arg:
                # List of tuples: [('col', '==', val), ...] -> AND
                # List of lists of tuples: [[('col', '==', val)], ...] -> OR (Wait, check docs)
                # PyArrow dataset: DNF. List of List of Tuples. Outer list is OR. Inner list is AND.
                # Pandas read_parquet "filters": "List of tuples or list of lists of tuples"
                # "List of tuples" is AND-ed.
                filters.append((col, 'in', values))

        # Origin Zones (Direct)
        if 'origin_zones' in self._filters:
            add_in_filter('dms_orig', self._filters['origin_zones'])

        # Origin States (Convert to Zones)
        elif 'origin_states' in self._filters:
            # We need to find all zones for these states
            state_codes = self._filters['origin_states']
            # Filter metadata zones
            zones_df = self._metadata.zones
            
            valid_zones = []
            for state in state_codes:
                # Column is 'Numeric Label' in metadata sheet
                state_zones = zones_df[zones_df['Numeric Label'] // 10 == state]['Numeric Label'].tolist()
                valid_zones.extend(state_zones)
            
            if valid_zones:
                add_in_filter('dms_orig', valid_zones)

        # Destination Zones (Direct)
        if 'destination_zones' in self._filters:
            add_in_filter('dms_dest', self._filters['destination_zones'])
            
        # Destination States (Convert to Zones)
        elif 'destination_states' in self._filters:
            state_codes = self._filters['destination_states']
            zones_df = self._metadata.zones
            valid_zones = []
            for state in state_codes:
                state_zones = zones_df[zones_df['Numeric Label'] // 10 == state]['Numeric Label'].tolist()
                valid_zones.extend(state_zones)
            
            if valid_zones:
                add_in_filter('dms_dest', valid_zones)

        # Commodities
        if 'commodities' in self._filters:
            add_in_filter('sctg2', self._filters['commodities'])

        # Modes
        if 'modes' in self._filters:
            add_in_filter('dms_mode', self._filters['modes'])

        # Trade Types
        if 'trade_types' in self._filters:
            add_in_filter('trade_type', self._filters['trade_types'])

        # Return single inner list for AND behavior across all conditions
        return [filters] if filters else None

    # === Filter Methods ===

    def origin_states(self, states):
        """
        Filter by origin states.

        Parameters
        ----------
        states : list
            State names or FIPS codes
            e.g., ['California', 'Texas'] or [6, 48]

        Returns
        -------
        FAFQuery
            New query instance with filter applied

        Examples
        --------
        >>> query.origin_states(['California', 'Texas'])
        """
        new_filters = self._filters.copy()
        codes, _ = resolve_geography(states, self._metadata, level='state')
        new_filters['origin_states'] = codes
        return FAFQuery(new_filters, self._cache, self._metadata)

    def destination_states(self, states):
        """
        Filter by destination states.

        Parameters
        ----------
        states : list
            State names or FIPS codes

        Returns
        -------
        FAFQuery
            New query instance with filter applied
        """
        new_filters = self._filters.copy()
        codes, _ = resolve_geography(states, self._metadata, level='state')
        new_filters['destination_states'] = codes
        return FAFQuery(new_filters, self._cache, self._metadata)

    def origin_zones(self, zones):
        """
        Filter by origin FAF zones.

        Parameters
        ----------
        zones : list
            Zone names or codes
            e.g., ['Zone 111', 'Birmingham AL'] or [111, 11]

        Returns
        -------
        FAFQuery
            New query instance with filter applied
        """
        new_filters = self._filters.copy()
        codes, _ = resolve_geography(zones, self._metadata, level='zone')
        new_filters['origin_zones'] = codes
        return FAFQuery(new_filters, self._cache, self._metadata)

    def destination_zones(self, zones):
        """
        Filter by destination FAF zones.

        Parameters
        ----------
        zones : list
            Zone names or codes

        Returns
        -------
        FAFQuery
            New query instance with filter applied
        """
        new_filters = self._filters.copy()
        codes, _ = resolve_geography(zones, self._metadata, level='zone')
        new_filters['destination_zones'] = codes
        return FAFQuery(new_filters, self._cache, self._metadata)

    def commodities(self, commodities):
        """
        Filter by commodities.

        Parameters
        ----------
        commodities : list
            Commodity names or SCTG2 codes
            e.g., ['Electronics', 'Pharmaceuticals'] or [34, 38]

        Returns
        -------
        FAFQuery
            New query instance with filter applied
        """
        new_filters = self._filters.copy()
        codes = resolve_commodities(commodities, self._metadata)
        new_filters['commodities'] = codes
        return FAFQuery(new_filters, self._cache, self._metadata)

    def modes(self, modes):
        """
        Filter by modes.

        Parameters
        ----------
        modes : list
            Mode names or codes
            e.g., ['Truck', 'Rail'] or [1, 2]

        Returns
        -------
        FAFQuery
            New query instance with filter applied
        """
        new_filters = self._filters.copy()
        codes = resolve_modes(modes, self._metadata)
        new_filters['modes'] = codes
        return FAFQuery(new_filters, self._cache, self._metadata)

    def years(self, years):
        """
        Select specific years.

        Parameters
        ----------
        years : list of int
            Years to include (e.g., [2020, 2025, 2030])
            Valid years: 2017-2024 (actual), 2030-2050 in 5-year intervals (forecast)

        Returns
        -------
        FAFQuery
            New query instance with filter applied
        """
        new_filters = self._filters.copy()
        validated_years = validate_years(years)
        new_filters['years'] = validated_years
        return FAFQuery(new_filters, self._cache, self._metadata)

    def year_range(self, start, end):
        """
        Select year range (inclusive).

        Handles gap between 2024 and 2030 automatically.

        Parameters
        ----------
        start : int
            Start year
        end : int
            End year (inclusive)

        Returns
        -------
        FAFQuery
            New query instance with filter applied

        Examples
        --------
        >>> query.year_range(2020, 2030)
        # Returns years: [2020, 2021, 2022, 2023, 2024, 2030]
        """
        # Actual years: 2017-2024
        actual_years = [y for y in range(start, min(end + 1, 2025))]

        # Forecast years: 2030, 2035, 2040, 2045, 2050 (5-year intervals)
        forecast_years = [y for y in range(max(start, 2030), end + 1, 5)
                         if y <= 2050]

        return self.years(actual_years + forecast_years)

    def trade_types(self, trade_types):
        """
        Filter by trade type.

        Parameters
        ----------
        trade_types : list
            Trade types: 'Domestic', 'Import', 'Export' or [1, 2, 3]

        Returns
        -------
        FAFQuery
            New query instance with filter applied
        """
        new_filters = self._filters.copy()

        # Map names to codes
        trade_type_map = {
            'Domestic': 1,
            'Import': 2,
            'Export': 3
        }

        codes = []
        for tt in trade_types:
            if isinstance(tt, int):
                codes.append(tt)
            else:
                codes.append(trade_type_map.get(tt, tt))

        new_filters['trade_types'] = codes
        return FAFQuery(new_filters, self._cache, self._metadata)

    def min_tons(self, value, year=2020):
        """
        Filter flows above tonnage threshold.

        Parameters
        ----------
        value : float
            Minimum tonnage (thousands of tons)
        year : int, default 2020
            Year to apply threshold

        Returns
        -------
        FAFQuery
            New query instance with filter applied
        """
        new_filters = self._filters.copy()
        new_filters['min_tons'] = (value, year)
        return FAFQuery(new_filters, self._cache, self._metadata)

    def min_value(self, value, year=2020):
        """
        Filter flows above value threshold.

        Parameters
        ----------
        value : float
            Minimum value (millions of dollars)
        year : int, default 2020
            Year to apply threshold

        Returns
        -------
        FAFQuery
            New query instance with filter applied
        """
        new_filters = self._filters.copy()
        new_filters['min_value'] = (value, year)
        return FAFQuery(new_filters, self._cache, self._metadata)

    # === Filter Application ===

    def _apply_filters(self, df):
        """Apply all filters to DataFrame."""

        # Origin filtering (handle state vs zone)
        if 'origin_states' in self._filters:
            # Filter by state: dms_orig // 10 == state_fips
            # FAF zone codes are structured as: state_fips * 10 + zone_within_state
            state_codes = self._filters['origin_states']
            df = df[(df['dms_orig'] // 10).isin(state_codes)]

        if 'origin_zones' in self._filters:
            df = df[df['dms_orig'].isin(self._filters['origin_zones'])]

        # Destination filtering
        if 'destination_states' in self._filters:
            state_codes = self._filters['destination_states']
            df = df[(df['dms_dest'] // 10).isin(state_codes)]

        if 'destination_zones' in self._filters:
            df = df[df['dms_dest'].isin(self._filters['destination_zones'])]

        # Commodity filtering
        if 'commodities' in self._filters:
            df = df[df['sctg2'].isin(self._filters['commodities'])]

        # Mode filtering
        if 'modes' in self._filters:
            df = df[df['dms_mode'].isin(self._filters['modes'])]

        # Trade type filtering
        if 'trade_types' in self._filters:
            df = df[df['trade_type'].isin(self._filters['trade_types'])]

        # Threshold filtering
        if 'min_tons' in self._filters:
            value, year = self._filters['min_tons']
            col = f'tons_{year}'
            if col in df.columns:
                df = df[df[col] >= value]

        if 'min_value' in self._filters:
            value, year = self._filters['min_value']
            col = f'value_{year}'
            if col in df.columns:
                df = df[df[col] >= value]

        return df

    # === Analysis Methods ===

    def group_by(self, fields, metrics=None, years=None):
        """
        Group by specified fields and aggregate metrics.

        Parameters
        ----------
        fields : list of str or str
            Fields to group by (e.g., ['dms_orig', 'sctg2'] or 'dms_orig')
        metrics : list of str, default ['tons', 'value']
            Metrics to aggregate
        years : list of int, optional
            Years to aggregate. If None, uses years from filter.

        Returns
        -------
        pd.DataFrame
            Aggregated data

        Examples
        --------
        >>> query.group_by(['dms_orig', 'sctg2'], metrics=['tons'], years=[2020])
        """
        if isinstance(fields, str):
            fields = [fields]

        if metrics is None:
            metrics = ['tons', 'value']

        df = self.get()

        # Determine years
        if years is None:
            years = self._filters.get('years', [2020])

        # Build aggregation dict
        agg_dict = {}
        for metric in metrics:
            for year in years:
                col = f'{metric}_{year}'
                if col in df.columns:
                    agg_dict[col] = 'sum'

        if not agg_dict:
            return pd.DataFrame()  # No columns to aggregate

        return df.groupby(fields).agg(agg_dict).reset_index()

    def by_origin(self, metrics=None, years=None):
        """
        Group by origin zone.

        Parameters
        ----------
        metrics : list of str, default ['tons', 'value']
            Metrics to aggregate
        years : list of int, optional
            Years to aggregate

        Returns
        -------
        pd.DataFrame
            Aggregated by origin
        """
        return self.group_by('dms_orig', metrics, years)

    def by_destination(self, metrics=None, years=None):
        """
        Group by destination zone.

        Returns
        -------
        pd.DataFrame
            Aggregated by destination
        """
        return self.group_by('dms_dest', metrics, years)

    def by_commodity(self, metrics=None, years=None):
        """
        Group by commodity.

        Returns
        -------
        pd.DataFrame
            Aggregated by commodity
        """
        return self.group_by('sctg2', metrics, years)

    def by_mode(self, metrics=None, years=None):
        """
        Group by mode.

        Returns
        -------
        pd.DataFrame
            Aggregated by mode
        """
        return self.group_by('dms_mode', metrics, years)

    def summarize(self, metric='tons', year=2020):
        """
        Quick summary statistics.

        Parameters
        ----------
        metric : str, default 'tons'
            Metric to summarize
        year : int, default 2020
            Year to summarize

        Returns
        -------
        dict
            Summary statistics
        """
        df = self.get()
        col = f'{metric}_{year}'

        if col not in df.columns:
            return {'error': f'Column {col} not found'}

        return {
            'total': df[col].sum(),
            'mean': df[col].mean(),
            'median': df[col].median(),
            'min': df[col].min(),
            'max': df[col].max(),
            'flows': len(df)
        }

    def top(self, n=10, by='tons', year=2020):
        """
        Top N flows by metric.

        Parameters
        ----------
        n : int, default 10
            Number of flows to return
        by : str, default 'tons'
            Metric to rank by
        year : int, default 2020
            Year to use

        Returns
        -------
        pd.DataFrame
            Top N flows
        """
        df = self.get()
        col = f'{by}_{year}'

        if col not in df.columns:
            return pd.DataFrame()

        return df.nlargest(n, col)

    def compare_years(self, years=None):
        """
        Year-over-year comparison.

        Parameters
        ----------
        years : list of int, optional
            Years to compare. If None, uses years from filter.

        Returns
        -------
        pd.DataFrame
            Data with selected years
        """
        if years is not None:
            return self.years(years).get()
        else:
            return self.get()

    # === Discovery Methods ===

    def available_commodities(self, search=None):
        """
        List/search available commodities.

        Parameters
        ----------
        search : str, optional
            Search string (case-insensitive)

        Returns
        -------
        pd.DataFrame
            Commodity codes and descriptions
        """
        df = self._metadata.commodities.copy()
        if search:
            df = df[df.astype(str).apply(
                lambda x: x.str.contains(search, case=False, na=False)
            ).any(axis=1)]
        return df

    def available_zones(self, search=None):
        """
        List/search available FAF zones.

        Parameters
        ----------
        search : str, optional
            Search string (case-insensitive)

        Returns
        -------
        pd.DataFrame
            Zone codes and descriptions
        """
        df = self._metadata.zones.copy()
        if search:
            df = df[df.astype(str).apply(
                lambda x: x.str.contains(search, case=False, na=False)
            ).any(axis=1)]
        return df

    def available_states(self, search=None):
        """
        List/search available states.

        Parameters
        ----------
        search : str, optional
            Search string (case-insensitive)

        Returns
        -------
        pd.DataFrame
            State codes and names
        """
        df = self._metadata.states.copy()
        if search:
            df = df[df.astype(str).apply(
                lambda x: x.str.contains(search, case=False, na=False)
            ).any(axis=1)]
        return df

    def available_modes(self):
        """
        List available modes.

        Returns
        -------
        pd.DataFrame
            Mode codes and descriptions
        """
        return self._metadata.modes.copy()

    def available_years(self):
        """
        List available years in dataset.

        Returns
        -------
        dict
            Dictionary with 'actual' and 'forecast' year lists
        """
        return {
            'actual': list(range(2017, 2025)),
            'forecast': list(range(2030, 2051, 5))
        }

    # === Geometry Methods ===

    def to_gdf(self):
        """
        Convert to GeoDataFrame with flow LineStrings.

        Creates LineString geometries connecting origin and destination
        zone centroids.

        Returns
        -------
        gpd.GeoDataFrame
            Flows with LineString geometry in EPSG:4326
        """
        from shapely.geometry import LineString

        df = self.get()

        if len(df) == 0:
            return gpd.GeoDataFrame(df, geometry=[], crs="EPSG:4326")

        # Load zone geometries
        zones_gdf = self._cache.get_raw_data('zones')

        # Calculate centroids
        centroids = zones_gdf.geometry.centroid
        zone_map = pd.DataFrame({
            'FAFZONE': zones_gdf['FAFZONE'],
            'lat': centroids.y,
            'lon': centroids.x
        }).set_index('FAFZONE')

        # Join origins
        plot_df = df.join(zone_map, on='dms_orig', rsuffix='_orig')
        plot_df.rename(columns={'lat': 'orig_lat', 'lon': 'orig_lon'}, inplace=True)

        # Join destinations
        plot_df = plot_df.join(zone_map, on='dms_dest', rsuffix='_dest')
        plot_df.rename(columns={'lat': 'dest_lat', 'lon': 'dest_lon'}, inplace=True)

        # Drop rows with missing coordinates
        len_before = len(plot_df)
        plot_df = plot_df.dropna(subset=['orig_lat', 'orig_lon', 'dest_lat', 'dest_lon'])
        len_after = len(plot_df)
        
        if len_after < len_before:
            import warnings
            warnings.warn(
                f"Dropped {len_before - len_after} flows due to missing zone coordinates. "
                "This may indicate mismatch between flow data and zone shapefiles."
            )

        if len(plot_df) == 0:
            print("Warning: No valid geometries created (missing zone coordinates)")
            return gpd.GeoDataFrame(plot_df, geometry=[], crs="EPSG:4326")

        # Create LineStrings
        plot_df['geometry'] = plot_df.apply(
            lambda row: LineString([
                (row['orig_lon'], row['orig_lat']),
                (row['dest_lon'], row['dest_lat'])
            ]),
            axis=1
        )

        return gpd.GeoDataFrame(plot_df, geometry='geometry', crs="EPSG:4326")
