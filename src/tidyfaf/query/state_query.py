"""
Query builder for FAF state-level flows.

Similar to FAFQuery but operates on state-aggregated data.
"""

import pandas as pd
from .faf_query import FAFQuery


class StateQuery(FAFQuery):
    """
    Query builder for FAF state-level flows.

    Similar to FAFQuery but uses state-aggregated data (dms_origst/dms_destst).
    Does not support zone-level filtering.

    Examples
    --------
    >>> import tidyfaf as faf
    >>> query = (faf.StateQuery()
    ...     .origin_states(['California', 'Texas'])
    ...     .destination_states(['Washington'])
    ...     .commodities(['Electronics'])
    ...     .years([2020, 2030])
    ... )
    >>> df = query.get()
    """

    _data_type = 'state'

    def __init__(self, filters=None, cache=None, metadata=None):
        super().__init__(filters, cache, metadata)

    def _load_data(self, columns=None, filters=None):
        """Load state flows parquet."""
        return self._cache.get_filtered_data('state', columns=columns, filters=filters)

    def _get_needed_columns(self):
        """Identify columns needed for state-level data."""
        # Standard metadata columns for state data
        meta_cols = ['dms_origst', 'dms_destst', 'sctg2', 'dms_mode',
                    'trade_type', 'dist_band', 'fr_orig', 'fr_dest',
                    'fr_inmode', 'fr_outmode']
        
        # Add year-specific metrics
        years = self._filters.get('years', None)
        if years is None:
            # If no year filter, load all columns.
            return None

        year_cols = []
        for year in years:
            for metric in ['tons', 'value', 'tmiles']:
                year_cols.append(f'{metric}_{year}')
        
        return meta_cols + year_cols

    def _get_pushdown_filters(self):
        """State-specific pushdown filters."""
        filters = []
        
        def add_in_filter(col, values):
            if values:
                filters.append((col, 'in', values))
        
        # State data uses dms_origst/dms_destst
        if 'origin_states' in self._filters:
            add_in_filter('dms_origst', self._filters['origin_states'])
            
        if 'destination_states' in self._filters:
            add_in_filter('dms_destst', self._filters['destination_states'])
            
        if 'commodities' in self._filters:
            add_in_filter('sctg2', self._filters['commodities'])
            
        if 'modes' in self._filters:
            add_in_filter('dms_mode', self._filters['modes'])
            
        if 'trade_types' in self._filters:
            add_in_filter('trade_type', self._filters['trade_types'])
            
        return [filters] if filters else None

    # Inherit most methods from FAFQuery, but override geography filters

    def origin_zones(self, zones):
        """
        Zone-level filtering not supported in StateQuery.

        Raises
        ------
        NotImplementedError
            StateQuery only supports state-level geography
        """
        raise NotImplementedError(
            "StateQuery does not support zone-level filtering. "
            "Use FAFQuery for zone-level analysis."
        )

    def destination_zones(self, zones):
        """
        Zone-level filtering not supported in StateQuery.

        Raises
        ------
        NotImplementedError
            StateQuery only supports state-level geography
        """
        raise NotImplementedError(
            "StateQuery does not support zone-level filtering. "
            "Use FAFQuery for zone-level analysis."
        )

    def _apply_filters(self, df):
        """Apply filters to state-level data."""

        # Origin filtering (use dms_origst instead of dms_orig)
        if 'origin_states' in self._filters:
            df = df[df['dms_origst'].isin(self._filters['origin_states'])]

        # Destination filtering
        if 'destination_states' in self._filters:
            df = df[df['dms_destst'].isin(self._filters['destination_states'])]

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

    # Override group_by methods to use state columns

    def by_origin(self, metrics=None, years=None):
        """
        Group by origin state.

        Returns
        -------
        pd.DataFrame
            Aggregated by origin state
        """
        return self.group_by('dms_origst', metrics, years)

    def by_destination(self, metrics=None, years=None):
        """
        Group by destination state.

        Returns
        -------
        pd.DataFrame
            Aggregated by destination state
        """
        return self.group_by('dms_destst', metrics, years)

    def to_gdf(self):
        """
        State-level data does not have zone geometries.

        Raises
        ------
        NotImplementedError
            Use FAFQuery for geometry support
        """
        raise NotImplementedError(
            "StateQuery does not support geometry conversion. "
            "Use FAFQuery with zone-level data for mapping."
        )

    # Need to override filter methods to return StateQuery instances

    def origin_states(self, states):
        """Filter by origin states."""
        from .validation import resolve_geography
        new_filters = self._filters.copy()
        codes, _ = resolve_geography(states, self._metadata, level='state')
        new_filters['origin_states'] = codes
        return StateQuery(new_filters, self._cache, self._metadata)

    def destination_states(self, states):
        """Filter by destination states."""
        from .validation import resolve_geography
        new_filters = self._filters.copy()
        codes, _ = resolve_geography(states, self._metadata, level='state')
        new_filters['destination_states'] = codes
        return StateQuery(new_filters, self._cache, self._metadata)

    def commodities(self, commodities):
        """Filter by commodities."""
        from .validation import resolve_commodities
        new_filters = self._filters.copy()
        codes = resolve_commodities(commodities, self._metadata)
        new_filters['commodities'] = codes
        return StateQuery(new_filters, self._cache, self._metadata)

    def modes(self, modes):
        """Filter by modes."""
        from .validation import resolve_modes
        new_filters = self._filters.copy()
        codes = resolve_modes(modes, self._metadata)
        new_filters['modes'] = codes
        return StateQuery(new_filters, self._cache, self._metadata)

    def years(self, years):
        """Select specific years."""
        from .validation import validate_years
        new_filters = self._filters.copy()
        validated_years = validate_years(years)
        new_filters['years'] = validated_years
        return StateQuery(new_filters, self._cache, self._metadata)

    def year_range(self, start, end):
        """Select year range."""
        actual_years = [y for y in range(start, min(end + 1, 2025))]
        forecast_years = [y for y in range(max(start, 2030), end + 1, 5)
                         if y <= 2050]
        return self.years(actual_years + forecast_years)

    def trade_types(self, trade_types):
        """Filter by trade type."""
        new_filters = self._filters.copy()
        trade_type_map = {'Domestic': 1, 'Import': 2, 'Export': 3}
        codes = []
        for tt in trade_types:
            if isinstance(tt, int):
                codes.append(tt)
            else:
                codes.append(trade_type_map.get(tt, tt))
        new_filters['trade_types'] = codes
        return StateQuery(new_filters, self._cache, self._metadata)

    def min_tons(self, value, year=2020):
        """Filter flows above tonnage threshold."""
        new_filters = self._filters.copy()
        new_filters['min_tons'] = (value, year)
        return StateQuery(new_filters, self._cache, self._metadata)

    def min_value(self, value, year=2020):
        """Filter flows above value threshold."""
        new_filters = self._filters.copy()
        new_filters['min_value'] = (value, year)
        return StateQuery(new_filters, self._cache, self._metadata)