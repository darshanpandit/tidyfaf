"""
Query builder for FAF HiLo forecast scenarios.

Extends FAFQuery to handle base/high/low scenario analysis.
"""

import pandas as pd
from .faf_query import FAFQuery


class ForecastQuery(FAFQuery):
    """
    Query builder for FAF HiLo forecast scenarios.

    Extends FAFQuery with scenario analysis capabilities.
    Automatically handles base/high/low forecast columns.

    Examples
    --------
    >>> import tidyfaf as faf
    >>> forecast = (faf.ForecastQuery()
    ...     .origin_states(['California'])
    ...     .destination_states(['Texas'])
    ...     .commodities(['Electronics'])
    ...     .years([2030, 2040, 2050])
    ...     .scenarios(['base', 'high', 'low'])
    ... )
    >>> df = forecast.get()
    """

    _data_type = 'hilo'
    
    def _load_data(self, columns=None, filters=None):
        """Load forecast parquet with optional projection/pushdown."""
        return self._cache.get_filtered_data('hilo', columns=columns, filters=filters)
        
    def _get_needed_columns(self):
        """Identify columns including scenario-specific ones."""
        # Get base columns from FAFQuery logic (handles meta + base years)
        base_cols = super()._get_needed_columns()
        
        if base_cols is None:
            return None
            
        # Add scenario-specific columns
        scenarios = self._filters.get('scenarios', ['base', 'high', 'low'])
        years = self._filters.get('years', [])
        
        # Filter to forecast years
        years = [y for y in years if y >= 2030]
        
        extra_cols = []
        for year in years:
            for scenario in scenarios:
                if scenario == 'base':
                    continue # Already added by super()
                
                # Only tons and value for High/Low
                for metric in ['tons', 'value']:
                    col = f'{metric}_{year}_{scenario}'
                    extra_cols.append(col)
                    
        return base_cols + extra_cols

    def scenarios(self, scenarios):
        """
        Select forecast scenarios to include.

        Parameters
        ----------
        scenarios : list
            Scenarios to include: 'base', 'high', 'low'
            e.g., ['base', 'high'] or ['base', 'high', 'low']

        Returns
        -------
        ForecastQuery
            New query instance with filter applied

        Examples
        --------
        >>> query.scenarios(['base', 'high', 'low'])
        """
        new_filters = self._filters.copy()

        # Validate scenarios
        valid_scenarios = ['base', 'high', 'low']
        for scenario in scenarios:
            if scenario not in valid_scenarios:
                raise ValueError(
                    f"Invalid scenario '{scenario}'. "
                    f"Valid scenarios: {valid_scenarios}"
                )

        new_filters['scenarios'] = scenarios
        return ForecastQuery(new_filters, self._cache, self._metadata)

    def get(self, format='long'):
        """
        Execute query and return DataFrame.

        For forecast data, default format is 'long' with scenario dimension.

        Parameters
        ----------
        format : {'wide', 'long'}, default 'long'
            Output format. 'long' returns tidy format with scenario column.

        Returns
        -------
        pd.DataFrame
            Query results with scenario dimension
        """
        # First get data in wide format
        df = super().get(format='wide')

        # If scenarios filter is specified and format is long, reshape
        if 'scenarios' in self._filters and format == 'long':
            df = self._reshape_scenarios(df)

        return df

    def _reshape_scenarios(self, df):
        """
        Reshape wide format with base/high/low columns to long format.
        """
        print(f"DEBUG: _reshape_scenarios input df shape: {df.shape}")
        print(f"DEBUG: _reshape_scenarios input df columns: {df.columns.tolist()}")

        # Identify metadata columns
        meta_cols = ['dms_orig', 'dms_dest', 'sctg2', 'dms_mode',
                    'trade_type', 'dist_band', 'fr_orig', 'fr_dest',
                    'fr_inmode', 'fr_outmode']
        meta_cols = [c for c in meta_cols if c in df.columns]

        # Get forecast years from filter
        years = self._filters.get('years', [2030, 2035, 2040, 2045, 2050])
        # Filter to forecast years only (>= 2030)
        years = [y for y in years if y >= 2030]

        scenarios_requested = self._filters.get('scenarios', ['base', 'high', 'low'])

        dfs = []

        for scenario in scenarios_requested:
            scenario_df = df[meta_cols].copy()

            for year in years:
                # Base scenario uses tons_YEAR, value_YEAR
                # High/Low scenarios use tons_YEAR_high, tons_YEAR_low, etc.
                if scenario == 'base':
                    tons_col = f'tons_{year}'
                    value_col = f'value_{year}'
                else:
                    tons_col = f'tons_{year}_{scenario}'
                    value_col = f'value_{year}_{scenario}'

                # Add columns if they exist
                if tons_col in df.columns:
                    scenario_df[f'tons_{year}'] = df[tons_col]
                if value_col in df.columns:
                    scenario_df[f'value_{year}'] = df[value_col]

            scenario_df['scenario'] = scenario
            dfs.append(scenario_df)

        # Concatenate all scenarios
        result = pd.concat(dfs, ignore_index=True)
        print(f"DEBUG: _reshape_scenarios concat result shape: {result.shape}")

        # Now convert to long format with year dimension
        metric_cols = [col for col in result.columns
                      if col.startswith('tons_') or col.startswith('value_')]
        
        print(f"DEBUG: _reshape_scenarios metric_cols: {metric_cols}")
        id_cols = meta_cols + ['scenario']

        result_long = result.melt(
            id_vars=id_cols,
            value_vars=metric_cols,
            var_name='metric_year',
            value_name='value'
        )

        # Split metric_year into metric and year
        result_long['metric'] = result_long['metric_year'].str.split('_').str[0]
        result_long['year'] = result_long['metric_year'].str.split('_').str[1].astype(int)
        result_long = result_long.drop(columns=['metric_year'])

        # Fill NaNs in index columns to prevent pivot_table from dropping rows
        result_long[id_cols] = result_long[id_cols].fillna(-1)

        # Pivot so each metric is a column
        result_long = result_long.pivot_table(
            index=id_cols + ['year'],
            columns='metric',
            values='value'
        ).reset_index()
        
        # Restore NaNs (optional, but cleaner)
        # Note: -1 is safe for FAF codes which are positive integers
        import numpy as np
        result_long[id_cols] = result_long[id_cols].replace(-1, np.nan)

        return result_long

    def compare_scenarios(self, year=2030):
        """
        Compare base/high/low scenarios for a specific year.

        Parameters
        ----------
        year : int, default 2030
            Year to compare

        Returns
        -------
        pd.DataFrame
            Data with scenario comparisons
        """
        # Get data with all scenarios
        query_with_scenarios = self.scenarios(['base', 'high', 'low']).years([year])
        return query_with_scenarios.get(format='long')

    # Need to override filter methods to return ForecastQuery instances

    def origin_states(self, states):
        """Filter by origin states."""
        from .validation import resolve_geography
        new_filters = self._filters.copy()
        codes, _ = resolve_geography(states, self._metadata, level='state')
        new_filters['origin_states'] = codes
        return ForecastQuery(new_filters, self._cache, self._metadata)

    def destination_states(self, states):
        """Filter by destination states."""
        from .validation import resolve_geography
        new_filters = self._filters.copy()
        codes, _ = resolve_geography(states, self._metadata, level='state')
        new_filters['destination_states'] = codes
        return ForecastQuery(new_filters, self._cache, self._metadata)

    def origin_zones(self, zones):
        """Filter by origin zones."""
        from .validation import resolve_geography
        new_filters = self._filters.copy()
        codes, _ = resolve_geography(zones, self._metadata, level='zone')
        new_filters['origin_zones'] = codes
        return ForecastQuery(new_filters, self._cache, self._metadata)

    def destination_zones(self, zones):
        """Filter by destination zones."""
        from .validation import resolve_geography
        new_filters = self._filters.copy()
        codes, _ = resolve_geography(zones, self._metadata, level='zone')
        new_filters['destination_zones'] = codes
        return ForecastQuery(new_filters, self._cache, self._metadata)

    def commodities(self, commodities):
        """Filter by commodities."""
        from .validation import resolve_commodities
        new_filters = self._filters.copy()
        codes = resolve_commodities(commodities, self._metadata)
        new_filters['commodities'] = codes
        return ForecastQuery(new_filters, self._cache, self._metadata)

    def modes(self, modes):
        """Filter by modes."""
        from .validation import resolve_modes
        new_filters = self._filters.copy()
        codes = resolve_modes(modes, self._metadata)
        new_filters['modes'] = codes
        return ForecastQuery(new_filters, self._cache, self._metadata)

    def years(self, years):
        """Select specific years."""
        from .validation import validate_years
        new_filters = self._filters.copy()
        validated_years = validate_years(years)
        new_filters['years'] = validated_years
        return ForecastQuery(new_filters, self._cache, self._metadata)

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
        return ForecastQuery(new_filters, self._cache, self._metadata)

    def min_tons(self, value, year=2030):
        """Filter flows above tonnage threshold (default year 2030 for forecasts)."""
        new_filters = self._filters.copy()
        new_filters['min_tons'] = (value, year)
        return ForecastQuery(new_filters, self._cache, self._metadata)

    def min_value(self, value, year=2030):
        """Filter flows above value threshold (default year 2030 for forecasts)."""
        new_filters = self._filters.copy()
        new_filters['min_value'] = (value, year)
        return ForecastQuery(new_filters, self._cache, self._metadata)