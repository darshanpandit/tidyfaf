"""
Query builder for FAF experimental county-level flows.

Implements county disaggregation logic by joining FAF zone flows
with county-level factors.
"""

import pandas as pd
from pathlib import Path
from .faf_query import FAFQuery

class CountyQuery(FAFQuery):
    """
    Query builder for FAF county-level flows (Experimental).

    Uses disaggregation factors to estimate county-to-county flows
    from FAF zone-level data.

    Supported Modes: Truck, Rail, Water, Pipeline.
    Other modes are currently excluded from county disaggregation.

    Examples
    --------
    >>> import tidyfaf as faf
    >>> query = (faf.CountyQuery()
    ...     .origin_counties(['06037'])  # Los Angeles County
    ...     .destination_states(['NV'])
    ...     .commodities(['Electronics'])
    ...     .years([2020])
    ... )
    >>> df = query.get()
    """

    _data_type = 'county'

    def origin_counties(self, counties):
        """
        Filter by origin county FIPS codes.

        Parameters
        ----------
        counties : list
            List of 5-digit county FIPS codes (strings) or integers.
            e.g. ['06037', 48201]

        Returns
        -------
        CountyQuery
            New query instance
        """
        new_filters = self._filters.copy()
        # Ensure integers for matching
        new_filters['origin_counties'] = [int(c) for c in counties]
        return CountyQuery(new_filters, self._cache, self._metadata)

    def destination_counties(self, counties):
        """
        Filter by destination county FIPS codes.

        Parameters
        ----------
        counties : list
            List of 5-digit county FIPS codes (strings) or integers.

        Returns
        -------
        CountyQuery
            New query instance
        """
        new_filters = self._filters.copy()
        new_filters['destination_counties'] = [int(c) for c in counties]
        return CountyQuery(new_filters, self._cache, self._metadata)

    def _get_sctg_group(self, sctg_code):
        """Map SCTG2 code to SCTG Group 5 string."""
        if sctg_code <= 9: return 'sctg0109'
        if sctg_code <= 14: return 'sctg1014'
        if sctg_code <= 19: return 'sctg1519'
        if sctg_code <= 33: return 'sctg2033'
        return 'sctg3499'

    def _load_data(self, columns=None, filters=None):
        """
        Load and disaggregate data to county level.
        """
        # 1. Load base regional data (using parent logic)
        # We can't push down county filters to regional parquet easily because counties aren't in it.
        # But we can infer zones from counties if needed. For now, rely on filters passed.
        
        # Get regional data
        df = self._cache.get_filtered_data('regional', columns=None, filters=None)
        
        # Apply standard filters (commodities, years, etc.)
        df = self._apply_filters(df)
        
        if df.empty:
            return pd.DataFrame()

        # Add SCTG Group column for joining
        df['sctgG5'] = df['sctg2'].apply(self._get_sctg_group)

        # 2. Prepare for Disaggregation
        mode_map = {
            1: 'truck',
            2: 'rail',
            3: 'water',
            6: 'pipeline'
        }
        
        results = []
        factors_dir = self._cache.data_dir / "county_factors"
        
        if not factors_dir.exists():
             raise FileNotFoundError(
                 "County factors not found. Please run tidyfaf.setup_county_data(zip_path) first."
             )

        # 3. Process each mode
        for mode_code, mode_name in mode_map.items():
            # Filter data for this mode
            mode_df = df[df['dms_mode'] == mode_code].copy()
            if mode_df.empty:
                continue
                
            # Load Factors
            try:
                orig_factors = pd.read_parquet(factors_dir / f"{mode_name}_origin_factors.parquet")
                dest_factors = pd.read_parquet(factors_dir / f"{mode_name}_destination_factors.parquet")
            except FileNotFoundError:
                print(f"Warning: Factors for {mode_name} not found. Skipping.")
                continue

            # Filter factors if county filters exist (Optimization)
            if 'origin_counties' in self._filters:
                orig_factors = orig_factors[orig_factors['dms_orig_cnty'].isin(self._filters['origin_counties'])]
            
            if 'destination_counties' in self._filters:
                dest_factors = dest_factors[dest_factors['dms_dest_cnty'].isin(self._filters['destination_counties'])]

            # Join Origin Factors
            # mode_df (dms_orig, sctgG5) <-> orig_factors (dms_orig, sctgG5)
            merged_orig = mode_df.merge(
                orig_factors,
                on=['dms_orig', 'sctgG5'],
                how='inner'
            )
            
            if merged_orig.empty:
                continue

            # Join Destination Factors
            # merged_orig (dms_dest, sctgG5) <-> dest_factors (dms_dest, sctgG5)
            final_df = merged_orig.merge(
                dest_factors,
                on=['dms_dest', 'sctgG5'],
                how='inner',
                suffixes=('_orig_fact', '_dest_fact') # Handle overlapping columns if any
            )
            
            if final_df.empty:
                continue

            # Calculate Disaggregated Values
            # Multiply all 'tons_' and 'value_' columns by factors
            factor = final_df['f_orig'] * final_df['f_dest']
            
            cols_to_scale = [c for c in final_df.columns if c.startswith('tons_') or c.startswith('value_') or c.startswith('tmiles_')]
            
            for col in cols_to_scale:
                final_df[col] = final_df[col] * factor
                
            # Rename county columns to standard origin/dest
            # Keep track of old zone cols if needed, but usually we replace them
            # Rename dms_orig_cnty -> dms_orig, dms_dest_cnty -> dms_dest ?
            # Or keep them distinct. Let's use distinct names for clarity.
            
            # But BaseQuery methods expect 'dms_orig' for grouping. 
            # Ideally we should swap them or update metadata cols.
            # Let's rename for compatibility with existing group_by methods.
            # BUT 'dms_orig' is usually Zone ID. County FIPS is different.
            # Let's keep 'dms_orig' as Zone and add 'dms_orig_county'.
            
            results.append(final_df)

        if not results:
            return pd.DataFrame()

        final_result = pd.concat(results, ignore_index=True)
        
        # Apply final county filtering (redundant if optimized above, but safe)
        if 'origin_counties' in self._filters:
            final_result = final_result[final_result['dms_orig_cnty'].isin(self._filters['origin_counties'])]
        if 'destination_counties' in self._filters:
            final_result = final_result[final_result['dms_dest_cnty'].isin(self._filters['destination_counties'])]

        return final_result

    # Need to override filter methods to return CountyQuery instances
    # ... (boilerplate overrides similar to StateQuery)
    def origin_states(self, states):
        from .validation import resolve_geography
        new_filters = self._filters.copy()
        codes, _ = resolve_geography(states, self._metadata, level='state')
        new_filters['origin_states'] = codes
        return CountyQuery(new_filters, self._cache, self._metadata)

    def destination_states(self, states):
        from .validation import resolve_geography
        new_filters = self._filters.copy()
        codes, _ = resolve_geography(states, self._metadata, level='state')
        new_filters['destination_states'] = codes
        return CountyQuery(new_filters, self._cache, self._metadata)

    def commodities(self, commodities):
        from .validation import resolve_commodities
        new_filters = self._filters.copy()
        codes = resolve_commodities(commodities, self._metadata)
        new_filters['commodities'] = codes
        return CountyQuery(new_filters, self._cache, self._metadata)

    def modes(self, modes):
        from .validation import resolve_modes
        new_filters = self._filters.copy()
        codes = resolve_modes(modes, self._metadata)
        new_filters['modes'] = codes
        return CountyQuery(new_filters, self._cache, self._metadata)

    def years(self, years):
        from .validation import validate_years
        new_filters = self._filters.copy()
        validated_years = validate_years(years)
        new_filters['years'] = validated_years
        return CountyQuery(new_filters, self._cache, self._metadata)
    
    # Add by_county aggregation methods
    def by_origin_county(self, metrics=None, years=None):
        return self.group_by('dms_orig_cnty', metrics, years)

    def by_destination_county(self, metrics=None, years=None):
        return self.group_by('dms_dest_cnty', metrics, years)