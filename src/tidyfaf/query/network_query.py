"""
Query builder for FAF highway network.

Provides filtering and analysis of FAF5 network links.
"""

import pandas as pd
import geopandas as gpd
from .base import BaseQuery


class NetworkQuery(BaseQuery):
    """
    Query builder for FAF highway network.

    Filters network links by route, state, functional class, etc.
    Returns GeoDataFrame with LineString geometries.

    Examples
    --------
    >>> import tidyfaf as faf
    >>> network = (faf.NetworkQuery()
    ...     .routes(['I-5', 'I-95'])
    ...     .states(['CA', 'OR', 'WA'])
    ...     .freight_network(True)
    ...     .truck_allowed(True)
    ... )
    >>> gdf = network.get()
    """

    _data_type = 'network'

    def _load_data(self, columns=None, filters=None):
        """Load network parquet."""
        return self._cache.get_filtered_data('network', columns=columns, filters=filters)

    # === Filter Methods ===

    def routes(self, routes):
        """
        Filter by route names/numbers.

        Parameters
        ----------
        routes : list
            Route names or numbers (e.g., ['I-5', 'US-101', 'I-95'])

        Returns
        -------
        NetworkQuery
            New query instance with filter applied
        """
        new_filters = self._filters.copy()
        new_filters['routes'] = routes
        return NetworkQuery(new_filters, self._cache, self._metadata)

    def states(self, states):
        """
        Filter by states.

        Parameters
        ----------
        states : list
            State abbreviations or FIPS codes (e.g., ['CA', 'OR', 'WA'])

        Returns
        -------
        NetworkQuery
            New query instance with filter applied
        """
        new_filters = self._filters.copy()
        # Normalize to 2-letter abbreviations
        new_filters['states'] = [s.upper() if isinstance(s, str) else s for s in states]
        return NetworkQuery(new_filters, self._cache, self._metadata)

    def zones(self, zones):
        """
        Filter by FAF zones.

        Parameters
        ----------
        zones : list
            FAF zone codes

        Returns
        -------
        NetworkQuery
            New query instance with filter applied
        """
        new_filters = self._filters.copy()
        new_filters['zones'] = zones
        return NetworkQuery(new_filters, self._cache, self._metadata)

    def functional_classes(self, classes):
        """
        Filter by functional class.

        Parameters
        ----------
        classes : list
            Functional class descriptions (e.g., ['Interstate', 'Arterial'])
            Valid values: 'Interstate Highway', 'Other Controlled Access Highway',
            'Arterial or Major Collector', etc.

        Returns
        -------
        NetworkQuery
            New query instance with filter applied
        """
        new_filters = self._filters.copy()
        new_filters['functional_classes'] = classes
        return NetworkQuery(new_filters, self._cache, self._metadata)

    def freight_network(self, nhfn=True):
        """
        Filter to National Highway Freight Network.

        Parameters
        ----------
        nhfn : bool, default True
            If True, include only NHFN segments

        Returns
        -------
        NetworkQuery
            New query instance with filter applied
        """
        new_filters = self._filters.copy()
        new_filters['nhfn'] = nhfn
        return NetworkQuery(new_filters, self._cache, self._metadata)

    def nhs(self, nhs=True):
        """
        Filter to National Highway System.

        Parameters
        ----------
        nhs : bool, default True
            If True, include only NHS segments

        Returns
        -------
        NetworkQuery
            New query instance with filter applied
        """
        new_filters = self._filters.copy()
        new_filters['nhs'] = nhs
        return NetworkQuery(new_filters, self._cache, self._metadata)

    def truck_allowed(self, allowed=True):
        """
        Filter by truck access.

        Parameters
        ----------
        allowed : bool, default True
            If True, exclude segments where trucks are prohibited

        Returns
        -------
        NetworkQuery
            New query instance with filter applied
        """
        new_filters = self._filters.copy()
        new_filters['truck_allowed'] = allowed
        return NetworkQuery(new_filters, self._cache, self._metadata)

    def toll_roads(self, toll=True):
        """
        Filter by toll status.

        Parameters
        ----------
        toll : bool, default True
            If True, include only toll roads. If False, exclude toll roads.

        Returns
        -------
        NetworkQuery
            New query instance with filter applied
        """
        new_filters = self._filters.copy()
        new_filters['toll'] = toll
        return NetworkQuery(new_filters, self._cache, self._metadata)

    # === Filter Application ===

    def _apply_filters(self, gdf):
        """Apply network filters to GeoDataFrame."""

        # Route filtering
        if 'routes' in self._filters:
            # Match on Road_Name or Sign_Rte
            pattern = '|'.join(self._filters['routes'])
            mask = (
                gdf['Road_Name'].str.contains(pattern, case=False, na=False, regex=True) |
                gdf['Sign_Rte'].str.contains(pattern, case=False, na=False, regex=True)
            )
            gdf = gdf[mask]

        # State filtering
        if 'states' in self._filters:
            gdf = gdf[gdf['STATE'].isin(self._filters['states'])]

        # Zone filtering
        if 'zones' in self._filters:
            gdf = gdf[gdf['FAFZONE'].isin(self._filters['zones'])]

        # Functional class filtering
        if 'functional_classes' in self._filters:
            # Match partial strings (e.g., 'Interstate' matches 'Interstate Highway')
            pattern = '|'.join(self._filters['functional_classes'])
            gdf = gdf[gdf['Class_Description'].str.contains(pattern, case=False, na=False, regex=True)]

        # NHFN filtering
        if 'nhfn' in self._filters and self._filters['nhfn']:
            gdf = gdf[gdf['NHFN'].notna()]

        # NHS filtering
        if 'nhs' in self._filters and self._filters['nhs']:
            gdf = gdf[gdf['NHS'].notna()]

        # Truck access filtering
        if 'truck_allowed' in self._filters and self._filters['truck_allowed']:
            gdf = gdf[gdf['Truck'] != 'Prohibited']

        # Toll filtering
        if 'toll' in self._filters:
            if self._filters['toll']:
                # Include only toll roads
                gdf = gdf[gdf['Toll_Type'].notna()]
            else:
                # Exclude toll roads
                gdf = gdf[gdf['Toll_Type'].isna()]

        return gdf

    # === Analysis Methods ===

    def total_length(self):
        """
        Calculate total length of filtered segments.

        Returns
        -------
        float
            Total length (same units as LENGTH column)
        """
        gdf = self.get()
        return gdf['LENGTH'].sum()

    def by_state(self):
        """
        Group by state.

        Returns
        -------
        pd.DataFrame
            Total length by state
        """
        gdf = self.get()
        return gdf.groupby('STATE')['LENGTH'].sum().reset_index()

    def by_functional_class(self):
        """
        Group by functional class.

        Returns
        -------
        pd.DataFrame
            Total length by functional class
        """
        gdf = self.get()
        return gdf.groupby('Class_Description')['LENGTH'].sum().reset_index()

    def by_zone(self):
        """
        Group by FAF zone.

        Returns
        -------
        pd.DataFrame
            Total length by zone
        """
        gdf = self.get()
        return gdf.groupby('FAFZONE')['LENGTH'].sum().reset_index()

    def summarize(self):
        """
        Quick summary statistics.

        Returns
        -------
        dict
            Summary statistics
        """
        gdf = self.get()
        return {
            'total_segments': len(gdf),
            'total_length': gdf['LENGTH'].sum(),
            'avg_length': gdf['LENGTH'].mean(),
            'states': gdf['STATE'].nunique(),
            'functional_classes': gdf['Class_Description'].nunique()
        }

    # Note: NetworkQuery returns GeoDataFrame by default
    # No separate to_gdf() method needed
