"""
tidyfaf - Tidy access to FAF freight flow data.

Inspired by tidycensus, provides chainable query builders for
FAF regional flows, state flows, network data, and forecasts.

Examples
--------
>>> import tidyfaf as faf

>>> # Discover available data
>>> faf.available_commodities(search='electronics')
>>> faf.available_zones(search='california')

>>> # Query regional flows
>>> query = (faf.FAFQuery()
...     .origin_states(['California', 'Texas'])
...     .destination_zones([111])
...     .commodities(['Electronics'])
...     .years([2020, 2030])
... )
>>> df = query.get()

>>> # Network analysis
>>> network = faf.NetworkQuery().routes(['I-5']).states(['CA', 'OR']).get()
"""

# Query builders (primary API)
from .query import FAFQuery, StateQuery, NetworkQuery, ForecastQuery

# Global discovery functions
from .metadata import FAFMetadata

# Create global metadata instance for discovery functions
_metadata = FAFMetadata()


def available_commodities(search=None):
    """
    List/search all commodities in FAF.

    Parameters
    ----------
    search : str, optional
        Search string (case-insensitive)

    Returns
    -------
    pd.DataFrame
        Commodity codes and descriptions

    Examples
    --------
    >>> faf.available_commodities(search='electronics')
    """
    df = _metadata.commodities.copy()
    if search:
        df = df[df.astype(str).apply(
            lambda x: x.str.contains(search, case=False, na=False)
        ).any(axis=1)]
    return df


def available_zones(search=None):
    """
    List/search all FAF zones.

    Parameters
    ----------
    search : str, optional
        Search string (case-insensitive)

    Returns
    -------
    pd.DataFrame
        Zone codes and descriptions

    Examples
    --------
    >>> faf.available_zones(search='washington')
    """
    df = _metadata.zones.copy()
    if search:
        df = df[df.astype(str).apply(
            lambda x: x.str.contains(search, case=False, na=False)
        ).any(axis=1)]
    return df


def available_states(search=None):
    """
    List/search all states.

    Parameters
    ----------
    search : str, optional
        Search string (case-insensitive)

    Returns
    -------
    pd.DataFrame
        State codes and names

    Examples
    --------
    >>> faf.available_states(search='california')
    """
    df = _metadata.states.copy()
    if search:
        df = df[df.astype(str).apply(
            lambda x: x.str.contains(search, case=False, na=False)
        ).any(axis=1)]
    return df


def available_modes():
    """
    List all modes.

    Returns
    -------
    pd.DataFrame
        Mode codes and descriptions

    Examples
    --------
    >>> faf.available_modes()
    """
    return _metadata.modes.copy()


# Visualization
from .visualization import FlowMap

# Data download
from .download import download_and_process

# Cache management
from .query.cache import clear_cache, clear_all_caches

# Version
__version__ = '0.1.1'

__all__ = [
    # Query builders (primary API)
    'FAFQuery',
    'StateQuery',
    'NetworkQuery',
    'ForecastQuery',

    # Discovery functions
    'available_commodities',
    'available_zones',
    'available_states',
    'available_modes',

    # Utilities
    'FlowMap',
    'download_and_process',
    'clear_cache',
    'clear_all_caches',

    # Version
    '__version__'
]
