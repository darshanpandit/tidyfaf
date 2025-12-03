"""
Schema utilities for handling FAF column names.
"""


def get_year_columns(df, metric='tons'):
    """
    Extract year columns for a given metric.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame to extract columns from
    metric : str, default 'tons'
        Metric prefix ('tons', 'value', 'tmiles', 'current_value')

    Returns
    -------
    list of str
        Column names like tons_2020, tons_2025, etc.

    Examples
    --------
    >>> get_year_columns(df, 'tons')
    ['tons_2017', 'tons_2018', ..., 'tons_2050']
    """
    return [col for col in df.columns if col.startswith(f'{metric}_')]


def get_available_years(df, metric='tons'):
    """
    Extract year values from column names.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame to extract years from
    metric : str, default 'tons'
        Metric prefix

    Returns
    -------
    list of int
        Sorted list of years

    Examples
    --------
    >>> get_available_years(df, 'tons')
    [2017, 2018, 2019, 2020, ..., 2050]
    """
    cols = get_year_columns(df, metric)
    years = [int(col.split('_')[1]) for col in cols]
    return sorted(years)


def get_metric_columns(year):
    """
    Get all metric columns for a specific year.

    Parameters
    ----------
    year : int
        Year (e.g., 2020)

    Returns
    -------
    list of str
        Column names [f'tons_{year}', f'value_{year}', f'tmiles_{year}']

    Examples
    --------
    >>> get_metric_columns(2020)
    ['tons_2020', 'value_2020', 'tmiles_2020']
    """
    return [f'tons_{year}', f'value_{year}', f'tmiles_{year}']


def get_metadata_columns():
    """
    Get standard metadata column names for FAF data.

    Returns
    -------
    list of str
        Metadata columns common across datasets
    """
    return [
        'dms_orig', 'dms_dest',           # Regional columns
        'dms_origst', 'dms_destst',        # State columns
        'sctg2',                           # Commodity
        'dms_mode',                        # Mode
        'trade_type',                      # Trade type
        'dist_band',                       # Distance band
        'fr_orig', 'fr_dest',              # Foreign regions
        'fr_inmode', 'fr_outmode'          # Foreign modes
    ]


def is_forecast_year(year):
    """
    Check if year is in forecast range (2030-2050).

    Parameters
    ----------
    year : int
        Year to check

    Returns
    -------
    bool
        True if year >= 2030
    """
    return year >= 2030


def get_actual_years():
    """
    Get list of actual (non-forecast) years in FAF5.

    Returns
    -------
    list of int
        [2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024]
    """
    return list(range(2017, 2025))


def get_forecast_years():
    """
    Get list of forecast years in FAF5.

    Returns
    -------
    list of int
        [2030, 2035, 2040, 2045, 2050]
    """
    return list(range(2030, 2051, 5))
