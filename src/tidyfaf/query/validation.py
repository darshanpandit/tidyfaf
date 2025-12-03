"""
Input validation utilities for query builders.
"""

import warnings


def resolve_geography(values, metadata, level='auto'):
    """
    Resolve geography inputs to codes.

    Handles state names, zone names, FIPS codes, and zone codes.
    Can auto-detect level or enforce specific level.

    Parameters
    ----------
    values : list
        List of state/zone names or codes
        e.g., ['California', 'Texas'] or [6, 48] or [111, 121]
    metadata : FAFMetadata
        Metadata instance for lookups
    level : {'state', 'zone', 'auto'}, default 'auto'
        Geography level to resolve to

    Returns
    -------
    codes : list of int
        Resolved codes
    detected_level : str
        Detected level ('state' or 'zone')

    Raises
    ------
    ValueError
        If value cannot be resolved to state or zone

    Examples
    --------
    >>> resolve_geography(['California', 'Texas'], metadata, level='state')
    ([6, 48], 'state')

    >>> resolve_geography([111, 121], metadata, level='zone')
    ([111, 121], 'zone')

    >>> resolve_geography(['California', 111], metadata, level='auto')
    ValueError: Mixed state and zone inputs not supported
    """
    if not isinstance(values, list):
        values = [values]

    codes = []
    detected_levels = []

    for val in values:
        if isinstance(val, (int, float)):
            code = int(val)
            codes.append(code)

            # Heuristic: codes > 100 are likely zones, codes <= 100 are states
            if level == 'auto':
                if code > 100:
                    detected_levels.append('zone')
                else:
                    detected_levels.append('state')
            else:
                detected_levels.append(level)

        else:
            # String input - try to resolve
            val_str = str(val).strip()

            if level in ('state', 'auto'):
                # Try state first
                try:
                    code = metadata.lookup_state(val_str)
                    codes.append(code)
                    detected_levels.append('state')
                    continue
                except ValueError:
                    pass

            if level in ('zone', 'auto'):
                # Try zone
                try:
                    code = metadata.lookup_zone(val_str)
                    codes.append(code)
                    detected_levels.append('zone')
                    continue
                except ValueError:
                    pass

            # Could not resolve
            raise ValueError(
                f"Could not resolve '{val}' to {level} geography. "
                f"Check spelling or use numeric codes."
            )

    # Check for mixed levels
    unique_levels = set(detected_levels)
    if len(unique_levels) > 1:
        if level == 'auto':
            raise ValueError(
                "Mixed state and zone inputs detected. "
                "Use .origin_states() and .origin_zones() separately for cross-level queries."
            )
        else:
            warnings.warn(
                f"Some inputs do not match requested level '{level}'. "
                "Results may be unexpected."
            )

    detected_level = detected_levels[0] if detected_levels else level

    return codes, detected_level


def resolve_commodities(values, metadata):
    """
    Resolve commodity names/codes to SCTG2 codes.

    Parameters
    ----------
    values : list
        Commodity names or SCTG2 codes
        e.g., ['Electronics', 'Pharmaceuticals'] or [34, 38]
    metadata : FAFMetadata
        Metadata instance for lookups

    Returns
    -------
    list of int
        SCTG2 codes

    Raises
    ------
    ValueError
        If commodity name not found

    Examples
    --------
    >>> resolve_commodities(['Electronics', 'Pharmaceuticals'], metadata)
    [34, 38]
    """
    if not isinstance(values, list):
        values = [values]

    codes = []
    for val in values:
        if isinstance(val, (int, float)):
            codes.append(int(val))
        else:
            val_str = str(val).strip()
            try:
                code = metadata.lookup_commodity(val_str)
                codes.append(code)
            except ValueError:
                raise ValueError(
                    f"Could not find commodity '{val}'. "
                    f"Use faf.available_commodities(search='{val}') to search."
                )

    return codes


def resolve_modes(values, metadata):
    """
    Resolve mode names/codes to mode codes.

    Parameters
    ----------
    values : list
        Mode names or codes
        e.g., ['Truck', 'Rail'] or [1, 2]
    metadata : FAFMetadata
        Metadata instance for lookups

    Returns
    -------
    list of int
        Mode codes

    Raises
    ------
    ValueError
        If mode name not found

    Examples
    --------
    >>> resolve_modes(['Truck', 'Rail'], metadata)
    [1, 2]
    """
    if not isinstance(values, list):
        values = [values]

    codes = []
    for val in values:
        if isinstance(val, (int, float)):
            codes.append(int(val))
        else:
            val_str = str(val).strip()
            try:
                code = metadata.lookup_mode(val_str)
                codes.append(code)
            except ValueError:
                raise ValueError(
                    f"Could not find mode '{val}'. "
                    "Available modes: Truck, Rail, Water, Air, Multiple/Intermodal, Pipeline, Other, Unknown"
                )

    return codes


def validate_years(years):
    """
    Validate year inputs.

    Parameters
    ----------
    years : list of int
        Years to validate

    Returns
    -------
    list of int
        Validated years

    Raises
    ------
    ValueError
        If years outside valid range
    """
    if not isinstance(years, list):
        years = [years]

    valid_actual = list(range(2017, 2025))  # 2017-2024
    valid_forecast = list(range(2030, 2051, 5))  # 2030, 2035, ..., 2050
    valid_years = valid_actual + valid_forecast

    invalid = [y for y in years if y not in valid_years]
    if invalid:
        raise ValueError(
            f"Invalid years: {invalid}. "
            f"Valid years are 2017-2024 (actual) and 2030-2050 in 5-year intervals (forecast)."
        )

    return sorted(years)
