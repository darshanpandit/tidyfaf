"""
Data loading and schema utilities.
"""

from .loader import DataLoader
from .schema import (
    get_year_columns,
    get_available_years,
    get_metric_columns
)

__all__ = [
    'DataLoader',
    'get_year_columns',
    'get_available_years',
    'get_metric_columns'
]
