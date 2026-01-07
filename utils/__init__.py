"""
Utils Package - Utility functions and helpers
Contains configuration, logging, and common helper functions
"""

from utils.config import Config
from utils.logger import setup_logger
from utils.helpers import (
    generate_id,
    format_currency,
    format_date,
    format_phone,
    calculate_days_between,
    sanitize_string,
    validate_email,
    validate_phone,
    validate_vin,
    get_risk_category,
    get_severity_color,
    calculate_percentage,
    truncate_string
)

__all__ = [
    'Config',
    'setup_logger',
    'generate_id',
    'format_currency',
    'format_date',
    'format_phone',
    'calculate_days_between',
    'sanitize_string',
    'validate_email',
    'validate_phone',
    'validate_vin',
    'get_risk_category',
    'get_severity_color',
    'calculate_percentage',
    'truncate_string'
]
