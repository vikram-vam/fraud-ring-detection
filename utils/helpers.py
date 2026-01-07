"""
Helper Functions - Utility functions for data processing and formatting
Common helper functions used across the application
"""
import re
import uuid
from typing import Optional, Union, List
from datetime import datetime, date, timedelta
from decimal import Decimal


def generate_id(prefix: str = "") -> str:
    """
    Generate a unique ID with optional prefix
    
    Args:
        prefix: Prefix for the ID (e.g., "CLAIM", "USER")
        
    Returns:
        Unique ID string
    """
    unique_id = uuid.uuid4().hex[:12].upper()
    
    if prefix:
        return f"{prefix}_{unique_id}"
    
    return unique_id


def format_currency(amount: Union[float, int, Decimal], symbol: str = "$") -> str:
    """
    Format number as currency
    
    Args:
        amount: Amount to format
        symbol: Currency symbol
        
    Returns:
        Formatted currency string (e.g., "$1,234.56")
    """
    if amount is None:
        return f"{symbol}0.00"
    
    try:
        amount = float(amount)
        return f"{symbol}{amount:,.2f}"
    except (ValueError, TypeError):
        return f"{symbol}0.00"


def format_date(
    date_value: Union[str, date, datetime],
    format_string: str = "%Y-%m-%d"
) -> str:
    """
    Format date to string
    
    Args:
        date_value: Date to format
        format_string: Date format string
        
    Returns:
        Formatted date string
    """
    if date_value is None:
        return ""
    
    try:
        if isinstance(date_value, str):
            # Try to parse string to datetime
            date_value = datetime.fromisoformat(date_value.replace('Z', '+00:00'))
        
        if isinstance(date_value, datetime):
            return date_value.strftime(format_string)
        elif isinstance(date_value, date):
            return date_value.strftime(format_string)
        
        return str(date_value)
    except Exception:
        return str(date_value)


def format_phone(phone: str) -> str:
    """
    Format phone number to (XXX) XXX-XXXX format
    
    Args:
        phone: Phone number string
        
    Returns:
        Formatted phone number
    """
    if not phone:
        return ""
    
    # Remove all non-digit characters
    digits = re.sub(r'\D', '', phone)
    
    if len(digits) == 10:
        return f"({digits[:3]}) {digits[3:6]}-{digits[6:]}"
    elif len(digits) == 11 and digits[0] == '1':
        return f"+1 ({digits[1:4]}) {digits[4:7]}-{digits[7:]}"
    else:
        return phone  # Return as-is if not standard format


def calculate_days_between(
    start_date: Union[str, date, datetime],
    end_date: Union[str, date, datetime]
) -> int:
    """
    Calculate days between two dates
    
    Args:
        start_date: Start date
        end_date: End date
        
    Returns:
        Number of days between dates
    """
    try:
        if isinstance(start_date, str):
            start_date = datetime.fromisoformat(start_date.replace('Z', '+00:00'))
        if isinstance(end_date, str):
            end_date = datetime.fromisoformat(end_date.replace('Z', '+00:00'))
        
        if isinstance(start_date, datetime):
            start_date = start_date.date()
        if isinstance(end_date, datetime):
            end_date = end_date.date()
        
        delta = end_date - start_date
        return delta.days
    except Exception:
        return 0


def sanitize_string(text: str, max_length: Optional[int] = None) -> str:
    """
    Sanitize string by removing special characters and trimming
    
    Args:
        text: Text to sanitize
        max_length: Maximum length to truncate to
        
    Returns:
        Sanitized string
    """
    if not text:
        return ""
    
    # Remove extra whitespace
    text = ' '.join(text.split())
    
    # Truncate if max_length specified
    if max_length and len(text) > max_length:
        text = text[:max_length].rstrip() + "..."
    
    return text


def validate_email(email: str) -> bool:
    """
    Validate email address format
    
    Args:
        email: Email address to validate
        
    Returns:
        True if valid, False otherwise
    """
    if not email:
        return False
    
    pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    return re.match(pattern, email) is not None


def validate_phone(phone: str) -> bool:
    """
    Validate phone number (US format)
    
    Args:
        phone: Phone number to validate
        
    Returns:
        True if valid, False otherwise
    """
    if not phone:
        return False
    
    # Remove all non-digit characters
    digits = re.sub(r'\D', '', phone)
    
    # Valid if 10 digits or 11 digits starting with 1
    return len(digits) == 10 or (len(digits) == 11 and digits[0] == '1')


def validate_vin(vin: str) -> bool:
    """
    Validate Vehicle Identification Number (VIN)
    
    Args:
        vin: VIN to validate
        
    Returns:
        True if valid format, False otherwise
    """
    if not vin:
        return False
    
    # VIN must be 17 characters, alphanumeric (excluding I, O, Q)
    pattern = r'^[A-HJ-NPR-Z0-9]{17}$'
    return re.match(pattern, vin.upper()) is not None


def get_risk_category(risk_score: float) -> str:
    """
    Get risk category based on risk score
    
    Args:
        risk_score: Risk score (0-100)
        
    Returns:
        Risk category string
    """
    if risk_score >= 85:
        return "CRITICAL"
    elif risk_score >= 70:
        return "HIGH"
    elif risk_score >= 50:
        return "MEDIUM"
    elif risk_score >= 30:
        return "LOW"
    else:
        return "MINIMAL"


def get_risk_color(risk_score: float) -> str:
    """
    Get color code for risk score (for UI display)
    
    Args:
        risk_score: Risk score (0-100)
        
    Returns:
        Color string (red, orange, yellow, green)
    """
    if risk_score >= 85:
        return "red"
    elif risk_score >= 70:
        return "orange"
    elif risk_score >= 50:
        return "yellow"
    else:
        return "green"


def get_severity_color(severity: str) -> str:
    """
    Get color code for alert severity
    
    Args:
        severity: Severity level (CRITICAL, HIGH, MEDIUM, LOW)
        
    Returns:
        Color string
    """
    color_map = {
        'CRITICAL': 'red',
        'HIGH': 'orange',
        'MEDIUM': 'yellow',
        'LOW': 'blue',
        'INFO': 'gray'
    }
    return color_map.get(severity.upper(), 'gray')


def calculate_percentage(part: float, total: float, decimals: int = 1) -> float:
    """
    Calculate percentage with handling for zero division
    
    Args:
        part: Part value
        total: Total value
        decimals: Number of decimal places
        
    Returns:
        Percentage value
    """
    if total == 0:
        return 0.0
    
    percentage = (part / total) * 100
    return round(percentage, decimals)


def truncate_string(text: str, max_length: int = 50, suffix: str = "...") -> str:
    """
    Truncate string to maximum length
    
    Args:
        text: Text to truncate
        max_length: Maximum length
        suffix: Suffix to add if truncated
        
    Returns:
        Truncated string
    """
    if not text:
        return ""
    
    if len(text) <= max_length:
        return text
    
    return text[:max_length - len(suffix)].rstrip() + suffix


def safe_divide(numerator: float, denominator: float, default: float = 0.0) -> float:
    """
    Safely divide two numbers, handling zero division
    
    Args:
        numerator: Numerator
        denominator: Denominator
        default: Default value if division by zero
        
    Returns:
        Division result or default
    """
    if denominator == 0:
        return default
    
    return numerator / denominator


def parse_date(date_string: str) -> Optional[datetime]:
    """
    Parse date string to datetime object, trying multiple formats
    
    Args:
        date_string: Date string to parse
        
    Returns:
        Datetime object or None if parsing fails
    """
    if not date_string:
        return None
    
    formats = [
        '%Y-%m-%d',
        '%Y-%m-%dT%H:%M:%S',
        '%Y-%m-%d %H:%M:%S',
        '%m/%d/%Y',
        '%d/%m/%Y',
        '%Y/%m/%d'
    ]
    
    for fmt in formats:
        try:
            return datetime.strptime(date_string, fmt)
        except ValueError:
            continue
    
    return None


def flatten_dict(d: dict, parent_key: str = '', sep: str = '_') -> dict:
    """
    Flatten nested dictionary
    
    Args:
        d: Dictionary to flatten
        parent_key: Parent key for recursion
        sep: Separator for nested keys
        
    Returns:
        Flattened dictionary
    """
    items = []
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.extend(flatten_dict(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))
    return dict(items)


def chunk_list(lst: List, chunk_size: int) -> List[List]:
    """
    Split list into chunks
    
    Args:
        lst: List to chunk
        chunk_size: Size of each chunk
        
    Returns:
        List of chunks
    """
    return [lst[i:i + chunk_size] for i in range(0, len(lst), chunk_size)]


def remove_duplicates(lst: List) -> List:
    """
    Remove duplicates from list while preserving order
    
    Args:
        lst: List with potential duplicates
        
    Returns:
        List without duplicates
    """
    seen = set()
    result = []
    for item in lst:
        if item not in seen:
            seen.add(item)
            result.append(item)
    return result


def calculate_age(birth_date: Union[str, date, datetime]) -> Optional[int]:
    """
    Calculate age from birth date
    
    Args:
        birth_date: Birth date
        
    Returns:
        Age in years
    """
    try:
        if isinstance(birth_date, str):
            birth_date = parse_date(birth_date)
        
        if isinstance(birth_date, datetime):
            birth_date = birth_date.date()
        
        today = date.today()
        age = today.year - birth_date.year
        
        # Adjust if birthday hasn't occurred this year
        if (today.month, today.day) < (birth_date.month, birth_date.day):
            age -= 1
        
        return age
    except Exception:
        return None


def format_large_number(number: Union[int, float], precision: int = 1) -> str:
    """
    Format large numbers with K, M, B suffixes
    
    Args:
        number: Number to format
        precision: Decimal precision
        
    Returns:
        Formatted string (e.g., "1.2M", "500K")
    """
    if number is None:
        return "0"
    
    try:
        number = float(number)
        
        if abs(number) >= 1_000_000_000:
            return f"{number / 1_000_000_000:.{precision}f}B"
        elif abs(number) >= 1_000_000:
            return f"{number / 1_000_000:.{precision}f}M"
        elif abs(number) >= 1_000:
            return f"{number / 1_000:.{precision}f}K"
        else:
            return f"{number:.{precision}f}"
    except (ValueError, TypeError):
        return str(number)


def get_time_ago(dt: Union[str, datetime]) -> str:
    """
    Get human-readable time difference (e.g., "2 hours ago")
    
    Args:
        dt: Datetime to compare
        
    Returns:
        Human-readable time difference
    """
    try:
        if isinstance(dt, str):
            dt = datetime.fromisoformat(dt.replace('Z', '+00:00'))
        
        now = datetime.now(dt.tzinfo) if dt.tzinfo else datetime.now()
        diff = now - dt
        
        seconds = diff.total_seconds()
        
        if seconds < 60:
            return "just now"
        elif seconds < 3600:
            minutes = int(seconds / 60)
            return f"{minutes} minute{'s' if minutes != 1 else ''} ago"
        elif seconds < 86400:
            hours = int(seconds / 3600)
            return f"{hours} hour{'s' if hours != 1 else ''} ago"
        elif seconds < 604800:
            days = int(seconds / 86400)
            return f"{days} day{'s' if days != 1 else ''} ago"
        elif seconds < 2592000:
            weeks = int(seconds / 604800)
            return f"{weeks} week{'s' if weeks != 1 else ''} ago"
        elif seconds < 31536000:
            months = int(seconds / 2592000)
            return f"{months} month{'s' if months != 1 else ''} ago"
        else:
            years = int(seconds / 31536000)
            return f"{years} year{'s' if years != 1 else ''} ago"
    except Exception:
        return "unknown"
