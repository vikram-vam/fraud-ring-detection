"""
Logger Configuration - Centralized logging setup
Configures logging for the entire application
"""
import logging
import sys
from pathlib import Path
from logging.handlers import RotatingFileHandler
from datetime import datetime
from typing import Optional


def setup_logger(
    name: str,
    log_file: Optional[str] = None,
    level: int = logging.INFO,
    max_bytes: int = 10485760,  # 10MB
    backup_count: int = 5
) -> logging.Logger:
    """
    Set up logger with console and file handlers
    
    Args:
        name: Logger name (usually __name__)
        log_file: Path to log file (optional)
        level: Logging level
        max_bytes: Maximum log file size before rotation
        backup_count: Number of backup files to keep
        
    Returns:
        Configured logger instance
    """
    # Create logger
    logger = logging.getLogger(name)
    
    # Avoid duplicate handlers if logger already exists
    if logger.handlers:
        return logger
    
    logger.setLevel(level)
    logger.propagate = False
    
    # Create formatters
    detailed_formatter = logging.Formatter(
        fmt='%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    simple_formatter = logging.Formatter(
        fmt='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(level)
    console_handler.setFormatter(simple_formatter)
    logger.addHandler(console_handler)
    
    # File handler (if log_file specified)
    if log_file:
        # Ensure log directory exists
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        
        file_handler = RotatingFileHandler(
            log_file,
            maxBytes=max_bytes,
            backupCount=backup_count
        )
        file_handler.setLevel(level)
        file_handler.setFormatter(detailed_formatter)
        logger.addHandler(file_handler)
    
    return logger


def get_logger(name: str) -> logging.Logger:
    """
    Get existing logger or create new one with default settings
    
    Args:
        name: Logger name
        
    Returns:
        Logger instance
    """
    logger = logging.getLogger(name)
    
    if not logger.handlers:
        return setup_logger(name, log_file='logs/fraud_detection.log')
    
    return logger


class LoggerContext:
    """
    Context manager for temporary logging level changes
    
    Usage:
        with LoggerContext(logger, logging.DEBUG):
            # Code here will run with DEBUG level
            pass
    """
    
    def __init__(self, logger: logging.Logger, level: int):
        self.logger = logger
        self.level = level
        self.old_level = logger.level
    
    def __enter__(self):
        self.logger.setLevel(self.level)
        return self.logger
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.logger.setLevel(self.old_level)


def configure_app_logging(
    log_level: str = 'INFO',
    log_file: str = 'logs/fraud_detection.log'
):
    """
    Configure application-wide logging
    
    Args:
        log_level: Logging level as string
        log_file: Path to main log file
    """
    # Map string level to logging constant
    level_map = {
        'DEBUG': logging.DEBUG,
        'INFO': logging.INFO,
        'WARNING': logging.WARNING,
        'ERROR': logging.ERROR,
        'CRITICAL': logging.CRITICAL
    }
    
    level = level_map.get(log_level.upper(), logging.INFO)
    
    # Configure root logger
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # Set up main application logger
    app_logger = setup_logger('fraud_detection', log_file=log_file, level=level)
    
    # Suppress noisy third-party loggers
    logging.getLogger('neo4j').setLevel(logging.WARNING)
    logging.getLogger('urllib3').setLevel(logging.WARNING)
    logging.getLogger('streamlit').setLevel(logging.WARNING)
    
    app_logger.info("=" * 80)
    app_logger.info(f"Fraud Detection System - Logging initialized at {datetime.now()}")
    app_logger.info(f"Log level: {log_level}")
    app_logger.info(f"Log file: {log_file}")
    app_logger.info("=" * 80)


def log_performance(func):
    """
    Decorator to log function execution time
    
    Usage:
        @log_performance
        def my_function():
            pass
    """
    import functools
    import time
    
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        logger = logging.getLogger(func.__module__)
        start_time = time.time()
        
        try:
            result = func(*args, **kwargs)
            execution_time = time.time() - start_time
            logger.debug(f"{func.__name__} executed in {execution_time:.4f} seconds")
            return result
        except Exception as e:
            execution_time = time.time() - start_time
            logger.error(f"{func.__name__} failed after {execution_time:.4f} seconds: {e}")
            raise
    
    return wrapper


# Module-level logger for testing
module_logger = setup_logger(__name__, log_file='logs/fraud_detection.log')
