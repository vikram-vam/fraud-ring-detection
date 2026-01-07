"""
Configuration Manager - Application configuration and settings
Manages environment variables and application settings
"""
import os
from typing import Optional
from pathlib import Path
from dotenv import load_dotenv


class Config:
    """
    Configuration manager for fraud detection system
    Loads settings from environment variables and provides defaults
    """
    
    def __init__(self):
        # Load environment variables from .env file
        env_path = Path('.') / '.env'
        load_dotenv(dotenv_path=env_path)
        
        # Neo4j Configuration
        self.NEO4J_URI = os.getenv('NEO4J_URI', 'bolt://localhost:7687')
        self.NEO4J_USER = os.getenv('NEO4J_USER', 'neo4j')
        self.NEO4J_PASSWORD = os.getenv('NEO4J_PASSWORD', 'password')
        self.NEO4J_DATABASE = os.getenv('NEO4J_DATABASE', 'neo4j')
        
        # Application Settings
        self.APP_NAME = os.getenv('APP_NAME', 'Auto Insurance Fraud Detection')
        self.APP_VERSION = os.getenv('APP_VERSION', '1.0.0')
        self.ENVIRONMENT = os.getenv('ENVIRONMENT', 'development')
        self.DEBUG = os.getenv('DEBUG', 'False').lower() == 'true'
        
        # Logging Configuration
        self.LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
        self.LOG_FILE = os.getenv('LOG_FILE', 'logs/fraud_detection.log')
        self.LOG_MAX_BYTES = int(os.getenv('LOG_MAX_BYTES', 10485760))  # 10MB
        self.LOG_BACKUP_COUNT = int(os.getenv('LOG_BACKUP_COUNT', 5))
        
        # Data Paths
        self.DATA_DIR = os.getenv('DATA_DIR', 'data')
        self.SYNTHETIC_DATA_DIR = os.path.join(self.DATA_DIR, 'synthetic')
        self.EXPORT_DIR = os.getenv('EXPORT_DIR', 'exports')
        self.REPORTS_DIR = os.getenv('REPORTS_DIR', 'reports')
        
        # Create directories if they don't exist
        self._create_directories()
        
        # Risk Score Thresholds
        self.RISK_THRESHOLD_LOW = float(os.getenv('RISK_THRESHOLD_LOW', 30))
        self.RISK_THRESHOLD_MEDIUM = float(os.getenv('RISK_THRESHOLD_MEDIUM', 50))
        self.RISK_THRESHOLD_HIGH = float(os.getenv('RISK_THRESHOLD_HIGH', 70))
        self.RISK_THRESHOLD_CRITICAL = float(os.getenv('RISK_THRESHOLD_CRITICAL', 85))
        
        # Fraud Detection Thresholds
        self.MIN_FRAUD_RING_MEMBERS = int(os.getenv('MIN_FRAUD_RING_MEMBERS', 3))
        self.MIN_SHARED_CONNECTIONS = int(os.getenv('MIN_SHARED_CONNECTIONS', 2))
        self.MIN_CONFIDENCE_SCORE = float(os.getenv('MIN_CONFIDENCE_SCORE', 0.6))
        self.HIGH_CLAIM_AMOUNT = float(os.getenv('HIGH_CLAIM_AMOUNT', 75000))
        self.VERY_HIGH_CLAIM_AMOUNT = float(os.getenv('VERY_HIGH_CLAIM_AMOUNT', 150000))
        
        # Pattern Detection Thresholds
        self.STAGED_ACCIDENT_MIN_AMOUNT = float(os.getenv('STAGED_ACCIDENT_MIN_AMOUNT', 25000))
        self.BODY_SHOP_MIN_CLAIMS = int(os.getenv('BODY_SHOP_MIN_CLAIMS', 15))
        self.MEDICAL_MILL_MIN_CLAIMS = int(os.getenv('MEDICAL_MILL_MIN_CLAIMS', 20))
        self.ATTORNEY_MIN_CASES = int(os.getenv('ATTORNEY_MIN_CASES', 25))
        self.PROFESSIONAL_WITNESS_THRESHOLD = int(os.getenv('PROFESSIONAL_WITNESS_THRESHOLD', 3))
        self.ACCIDENT_HOTSPOT_THRESHOLD = int(os.getenv('ACCIDENT_HOTSPOT_THRESHOLD', 5))
        self.TOW_KICKBACK_MIN_TOWS = int(os.getenv('TOW_KICKBACK_MIN_TOWS', 15))
        self.TOW_KICKBACK_MIN_CONCENTRATION = float(os.getenv('TOW_KICKBACK_MIN_CONCENTRATION', 0.7))
        
        # Alert Settings
        self.ALERT_RETENTION_DAYS = int(os.getenv('ALERT_RETENTION_DAYS', 365))
        self.MAX_OPEN_ALERTS_PER_ENTITY = int(os.getenv('MAX_OPEN_ALERTS_PER_ENTITY', 5))
        
        # Case Management
        self.CASE_RETENTION_DAYS = int(os.getenv('CASE_RETENTION_DAYS', 2555))  # 7 years
        self.AUTO_CLOSE_INACTIVE_DAYS = int(os.getenv('AUTO_CLOSE_INACTIVE_DAYS', 90))
        
        # Streamlit UI Settings
        self.STREAMLIT_PAGE_TITLE = os.getenv('STREAMLIT_PAGE_TITLE', 'Auto Insurance Fraud Detection')
        self.STREAMLIT_PAGE_ICON = os.getenv('STREAMLIT_PAGE_ICON', '🚗')
        self.STREAMLIT_LAYOUT = os.getenv('STREAMLIT_LAYOUT', 'wide')
        
        # Pagination
        self.DEFAULT_PAGE_SIZE = int(os.getenv('DEFAULT_PAGE_SIZE', 25))
        self.MAX_PAGE_SIZE = int(os.getenv('MAX_PAGE_SIZE', 100))
        
        # Feature Flags
        self.ENABLE_ML_PREDICTIONS = os.getenv('ENABLE_ML_PREDICTIONS', 'False').lower() == 'true'
        self.ENABLE_AUTO_ALERTS = os.getenv('ENABLE_AUTO_ALERTS', 'True').lower() == 'true'
        self.ENABLE_EXPORT = os.getenv('ENABLE_EXPORT', 'True').lower() == 'true'
        
        # API Settings (if needed)
        self.API_HOST = os.getenv('API_HOST', '0.0.0.0')
        self.API_PORT = int(os.getenv('API_PORT', 8000))
        self.API_WORKERS = int(os.getenv('API_WORKERS', 4))
        
    def _create_directories(self):
        """Create necessary directories if they don't exist"""
        directories = [
            'logs',
            self.DATA_DIR,
            self.SYNTHETIC_DATA_DIR,
            self.EXPORT_DIR,
            self.REPORTS_DIR
        ]
        
        for directory in directories:
            Path(directory).mkdir(parents=True, exist_ok=True)
    
    def get_risk_category(self, risk_score: float) -> str:
        """
        Get risk category based on risk score
        
        Args:
            risk_score: Risk score (0-100)
            
        Returns:
            Risk category string
        """
        if risk_score >= self.RISK_THRESHOLD_CRITICAL:
            return "CRITICAL"
        elif risk_score >= self.RISK_THRESHOLD_HIGH:
            return "HIGH"
        elif risk_score >= self.RISK_THRESHOLD_MEDIUM:
            return "MEDIUM"
        elif risk_score >= self.RISK_THRESHOLD_LOW:
            return "LOW"
        else:
            return "MINIMAL"
    
    def is_production(self) -> bool:
        """Check if running in production environment"""
        return self.ENVIRONMENT.lower() == 'production'
    
    def is_development(self) -> bool:
        """Check if running in development environment"""
        return self.ENVIRONMENT.lower() == 'development'
    
    def get_neo4j_config(self) -> dict:
        """Get Neo4j connection configuration"""
        return {
            'uri': self.NEO4J_URI,
            'user': self.NEO4J_USER,
            'password': self.NEO4J_PASSWORD,
            'database': self.NEO4J_DATABASE
        }
    
    def get_detection_thresholds(self) -> dict:
        """Get all fraud detection thresholds"""
        return {
            'staged_accident_min_amount': self.STAGED_ACCIDENT_MIN_AMOUNT,
            'body_shop_min_claims': self.BODY_SHOP_MIN_CLAIMS,
            'medical_mill_min_claims': self.MEDICAL_MILL_MIN_CLAIMS,
            'attorney_min_cases': self.ATTORNEY_MIN_CASES,
            'professional_witness_threshold': self.PROFESSIONAL_WITNESS_THRESHOLD,
            'accident_hotspot_threshold': self.ACCIDENT_HOTSPOT_THRESHOLD,
            'tow_kickback_min_tows': self.TOW_KICKBACK_MIN_TOWS,
            'tow_kickback_min_concentration': self.TOW_KICKBACK_MIN_CONCENTRATION,
            'min_fraud_ring_members': self.MIN_FRAUD_RING_MEMBERS,
            'min_shared_connections': self.MIN_SHARED_CONNECTIONS
        }
    
    def __repr__(self) -> str:
        """String representation of configuration"""
        return (f"Config(environment={self.ENVIRONMENT}, "
                f"neo4j_uri={self.NEO4J_URI}, "
                f"debug={self.DEBUG})")


# Global configuration instance
config = Config()
