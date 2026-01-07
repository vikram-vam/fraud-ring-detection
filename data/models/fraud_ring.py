"""
FraudRing Model - Represents a fraud ring entity
"""
from dataclasses import dataclass
from typing import Optional, List
from datetime import datetime


@dataclass
class FraudRing:
    """
    Fraud Ring entity model
    """
    ring_id: str
    ring_type: str  # KNOWN, DISCOVERED, SUSPICIOUS, EMERGING
    pattern_type: str  # address_farm, provider_centric, attorney_centric, mixed
    status: str  # CONFIRMED, UNDER_REVIEW, DISMISSED
    confidence_score: float
    member_count: int
    estimated_fraud_amount: float
    discovered_date: Optional[datetime] = None
    discovered_by: Optional[str] = None
    confirmed_date: Optional[datetime] = None
    confirmed_by: Optional[str] = None
    dismissed_date: Optional[datetime] = None
    dismissed_by: Optional[str] = None
    dismissal_reason: Optional[str] = None
    
    def __post_init__(self):
        """Validate and normalize data after initialization"""
        # Ensure confidence score is between 0 and 1
        if self.confidence_score > 1.0:
            self.confidence_score = self.confidence_score / 100.0
        
        # Validate ring_type
        valid_ring_types = ['KNOWN', 'DISCOVERED', 'SUSPICIOUS', 'EMERGING']
        if self.ring_type not in valid_ring_types:
            raise ValueError(f"Invalid ring_type: {self.ring_type}")
        
        # Validate status
        valid_statuses = ['CONFIRMED', 'UNDER_REVIEW', 'DISMISSED']
        if self.status not in valid_statuses:
            raise ValueError(f"Invalid status: {self.status}")
    
    @classmethod
    def from_dict(cls, data: dict) -> 'FraudRing':
        """
        Create FraudRing instance from dictionary
        
        Args:
            data: Dictionary with fraud ring data
            
        Returns:
            FraudRing instance
        """
        return cls(
            ring_id=data.get('ring_id'),
            ring_type=data.get('ring_type'),
            pattern_type=data.get('pattern_type'),
            status=data.get('status'),
            confidence_score=data.get('confidence_score', 0.0),
            member_count=data.get('member_count', 0),
            estimated_fraud_amount=data.get('estimated_fraud_amount', 0.0),
            discovered_date=data.get('discovered_date'),
            discovered_by=data.get('discovered_by'),
            confirmed_date=data.get('confirmed_date'),
            confirmed_by=data.get('confirmed_by'),
            dismissed_date=data.get('dismissed_date'),
            dismissed_by=data.get('dismissed_by'),
            dismissal_reason=data.get('dismissal_reason')
        )
    
    def to_dict(self) -> dict:
        """
        Convert FraudRing instance to dictionary
        
        Returns:
            Dictionary representation
        """
        return {
            'ring_id': self.ring_id,
            'ring_type': self.ring_type,
            'pattern_type': self.pattern_type,
            'status': self.status,
            'confidence_score': self.confidence_score,
            'member_count': self.member_count,
            'estimated_fraud_amount': self.estimated_fraud_amount,
            'discovered_date': self.discovered_date,
            'discovered_by': self.discovered_by,
            'confirmed_date': self.confirmed_date,
            'confirmed_by': self.confirmed_by,
            'dismissed_date': self.dismissed_date,
            'dismissed_by': self.dismissed_by,
            'dismissal_reason': self.dismissal_reason
        }
    
    def get_confidence_percentage(self) -> float:
        """Get confidence score as percentage"""
        return self.confidence_score * 100 if self.confidence_score <= 1 else self.confidence_score
    
    def is_confirmed(self) -> bool:
        """Check if ring is confirmed"""
        return self.status == 'CONFIRMED'
    
    def is_under_review(self) -> bool:
        """Check if ring is under review"""
        return self.status == 'UNDER_REVIEW'
    
    def is_dismissed(self) -> bool:
        """Check if ring is dismissed"""
        return self.status == 'DISMISSED'
    
    def is_high_confidence(self, threshold: float = 0.7) -> bool:
        """Check if ring has high confidence"""
        return self.confidence_score >= threshold
    
    def __repr__(self) -> str:
        return f"FraudRing(ring_id={self.ring_id}, type={self.ring_type}, pattern={self.pattern_type}, status={self.status})"
