"""
Attorney Model - Represents an attorney entity
"""
from dataclasses import dataclass
from typing import Optional
from datetime import datetime


@dataclass
class Attorney:
    """
    Attorney entity model
    """
    attorney_id: str
    name: str
    firm: str
    bar_number: Optional[str] = None
    street: Optional[str] = None
    city: Optional[str] = None
    state: Optional[str] = None
    zip_code: Optional[str] = None
    phone: Optional[str] = None
    email: Optional[str] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    
    # Aggregated statistics
    claim_count: Optional[int] = None
    client_count: Optional[int] = None
    total_amount: Optional[float] = None
    avg_risk_score: Optional[float] = None
    ring_connections: Optional[int] = None
    
    @classmethod
    def from_dict(cls, data: dict) -> 'Attorney':
        """
        Create Attorney instance from dictionary
        
        Args:
            data: Dictionary with attorney data
            
        Returns:
            Attorney instance
        """
        return cls(
            attorney_id=data.get('attorney_id'),
            name=data.get('name'),
            firm=data.get('firm'),
            bar_number=data.get('bar_number'),
            street=data.get('street'),
            city=data.get('city'),
            state=data.get('state'),
            zip_code=data.get('zip_code'),
            phone=data.get('phone'),
            email=data.get('email'),
            created_at=data.get('created_at'),
            updated_at=data.get('updated_at'),
            claim_count=data.get('claim_count'),
            client_count=data.get('client_count'),
            total_amount=data.get('total_amount'),
            avg_risk_score=data.get('avg_risk_score'),
            ring_connections=data.get('ring_connections')
        )
    
    def to_dict(self) -> dict:
        """
        Convert Attorney instance to dictionary
        
        Returns:
            Dictionary representation
        """
        return {
            'attorney_id': self.attorney_id,
            'name': self.name,
            'firm': self.firm,
            'bar_number': self.bar_number,
            'street': self.street,
            'city': self.city,
            'state': self.state,
            'zip_code': self.zip_code,
            'phone': self.phone,
            'email': self.email,
            'created_at': self.created_at,
            'updated_at': self.updated_at,
            'claim_count': self.claim_count,
            'client_count': self.client_count,
            'total_amount': self.total_amount,
            'avg_risk_score': self.avg_risk_score,
            'ring_connections': self.ring_connections
        }
    
    def get_full_address(self) -> str:
        """Get formatted full address"""
        parts = []
        if self.street:
            parts.append(self.street)
        if self.city:
            parts.append(self.city)
        if self.state:
            parts.append(self.state)
        if self.zip_code:
            parts.append(self.zip_code)
        
        return ', '.join(parts) if parts else 'No address available'
    
    def is_high_volume(self, threshold: int = 10) -> bool:
        """Check if attorney has high client volume"""
        return self.client_count and self.client_count >= threshold
    
    def is_suspicious(self, risk_threshold: float = 70.0) -> bool:
        """Check if attorney has suspicious patterns"""
        if not self.avg_risk_score:
            return False
        return self.avg_risk_score >= risk_threshold
    
    def has_ring_connections(self) -> bool:
        """Check if attorney is connected to fraud rings"""
        return self.ring_connections and self.ring_connections > 0
    
    def __repr__(self) -> str:
        return f"Attorney(attorney_id={self.attorney_id}, name={self.name}, firm={self.firm})"
