"""
Provider Model - Represents a provider entity
"""
from dataclasses import dataclass
from typing import Optional
from datetime import datetime


@dataclass
class Provider:
    """
    Provider entity model
    """
    provider_id: str
    name: str
    provider_type: str
    license_number: Optional[str] = None
    street: Optional[str] = None
    city: Optional[str] = None
    state: Optional[str] = None
    zip_code: Optional[str] = None
    phone: Optional[str] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    
    # Aggregated statistics
    claim_count: Optional[int] = None
    claimant_count: Optional[int] = None
    total_amount: Optional[float] = None
    avg_risk_score: Optional[float] = None
    ring_connections: Optional[int] = None
    
    @classmethod
    def from_dict(cls, data: dict) -> 'Provider':
        """
        Create Provider instance from dictionary
        
        Args:
            data: Dictionary with provider data
            
        Returns:
            Provider instance
        """
        return cls(
            provider_id=data.get('provider_id'),
            name=data.get('name'),
            provider_type=data.get('provider_type'),
            license_number=data.get('license_number'),
            street=data.get('street'),
            city=data.get('city'),
            state=data.get('state'),
            zip_code=data.get('zip_code'),
            phone=data.get('phone'),
            created_at=data.get('created_at'),
            updated_at=data.get('updated_at'),
            claim_count=data.get('claim_count'),
            claimant_count=data.get('claimant_count'),
            total_amount=data.get('total_amount'),
            avg_risk_score=data.get('avg_risk_score'),
            ring_connections=data.get('ring_connections')
        )
    
    def to_dict(self) -> dict:
        """
        Convert Provider instance to dictionary
        
        Returns:
            Dictionary representation
        """
        return {
            'provider_id': self.provider_id,
            'name': self.name,
            'provider_type': self.provider_type,
            'license_number': self.license_number,
            'street': self.street,
            'city': self.city,
            'state': self.state,
            'zip_code': self.zip_code,
            'phone': self.phone,
            'created_at': self.created_at,
            'updated_at': self.updated_at,
            'claim_count': self.claim_count,
            'claimant_count': self.claimant_count,
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
    
    def is_high_volume(self, threshold: int = 15) -> bool:
        """Check if provider has high claimant volume"""
        return self.claimant_count and self.claimant_count >= threshold
    
    def is_suspicious(self, risk_threshold: float = 70.0) -> bool:
        """Check if provider has suspicious patterns"""
        if not self.avg_risk_score:
            return False
        return self.avg_risk_score >= risk_threshold
    
    def has_ring_connections(self) -> bool:
        """Check if provider is connected to fraud rings"""
        return self.ring_connections and self.ring_connections > 0
    
    def __repr__(self) -> str:
        return f"Provider(provider_id={self.provider_id}, name={self.name}, type={self.provider_type})"
