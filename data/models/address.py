"""
Address Model - Represents an address entity
"""
from dataclasses import dataclass
from typing import Optional
from datetime import datetime


@dataclass
class Address:
    """
    Address entity model
    """
    address_id: str
    street: str
    city: str
    state: str
    zip_code: str
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    
    # Aggregated statistics
    resident_count: Optional[int] = None
    claim_count: Optional[int] = None
    total_claim_amount: Optional[float] = None
    is_address_farm: Optional[bool] = None
    
    @classmethod
    def from_dict(cls, data: dict) -> 'Address':
        """
        Create Address instance from dictionary
        
        Args:
            data: Dictionary with address data
            
        Returns:
            Address instance
        """
        return cls(
            address_id=data.get('address_id'),
            street=data.get('street'),
            city=data.get('city'),
            state=data.get('state'),
            zip_code=data.get('zip_code'),
            created_at=data.get('created_at'),
            updated_at=data.get('updated_at'),
            resident_count=data.get('resident_count'),
            claim_count=data.get('claim_count'),
            total_claim_amount=data.get('total_claim_amount'),
            is_address_farm=data.get('is_address_farm')
        )
    
    def to_dict(self) -> dict:
        """
        Convert Address instance to dictionary
        
        Returns:
            Dictionary representation
        """
        return {
            'address_id': self.address_id,
            'street': self.street,
            'city': self.city,
            'state': self.state,
            'zip_code': self.zip_code,
            'created_at': self.created_at,
            'updated_at': self.updated_at,
            'resident_count': self.resident_count,
            'claim_count': self.claim_count,
            'total_claim_amount': self.total_claim_amount,
            'is_address_farm': self.is_address_farm
        }
    
    def get_full_address(self) -> str:
        """Get formatted full address"""
        return f"{self.street}, {self.city}, {self.state} {self.zip_code}"
    
    def get_short_address(self) -> str:
        """Get short address (city, state)"""
        return f"{self.city}, {self.state}"
    
    def is_farm(self, threshold: int = 3) -> bool:
        """Check if address is an address farm"""
        if self.is_address_farm is not None:
            return self.is_address_farm
        
        return self.resident_count and self.resident_count >= threshold
    
    def has_multiple_residents(self) -> bool:
        """Check if address has multiple residents"""
        return self.resident_count and self.resident_count > 1
    
    def is_high_activity(self, claim_threshold: int = 5) -> bool:
        """Check if address has high claim activity"""
        return self.claim_count and self.claim_count >= claim_threshold
    
    def __repr__(self) -> str:
        return f"Address(address_id={self.address_id}, address={self.get_full_address()})"
