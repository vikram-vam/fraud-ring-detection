"""
Body Shop Model - Data model for auto body repair shops
"""
from dataclasses import dataclass
from typing import Dict, Optional


@dataclass
class BodyShop:
    """
    Body shop model representing auto repair facilities
    """
    body_shop_id: str
    name: str
    license_number: str
    phone: str
    
    # Address
    street: str
    city: str
    state: str
    zip_code: str
    
    def to_dict(self) -> Dict:
        """Convert body shop to dictionary for Neo4j"""
        return {
            'body_shop_id': self.body_shop_id,
            'name': self.name,
            'license_number': self.license_number,
            'phone': self.phone,
            'street': self.street,
            'city': self.city,
            'state': self.state,
            'zip_code': self.zip_code
        }
    
    @classmethod
    def from_dict(cls, data: Dict) -> 'BodyShop':
        """Create body shop from dictionary"""
        return cls(
            body_shop_id=data['body_shop_id'],
            name=data['name'],
            license_number=data['license_number'],
            phone=data['phone'],
            street=data['street'],
            city=data['city'],
            state=data['state'],
            zip_code=data['zip_code']
        )
    
    def get_full_address(self) -> str:
        """Get full address string"""
        return f"{self.street}, {self.city}, {self.state} {self.zip_code}"
    
    def get_city_state(self) -> str:
        """Get city and state only"""
        return f"{self.city}, {self.state}"
    
    def get_display_name(self) -> str:
        """Get display name with location"""
        return f"{self.name} ({self.city}, {self.state})"
    
    def __str__(self) -> str:
        """String representation of body shop"""
        return f"BodyShop({self.name}, {self.city}, {self.state})"
    
    def __repr__(self) -> str:
        """Developer representation of body shop"""
        return f"BodyShop(body_shop_id='{self.body_shop_id}', name='{self.name}', city='{self.city}')"
