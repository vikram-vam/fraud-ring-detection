"""
Tow Company Model - Data model for towing companies
"""
from dataclasses import dataclass
from typing import Dict


@dataclass
class TowCompany:
    """
    Tow company model representing vehicle towing services
    """
    tow_company_id: str
    name: str
    license_number: str
    phone: str
    city: str
    state: str
    
    def to_dict(self) -> Dict:
        """Convert tow company to dictionary for Neo4j"""
        return {
            'tow_company_id': self.tow_company_id,
            'name': self.name,
            'license_number': self.license_number,
            'phone': self.phone,
            'city': self.city,
            'state': self.state
        }
    
    @classmethod
    def from_dict(cls, data: Dict) -> 'TowCompany':
        """Create tow company from dictionary"""
        return cls(
            tow_company_id=data['tow_company_id'],
            name=data['name'],
            license_number=data['license_number'],
            phone=data['phone'],
            city=data['city'],
            state=data['state']
        )
    
    def get_city_state(self) -> str:
        """Get city and state"""
        return f"{self.city}, {self.state}"
    
    def get_display_name(self) -> str:
        """Get display name with location"""
        return f"{self.name} ({self.city}, {self.state})"
    
    def __str__(self) -> str:
        """String representation of tow company"""
        return f"TowCompany({self.name}, {self.city}, {self.state})"
    
    def __repr__(self) -> str:
        """Developer representation of tow company"""
        return f"TowCompany(tow_company_id='{self.tow_company_id}', name='{self.name}', city='{self.city}')"
