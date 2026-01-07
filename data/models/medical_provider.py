"""
Medical Provider Model - Data model for medical providers and facilities
"""
from dataclasses import dataclass
from typing import Dict
from enum import Enum


class ProviderType(Enum):
    """Medical provider type enumeration"""
    CHIROPRACTOR = "Chiropractor"
    PHYSICAL_THERAPIST = "Physical Therapist"
    CLINIC = "Clinic"
    HOSPITAL = "Hospital"
    URGENT_CARE = "Urgent Care"
    ORTHOPEDIC = "Orthopedic"
    NEUROLOGIST = "Neurologist"
    PAIN_MANAGEMENT = "Pain Management"


@dataclass
class MedicalProvider:
    """
    Medical provider model representing healthcare facilities and practitioners
    """
    provider_id: str
    name: str
    provider_type: str  # Chiropractor, Physical Therapist, Clinic, Hospital, etc.
    license_number: str
    phone: str
    
    # Address
    street: str
    city: str
    state: str
    zip_code: str
    
    def to_dict(self) -> Dict:
        """Convert medical provider to dictionary for Neo4j"""
        return {
            'provider_id': self.provider_id,
            'name': self.name,
            'provider_type': self.provider_type,
            'license_number': self.license_number,
            'phone': self.phone,
            'street': self.street,
            'city': self.city,
            'state': self.state,
            'zip_code': self.zip_code
        }
    
    @classmethod
    def from_dict(cls, data: Dict) -> 'MedicalProvider':
        """Create medical provider from dictionary"""
        return cls(
            provider_id=data['provider_id'],
            name=data['name'],
            provider_type=data['provider_type'],
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
        """Get display name with type and location"""
        return f"{self.name} ({self.provider_type}, {self.city})"
    
    def is_soft_tissue_specialist(self) -> bool:
        """Check if provider specializes in soft tissue injuries"""
        soft_tissue_types = ['Chiropractor', 'Physical Therapist', 'Pain Management']
        return self.provider_type in soft_tissue_types
    
    def __str__(self) -> str:
        """String representation of medical provider"""
        return f"MedicalProvider({self.name}, {self.provider_type}, {self.city})"
    
    def __repr__(self) -> str:
        """Developer representation of medical provider"""
        return f"MedicalProvider(provider_id='{self.provider_id}', name='{self.name}', type='{self.provider_type}')"
