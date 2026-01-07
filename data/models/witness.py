"""
Witness Model - Data model for accident witnesses
"""
from dataclasses import dataclass
from typing import Dict


@dataclass
class Witness:
    """
    Witness model representing people who witnessed accidents
    """
    witness_id: str
    name: str
    phone: str
    
    def to_dict(self) -> Dict:
        """Convert witness to dictionary for Neo4j"""
        return {
            'witness_id': self.witness_id,
            'name': self.name,
            'phone': self.phone
        }
    
    @classmethod
    def from_dict(cls, data: Dict) -> 'Witness':
        """Create witness from dictionary"""
        return cls(
            witness_id=data['witness_id'],
            name=data['name'],
            phone=data['phone']
        )
    
    def get_formatted_phone(self) -> str:
        """
        Get formatted phone number
        Assumes 10-digit US phone format: (XXX) XXX-XXXX
        """
        # Remove any non-digit characters
        digits = ''.join(filter(str.isdigit, self.phone))
        
        if len(digits) == 10:
            return f"({digits[:3]}) {digits[3:6]}-{digits[6:]}"
        else:
            return self.phone  # Return as-is if not standard format
    
    def get_contact_info(self) -> str:
        """Get contact information string"""
        return f"{self.name} - {self.get_formatted_phone()}"
    
    def __str__(self) -> str:
        """String representation of witness"""
        return f"Witness({self.name}, {self.phone})"
    
    def __repr__(self) -> str:
        """Developer representation of witness"""
        return f"Witness(witness_id='{self.witness_id}', name='{self.name}')"
    
    def __eq__(self, other) -> bool:
        """Check equality based on witness_id"""
        if not isinstance(other, Witness):
            return False
        return self.witness_id == other.witness_id
    
    def __hash__(self) -> int:
        """Hash based on witness_id for use in sets and dicts"""
        return hash(self.witness_id)
