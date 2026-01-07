"""
Vehicle Model - Data model for vehicles in auto insurance claims
"""
from dataclasses import dataclass
from typing import Dict


@dataclass
class Vehicle:
    """
    Vehicle model representing vehicles involved in accidents
    """
    vehicle_id: str
    vin: str
    license_plate: str
    make: str
    model: str
    year: str
    color: str
    
    def to_dict(self) -> Dict:
        """Convert vehicle to dictionary for Neo4j"""
        return {
            'vehicle_id': self.vehicle_id,
            'vin': self.vin,
            'license_plate': self.license_plate,
            'make': self.make,
            'model': self.model,
            'year': self.year,
            'color': self.color
        }
    
    @classmethod
    def from_dict(cls, data: Dict) -> 'Vehicle':
        """Create vehicle from dictionary"""
        return cls(
            vehicle_id=data['vehicle_id'],
            vin=data['vin'],
            license_plate=data['license_plate'],
            make=data['make'],
            model=data['model'],
            year=data['year'],
            color=data['color']
        )
    
    def get_full_description(self) -> str:
        """Get full vehicle description"""
        return f"{self.year} {self.make} {self.model} ({self.color})"
    
    def get_short_description(self) -> str:
        """Get short vehicle description"""
        return f"{self.make} {self.model} {self.year}"
    
    def __str__(self) -> str:
        """String representation of vehicle"""
        return f"Vehicle({self.get_full_description()}, VIN: {self.vin}, Plate: {self.license_plate})"
    
    def __repr__(self) -> str:
        """Developer representation of vehicle"""
        return f"Vehicle(vehicle_id='{self.vehicle_id}', vin='{self.vin}', make='{self.make}', model='{self.model}')"
