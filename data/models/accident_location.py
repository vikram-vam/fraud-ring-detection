"""
Accident Location Model - Data model for accident locations
"""
from dataclasses import dataclass
from typing import Dict, Optional, Tuple
import math


@dataclass
class AccidentLocation:
    """
    Accident location model representing where accidents occurred
    """
    location_id: str
    intersection: str
    city: str
    state: str
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    
    def to_dict(self) -> Dict:
        """Convert accident location to dictionary for Neo4j"""
        data = {
            'location_id': self.location_id,
            'intersection': self.intersection,
            'city': self.city,
            'state': self.state
        }
        
        if self.latitude is not None:
            data['latitude'] = self.latitude
        if self.longitude is not None:
            data['longitude'] = self.longitude
        
        return data
    
    @classmethod
    def from_dict(cls, data: Dict) -> 'AccidentLocation':
        """Create accident location from dictionary"""
        return cls(
            location_id=data['location_id'],
            intersection=data['intersection'],
            city=data['city'],
            state=data['state'],
            latitude=data.get('latitude'),
            longitude=data.get('longitude')
        )
    
    def get_full_address(self) -> str:
        """Get full address string"""
        return f"{self.intersection}, {self.city}, {self.state}"
    
    def get_city_state(self) -> str:
        """Get city and state only"""
        return f"{self.city}, {self.state}"
    
    def has_coordinates(self) -> bool:
        """Check if location has GPS coordinates"""
        return self.latitude is not None and self.longitude is not None
    
    def get_coordinates(self) -> Optional[Tuple[float, float]]:
        """Get coordinates as tuple (latitude, longitude)"""
        if self.has_coordinates():
            return (self.latitude, self.longitude)
        return None
    
    def calculate_distance_to(self, other: 'AccidentLocation') -> Optional[float]:
        """
        Calculate distance to another location in kilometers using Haversine formula
        
        Args:
            other: Another AccidentLocation object
            
        Returns:
            Distance in kilometers, or None if coordinates not available
        """
        if not self.has_coordinates() or not other.has_coordinates():
            return None
        
        # Haversine formula
        R = 6371  # Earth's radius in kilometers
        
        lat1_rad = math.radians(self.latitude)
        lat2_rad = math.radians(other.latitude)
        dlat = math.radians(other.latitude - self.latitude)
        dlon = math.radians(other.longitude - self.longitude)
        
        a = (math.sin(dlat / 2) ** 2 + 
             math.cos(lat1_rad) * math.cos(lat2_rad) * 
             math.sin(dlon / 2) ** 2)
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
        
        distance = R * c
        return round(distance, 2)
    
    def is_near(self, other: 'AccidentLocation', threshold_km: float = 5.0) -> bool:
        """
        Check if this location is near another location
        
        Args:
            other: Another AccidentLocation object
            threshold_km: Distance threshold in kilometers (default 5km)
            
        Returns:
            True if within threshold, False otherwise
        """
        distance = self.calculate_distance_to(other)
        if distance is None:
            return False
        return distance <= threshold_km
    
    def __str__(self) -> str:
        """String representation of accident location"""
        coords = f" ({self.latitude}, {self.longitude})" if self.has_coordinates() else ""
        return f"AccidentLocation({self.intersection}, {self.city}, {self.state}{coords})"
    
    def __repr__(self) -> str:
        """Developer representation of accident location"""
        return f"AccidentLocation(location_id='{self.location_id}', intersection='{self.intersection}', city='{self.city}')"
