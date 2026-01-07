"""
Claim Model - Data models for auto insurance entities
Defines structure for claims, claimants, vehicles, and related entities
"""
from dataclasses import dataclass, field
from datetime import datetime, date
from typing import Optional, List, Dict
from enum import Enum


class ClaimStatus(Enum):
    """Claim status enumeration"""
    OPEN = "Open"
    UNDER_INVESTIGATION = "Under Investigation"
    UNDER_REVIEW = "Under Review"
    PENDING_PAYMENT = "Pending Payment"
    CLOSED = "Closed"
    DENIED = "Denied"


class AccidentType(Enum):
    """Accident type enumeration"""
    REAR_END_COLLISION = "Rear-End Collision"
    SIDE_IMPACT_COLLISION = "Side-Impact Collision"
    HEAD_ON_COLLISION = "Head-On Collision"
    HIT_AND_RUN = "Hit and Run"
    SINGLE_VEHICLE_ACCIDENT = "Single Vehicle Accident"
    PARKING_LOT_COLLISION = "Parking Lot Collision"
    INTERSECTION_COLLISION = "Intersection Collision"
    MULTI_VEHICLE_PILEUP = "Multi-Vehicle Pileup"


class InjuryType(Enum):
    """Injury type enumeration"""
    NO_INJURY = "No Injury"
    WHIPLASH = "Whiplash"
    BACK_PAIN = "Back Pain"
    NECK_PAIN = "Neck Pain"
    SOFT_TISSUE_INJURY = "Soft Tissue Injury"
    HEADACHES = "Headaches"
    SHOULDER_PAIN = "Shoulder Pain"
    KNEE_INJURY = "Knee Injury"
    HIP_INJURY = "Hip Injury"


@dataclass
class Claim:
    """
    Auto insurance claim model
    """
    claim_id: str
    claim_number: str
    claimant_id: str
    
    # Accident information
    accident_date: date
    report_date: date
    accident_type: str
    injury_type: str = "No Injury"
    
    # Financial information
    total_claim_amount: float = 0.0
    property_damage_amount: float = 0.0
    bodily_injury_amount: float = 0.0
    
    # Status and risk
    status: str = "Open"
    risk_score: float = 0.0
    
    # Metadata
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    
    def __post_init__(self):
        """Validate and convert data types"""
        # Convert string dates to date objects if needed
        if isinstance(self.accident_date, str):
            self.accident_date = datetime.strptime(self.accident_date, '%Y-%m-%d').date()
        
        if isinstance(self.report_date, str):
            self.report_date = datetime.strptime(self.report_date, '%Y-%m-%d').date()
        
        # Ensure amounts are floats
        self.total_claim_amount = float(self.total_claim_amount)
        self.property_damage_amount = float(self.property_damage_amount)
        self.bodily_injury_amount = float(self.bodily_injury_amount)
        self.risk_score = float(self.risk_score)
    
    def to_dict(self) -> Dict:
        """Convert claim to dictionary for Neo4j"""
        return {
            'claim_id': self.claim_id,
            'claim_number': self.claim_number,
            'claimant_id': self.claimant_id,
            'accident_date': self.accident_date.strftime('%Y-%m-%d'),
            'report_date': self.report_date.strftime('%Y-%m-%d'),
            'accident_type': self.accident_type,
            'injury_type': self.injury_type,
            'total_claim_amount': self.total_claim_amount,
            'property_damage_amount': self.property_damage_amount,
            'bodily_injury_amount': self.bodily_injury_amount,
            'status': self.status,
            'risk_score': self.risk_score
        }
    
    @classmethod
    def from_dict(cls, data: Dict) -> 'Claim':
        """Create claim from dictionary"""
        return cls(
            claim_id=data['claim_id'],
            claim_number=data['claim_number'],
            claimant_id=data['claimant_id'],
            accident_date=data['accident_date'],
            report_date=data['report_date'],
            accident_type=data['accident_type'],
            injury_type=data.get('injury_type', 'No Injury'),
            total_claim_amount=data.get('total_claim_amount', 0.0),
            property_damage_amount=data.get('property_damage_amount', 0.0),
            bodily_injury_amount=data.get('bodily_injury_amount', 0.0),
            status=data.get('status', 'Open'),
            risk_score=data.get('risk_score', 0.0)
        )
    
    def days_to_report(self) -> int:
        """Calculate days between accident and report"""
        return (self.report_date - self.accident_date).days
    
    def is_high_risk(self) -> bool:
        """Check if claim is high risk"""
        return self.risk_score >= 70
    
    def has_injury_claim(self) -> bool:
        """Check if claim includes injury"""
        return self.bodily_injury_amount > 0


@dataclass
class Claimant:
    """
    Insurance claimant model
    """
    claimant_id: str
    name: str
    email: str
    phone: str
    
    # Personal information
    drivers_license: str
    date_of_birth: Optional[date] = None
    
    # Address
    street: Optional[str] = None
    city: Optional[str] = None
    state: Optional[str] = None
    zip_code: Optional[str] = None
    
    # Metadata
    created_at: Optional[datetime] = None
    
    def to_dict(self) -> Dict:
        """Convert claimant to dictionary"""
        data = {
            'claimant_id': self.claimant_id,
            'name': self.name,
            'email': self.email,
            'phone': self.phone,
            'drivers_license': self.drivers_license
        }
        
        if self.date_of_birth:
            dob = self.date_of_birth if isinstance(self.date_of_birth, str) else self.date_of_birth.strftime('%Y-%m-%d')
            data['date_of_birth'] = dob
        
        if self.street:
            data['street'] = self.street
        if self.city:
            data['city'] = self.city
        if self.state:
            data['state'] = self.state
        if self.zip_code:
            data['zip_code'] = self.zip_code
        
        return data
    
    @classmethod
    def from_dict(cls, data: Dict) -> 'Claimant':
        """Create claimant from dictionary"""
        return cls(
            claimant_id=data['claimant_id'],
            name=data['name'],
            email=data['email'],
            phone=data['phone'],
            drivers_license=data['drivers_license'],
            date_of_birth=data.get('date_of_birth'),
            street=data.get('street'),
            city=data.get('city'),
            state=data.get('state'),
            zip_code=data.get('zip_code')
        )


@dataclass
class Vehicle:
    """
    Vehicle model
    """
    vehicle_id: str
    vin: str
    license_plate: str
    make: str
    model: str
    year: str
    color: str
    
    def to_dict(self) -> Dict:
        """Convert vehicle to dictionary"""
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


@dataclass
class BodyShop:
    """
    Body shop model
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
        """Convert body shop to dictionary"""
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


@dataclass
class MedicalProvider:
    """
    Medical provider model
    """
    provider_id: str
    name: str
    provider_type: str  # Chiropractor, Physical Therapist, Clinic, Hospital
    license_number: str
    phone: str
    
    # Address
    street: str
    city: str
    state: str
    zip_code: str
    
    def to_dict(self) -> Dict:
        """Convert medical provider to dictionary"""
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


@dataclass
class Attorney:
    """
    Attorney model
    """
    attorney_id: str
    name: str
    firm: str
    bar_number: str
    phone: str
    email: str
    
    # Address
    street: str
    city: str
    state: str
    zip_code: str
    
    def to_dict(self) -> Dict:
        """Convert attorney to dictionary"""
        return {
            'attorney_id': self.attorney_id,
            'name': self.name,
            'firm': self.firm,
            'bar_number': self.bar_number,
            'phone': self.phone,
            'email': self.email,
            'street': self.street,
            'city': self.city,
            'state': self.state,
            'zip_code': self.zip_code
        }
    
    @classmethod
    def from_dict(cls, data: Dict) -> 'Attorney':
        """Create attorney from dictionary"""
        return cls(
            attorney_id=data['attorney_id'],
            name=data['name'],
            firm=data['firm'],
            bar_number=data['bar_number'],
            phone=data['phone'],
            email=data['email'],
            street=data['street'],
            city=data['city'],
            state=data['state'],
            zip_code=data['zip_code']
        )


@dataclass
class TowCompany:
    """
    Tow company model
    """
    tow_company_id: str
    name: str
    license_number: str
    phone: str
    city: str
    state: str
    
    def to_dict(self) -> Dict:
        """Convert tow company to dictionary"""
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


@dataclass
class AccidentLocation:
    """
    Accident location model
    """
    location_id: str
    intersection: str
    city: str
    state: str
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    
    def to_dict(self) -> Dict:
        """Convert accident location to dictionary"""
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


@dataclass
class Witness:
    """
    Witness model
    """
    witness_id: str
    name: str
    phone: str
    
    def to_dict(self) -> Dict:
        """Convert witness to dictionary"""
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


@dataclass
class FraudRing:
    """
    Fraud ring model
    """
    ring_id: str
    ring_type: str  # KNOWN, DISCOVERED, SUSPICIOUS, EMERGING
    pattern_type: str  # staged_accident, body_shop_fraud, medical_mill, etc.
    status: str  # CONFIRMED, UNDER_REVIEW, DISMISSED
    confidence_score: float
    member_count: int
    estimated_fraud_amount: float
    discovered_date: date
    discovered_by: str = "System"
    
    def to_dict(self) -> Dict:
        """Convert fraud ring to dictionary"""
        return {
            'ring_id': self.ring_id,
            'ring_type': self.ring_type,
            'pattern_type': self.pattern_type,
            'status': self.status,
            'confidence_score': self.confidence_score,
            'member_count': self.member_count,
            'estimated_fraud_amount': self.estimated_fraud_amount,
            'discovered_date': self.discovered_date.strftime('%Y-%m-%d') if isinstance(self.discovered_date, date) else self.discovered_date,
            'discovered_by': self.discovered_by
        }
    
    @classmethod
    def from_dict(cls, data: Dict) -> 'FraudRing':
        """Create fraud ring from dictionary"""
        return cls(
            ring_id=data['ring_id'],
            ring_type=data['ring_type'],
            pattern_type=data['pattern_type'],
            status=data['status'],
            confidence_score=data['confidence_score'],
            member_count=data['member_count'],
            estimated_fraud_amount=data['estimated_fraud_amount'],
            discovered_date=data['discovered_date'],
            discovered_by=data.get('discovered_by', 'System')
        )
