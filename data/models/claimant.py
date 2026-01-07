"""
Claimant Model - Represents a claimant entity
"""
from dataclasses import dataclass
from typing import Optional
from datetime import datetime, date


@dataclass
class Claimant:
    """
    Claimant entity model
    """
    claimant_id: str
    name: str
    email: Optional[str] = None
    date_of_birth: Optional[date] = None
    ssn: Optional[str] = None
    phone: Optional[str] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    
    # Aggregated statistics (populated from queries)
    total_claims: Optional[int] = None
    total_claimed: Optional[float] = None
    avg_claim_amount: Optional[float] = None
    avg_risk_score: Optional[float] = None
    ring_memberships: Optional[int] = None
    
    @classmethod
    def from_dict(cls, data: dict) -> 'Claimant':
        """
        Create Claimant instance from dictionary
        
        Args:
            data: Dictionary with claimant data
            
        Returns:
            Claimant instance
        """
        return cls(
            claimant_id=data.get('claimant_id'),
            name=data.get('name'),
            email=data.get('email'),
            date_of_birth=data.get('date_of_birth'),
            ssn=data.get('ssn'),
            phone=data.get('phone'),
            created_at=data.get('created_at'),
            updated_at=data.get('updated_at'),
            total_claims=data.get('total_claims') or data.get('claim_count'),
            total_claimed=data.get('total_claimed'),
            avg_claim_amount=data.get('avg_claim_amount'),
            avg_risk_score=data.get('avg_risk_score'),
            ring_memberships=data.get('ring_memberships') or data.get('ring_count')
        )
    
    def to_dict(self) -> dict:
        """
        Convert Claimant instance to dictionary
        
        Returns:
            Dictionary representation
        """
        return {
            'claimant_id': self.claimant_id,
            'name': self.name,
            'email': self.email,
            'date_of_birth': self.date_of_birth,
            'ssn': self.ssn,
            'phone': self.phone,
            'created_at': self.created_at,
            'updated_at': self.updated_at,
            'total_claims': self.total_claims,
            'total_claimed': self.total_claimed,
            'avg_claim_amount': self.avg_claim_amount,
            'avg_risk_score': self.avg_risk_score,
            'ring_memberships': self.ring_memberships
        }
    
    def get_risk_level(self) -> str:
        """Get overall risk level based on average risk score"""
        if not self.avg_risk_score:
            return 'UNKNOWN'
        
        if self.avg_risk_score >= 70:
            return 'HIGH'
        elif self.avg_risk_score >= 40:
            return 'MEDIUM'
        else:
            return 'LOW'
    
    def is_frequent_filer(self, threshold: int = 5) -> bool:
        """Check if claimant is a frequent filer"""
        return self.total_claims and self.total_claims >= threshold
    
    def is_high_value_claimant(self, threshold: float = 50000.0) -> bool:
        """Check if claimant has high total claimed amount"""
        return self.total_claimed and self.total_claimed >= threshold
    
    def is_ring_member(self) -> bool:
        """Check if claimant is member of any fraud ring"""
        return self.ring_memberships and self.ring_memberships > 0
    
    def get_age(self) -> Optional[int]:
        """Calculate age from date of birth"""
        if not self.date_of_birth:
            return None
        
        try:
            if isinstance(self.date_of_birth, str):
                dob = datetime.strptime(self.date_of_birth, '%Y-%m-%d').date()
            else:
                dob = self.date_of_birth
            
            today = date.today()
            age = today.year - dob.year - ((today.month, today.day) < (dob.month, dob.day))
            return age
        except:
            return None
    
    def __repr__(self) -> str:
        return f"Claimant(claimant_id={self.claimant_id}, name={self.name}, claims={self.total_claims})"
