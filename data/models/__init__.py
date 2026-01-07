"""
Data Models Module
Entity models for fraud detection system
"""
from data.models.fraud_ring import FraudRing
from data.models.claim import Claim
from data.models.claimant import Claimant
from data.models.provider import Provider
from data.models.attorney import Attorney
from data.models.address import Address

__all__ = [
    'FraudRing',
    'Claim',
    'Claimant',
    'Provider',
    'Attorney',
    'Address'
]
