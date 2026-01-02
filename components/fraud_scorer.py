"""
FRAUD SCORER COMPONENT
Wrapper for fraud detection scoring
"""

from fraud_detection import FraudDetector

class FraudScorer:
    def __init__(self, driver):
        self.detector = FraudDetector(driver)
    
    def score_claim(self, claim_id: str):
        """Score a claim for fraud propensity"""
        return self.detector.calculate_fraud_propensity_for_claim(claim_id)
    
    def get_fraud_rings(self):
        """Get detected fraud rings"""
        return self.detector.detect_fraud_rings_by_patterns()
