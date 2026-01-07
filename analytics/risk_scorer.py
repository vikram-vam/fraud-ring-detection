"""
Risk Scorer - Calculate risk scores for auto insurance fraud detection
Analyzes claims, claimants, and entities for fraud indicators
"""
import logging
from typing import Dict, List, Optional
from datetime import datetime, timedelta

from data.neo4j_driver import get_neo4j_driver
from utils.logger import setup_logger

logger = setup_logger(__name__)


class RiskScorer:
    """
    Risk scoring engine for auto insurance fraud detection
    Calculates risk scores based on multiple fraud indicators
    """
    
    def __init__(self):
        self.driver = get_neo4j_driver()
        
        # Risk scoring weights for auto insurance
        self.weights = {
            # Claim-level factors
            'claim_amount': 0.15,
            'reporting_delay': 0.12,
            'injury_severity': 0.10,
            'witness_suspicious': 0.08,
            'location_hotspot': 0.10,
            
            # Entity-level factors
            'body_shop_risk': 0.10,
            'medical_provider_risk': 0.10,
            'attorney_risk': 0.08,
            'tow_company_risk': 0.05,
            
            # Network factors
            'fraud_ring_member': 0.20,
            'repeat_entities': 0.12,
            'vehicle_history': 0.10
        }
    
    def calculate_claim_risk_score(self, claim_id: str) -> Dict:
        """
        Calculate comprehensive risk score for a claim
        
        Args:
            claim_id: Claim identifier
            
        Returns:
            Dictionary with risk score and breakdown
        """
        try:
            # Get claim data
            claim_data = self._get_claim_data(claim_id)
            
            if not claim_data:
                logger.warning(f"No data found for claim {claim_id}")
                return {'error': 'Claim not found'}
            
            # Calculate individual risk factors
            risk_factors = {}
            
            # 1. Claim amount risk
            risk_factors['claim_amount'] = self._score_claim_amount(claim_data)
            
            # 2. Reporting delay risk
            risk_factors['reporting_delay'] = self._score_reporting_delay(claim_data)
            
            # 3. Injury severity vs damage consistency
            risk_factors['injury_severity'] = self._score_injury_consistency(claim_data)
            
            # 4. Witness credibility
            risk_factors['witness_suspicious'] = self._score_witness_credibility(claim_id)
            
            # 5. Location hotspot risk
            risk_factors['location_hotspot'] = self._score_location_risk(claim_id)
            
            # 6. Body shop risk
            risk_factors['body_shop_risk'] = self._score_body_shop_risk(claim_id)
            
            # 7. Medical provider risk
            risk_factors['medical_provider_risk'] = self._score_medical_provider_risk(claim_id)
            
            # 8. Attorney risk
            risk_factors['attorney_risk'] = self._score_attorney_risk(claim_id)
            
            # 9. Tow company risk
            risk_factors['tow_company_risk'] = self._score_tow_company_risk(claim_id)
            
            # 10. Fraud ring membership
            risk_factors['fraud_ring_member'] = self._score_fraud_ring_membership(claim_id)
            
            # 11. Repeat entity patterns
            risk_factors['repeat_entities'] = self._score_repeat_entity_patterns(claim_id)
            
            # 12. Vehicle history
            risk_factors['vehicle_history'] = self._score_vehicle_history(claim_id)
            
            # Calculate weighted total risk score
            total_risk = 0.0
            weighted_factors = {}
            
            for factor, score in risk_factors.items():
                weight = self.weights.get(factor, 0)
                weighted_score = score * weight
                weighted_factors[factor] = {
                    'raw_score': round(score, 2),
                    'weight': weight,
                    'weighted_score': round(weighted_score, 2)
                }
                total_risk += weighted_score
            
            # Normalize to 0-100 scale
            total_risk = min(total_risk * 100, 100)
            
            # Generate risk explanation
            explanation = self._generate_risk_explanation(weighted_factors, total_risk)
            
            return {
                'claim_id': claim_id,
                'claim_number': claim_data.get('claim_number'),
                'total_risk_score': round(total_risk, 2),
                'risk_level': self._get_risk_level(total_risk),
                'risk_factors': weighted_factors,
                'explanation': explanation,
                'top_risk_factors': self._get_top_risk_factors(weighted_factors, top_n=5)
            }
            
        except Exception as e:
            logger.error(f"Error calculating risk score for claim {claim_id}: {e}", exc_info=True)
            return {'error': str(e)}
    
    def calculate_claimant_risk_score(self, claimant_id: str) -> Dict:
        """
        Calculate risk score for a claimant based on history and patterns
        
        Args:
            claimant_id: Claimant identifier
            
        Returns:
            Dictionary with risk score and analysis
        """
        try:
            query = """
            MATCH (c:Claimant {claimant_id: $claimant_id})-[:FILED]->(cl:Claim)
            
            OPTIONAL MATCH (c)-[:MEMBER_OF]->(r:FraudRing)
            
            WITH c, 
                 count(cl) as claim_count,
                 sum(cl.total_claim_amount) as total_claimed,
                 avg(cl.risk_score) as avg_claim_risk,
                 collect(DISTINCT r.ring_id) as rings
            
            RETURN 
                c.name as name,
                claim_count,
                total_claimed,
                avg_claim_risk,
                rings
            """
            
            results = self.driver.execute_query(query, {'claimant_id': claimant_id})
            
            if not results:
                return {'error': 'Claimant not found'}
            
            data = results[0]
            
            # Calculate claimant risk factors
            risk_score = 0.0
            factors = {}
            
            # 1. High claim frequency (0-30 points)
            claim_count = data.get('claim_count', 0)
            if claim_count >= 5:
                factors['claim_frequency'] = 30
            elif claim_count >= 3:
                factors['claim_frequency'] = 20
            elif claim_count >= 2:
                factors['claim_frequency'] = 10
            else:
                factors['claim_frequency'] = 0
            
            # 2. High total claimed amount (0-20 points)
            total_claimed = data.get('total_claimed', 0)
            if total_claimed >= 200000:
                factors['total_amount'] = 20
            elif total_claimed >= 100000:
                factors['total_amount'] = 15
            elif total_claimed >= 50000:
                factors['total_amount'] = 10
            else:
                factors['total_amount'] = 0
            
            # 3. Average claim risk (0-30 points)
            avg_claim_risk = data.get('avg_claim_risk', 0)
            factors['avg_claim_risk'] = min(avg_claim_risk * 0.3, 30)
            
            # 4. Fraud ring membership (0-40 points)
            rings = [r for r in data.get('rings', []) if r]
            if rings:
                factors['fraud_ring_member'] = 40
            else:
                factors['fraud_ring_member'] = 0
            
            # Calculate total
            risk_score = sum(factors.values())
            
            return {
                'claimant_id': claimant_id,
                'name': data.get('name'),
                'risk_score': round(risk_score, 2),
                'risk_level': self._get_risk_level(risk_score),
                'claim_count': claim_count,
                'total_claimed': total_claimed,
                'avg_claim_risk': round(avg_claim_risk, 2),
                'fraud_rings': len(rings),
                'risk_factors': factors
            }
            
        except Exception as e:
            logger.error(f"Error calculating claimant risk: {e}", exc_info=True)
            return {'error': str(e)}
    
    def calculate_vehicle_risk_score(self, vehicle_id: str) -> Dict:
        """
        Calculate risk score for a vehicle
        
        Args:
            vehicle_id: Vehicle identifier
            
        Returns:
            Dictionary with vehicle risk analysis
        """
        try:
            query = """
            MATCH (v:Vehicle {vehicle_id: $vehicle_id})<-[:INVOLVES_VEHICLE]-(cl:Claim)
            MATCH (c:Claimant)-[:FILED]->(cl)
            
            WITH v, 
                 count(cl) as accident_count,
                 count(DISTINCT c) as unique_claimants,
                 sum(cl.total_claim_amount) as total_damage,
                 avg(cl.risk_score) as avg_risk
            
            RETURN 
                v.make + ' ' + v.model + ' ' + v.year as vehicle_info,
                v.vin as vin,
                accident_count,
                unique_claimants,
                total_damage,
                avg_risk
            """
            
            results = self.driver.execute_query(query, {'vehicle_id': vehicle_id})
            
            if not results:
                return {'error': 'Vehicle not found'}
            
            data = results[0]
            
            risk_score = 0.0
            factors = {}
            
            # 1. Multiple accidents (0-40 points)
            accident_count = data.get('accident_count', 0)
            if accident_count >= 4:
                factors['accident_frequency'] = 40
            elif accident_count >= 3:
                factors['accident_frequency'] = 30
            elif accident_count >= 2:
                factors['accident_frequency'] = 15
            else:
                factors['accident_frequency'] = 0
            
            # 2. Multiple claimants (0-30 points) - HIGHLY SUSPICIOUS
            unique_claimants = data.get('unique_claimants', 0)
            if unique_claimants >= 3:
                factors['multiple_claimants'] = 30
            elif unique_claimants >= 2:
                factors['multiple_claimants'] = 20
            else:
                factors['multiple_claimants'] = 0
            
            # 3. Average claim risk (0-30 points)
            avg_risk = data.get('avg_risk', 0)
            factors['avg_claim_risk'] = min(avg_risk * 0.3, 30)
            
            risk_score = sum(factors.values())
            
            return {
                'vehicle_id': vehicle_id,
                'vehicle_info': data.get('vehicle_info'),
                'vin': data.get('vin'),
                'risk_score': round(risk_score, 2),
                'risk_level': self._get_risk_level(risk_score),
                'accident_count': accident_count,
                'unique_claimants': unique_claimants,
                'total_damage': data.get('total_damage', 0),
                'risk_factors': factors
            }
            
        except Exception as e:
            logger.error(f"Error calculating vehicle risk: {e}", exc_info=True)
            return {'error': str(e)}
    
    def calculate_entity_risk_score(
        self, 
        entity_type: str, 
        entity_id: str
    ) -> Dict:
        """
        Calculate risk score for body shops, medical providers, attorneys
        
        Args:
            entity_type: Type of entity (BodyShop, MedicalProvider, Attorney)
            entity_id: Entity identifier
            
        Returns:
            Dictionary with entity risk analysis
        """
        try:
            # Map entity types to relationships
            rel_map = {
                'BodyShop': ('REPAIRED_AT', 'body_shop_id'),
                'MedicalProvider': ('TREATED_BY', 'provider_id'),
                'Attorney': ('REPRESENTED_BY', 'attorney_id'),
                'TowCompany': ('TOWED_BY', 'tow_company_id')
            }
            
            if entity_type not in rel_map:
                return {'error': f'Unknown entity type: {entity_type}'}
            
            rel_type, id_field = rel_map[entity_type]
            
            query = f"""
            MATCH (e:{entity_type} {{{id_field}: $entity_id}})
            MATCH (cl:Claim)-[:{rel_type}]->(e)
            MATCH (c:Claimant)-[:FILED]->(cl)
            
            OPTIONAL MATCH (c)-[:MEMBER_OF]->(r:FraudRing)
            
            WITH e,
                 count(cl) as claim_count,
                 count(DISTINCT c) as unique_claimants,
                 sum(cl.total_claim_amount) as total_amount,
                 avg(cl.risk_score) as avg_risk,
                 count(DISTINCT r) as ring_count
            
            RETURN 
                e.name as name,
                claim_count,
                unique_claimants,
                total_amount,
                avg_risk,
                ring_count
            """
            
            results = self.driver.execute_query(query, {'entity_id': entity_id})
            
            if not results:
                return {'error': f'{entity_type} not found'}
            
            data = results[0]
            
            risk_score = 0.0
            factors = {}
            
            # 1. High volume (0-25 points)
            claim_count = data.get('claim_count', 0)
            if claim_count >= 50:
                factors['high_volume'] = 25
            elif claim_count >= 30:
                factors['high_volume'] = 20
            elif claim_count >= 20:
                factors['high_volume'] = 15
            elif claim_count >= 10:
                factors['high_volume'] = 10
            else:
                factors['high_volume'] = 0
            
            # 2. Average claim risk (0-30 points)
            avg_risk = data.get('avg_risk', 0)
            factors['avg_claim_risk'] = min(avg_risk * 0.3, 30)
            
            # 3. Fraud ring connections (0-35 points)
            ring_count = data.get('ring_count', 0)
            if ring_count >= 3:
                factors['ring_connections'] = 35
            elif ring_count >= 2:
                factors['ring_connections'] = 25
            elif ring_count >= 1:
                factors['ring_connections'] = 15
            else:
                factors['ring_connections'] = 0
            
            # 4. Concentration of claimants (0-10 points)
            unique_claimants = data.get('unique_claimants', 0)
            if claim_count > 0:
                concentration_ratio = unique_claimants / claim_count
                if concentration_ratio < 0.5:  # Same claimants repeatedly
                    factors['claimant_concentration'] = 10
                else:
                    factors['claimant_concentration'] = 0
            else:
                factors['claimant_concentration'] = 0
            
            risk_score = sum(factors.values())
            
            return {
                'entity_type': entity_type,
                'entity_id': entity_id,
                'name': data.get('name'),
                'risk_score': round(risk_score, 2),
                'risk_level': self._get_risk_level(risk_score),
                'claim_count': claim_count,
                'unique_claimants': unique_claimants,
                'total_amount': data.get('total_amount', 0),
                'avg_claim_risk': round(avg_risk, 2),
                'ring_connections': ring_count,
                'risk_factors': factors
            }
            
        except Exception as e:
            logger.error(f"Error calculating entity risk: {e}", exc_info=True)
            return {'error': str(e)}
    
    # Helper methods for claim risk scoring
    
    def _get_claim_data(self, claim_id: str) -> Optional[Dict]:
        """Get claim data for risk scoring"""
        query = """
        MATCH (c:Claimant)-[:FILED]->(cl:Claim {claim_id: $claim_id})
        RETURN 
            cl.claim_number as claim_number,
            cl.total_claim_amount as total_amount,
            cl.property_damage_amount as property_damage,
            cl.bodily_injury_amount as bodily_injury,
            cl.accident_date as accident_date,
            cl.report_date as report_date,
            cl.accident_type as accident_type,
            cl.injury_type as injury_type
        """
        
        results = self.driver.execute_query(query, {'claim_id': claim_id})
        return results[0] if results else None
    
    def _score_claim_amount(self, claim_data: Dict) -> float:
        """Score based on claim amount (0-1 scale)"""
        total_amount = claim_data.get('total_amount', 0)
        
        if total_amount >= 100000:
            return 1.0
        elif total_amount >= 75000:
            return 0.8
        elif total_amount >= 50000:
            return 0.6
        elif total_amount >= 30000:
            return 0.4
        elif total_amount >= 15000:
            return 0.2
        else:
            return 0.0
    
    def _score_reporting_delay(self, claim_data: Dict) -> float:
        """Score based on reporting delay (0-1 scale)"""
        try:
            accident_date = claim_data.get('accident_date')
            report_date = claim_data.get('report_date')
            
            if not accident_date or not report_date:
                return 0.0
            
            if isinstance(accident_date, str):
                accident_date = datetime.strptime(accident_date, '%Y-%m-%d')
            if isinstance(report_date, str):
                report_date = datetime.strptime(report_date, '%Y-%m-%d')
            
            days_diff = (report_date - accident_date).days
            
            # Same day or very delayed are both suspicious
            if days_diff == 0:
                return 0.8  # Suspiciously fast
            elif days_diff > 60:
                return 1.0  # Very delayed
            elif days_diff > 30:
                return 0.7  # Delayed
            elif days_diff > 14:
                return 0.3  # Slightly delayed
            else:
                return 0.0  # Normal
                
        except Exception as e:
            logger.error(f"Error scoring reporting delay: {e}")
            return 0.0
    
    def _score_injury_consistency(self, claim_data: Dict) -> float:
        """Score injury severity vs property damage consistency (0-1 scale)"""
        property_damage = claim_data.get('property_damage', 0)
        bodily_injury = claim_data.get('bodily_injury', 0)
        injury_type = claim_data.get('injury_type', 'No Injury')
        
        # High bodily injury with low property damage is suspicious
        if bodily_injury > 0 and property_damage > 0:
            ratio = bodily_injury / property_damage
            
            if ratio > 5:  # Injury claim >> property damage
                return 1.0
            elif ratio > 3:
                return 0.7
            elif ratio > 2:
                return 0.5
            else:
                return 0.0
        
        # No injury type but high bodily injury claim
        if injury_type == 'No Injury' and bodily_injury > 10000:
            return 0.9
        
        return 0.0
    
    def _score_witness_credibility(self, claim_id: str) -> float:
        """Score based on witness credibility (0-1 scale)"""
        query = """
        MATCH (cl:Claim {claim_id: $claim_id})<-[:WITNESSED]-(w:Witness)
        
        WITH w, count{(w)-[:WITNESSED]->(:Claim)} as witness_count
        
        RETURN w.witness_id as witness_id, witness_count
        ORDER BY witness_count DESC
        LIMIT 1
        """
        
        results = self.driver.execute_query(query, {'claim_id': claim_id})
        
        if not results:
            return 0.0
        
        witness_count = results[0].get('witness_count', 0)
        
        if witness_count >= 5:
            return 1.0  # Professional witness
        elif witness_count >= 3:
            return 0.8
        elif witness_count >= 2:
            return 0.5
        else:
            return 0.0
    
    def _score_location_risk(self, claim_id: str) -> float:
        """Score based on accident location hotspot (0-1 scale)"""
        query = """
        MATCH (cl:Claim {claim_id: $claim_id})-[:OCCURRED_AT]->(l:AccidentLocation)
        
        WITH l, count{(l)<-[:OCCURRED_AT]-(:Claim)} as location_count
        
        RETURN location_count
        """
        
        results = self.driver.execute_query(query, {'claim_id': claim_id})
        
        if not results:
            return 0.0
        
        location_count = results[0].get('location_count', 0)
        
        if location_count >= 10:
            return 1.0  # Major hotspot
        elif location_count >= 7:
            return 0.8
        elif location_count >= 5:
            return 0.6
        elif location_count >= 3:
            return 0.3
        else:
            return 0.0
    
    def _score_body_shop_risk(self, claim_id: str) -> float:
        """Score based on body shop risk (0-1 scale)"""
        return self._score_entity_risk(claim_id, 'BodyShop', 'REPAIRED_AT')
    
    def _score_medical_provider_risk(self, claim_id: str) -> float:
        """Score based on medical provider risk (0-1 scale)"""
        return self._score_entity_risk(claim_id, 'MedicalProvider', 'TREATED_BY')
    
    def _score_attorney_risk(self, claim_id: str) -> float:
        """Score based on attorney risk (0-1 scale)"""
        return self._score_entity_risk(claim_id, 'Attorney', 'REPRESENTED_BY')
    
    def _score_tow_company_risk(self, claim_id: str) -> float:
        """Score based on tow company risk (0-1 scale)"""
        return self._score_entity_risk(claim_id, 'TowCompany', 'TOWED_BY')
    
    def _score_entity_risk(self, claim_id: str, entity_type: str, rel_type: str) -> float:
        """Generic entity risk scoring"""
        query = f"""
        MATCH (cl:Claim {{claim_id: $claim_id}})-[:{rel_type}]->(e:{entity_type})
        
        // Get entity statistics
        OPTIONAL MATCH (e)<-[:{rel_type}]-(other_cl:Claim)
        OPTIONAL MATCH (c:Claimant)-[:FILED]->(other_cl)
        OPTIONAL MATCH (c)-[:MEMBER_OF]->(r:FraudRing)
        
        WITH e,
             count(DISTINCT other_cl) as entity_claim_count,
             avg(other_cl.risk_score) as avg_risk,
             count(DISTINCT r) as ring_count
        
        RETURN entity_claim_count, avg_risk, ring_count
        """
        
        results = self.driver.execute_query(query, {'claim_id': claim_id})
        
        if not results:
            return 0.0
        
        data = results[0]
        
        # High volume + high avg risk + ring connections = high risk
        entity_claim_count = data.get('entity_claim_count', 0)
        avg_risk = data.get('avg_risk', 0)
        ring_count = data.get('ring_count', 0)
        
        risk_score = 0.0
        
        # Volume factor
        if entity_claim_count >= 30:
            risk_score += 0.4
        elif entity_claim_count >= 20:
            risk_score += 0.3
        elif entity_claim_count >= 10:
            risk_score += 0.2
        
        # Average risk factor
        if avg_risk >= 70:
            risk_score += 0.3
        elif avg_risk >= 50:
            risk_score += 0.2
        
        # Ring connections
        if ring_count >= 2:
            risk_score += 0.3
        elif ring_count >= 1:
            risk_score += 0.2
        
        return min(risk_score, 1.0)
    
    def _score_fraud_ring_membership(self, claim_id: str) -> float:
        """Score based on fraud ring membership (0-1 scale)"""
        query = """
        MATCH (c:Claimant)-[:FILED]->(cl:Claim {claim_id: $claim_id})
        OPTIONAL MATCH (c)-[:MEMBER_OF]->(r:FraudRing)
        
        RETURN 
            count(DISTINCT r) as ring_count,
            collect(r.confidence_score) as confidence_scores
        """
        
        results = self.driver.execute_query(query, {'claim_id': claim_id})
        
        if not results:
            return 0.0
        
        data = results[0]
        ring_count = data.get('ring_count', 0)
        confidence_scores = [s for s in data.get('confidence_scores', []) if s]
        
        if ring_count == 0:
            return 0.0
        
        # Use highest confidence score
        max_confidence = max(confidence_scores) if confidence_scores else 0.7
        
        # Multiple rings = higher risk
        if ring_count >= 2:
            return 1.0
        else:
            return max_confidence
    
    def _score_repeat_entity_patterns(self, claim_id: str) -> float:
        """Score based on repeat entity usage patterns (0-1 scale)"""
        query = """
        MATCH (c:Claimant)-[:FILED]->(cl:Claim {claim_id: $claim_id})
        MATCH (c)-[:FILED]->(other_cl:Claim)
        WHERE other_cl.claim_id <> cl.claim_id
        
        // Check if same entities are used across claims
        OPTIONAL MATCH (cl)-[:REPAIRED_AT]->(b:BodyShop)<-[:REPAIRED_AT]-(other_cl)
        OPTIONAL MATCH (cl)-[:TREATED_BY]->(m:MedicalProvider)<-[:TREATED_BY]-(other_cl)
        OPTIONAL MATCH (cl)-[:REPRESENTED_BY]->(a:Attorney)<-[:REPRESENTED_BY]-(other_cl)
        
        RETURN 
            count(DISTINCT b) as same_body_shops,
            count(DISTINCT m) as same_medical_providers,
            count(DISTINCT a) as same_attorneys,
            count(DISTINCT other_cl) as other_claim_count
        """
        
        results = self.driver.execute_query(query, {'claim_id': claim_id})
        
        if not results:
            return 0.0
        
        data = results[0]
        
        same_body_shops = data.get('same_body_shops', 0)
        same_medical_providers = data.get('same_medical_providers', 0)
        same_attorneys = data.get('same_attorneys', 0)
        other_claim_count = data.get('other_claim_count', 0)
        
        if other_claim_count == 0:
            return 0.0
        
        # Using same entities repeatedly is suspicious
        repeat_score = 0.0
        
        if same_body_shops >= 2:
            repeat_score += 0.4
        if same_medical_providers >= 2:
            repeat_score += 0.3
        if same_attorneys >= 2:
            repeat_score += 0.3
        
        return min(repeat_score, 1.0)
    
    def _score_vehicle_history(self, claim_id: str) -> float:
        """Score based on vehicle accident history (0-1 scale)"""
        query = """
        MATCH (cl:Claim {claim_id: $claim_id})-[:INVOLVES_VEHICLE]->(v:Vehicle)
        
        WITH v, count{(v)<-[:INVOLVES_VEHICLE]-(:Claim)} as accident_count
        
        RETURN accident_count
        """
        
        results = self.driver.execute_query(query, {'claim_id': claim_id})
        
        if not results:
            return 0.0
        
        accident_count = results[0].get('accident_count', 0)
        
        if accident_count >= 4:
            return 1.0
        elif accident_count >= 3:
            return 0.8
        elif accident_count >= 2:
            return 0.5
        else:
            return 0.0
    
    def _get_risk_level(self, risk_score: float) -> str:
        """Convert risk score to risk level"""
        if risk_score >= 70:
            return 'HIGH'
        elif risk_score >= 40:
            return 'MEDIUM'
        else:
            return 'LOW'
    
    def _generate_risk_explanation(self, weighted_factors: Dict, total_risk: float) -> str:
        """Generate human-readable risk explanation"""
        risk_level = self._get_risk_level(total_risk)
        
        # Get top risk factors
        sorted_factors = sorted(
            weighted_factors.items(),
            key=lambda x: x[1]['weighted_score'],
            reverse=True
        )
        
        top_factors = sorted_factors[:3]
        
        explanation = f"This claim has a {risk_level} risk score of {total_risk:.1f}. "
        
        if risk_level == 'HIGH':
            explanation += "Multiple fraud indicators detected. "
        elif risk_level == 'MEDIUM':
            explanation += "Some fraud indicators present. "
        else:
            explanation += "Few fraud indicators detected. "
        
        if top_factors:
            explanation += "Primary risk factors: "
            factor_names = [f.replace('_', ' ').title() for f, _ in top_factors]
            explanation += ", ".join(factor_names) + "."
        
        return explanation
    
    def _get_top_risk_factors(self, weighted_factors: Dict, top_n: int = 5) -> List[Dict]:
        """Get top N risk factors"""
        sorted_factors = sorted(
            weighted_factors.items(),
            key=lambda x: x[1]['weighted_score'],
            reverse=True
        )
        
        return [
            {
                'factor': factor.replace('_', ' ').title(),
                'score': data['weighted_score'],
                'raw_score': data['raw_score']
            }
            for factor, data in sorted_factors[:top_n]
            if data['weighted_score'] > 0
        ]
