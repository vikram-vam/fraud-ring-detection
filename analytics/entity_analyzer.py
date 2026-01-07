"""
Entity Analyzer - Analyze connections and patterns for auto insurance entities
Identifies suspicious relationships and fraud indicators
"""
import logging
from typing import Dict, List, Optional, Set
from collections import defaultdict

from data.neo4j_driver import get_neo4j_driver
from utils.logger import setup_logger

logger = setup_logger(__name__)


class EntityAnalyzer:
    """
    Entity relationship and pattern analyzer for auto insurance fraud
    """
    
    def __init__(self):
        self.driver = get_neo4j_driver()
    
    def analyze_claimant_connections(self, claimant_id: str) -> Dict:
        """
        Analyze claimant's connections and identify suspicious patterns
        
        Args:
            claimant_id: Claimant identifier
            
        Returns:
            Dictionary with connection analysis
        """
        try:
            # Get all connections
            query = """
            MATCH (c:Claimant {claimant_id: $claimant_id})
            
            // Get claims
            OPTIONAL MATCH (c)-[:FILED]->(cl:Claim)
            
            // Get vehicles
            OPTIONAL MATCH (cl)-[:INVOLVES_VEHICLE]->(v:Vehicle)
            
            // Get body shops
            OPTIONAL MATCH (cl)-[:REPAIRED_AT]->(b:BodyShop)
            
            // Get medical providers
            OPTIONAL MATCH (cl)-[:TREATED_BY]->(m:MedicalProvider)
            
            // Get attorneys
            OPTIONAL MATCH (cl)-[:REPRESENTED_BY]->(a:Attorney)
            
            // Get tow companies
            OPTIONAL MATCH (cl)-[:TOWED_BY]->(t:TowCompany)
            
            // Get accident locations
            OPTIONAL MATCH (cl)-[:OCCURRED_AT]->(l:AccidentLocation)
            
            // Get witnesses
            OPTIONAL MATCH (w:Witness)-[:WITNESSED]->(cl)
            
            // Get fraud rings
            OPTIONAL MATCH (c)-[:MEMBER_OF]->(r:FraudRing)
            
            RETURN 
                c.name as name,
                collect(DISTINCT cl.claim_id) as claim_ids,
                collect(DISTINCT v.vehicle_id) as vehicle_ids,
                collect(DISTINCT b.body_shop_id) as body_shop_ids,
                collect(DISTINCT m.provider_id) as medical_provider_ids,
                collect(DISTINCT a.attorney_id) as attorney_ids,
                collect(DISTINCT t.tow_company_id) as tow_company_ids,
                collect(DISTINCT l.location_id) as location_ids,
                collect(DISTINCT w.witness_id) as witness_ids,
                collect(DISTINCT r.ring_id) as ring_ids
            """
            
            results = self.driver.execute_query(query, {'claimant_id': claimant_id})
            
            if not results:
                return {'error': 'Claimant not found'}
            
            data = results[0]
            
            # Analyze entity reuse patterns
            entity_reuse = self._analyze_entity_reuse(claimant_id)
            
            # Find shared connections with other claimants
            shared_connections = self._find_shared_connections(claimant_id)
            
            # Calculate suspicion score
            suspicion_score = self._calculate_claimant_suspicion_score(
                data,
                entity_reuse,
                shared_connections
            )
            
            # Generate flags
            flags = self._generate_claimant_flags(
                data,
                entity_reuse,
                shared_connections,
                suspicion_score
            )
            
            return {
                'claimant_id': claimant_id,
                'name': data.get('name'),
                'claim_count': len([c for c in data.get('claim_ids', []) if c]),
                'unique_vehicles': len([v for v in data.get('vehicle_ids', []) if v]),
                'unique_body_shops': len([b for b in data.get('body_shop_ids', []) if b]),
                'unique_medical_providers': len([m for m in data.get('medical_provider_ids', []) if m]),
                'unique_attorneys': len([a for a in data.get('attorney_ids', []) if a]),
                'unique_locations': len([l for l in data.get('location_ids', []) if l]),
                'witness_count': len([w for w in data.get('witness_ids', []) if w]),
                'fraud_rings': len([r for r in data.get('ring_ids', []) if r]),
                'entity_reuse': entity_reuse,
                'shared_connections': shared_connections,
                'suspicion_score': suspicion_score,
                'flags': flags
            }
            
        except Exception as e:
            logger.error(f"Error analyzing claimant connections: {e}", exc_info=True)
            return {'error': str(e)}
    
    def analyze_vehicle_usage(self, vehicle_id: str) -> Dict:
        """
        Analyze vehicle usage patterns across claims
        
        Args:
            vehicle_id: Vehicle identifier
            
        Returns:
            Dictionary with vehicle analysis
        """
        try:
            query = """
            MATCH (v:Vehicle {vehicle_id: $vehicle_id})<-[:INVOLVES_VEHICLE]-(cl:Claim)
            MATCH (c:Claimant)-[:FILED]->(cl)
            
            OPTIONAL MATCH (cl)-[:OCCURRED_AT]->(l:AccidentLocation)
            OPTIONAL MATCH (w:Witness)-[:WITNESSED]->(cl)
            
            RETURN 
                v.make + ' ' + v.model + ' ' + v.year as vehicle_info,
                v.vin as vin,
                v.license_plate as license_plate,
                collect(DISTINCT c.claimant_id) as claimant_ids,
                collect(DISTINCT c.name) as claimant_names,
                collect(DISTINCT cl.claim_id) as claim_ids,
                collect(DISTINCT cl.accident_date) as accident_dates,
                collect(DISTINCT l.intersection) as accident_locations,
                collect(DISTINCT w.witness_id) as witness_ids
            """
            
            results = self.driver.execute_query(query, {'vehicle_id': vehicle_id})
            
            if not results:
                return {'error': 'Vehicle not found'}
            
            data = results[0]
            
            claimant_ids = [c for c in data.get('claimant_ids', []) if c]
            claim_ids = [c for c in data.get('claim_ids', []) if c]
            accident_dates = [d for d in data.get('accident_dates', []) if d]
            witness_ids = [w for w in data.get('witness_ids', []) if w]
            
            # Analyze patterns
            suspicion_score = 0
            flags = []
            
            # Multiple claimants - HIGHLY SUSPICIOUS
            if len(claimant_ids) >= 3:
                suspicion_score += 40
                flags.append(f"Vehicle used by {len(claimant_ids)} different claimants - SUSPICIOUS")
            elif len(claimant_ids) >= 2:
                suspicion_score += 25
                flags.append(f"Vehicle used by {len(claimant_ids)} different claimants")
            
            # Multiple accidents
            if len(claim_ids) >= 4:
                suspicion_score += 30
                flags.append(f"{len(claim_ids)} accidents - High frequency")
            elif len(claim_ids) >= 3:
                suspicion_score += 20
                flags.append(f"{len(claim_ids)} accidents")
            
            # Shared witnesses across accidents
            if len(witness_ids) >= 2 and len(claim_ids) >= 2:
                suspicion_score += 20
                flags.append(f"Same witnesses in multiple accidents")
            
            return {
                'vehicle_id': vehicle_id,
                'vehicle_info': data.get('vehicle_info'),
                'vin': data.get('vin'),
                'license_plate': data.get('license_plate'),
                'total_accidents': len(claim_ids),
                'unique_claimants': len(claimant_ids),
                'claimant_names': data.get('claimant_names', []),
                'accident_locations': [l for l in data.get('accident_locations', []) if l],
                'shared_witnesses': len(witness_ids),
                'suspicion_score': min(suspicion_score, 100),
                'flags': flags
            }
            
        except Exception as e:
            logger.error(f"Error analyzing vehicle usage: {e}", exc_info=True)
            return {'error': str(e)}
    
    def analyze_entity_network(
        self,
        entity_type: str,
        entity_id: str
    ) -> Dict:
        """
        Analyze network of connections for body shops, medical providers, attorneys
        
        Args:
            entity_type: Type of entity (BodyShop, MedicalProvider, Attorney)
            entity_id: Entity identifier
            
        Returns:
            Dictionary with network analysis
        """
        try:
            # Map entity types to relationships and ID fields
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
            
            // Check for co-occurring entities
            OPTIONAL MATCH (cl)-[:REPAIRED_AT]->(b:BodyShop)
            WHERE b <> e OR '{entity_type}' <> 'BodyShop'
            
            OPTIONAL MATCH (cl)-[:TREATED_BY]->(m:MedicalProvider)
            WHERE m <> e OR '{entity_type}' <> 'MedicalProvider'
            
            OPTIONAL MATCH (cl)-[:REPRESENTED_BY]->(a:Attorney)
            WHERE a <> e OR '{entity_type}' <> 'Attorney'
            
            RETURN 
                e.name as name,
                collect(DISTINCT c.claimant_id) as claimant_ids,
                collect(DISTINCT c.name) as claimant_names,
                collect(DISTINCT r.ring_id) as ring_ids,
                collect(DISTINCT b.body_shop_id) as body_shop_ids,
                collect(DISTINCT m.provider_id) as medical_provider_ids,
                collect(DISTINCT a.attorney_id) as attorney_ids,
                count(DISTINCT cl) as claim_count,
                sum(cl.total_claim_amount) as total_amount,
                avg(cl.risk_score) as avg_risk
            """
            
            results = self.driver.execute_query(query, {'entity_id': entity_id})
            
            if not results:
                return {'error': f'{entity_type} not found'}
            
            data = results[0]
            
            claimant_ids = [c for c in data.get('claimant_ids', []) if c]
            ring_ids = [r for r in data.get('ring_ids', []) if r]
            
            # Analyze referral patterns
            referral_patterns = self._analyze_referral_patterns(
                entity_type,
                entity_id,
                data
            )
            
            # Calculate suspicion score
            suspicion_score = self._calculate_entity_suspicion_score(
                data,
                referral_patterns
            )
            
            # Generate flags
            flags = self._generate_entity_flags(
                entity_type,
                data,
                referral_patterns,
                suspicion_score
            )
            
            return {
                'entity_type': entity_type,
                'entity_id': entity_id,
                'name': data.get('name'),
                'claim_count': data.get('claim_count', 0),
                'unique_claimants': len(claimant_ids),
                'total_amount': data.get('total_amount', 0),
                'avg_risk_score': round(data.get('avg_risk', 0), 2),
                'fraud_ring_connections': len(ring_ids),
                'referral_patterns': referral_patterns,
                'suspicion_score': suspicion_score,
                'flags': flags
            }
            
        except Exception as e:
            logger.error(f"Error analyzing entity network: {e}", exc_info=True)
            return {'error': str(e)}
    
    def analyze_accident_location(self, location_id: str) -> Dict:
        """
        Analyze accident location for hotspot patterns
        
        Args:
            location_id: Location identifier
            
        Returns:
            Dictionary with location analysis
        """
        try:
            query = """
            MATCH (l:AccidentLocation {location_id: $location_id})<-[:OCCURRED_AT]-(cl:Claim)
            MATCH (c:Claimant)-[:FILED]->(cl)
            
            OPTIONAL MATCH (w:Witness)-[:WITNESSED]->(cl)
            OPTIONAL MATCH (c)-[:MEMBER_OF]->(r:FraudRing)
            
            RETURN 
                l.intersection as intersection,
                l.city as city,
                collect(DISTINCT c.claimant_id) as claimant_ids,
                collect(DISTINCT w.witness_id) as witness_ids,
                collect(DISTINCT r.ring_id) as ring_ids,
                count(DISTINCT cl) as accident_count,
                sum(cl.total_claim_amount) as total_amount,
                avg(cl.risk_score) as avg_risk,
                collect(DISTINCT cl.accident_date) as accident_dates
            """
            
            results = self.driver.execute_query(query, {'location_id': location_id})
            
            if not results:
                return {'error': 'Location not found'}
            
            data = results[0]
            
            accident_count = data.get('accident_count', 0)
            claimant_ids = [c for c in data.get('claimant_ids', []) if c]
            witness_ids = [w for w in data.get('witness_ids', []) if w]
            ring_ids = [r for r in data.get('ring_ids', []) if r]
            
            # Calculate hotspot score
            suspicion_score = 0
            flags = []
            
            if accident_count >= 10:
                suspicion_score += 50
                flags.append(f"MAJOR HOTSPOT: {accident_count} accidents at this location")
            elif accident_count >= 7:
                suspicion_score += 40
                flags.append(f"Accident hotspot: {accident_count} accidents")
            elif accident_count >= 5:
                suspicion_score += 30
                flags.append(f"Multiple accidents: {accident_count}")
            
            # Shared witnesses
            if len(witness_ids) >= 3:
                suspicion_score += 30
                flags.append(f"Same witnesses in multiple accidents at this location")
            
            # Ring connections
            if len(ring_ids) >= 2:
                suspicion_score += 20
                flags.append(f"Connected to {len(ring_ids)} fraud rings")
            
            return {
                'location_id': location_id,
                'intersection': data.get('intersection'),
                'city': data.get('city'),
                'accident_count': accident_count,
                'unique_claimants': len(claimant_ids),
                'shared_witnesses': len(witness_ids),
                'ring_connections': len(ring_ids),
                'total_amount': data.get('total_amount', 0),
                'avg_risk_score': round(data.get('avg_risk', 0), 2),
                'suspicion_score': min(suspicion_score, 100),
                'flags': flags
            }
            
        except Exception as e:
            logger.error(f"Error analyzing accident location: {e}", exc_info=True)
            return {'error': str(e)}
    
    def analyze_witness_credibility(self, witness_id: str) -> Dict:
        """
        Analyze witness credibility and appearance patterns
        
        Args:
            witness_id: Witness identifier
            
        Returns:
            Dictionary with witness analysis
        """
        try:
            query = """
            MATCH (w:Witness {witness_id: $witness_id})-[:WITNESSED]->(cl:Claim)
            MATCH (c:Claimant)-[:FILED]->(cl)
            
            OPTIONAL MATCH (cl)-[:OCCURRED_AT]->(l:AccidentLocation)
            OPTIONAL MATCH (c)-[:MEMBER_OF]->(r:FraudRing)
            
            RETURN 
                w.name as name,
                w.phone as phone,
                collect(DISTINCT c.claimant_id) as claimant_ids,
                collect(DISTINCT c.name) as claimant_names,
                collect(DISTINCT l.intersection) as locations,
                collect(DISTINCT r.ring_id) as ring_ids,
                count(DISTINCT cl) as witnessed_count,
                avg(cl.risk_score) as avg_risk
            """
            
            results = self.driver.execute_query(query, {'witness_id': witness_id})
            
            if not results:
                return {'error': 'Witness not found'}
            
            data = results[0]
            
            witnessed_count = data.get('witnessed_count', 0)
            claimant_ids = [c for c in data.get('claimant_ids', []) if c]
            ring_ids = [r for r in data.get('ring_ids', []) if r]
            
            # Calculate credibility score (inverse of suspicion)
            suspicion_score = 0
            flags = []
            
            if witnessed_count >= 5:
                suspicion_score += 50
                flags.append(f"PROFESSIONAL WITNESS: Appeared in {witnessed_count} accidents")
            elif witnessed_count >= 3:
                suspicion_score += 35
                flags.append(f"Suspicious witness pattern: {witnessed_count} appearances")
            elif witnessed_count >= 2:
                suspicion_score += 20
                flags.append(f"Multiple accident witness: {witnessed_count} appearances")
            
            if len(ring_ids) >= 1:
                suspicion_score += 30
                flags.append(f"Connected to {len(ring_ids)} fraud ring(s)")
            
            credibility_score = 100 - min(suspicion_score, 100)
            
            return {
                'witness_id': witness_id,
                'name': data.get('name'),
                'phone': data.get('phone'),
                'witnessed_count': witnessed_count,
                'unique_claimants': len(claimant_ids),
                'claimant_names': data.get('claimant_names', []),
                'unique_locations': len([l for l in data.get('locations', []) if l]),
                'ring_connections': len(ring_ids),
                'avg_claim_risk': round(data.get('avg_risk', 0), 2),
                'credibility_score': credibility_score,
                'suspicion_score': min(suspicion_score, 100),
                'flags': flags
            }
            
        except Exception as e:
            logger.error(f"Error analyzing witness: {e}", exc_info=True)
            return {'error': str(e)}
    
    # Helper methods
    
    def _analyze_entity_reuse(self, claimant_id: str) -> Dict:
        """Analyze how often claimant reuses same entities"""
        query = """
        MATCH (c:Claimant {claimant_id: $claimant_id})-[:FILED]->(cl:Claim)
        
        OPTIONAL MATCH (cl)-[:REPAIRED_AT]->(b:BodyShop)
        OPTIONAL MATCH (cl)-[:TREATED_BY]->(m:MedicalProvider)
        OPTIONAL MATCH (cl)-[:REPRESENTED_BY]->(a:Attorney)
        
        WITH c,
             collect(DISTINCT b.body_shop_id) as body_shops,
             collect(DISTINCT m.provider_id) as medical_providers,
             collect(DISTINCT a.attorney_id) as attorneys,
             count(DISTINCT cl) as claim_count
        
        RETURN 
            size(body_shops) as unique_body_shops,
            size(medical_providers) as unique_medical_providers,
            size(attorneys) as unique_attorneys,
            claim_count
        """
        
        results = self.driver.execute_query(query, {'claimant_id': claimant_id})
        
        if not results:
            return {}
        
        data = results[0]
        claim_count = data.get('claim_count', 0)
        
        if claim_count == 0:
            return {}
        
        # Calculate reuse ratios
        return {
            'body_shop_reuse_ratio': 1 - (data.get('unique_body_shops', 0) / claim_count) if claim_count > 0 else 0,
            'medical_provider_reuse_ratio': 1 - (data.get('unique_medical_providers', 0) / claim_count) if claim_count > 0 else 0,
            'attorney_reuse_ratio': 1 - (data.get('unique_attorneys', 0) / claim_count) if claim_count > 0 else 0
        }
    
    def _find_shared_connections(self, claimant_id: str) -> Dict:
        """Find connections shared with other claimants"""
        query = """
        MATCH (c:Claimant {claimant_id: $claimant_id})-[:FILED]->(cl:Claim)
        
        // Find other claimants sharing same body shops
        OPTIONAL MATCH (cl)-[:REPAIRED_AT]->(b:BodyShop)<-[:REPAIRED_AT]-(other_cl:Claim)<-[:FILED]-(other_c:Claimant)
        WHERE other_c.claimant_id <> c.claimant_id
        
        WITH c, collect(DISTINCT other_c.claimant_id) as body_shop_connections
        
        // Find other claimants sharing same medical providers
        MATCH (c)-[:FILED]->(cl2:Claim)
        OPTIONAL MATCH (cl2)-[:TREATED_BY]->(m:MedicalProvider)<-[:TREATED_BY]-(other_cl2:Claim)<-[:FILED]-(other_c2:Claimant)
        WHERE other_c2.claimant_id <> c.claimant_id
        
        RETURN 
            size(body_shop_connections) as shared_body_shop_count,
            body_shop_connections
        """
        
        results = self.driver.execute_query(query, {'claimant_id': claimant_id})
        
        if not results:
            return {}
        
        return {
            'shared_body_shop_count': results[0].get('shared_body_shop_count', 0)
        }
    
    def _calculate_claimant_suspicion_score(
        self,
        data: Dict,
        entity_reuse: Dict,
        shared_connections: Dict
    ) -> int:
        """Calculate overall suspicion score for claimant"""
        score = 0
        
        # Multiple claims
        claim_count = len([c for c in data.get('claim_ids', []) if c])
        if claim_count >= 5:
            score += 30
        elif claim_count >= 3:
            score += 20
        
        # Fraud ring membership
        if len([r for r in data.get('ring_ids', []) if r]) > 0:
            score += 40
        
        # High entity reuse
        if entity_reuse.get('body_shop_reuse_ratio', 0) > 0.7:
            score += 15
        if entity_reuse.get('medical_provider_reuse_ratio', 0) > 0.7:
            score += 15
        
        return min(score, 100)
    
    def _generate_claimant_flags(
        self,
        data: Dict,
        entity_reuse: Dict,
        shared_connections: Dict,
        suspicion_score: int
    ) -> List[str]:
        """Generate warning flags for claimant"""
        flags = []
        
        claim_count = len([c for c in data.get('claim_ids', []) if c])
        ring_count = len([r for r in data.get('ring_ids', []) if r])
        
        if ring_count > 0:
            flags.append(f"Member of {ring_count} fraud ring(s)")
        
        if claim_count >= 5:
            flags.append(f"High claim frequency: {claim_count} claims")
        
        if entity_reuse.get('body_shop_reuse_ratio', 0) > 0.7:
            flags.append("Repeatedly uses same body shop(s)")
        
        if entity_reuse.get('medical_provider_reuse_ratio', 0) > 0.7:
            flags.append("Repeatedly uses same medical provider(s)")
        
        return flags
    
    def _analyze_referral_patterns(
        self,
        entity_type: str,
        entity_id: str,
        data: Dict
    ) -> Dict:
        """Analyze referral patterns between entities"""
        # Simplified referral analysis
        return {
            'co_occurring_body_shops': len([b for b in data.get('body_shop_ids', []) if b]),
            'co_occurring_medical_providers': len([m for m in data.get('medical_provider_ids', []) if m]),
            'co_occurring_attorneys': len([a for a in data.get('attorney_ids', []) if a])
        }
    
    def _calculate_entity_suspicion_score(
        self,
        data: Dict,
        referral_patterns: Dict
    ) -> int:
        """Calculate suspicion score for entity"""
        score = 0
        
        # High volume
        claim_count = data.get('claim_count', 0)
        if claim_count >= 50:
            score += 30
        elif claim_count >= 30:
            score += 20
        
        # High average risk
        avg_risk = data.get('avg_risk', 0)
        if avg_risk >= 70:
            score += 35
        elif avg_risk >= 50:
            score += 20
        
        # Ring connections
        ring_ids = [r for r in data.get('ring_ids', []) if r]
        if len(ring_ids) >= 2:
            score += 35
        elif len(ring_ids) >= 1:
            score += 20
        
        return min(score, 100)
    
    def _generate_entity_flags(
        self,
        entity_type: str,
        data: Dict,
        referral_patterns: Dict,
        suspicion_score: int
    ) -> List[str]:
        """Generate warning flags for entity"""
        flags = []
        
        claim_count = data.get('claim_count', 0)
        avg_risk = data.get('avg_risk', 0)
        ring_count = len([r for r in data.get('ring_ids', []) if r])
        
        if ring_count >= 2:
            flags.append(f"Linked to {ring_count} fraud rings")
        elif ring_count >= 1:
            flags.append("Linked to fraud ring")
        
        if claim_count >= 50:
            flags.append(f"Very high volume: {claim_count} claims")
        elif claim_count >= 30:
            flags.append(f"High volume: {claim_count} claims")
        
        if avg_risk >= 70:
            flags.append(f"High average risk score: {avg_risk:.1f}")
        
        return flags
