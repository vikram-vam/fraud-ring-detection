"""
Claimant Repository - Data access for claimants
"""
from typing import List, Dict, Optional
import logging

from data.neo4j_driver import Neo4jDriver
from data.models.claimant import Claimant

logger = logging.getLogger(__name__)


class ClaimantRepository:
    """Repository for Claimant entity operations"""
    
    def __init__(self):
        self.driver = Neo4jDriver()
    
    def get_all_claimants(
        self,
        limit: int = 100,
        offset: int = 0
    ) -> List[Claimant]:
        """
        Get all claimants with pagination
        
        Args:
            limit: Maximum number to return
            offset: Number to skip
            
        Returns:
            List of Claimant objects
        """
        query = """
        MATCH (c:Claimant)
        OPTIONAL MATCH (c)-[:FILED]->(cl:Claim)
        OPTIONAL MATCH (c)-[:MEMBER_OF]->(r:FraudRing)
        WITH c,
             count(DISTINCT cl) as total_claims,
             sum(cl.claim_amount) as total_claimed,
             avg(cl.claim_amount) as avg_claim_amount,
             avg(cl.risk_score) as avg_risk_score,
             count(DISTINCT r) as ring_count
        ORDER BY c.name
        SKIP $offset
        LIMIT $limit
        RETURN 
            c.claimant_id as claimant_id,
            c.name as name,
            c.email as email,
            c.date_of_birth as date_of_birth,
            c.phone as phone,
            total_claims,
            total_claimed,
            avg_claim_amount,
            avg_risk_score,
            ring_count
        """
        
        results = self.driver.execute_query(query, {
            'limit': limit,
            'offset': offset
        })
        
        return [Claimant.from_dict(r) for r in results] if results else []
    
    def get_claimant_by_id(self, claimant_id: str) -> Optional[Claimant]:
        """
        Get claimant by ID with aggregated stats
        
        Args:
            claimant_id: Claimant ID
            
        Returns:
            Claimant object or None
        """
        query = """
        MATCH (c:Claimant {claimant_id: $claimant_id})
        OPTIONAL MATCH (c)-[:FILED]->(cl:Claim)
        OPTIONAL MATCH (c)-[:MEMBER_OF]->(r:FraudRing)
        WITH c,
             count(DISTINCT cl) as total_claims,
             sum(cl.claim_amount) as total_claimed,
             avg(cl.claim_amount) as avg_claim_amount,
             avg(cl.risk_score) as avg_risk_score,
             count(DISTINCT r) as ring_count
        RETURN 
            c.claimant_id as claimant_id,
            c.name as name,
            c.email as email,
            c.date_of_birth as date_of_birth,
            c.ssn as ssn,
            c.phone as phone,
            total_claims,
            total_claimed,
            avg_claim_amount,
            avg_risk_score,
            ring_count
        """
        
        results = self.driver.execute_query(query, {'claimant_id': claimant_id})
        
        return Claimant.from_dict(results[0]) if results else None
    
    def get_claimant_details(self, claimant_id: str) -> Optional[Dict]:
        """
        Get comprehensive claimant details with relationships
        
        Args:
            claimant_id: Claimant ID
            
        Returns:
            Detailed claimant dictionary
        """
        # Get basic claimant info
        claimant = self.get_claimant_by_id(claimant_id)
        
        if not claimant:
            return None
        
        claimant_data = claimant.to_dict()
        
        # Get addresses
        address_query = """
        MATCH (c:Claimant {claimant_id: $claimant_id})-[:LIVES_AT]->(a:Address)
        RETURN a {.address_id, .street, .city, .state, .zip_code}
        """
        
        addresses = self.driver.execute_query(address_query, {'claimant_id': claimant_id})
        claimant_data['addresses'] = addresses if addresses else []
        
        # Get providers
        provider_query = """
        MATCH (c:Claimant {claimant_id: $claimant_id})-[:FILED]->(:Claim)-[:TREATED_BY]->(p:Provider)
        RETURN DISTINCT p {.provider_id, .name, .provider_type}
        """
        
        providers = self.driver.execute_query(provider_query, {'claimant_id': claimant_id})
        claimant_data['providers'] = providers if providers else []
        
        # Get attorneys
        attorney_query = """
        MATCH (c:Claimant {claimant_id: $claimant_id})-[:FILED]->(:Claim)-[:REPRESENTED_BY]->(att:Attorney)
        RETURN DISTINCT att {.attorney_id, .name, .firm}
        """
        
        attorneys = self.driver.execute_query(attorney_query, {'claimant_id': claimant_id})
        claimant_data['attorneys'] = attorneys if attorneys else []
        
        # Get fraud rings
        ring_query = """
        MATCH (c:Claimant {claimant_id: $claimant_id})-[:MEMBER_OF]->(r:FraudRing)
        RETURN r {.ring_id, .ring_type, .pattern_type, .status, .confidence_score}
        """
        
        rings = self.driver.execute_query(ring_query, {'claimant_id': claimant_id})
        claimant_data['fraud_rings'] = rings if rings else []
        
        return claimant_data
    
    def get_high_risk_claimants(
        self,
        threshold: float = 70.0,
        limit: int = 50
    ) -> List[Claimant]:
        """
        Get claimants with high average risk scores
        
        Args:
            threshold: Minimum average risk score
            limit: Maximum number to return
            
        Returns:
            List of high-risk claimants
        """
        query = """
        MATCH (c:Claimant)-[:FILED]->(cl:Claim)
        WITH c,
             count(cl) as total_claims,
             sum(cl.claim_amount) as total_claimed,
             avg(cl.claim_amount) as avg_claim_amount,
             avg(cl.risk_score) as avg_risk_score
        WHERE avg_risk_score >= $threshold
        
        OPTIONAL MATCH (c)-[:MEMBER_OF]->(r:FraudRing)
        WITH c, total_claims, total_claimed, avg_claim_amount, avg_risk_score,
             count(r) as ring_count
        
        ORDER BY avg_risk_score DESC
        LIMIT $limit
        
        RETURN 
            c.claimant_id as claimant_id,
            c.name as name,
            c.email as email,
            total_claims,
            total_claimed,
            avg_claim_amount,
            avg_risk_score,
            ring_count
        """
        
        results = self.driver.execute_query(query, {
            'threshold': threshold,
            'limit': limit
        })
        
        return [Claimant.from_dict(r) for r in results] if results else []
    
    def get_frequent_filers(
        self,
        min_claims: int = 5,
        limit: int = 50
    ) -> List[Claimant]:
        """
        Get frequent filing claimants
        
        Args:
            min_claims: Minimum number of claims
            limit: Maximum number to return
            
        Returns:
            List of frequent filer claimants
        """
        query = """
        MATCH (c:Claimant)-[:FILED]->(cl:Claim)
        WITH c,
             count(cl) as total_claims,
             sum(cl.claim_amount) as total_claimed,
             avg(cl.claim_amount) as avg_claim_amount,
             avg(cl.risk_score) as avg_risk_score
        WHERE total_claims >= $min_claims
        
        OPTIONAL MATCH (c)-[:MEMBER_OF]->(r:FraudRing)
        WITH c, total_claims, total_claimed, avg_claim_amount, avg_risk_score,
             count(r) as ring_count
        
        ORDER BY total_claims DESC
        LIMIT $limit
        
        RETURN 
            c.claimant_id as claimant_id,
            c.name as name,
            c.email as email,
            total_claims,
            total_claimed,
            avg_claim_amount,
            avg_risk_score,
            ring_count
        """
        
        results = self.driver.execute_query(query, {
            'min_claims': min_claims,
            'limit': limit
        })
        
        return [Claimant.from_dict(r) for r in results] if results else []
    
    def get_claimant_connections(self, claimant_id: str) -> Dict:
        """
        Get all connections for a claimant
        
        Args:
            claimant_id: Claimant ID
            
        Returns:
            Dictionary with connection counts and details
        """
        query = """
        MATCH (c:Claimant {claimant_id: $claimant_id})
        
        // Shared addresses
        OPTIONAL MATCH (c)-[:LIVES_AT]->(a:Address)<-[:LIVES_AT]-(other_c:Claimant)
        WHERE other_c.claimant_id <> c.claimant_id
        WITH c, count(DISTINCT other_c) as shared_address_count, collect(DISTINCT other_c.claimant_id) as shared_address_claimants
        
        // Shared providers
        OPTIONAL MATCH (c)-[:FILED]->(:Claim)-[:TREATED_BY]->(p:Provider)<-[:TREATED_BY]-(:Claim)<-[:FILED]-(other_p:Claimant)
        WHERE other_p.claimant_id <> c.claimant_id
        WITH c, shared_address_count, shared_address_claimants,
             count(DISTINCT other_p) as shared_provider_count,
             collect(DISTINCT other_p.claimant_id) as shared_provider_claimants
        
        // Shared attorneys
        OPTIONAL MATCH (c)-[:FILED]->(:Claim)-[:REPRESENTED_BY]->(att:Attorney)<-[:REPRESENTED_BY]-(:Claim)<-[:FILED]-(other_a:Claimant)
        WHERE other_a.claimant_id <> c.claimant_id
        WITH c, shared_address_count, shared_address_claimants, shared_provider_count, shared_provider_claimants,
             count(DISTINCT other_a) as shared_attorney_count,
             collect(DISTINCT other_a.claimant_id) as shared_attorney_claimants
        
        RETURN 
            shared_address_count,
            shared_address_claimants,
            shared_provider_count,
            shared_provider_claimants,
            shared_attorney_count,
            shared_attorney_claimants
        """
        
        results = self.driver.execute_query(query, {'claimant_id': claimant_id})
        
        return results[0] if results else {}
    
    def search_claimants(
        self,
        search_term: str,
        limit: int = 50
    ) -> List[Claimant]:
        """
        Search claimants by name, email, or ID
        
        Args:
            search_term: Search term
            limit: Maximum results
            
        Returns:
            List of matching claimants
        """
        query = """
        MATCH (c:Claimant)
        WHERE c.name CONTAINS $search_term 
           OR c.email CONTAINS $search_term
           OR c.claimant_id CONTAINS $search_term
        
        OPTIONAL MATCH (c)-[:FILED]->(cl:Claim)
        OPTIONAL MATCH (c)-[:MEMBER_OF]->(r:FraudRing)
        
        WITH c,
             count(DISTINCT cl) as total_claims,
             sum(cl.claim_amount) as total_claimed,
             avg(cl.risk_score) as avg_risk_score,
             count(DISTINCT r) as ring_count
        
        ORDER BY c.name
        LIMIT $limit
        
        RETURN 
            c.claimant_id as claimant_id,
            c.name as name,
            c.email as email,
            total_claims,
            total_claimed,
            avg_risk_score,
            ring_count
        """
        
        results = self.driver.execute_query(query, {
            'search_term': search_term,
            'limit': limit
        })
        
        return [Claimant.from_dict(r) for r in results] if results else []
