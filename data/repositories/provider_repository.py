"""
Provider Repository - Data access for providers
"""
from typing import List, Dict, Optional
import logging

from data.neo4j_driver import Neo4jDriver
from data.models.provider import Provider

logger = logging.getLogger(__name__)


class ProviderRepository:
    """Repository for Provider entity operations"""
    
    def __init__(self):
        self.driver = Neo4jDriver()
    
    def get_all_providers(
        self,
        limit: int = 100,
        offset: int = 0
    ) -> List[Provider]:
        """
        Get all providers with pagination
        
        Args:
            limit: Maximum number to return
            offset: Number to skip
            
        Returns:
            List of Provider objects
        """
        query = """
        MATCH (p:Provider)
        OPTIONAL MATCH (p)<-[:TREATED_BY]-(cl:Claim)<-[:FILED]-(c:Claimant)
        WITH p,
             count(DISTINCT cl) as claim_count,
             count(DISTINCT c) as claimant_count,
             sum(cl.claim_amount) as total_amount,
             avg(cl.risk_score) as avg_risk_score
        ORDER BY p.name
        SKIP $offset
        LIMIT $limit
        RETURN 
            p.provider_id as provider_id,
            p.name as name,
            p.provider_type as provider_type,
            p.license_number as license_number,
            p.street as street,
            p.city as city,
            p.state as state,
            p.zip_code as zip_code,
            p.phone as phone,
            claim_count,
            claimant_count,
            total_amount,
            avg_risk_score
        """
        
        results = self.driver.execute_query(query, {
            'limit': limit,
            'offset': offset
        })
        
        return [Provider.from_dict(r) for r in results] if results else []
    
    def get_provider_by_id(self, provider_id: str) -> Optional[Provider]:
        """
        Get provider by ID with statistics
        
        Args:
            provider_id: Provider ID
            
        Returns:
            Provider object or None
        """
        query = """
        MATCH (p:Provider {provider_id: $provider_id})
        OPTIONAL MATCH (p)<-[:TREATED_BY]-(cl:Claim)<-[:FILED]-(c:Claimant)
        WITH p,
             count(DISTINCT cl) as claim_count,
             count(DISTINCT c) as claimant_count,
             sum(cl.claim_amount) as total_amount,
             avg(cl.risk_score) as avg_risk_score
        
        OPTIONAL MATCH (p)<-[:TREATED_BY]-(:Claim)<-[:FILED]-(ring_c:Claimant)-[:MEMBER_OF]->(:FraudRing)
        WITH p, claim_count, claimant_count, total_amount, avg_risk_score,
             count(DISTINCT ring_c) as ring_connections
        
        RETURN 
            p.provider_id as provider_id,
            p.name as name,
            p.provider_type as provider_type,
            p.license_number as license_number,
            p.street as street,
            p.city as city,
            p.state as state,
            p.zip_code as zip_code,
            p.phone as phone,
            claim_count,
            claimant_count,
            total_amount,
            avg_risk_score,
            ring_connections
        """
        
        results = self.driver.execute_query(query, {'provider_id': provider_id})
        
        return Provider.from_dict(results[0]) if results else None
    
    def get_provider_claimants(
        self,
        provider_id: str,
        limit: int = 50
    ) -> List[Dict]:
        """
        Get all claimants for a provider
        
        Args:
            provider_id: Provider ID
            limit: Maximum number to return
            
        Returns:
            List of claimant dictionaries
        """
        query = """
        MATCH (p:Provider {provider_id: $provider_id})<-[:TREATED_BY]-(cl:Claim)<-[:FILED]-(c:Claimant)
        WITH c,
             count(cl) as claim_count,
             sum(cl.claim_amount) as total_amount,
             avg(cl.risk_score) as avg_risk_score
        ORDER BY claim_count DESC
        LIMIT $limit
        RETURN 
            c.claimant_id as claimant_id,
            c.name as name,
            claim_count,
            total_amount,
            avg_risk_score
        """
        
        results = self.driver.execute_query(query, {
            'provider_id': provider_id,
            'limit': limit
        })
        
        return results if results else []
    
    def get_high_volume_providers(
        self,
        min_claimants: int = 15,
        limit: int = 50
    ) -> List[Provider]:
        """
        Get providers with high claimant volume
        
        Args:
            min_claimants: Minimum number of claimants
            limit: Maximum number to return
            
        Returns:
            List of high-volume providers
        """
        query = """
        MATCH (p:Provider)<-[:TREATED_BY]-(cl:Claim)<-[:FILED]-(c:Claimant)
        WITH p,
             count(DISTINCT cl) as claim_count,
             count(DISTINCT c) as claimant_count,
             sum(cl.claim_amount) as total_amount,
             avg(cl.risk_score) as avg_risk_score
        WHERE claimant_count >= $min_claimants
        ORDER BY claimant_count DESC
        LIMIT $limit
        RETURN 
            p.provider_id as provider_id,
            p.name as name,
            p.provider_type as provider_type,
            p.city as city,
            p.state as state,
            claim_count,
            claimant_count,
            total_amount,
            avg_risk_score
        """
        
        results = self.driver.execute_query(query, {
            'min_claimants': min_claimants,
            'limit': limit
        })
        
        return [Provider.from_dict(r) for r in results] if results else []
    
    def get_suspicious_providers(
        self,
        risk_threshold: float = 70.0,
        limit: int = 50
    ) -> List[Provider]:
        """
        Get providers with high average risk scores
        
        Args:
            risk_threshold: Minimum average risk score
            limit: Maximum number to return
            
        Returns:
            List of suspicious providers
        """
        query = """
        MATCH (p:Provider)<-[:TREATED_BY]-(cl:Claim)
        WITH p,
             count(cl) as claim_count,
             avg(cl.risk_score) as avg_risk_score
        WHERE avg_risk_score >= $risk_threshold
        
        OPTIONAL MATCH (p)<-[:TREATED_BY]-(:Claim)<-[:FILED]-(c:Claimant)
        WITH p, claim_count, avg_risk_score,
             count(DISTINCT c) as claimant_count
        
        ORDER BY avg_risk_score DESC
        LIMIT $limit
        
        RETURN 
            p.provider_id as provider_id,
            p.name as name,
            p.provider_type as provider_type,
            p.city as city,
            p.state as state,
            claim_count,
            claimant_count,
            avg_risk_score
        """
        
        results = self.driver.execute_query(query, {
            'risk_threshold': risk_threshold,
            'limit': limit
        })
        
        return [Provider.from_dict(r) for r in results] if results else []
    
    def search_providers(
        self,
        search_term: str,
        limit: int = 50
    ) -> List[Provider]:
        """
        Search providers by name, ID, or license number
        
        Args:
            search_term: Search term
            limit: Maximum results
            
        Returns:
            List of matching providers
        """
        query = """
        MATCH (p:Provider)
        WHERE p.name CONTAINS $search_term 
           OR p.provider_id CONTAINS $search_term
           OR p.license_number CONTAINS $search_term
        
        OPTIONAL MATCH (p)<-[:TREATED_BY]-(cl:Claim)<-[:FILED]-(c:Claimant)
        WITH p,
             count(DISTINCT cl) as claim_count,
             count(DISTINCT c) as claimant_count,
             avg(cl.risk_score) as avg_risk_score
        
        ORDER BY p.name
        LIMIT $limit
        
        RETURN 
            p.provider_id as provider_id,
            p.name as name,
            p.provider_type as provider_type,
            p.city as city,
            p.state as state,
            claim_count,
            claimant_count,
            avg_risk_score
        """
        
        results = self.driver.execute_query(query, {
            'search_term': search_term,
            'limit': limit
        })
        
        return [Provider.from_dict(r) for r in results] if results else []
