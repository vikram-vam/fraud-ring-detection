"""
Attorney Repository - Data access for attorneys
"""
from typing import List, Dict, Optional
import logging

from data.neo4j_driver import Neo4jDriver
from data.models.attorney import Attorney

logger = logging.getLogger(__name__)


class AttorneyRepository:
    """Repository for Attorney entity operations"""
    
    def __init__(self):
        self.driver = Neo4jDriver()
    
    def get_all_attorneys(
        self,
        limit: int = 100,
        offset: int = 0
    ) -> List[Attorney]:
        """
        Get all attorneys with pagination
        
        Args:
            limit: Maximum number to return
            offset: Number to skip
            
        Returns:
            List of Attorney objects
        """
        query = """
        MATCH (a:Attorney)
        OPTIONAL MATCH (a)<-[:REPRESENTED_BY]-(cl:Claim)<-[:FILED]-(c:Claimant)
        WITH a,
             count(DISTINCT cl) as claim_count,
             count(DISTINCT c) as client_count,
             sum(cl.claim_amount) as total_amount,
             avg(cl.risk_score) as avg_risk_score
        ORDER BY a.name
        SKIP $offset
        LIMIT $limit
        RETURN 
            a.attorney_id as attorney_id,
            a.name as name,
            a.firm as firm,
            a.bar_number as bar_number,
            a.street as street,
            a.city as city,
            a.state as state,
            a.zip_code as zip_code,
            a.phone as phone,
            a.email as email,
            claim_count,
            client_count,
            total_amount,
            avg_risk_score
        """
        
        results = self.driver.execute_query(query, {
            'limit': limit,
            'offset': offset
        })
        
        return [Attorney.from_dict(r) for r in results] if results else []
    
    def get_attorney_by_id(self, attorney_id: str) -> Optional[Attorney]:
        """
        Get attorney by ID with statistics
        
        Args:
            attorney_id: Attorney ID
            
        Returns:
            Attorney object or None
        """
        query = """
        MATCH (a:Attorney {attorney_id: $attorney_id})
        OPTIONAL MATCH (a)<-[:REPRESENTED_BY]-(cl:Claim)<-[:FILED]-(c:Claimant)
        WITH a,
             count(DISTINCT cl) as claim_count,
             count(DISTINCT c) as client_count,
             sum(cl.claim_amount) as total_amount,
             avg(cl.risk_score) as avg_risk_score
        
        OPTIONAL MATCH (a)<-[:REPRESENTED_BY]-(:Claim)<-[:FILED]-(ring_c:Claimant)-[:MEMBER_OF]->(:FraudRing)
        WITH a, claim_count, client_count, total_amount, avg_risk_score,
             count(DISTINCT ring_c) as ring_connections
        
        RETURN 
            a.attorney_id as attorney_id,
            a.name as name,
            a.firm as firm,
            a.bar_number as bar_number,
            a.street as street,
            a.city as city,
            a.state as state,
            a.zip_code as zip_code,
            a.phone as phone,
            a.email as email,
            claim_count,
            client_count,
            total_amount,
            avg_risk_score,
            ring_connections
        """
        
        results = self.driver.execute_query(query, {'attorney_id': attorney_id})
        
        return Attorney.from_dict(results[0]) if results else None
    
    def get_attorney_clients(
        self,
        attorney_id: str,
        limit: int = 50
    ) -> List[Dict]:
        """
        Get all clients for an attorney
        
        Args:
            attorney_id: Attorney ID
            limit: Maximum number to return
            
        Returns:
            List of client dictionaries
        """
        query = """
        MATCH (a:Attorney {attorney_id: $attorney_id})<-[:REPRESENTED_BY]-(cl:Claim)<-[:FILED]-(c:Claimant)
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
            'attorney_id': attorney_id,
            'limit': limit
        })
        
        return results if results else []
    
    def get_high_volume_attorneys(
        self,
        min_clients: int = 10,
        limit: int = 50
    ) -> List[Attorney]:
        """
        Get attorneys with high client volume
        
        Args:
            min_clients: Minimum number of clients
            limit: Maximum number to return
            
        Returns:
            List of high-volume attorneys
        """
        query = """
        MATCH (a:Attorney)<-[:REPRESENTED_BY]-(cl:Claim)<-[:FILED]-(c:Claimant)
        WITH a,
             count(DISTINCT cl) as claim_count,
             count(DISTINCT c) as client_count,
             sum(cl.claim_amount) as total_amount,
             avg(cl.risk_score) as avg_risk_score
        WHERE client_count >= $min_clients
        ORDER BY client_count DESC
        LIMIT $limit
        RETURN 
            a.attorney_id as attorney_id,
            a.name as name,
            a.firm as firm,
            a.city as city,
            a.state as state,
            claim_count,
            client_count,
            total_amount,
            avg_risk_score
        """
        
        results = self.driver.execute_query(query, {
            'min_clients': min_clients,
            'limit': limit
        })
        
        return [Attorney.from_dict(r) for r in results] if results else []
    
    def get_suspicious_attorneys(
        self,
        risk_threshold: float = 70.0,
        limit: int = 50
    ) -> List[Attorney]:
        """
        Get attorneys with high average risk scores
        
        Args:
            risk_threshold: Minimum average risk score
            limit: Maximum number to return
            
        Returns:
            List of suspicious attorneys
        """
        query = """
        MATCH (a:Attorney)<-[:REPRESENTED_BY]-(cl:Claim)
        WITH a,
             count(cl) as claim_count,
             avg(cl.risk_score) as avg_risk_score
        WHERE avg_risk_score >= $risk_threshold
        
        OPTIONAL MATCH (a)<-[:REPRESENTED_BY]-(:Claim)<-[:FILED]-(c:Claimant)
        WITH a, claim_count, avg_risk_score,
             count(DISTINCT c) as client_count
        
        ORDER BY avg_risk_score DESC
        LIMIT $limit
        
        RETURN 
            a.attorney_id as attorney_id,
            a.name as name,
            a.firm as firm,
            a.city as city,
            a.state as state,
            claim_count,
            client_count,
            avg_risk_score
        """
        
        results = self.driver.execute_query(query, {
            'risk_threshold': risk_threshold,
            'limit': limit
        })
        
        return [Attorney.from_dict(r) for r in results] if results else []
    
    def search_attorneys(
        self,
        search_term: str,
        limit: int = 50
    ) -> List[Attorney]:
        """
        Search attorneys by name, firm, or bar number
        
        Args:
            search_term: Search term
            limit: Maximum results
            
        Returns:
            List of matching attorneys
        """
        query = """
        MATCH (a:Attorney)
        WHERE a.name CONTAINS $search_term 
           OR a.firm CONTAINS $search_term
           OR a.bar_number CONTAINS $search_term
        
        OPTIONAL MATCH (a)<-[:REPRESENTED_BY]-(cl:Claim)<-[:FILED]-(c:Claimant)
        WITH a,
             count(DISTINCT cl) as claim_count,
             count(DISTINCT c) as client_count,
             avg(cl.risk_score) as avg_risk_score
        
        ORDER BY a.name
        LIMIT $limit
        
        RETURN 
            a.attorney_id as attorney_id,
            a.name as name,
            a.firm as firm,
            a.city as city,
            a.state as state,
            claim_count,
            client_count,
            avg_risk_score
        """
        
        results = self.driver.execute_query(query, {
            'search_term': search_term,
            'limit': limit
        })
        
        return [Attorney.from_dict(r) for r in results] if results else []
