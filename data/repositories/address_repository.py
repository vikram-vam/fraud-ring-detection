"""
Address Repository - Data access for addresses
"""
from typing import List, Dict, Optional
import logging

from data.neo4j_driver import Neo4jDriver
from data.models.address import Address

logger = logging.getLogger(__name__)


class AddressRepository:
    """Repository for Address entity operations"""
    
    def __init__(self):
        self.driver = Neo4jDriver()
    
    def get_all_addresses(
        self,
        limit: int = 100,
        offset: int = 0
    ) -> List[Address]:
        """
        Get all addresses with pagination
        
        Args:
            limit: Maximum number to return
            offset: Number to skip
            
        Returns:
            List of Address objects
        """
        query = """
        MATCH (a:Address)
        OPTIONAL MATCH (a)<-[:LIVES_AT]-(c:Claimant)
        OPTIONAL MATCH (c)-[:FILED]->(cl:Claim)
        WITH a,
             count(DISTINCT c) as resident_count,
             count(DISTINCT cl) as claim_count,
             sum(cl.claim_amount) as total_claim_amount
        ORDER BY a.city, a.street
        SKIP $offset
        LIMIT $limit
        RETURN 
            a.address_id as address_id,
            a.street as street,
            a.city as city,
            a.state as state,
            a.zip_code as zip_code,
            resident_count,
            claim_count,
            total_claim_amount,
            CASE WHEN resident_count >= 3 THEN true ELSE false END as is_address_farm
        """
        
        results = self.driver.execute_query(query, {
            'limit': limit,
            'offset': offset
        })
        
        return [Address.from_dict(r) for r in results] if results else []
    
    def get_address_by_id(self, address_id: str) -> Optional[Address]:
        """
        Get address by ID with statistics
        
        Args:
            address_id: Address ID
            
        Returns:
            Address object or None
        """
        query = """
        MATCH (a:Address {address_id: $address_id})
        OPTIONAL MATCH (a)<-[:LIVES_AT]-(c:Claimant)
        OPTIONAL MATCH (c)-[:FILED]->(cl:Claim)
        WITH a,
             count(DISTINCT c) as resident_count,
             count(DISTINCT cl) as claim_count,
             sum(cl.claim_amount) as total_claim_amount
        RETURN 
            a.address_id as address_id,
            a.street as street,
            a.city as city,
            a.state as state,
            a.zip_code as zip_code,
            resident_count,
            claim_count,
            total_claim_amount,
            CASE WHEN resident_count >= 3 THEN true ELSE false END as is_address_farm
        """
        
        results = self.driver.execute_query(query, {'address_id': address_id})
        
        return Address.from_dict(results[0]) if results else None
    
    def get_address_residents(
        self,
        address_id: str,
        limit: int = 50
    ) -> List[Dict]:
        """
        Get all residents at an address
        
        Args:
            address_id: Address ID
            limit: Maximum number to return
            
        Returns:
            List of resident dictionaries
        """
        query = """
        MATCH (a:Address {address_id: $address_id})<-[:LIVES_AT]-(c:Claimant)
        OPTIONAL MATCH (c)-[:FILED]->(cl:Claim)
        WITH c,
             count(cl) as claim_count,
             sum(cl.claim_amount) as total_amount,
             avg(cl.risk_score) as avg_risk_score
        ORDER BY claim_count DESC
        LIMIT $limit
        RETURN 
            c.claimant_id as claimant_id,
            c.name as name,
            c.email as email,
            claim_count,
            total_amount,
            avg_risk_score
        """
        
        results = self.driver.execute_query(query, {
            'address_id': address_id,
            'limit': limit
        })
        
        return results if results else []
    
    def get_address_farms(
        self,
        min_residents: int = 3,
        limit: int = 50
    ) -> List[Address]:
        """
        Get address farms (addresses with multiple residents)
        
        Args:
            min_residents: Minimum number of residents
            limit: Maximum number to return
            
        Returns:
            List of address farm Address objects
        """
        query = """
        MATCH (a:Address)<-[:LIVES_AT]-(c:Claimant)
        WITH a, count(DISTINCT c) as resident_count
        WHERE resident_count >= $min_residents
        
        OPTIONAL MATCH (a)<-[:LIVES_AT]-(c2:Claimant)-[:FILED]->(cl:Claim)
        WITH a, resident_count,
             count(DISTINCT cl) as claim_count,
             sum(cl.claim_amount) as total_claim_amount
        
        ORDER BY resident_count DESC
        LIMIT $limit
        
        RETURN 
            a.address_id as address_id,
            a.street as street,
            a.city as city,
            a.state as state,
            a.zip_code as zip_code,
            resident_count,
            claim_count,
            total_claim_amount,
            true as is_address_farm
        """
        
        results = self.driver.execute_query(query, {
            'min_residents': min_residents,
            'limit': limit
        })
        
        return [Address.from_dict(r) for r in results] if results else []
    
    def search_addresses(
        self,
        search_term: str,
        limit: int = 50
    ) -> List[Address]:
        """
        Search addresses by street, city, or zip code
        
        Args:
            search_term: Search term
            limit: Maximum results
            
        Returns:
            List of matching addresses
        """
        query = """
        MATCH (a:Address)
        WHERE a.street CONTAINS $search_term 
           OR a.city CONTAINS $search_term
           OR a.zip_code CONTAINS $search_term
        
        OPTIONAL MATCH (a)<-[:LIVES_AT]-(c:Claimant)
        OPTIONAL MATCH (c)-[:FILED]->(cl:Claim)
        WITH a,
             count(DISTINCT c) as resident_count,
             count(DISTINCT cl) as claim_count,
             sum(cl.claim_amount) as total_claim_amount
        
        ORDER BY a.city, a.street
        LIMIT $limit
        
        RETURN 
            a.address_id as address_id,
            a.street as street,
            a.city as city,
            a.state as state,
            a.zip_code as zip_code,
            resident_count,
            claim_count,
            total_claim_amount,
            CASE WHEN resident_count >= 3 THEN true ELSE false END as is_address_farm
        """
        
        results = self.driver.execute_query(query, {
            'search_term': search_term,
            'limit': limit
        })
        
        return [Address.from_dict(r) for r in results] if results else []
