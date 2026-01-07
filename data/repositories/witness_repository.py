"""
Witness Repository - Database operations for witnesses
Handles witness CRUD and professional witness detection
"""
import logging
from typing import Dict, List, Optional

from data.neo4j_driver import get_neo4j_driver
from data.models.claim import Witness
from utils.logger import setup_logger

logger = setup_logger(__name__)


class WitnessRepository:
    """
    Repository for witness database operations
    """
    
    def __init__(self):
        self.driver = get_neo4j_driver()
    
    # ==================== CREATE OPERATIONS ====================
    
    def create_witness(self, witness: Witness) -> bool:
        """
        Create a new witness in the database
        
        Args:
            witness: Witness object
            
        Returns:
            True if successful, False otherwise
        """
        try:
            query = """
            CREATE (w:Witness {
                witness_id: $witness_id,
                name: $name,
                phone: $phone
            })
            RETURN w.witness_id as witness_id
            """
            
            result = self.driver.execute_write(query, witness.to_dict())
            
            if result:
                logger.info(f"Created witness: {witness.witness_id}")
                return True
            
            return False
            
        except Exception as e:
            logger.error(f"Error creating witness: {e}", exc_info=True)
            return False
    
    def create_or_update_witness(self, witness: Witness) -> bool:
        """Create witness if not exists, update if exists"""
        try:
            query = """
            MERGE (w:Witness {witness_id: $witness_id})
            SET w.name = $name,
                w.phone = $phone
            RETURN w.witness_id as witness_id
            """
            
            result = self.driver.execute_write(query, witness.to_dict())
            
            if result:
                logger.info(f"Created/Updated witness: {witness.witness_id}")
                return True
            
            return False
            
        except Exception as e:
            logger.error(f"Error creating/updating witness: {e}", exc_info=True)
            return False
    
    # ==================== READ OPERATIONS ====================
    
    def get_witness_by_id(self, witness_id: str) -> Optional[Witness]:
        """Get witness by ID"""
        try:
            query = """
            MATCH (w:Witness {witness_id: $witness_id})
            RETURN 
                w.witness_id as witness_id,
                w.name as name,
                w.phone as phone
            """
            
            results = self.driver.execute_query(query, {'witness_id': witness_id})
            
            if results:
                return Witness.from_dict(results[0])
            
            return None
            
        except Exception as e:
            logger.error(f"Error getting witness: {e}", exc_info=True)
            return None
    
    def get_witness_claims(self, witness_id: str) -> List[Dict]:
        """Get all claims witnessed by this person"""
        try:
            query = """
            MATCH (w:Witness {witness_id: $witness_id})-[:WITNESSED]->(cl:Claim)
            MATCH (c:Claimant)-[:FILED]->(cl)
            
            OPTIONAL MATCH (cl)-[:OCCURRED_AT]->(l:AccidentLocation)
            
            RETURN 
                cl.claim_id as claim_id,
                cl.claim_number as claim_number,
                c.name as claimant_name,
                cl.accident_date as accident_date,
                cl.accident_type as accident_type,
                l.intersection as location,
                cl.total_claim_amount as total_amount,
                cl.risk_score as risk_score,
                cl.status as status
            ORDER BY cl.accident_date DESC
            """
            
            results = self.driver.execute_query(query, {'witness_id': witness_id})
            return results
            
        except Exception as e:
            logger.error(f"Error getting witness claims: {e}", exc_info=True)
            return []
    
    def get_witness_statistics(self, witness_id: str) -> Dict:
        """Get statistics for a witness"""
        try:
            query = """
            MATCH (w:Witness {witness_id: $witness_id})
            OPTIONAL MATCH (w)-[:WITNESSED]->(cl:Claim)
            OPTIONAL MATCH (c:Claimant)-[:FILED]->(cl)
            OPTIONAL MATCH (c)-[:MEMBER_OF]->(r:FraudRing)
            
            WITH w, cl, c, r
            
            RETURN 
                w.witness_id as witness_id,
                w.name as name,
                count(DISTINCT cl) as witnessed_count,
                count(DISTINCT c) as unique_claimants,
                sum(cl.total_claim_amount) as total_claim_value,
                avg(cl.risk_score) as avg_risk_score,
                count(DISTINCT r) as fraud_ring_connections,
                max(cl.accident_date) as last_witnessed_date,
                min(cl.accident_date) as first_witnessed_date
            """
            
            results = self.driver.execute_query(query, {'witness_id': witness_id})
            
            if results:
                return results[0]
            
            return {}
            
        except Exception as e:
            logger.error(f"Error getting witness statistics: {e}", exc_info=True)
            return {}
    
    def get_professional_witnesses(self, min_witnessed: int = 3, limit: int = 50) -> List[Dict]:
        """Get professional witnesses (appeared in multiple accidents)"""
        try:
            query = """
            MATCH (w:Witness)-[:WITNESSED]->(cl:Claim)
            MATCH (c:Claimant)-[:FILED]->(cl)
            
            OPTIONAL MATCH (c)-[:MEMBER_OF]->(r:FraudRing)
            
            WITH w,
                 count(DISTINCT cl) as witnessed_count,
                 count(DISTINCT c) as unique_claimants,
                 avg(cl.risk_score) as avg_risk,
                 collect(DISTINCT r.ring_id) as ring_ids,
                 max(cl.accident_date) as last_witnessed
            
            WHERE witnessed_count >= $min_witnessed
            
            RETURN 
                w.witness_id as witness_id,
                w.name as name,
                w.phone as phone,
                witnessed_count,
                unique_claimants,
                avg_risk,
                size([r IN ring_ids WHERE r IS NOT NULL]) as ring_connections,
                last_witnessed
            ORDER BY witnessed_count DESC, avg_risk DESC
            LIMIT $limit
            """
            
            results = self.driver.execute_query(query, {
                'min_witnessed': min_witnessed,
                'limit': limit
            })
            
            return results
            
        except Exception as e:
            logger.error(f"Error getting professional witnesses: {e}", exc_info=True)
            return []
    
    def find_witness_networks(self, witness_id: str) -> Dict:
        """Find claimants and patterns connected to this witness"""
        try:
            query = """
            MATCH (w:Witness {witness_id: $witness_id})-[:WITNESSED]->(cl:Claim)
            MATCH (c:Claimant)-[:FILED]->(cl)
            
            // Find other witnesses who also witnessed same claims
            OPTIONAL MATCH (other_w:Witness)-[:WITNESSED]->(cl)
            WHERE other_w.witness_id <> w.witness_id
            
            // Find locations
            OPTIONAL MATCH (cl)-[:OCCURRED_AT]->(l:AccidentLocation)
            
            WITH w, c,
                 count(DISTINCT cl) as shared_claims,
                 collect(DISTINCT other_w.name) as co_witnesses,
                 collect(DISTINCT l.intersection) as locations
            
            RETURN 
                c.claimant_id as claimant_id,
                c.name as claimant_name,
                shared_claims,
                co_witnesses,
                locations
            ORDER BY shared_claims DESC
            LIMIT 20
            """
            
            results = self.driver.execute_query(query, {'witness_id': witness_id})
            
            return {
                'claimants': results,
                'total_connections': len(results)
            }
            
        except Exception as e:
            logger.error(f"Error finding witness networks: {e}", exc_info=True)
            return {}
    
    def find_shared_witnesses(self, claimant_id: str) -> List[Dict]:
        """Find other claimants who share witnesses with this claimant"""
        try:
            query = """
            MATCH (c1:Claimant {claimant_id: $claimant_id})-[:FILED]->(cl1:Claim)<-[:WITNESSED]-(w:Witness)
            MATCH (w)-[:WITNESSED]->(cl2:Claim)<-[:FILED]-(c2:Claimant)
            WHERE c1.claimant_id <> c2.claimant_id
            
            WITH c2, 
                 collect(DISTINCT w.name) as shared_witnesses,
                 count(DISTINCT w) as witness_count
            
            RETURN 
                c2.claimant_id as claimant_id,
                c2.name as claimant_name,
                witness_count,
                shared_witnesses
            ORDER BY witness_count DESC
            LIMIT 20
            """
            
            results = self.driver.execute_query(query, {'claimant_id': claimant_id})
            return results
            
        except Exception as e:
            logger.error(f"Error finding shared witnesses: {e}", exc_info=True)
            return []
    
    def find_witnesses_by_phone(self, phone: str) -> List[Witness]:
        """Find witnesses by phone number (duplicate detection)"""
        try:
            query = """
            MATCH (w:Witness {phone: $phone})
            RETURN 
                w.witness_id as witness_id,
                w.name as name,
                w.phone as phone
            """
            
            results = self.driver.execute_query(query, {'phone': phone})
            
            return [Witness.from_dict(row) for row in results]
            
        except Exception as e:
            logger.error(f"Error finding witnesses by phone: {e}", exc_info=True)
            return []
    
    # ==================== UPDATE OPERATIONS ====================
    
    def update_witness(self, witness_id: str, updates: Dict) -> bool:
        """Update witness properties"""
        try:
            set_clauses = [f"w.{key} = ${key}" for key in updates.keys()]
            set_clause = ", ".join(set_clauses)
            
            query = f"""
            MATCH (w:Witness {{witness_id: $witness_id}})
            SET {set_clause}
            RETURN w.witness_id as witness_id
            """
            
            params = {'witness_id': witness_id}
            params.update(updates)
            
            result = self.driver.execute_write(query, params)
            
            if result:
                logger.info(f"Updated witness: {witness_id}")
                return True
            
            return False
            
        except Exception as e:
            logger.error(f"Error updating witness: {e}", exc_info=True)
            return False
    
    # ==================== DELETE OPERATIONS ====================
    
    def delete_witness(self, witness_id: str) -> bool:
        """Delete a witness and its relationships"""
        try:
            query = """
            MATCH (w:Witness {witness_id: $witness_id})
            DETACH DELETE w
            """
            
            self.driver.execute_write(query, {'witness_id': witness_id})
            logger.info(f"Deleted witness: {witness_id}")
            return True
            
        except Exception as e:
            logger.error(f"Error deleting witness: {e}", exc_info=True)
            return False
    
    # ==================== SEARCH OPERATIONS ====================
    
    def search_witnesses(
        self,
        name: Optional[str] = None,
        phone: Optional[str] = None,
        limit: int = 100
    ) -> List[Witness]:
        """Search witnesses by name or phone"""
        try:
            where_clauses = []
            params = {'limit': limit}
            
            if name:
                where_clauses.append("w.name CONTAINS $name")
                params['name'] = name
            
            if phone:
                where_clauses.append("w.phone = $phone")
                params['phone'] = phone
            
            where_clause = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""
            
            query = f"""
            MATCH (w:Witness)
            {where_clause}
            RETURN 
                w.witness_id as witness_id,
                w.name as name,
                w.phone as phone
            LIMIT $limit
            """
            
            results = self.driver.execute_query(query, params)
            
            return [Witness.from_dict(row) for row in results]
            
        except Exception as e:
            logger.error(f"Error searching witnesses: {e}", exc_info=True)
            return []
