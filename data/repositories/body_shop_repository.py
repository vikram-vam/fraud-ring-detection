"""
Body Shop Repository - Database operations for body shops
Handles body shop CRUD and fraud detection queries
"""
import logging
from typing import Dict, List, Optional

from data.neo4j_driver import get_neo4j_driver
from data.models.claim import BodyShop
from utils.logger import setup_logger

logger = setup_logger(__name__)


class BodyShopRepository:
    """
    Repository for body shop database operations
    """
    
    def __init__(self):
        self.driver = get_neo4j_driver()
    
    # ==================== CREATE OPERATIONS ====================
    
    def create_body_shop(self, body_shop: BodyShop) -> bool:
        """
        Create a new body shop in the database
        
        Args:
            body_shop: BodyShop object
            
        Returns:
            True if successful, False otherwise
        """
        try:
            query = """
            CREATE (b:BodyShop {
                body_shop_id: $body_shop_id,
                name: $name,
                license_number: $license_number,
                phone: $phone,
                street: $street,
                city: $city,
                state: $state,
                zip_code: $zip_code
            })
            RETURN b.body_shop_id as body_shop_id
            """
            
            result = self.driver.execute_write(query, body_shop.to_dict())
            
            if result:
                logger.info(f"Created body shop: {body_shop.body_shop_id}")
                return True
            
            return False
            
        except Exception as e:
            logger.error(f"Error creating body shop: {e}", exc_info=True)
            return False
    
    def create_or_update_body_shop(self, body_shop: BodyShop) -> bool:
        """Create body shop if not exists, update if exists"""
        try:
            query = """
            MERGE (b:BodyShop {body_shop_id: $body_shop_id})
            SET b.name = $name,
                b.license_number = $license_number,
                b.phone = $phone,
                b.street = $street,
                b.city = $city,
                b.state = $state,
                b.zip_code = $zip_code
            RETURN b.body_shop_id as body_shop_id
            """
            
            result = self.driver.execute_write(query, body_shop.to_dict())
            
            if result:
                logger.info(f"Created/Updated body shop: {body_shop.body_shop_id}")
                return True
            
            return False
            
        except Exception as e:
            logger.error(f"Error creating/updating body shop: {e}", exc_info=True)
            return False
    
    # ==================== READ OPERATIONS ====================
    
    def get_body_shop_by_id(self, body_shop_id: str) -> Optional[BodyShop]:
        """Get body shop by ID"""
        try:
            query = """
            MATCH (b:BodyShop {body_shop_id: $body_shop_id})
            RETURN 
                b.body_shop_id as body_shop_id,
                b.name as name,
                b.license_number as license_number,
                b.phone as phone,
                b.street as street,
                b.city as city,
                b.state as state,
                b.zip_code as zip_code
            """
            
            results = self.driver.execute_query(query, {'body_shop_id': body_shop_id})
            
            if results:
                return BodyShop.from_dict(results[0])
            
            return None
            
        except Exception as e:
            logger.error(f"Error getting body shop: {e}", exc_info=True)
            return None
    
    def get_body_shop_claims(self, body_shop_id: str) -> List[Dict]:
        """Get all claims repaired at this body shop"""
        try:
            query = """
            MATCH (b:BodyShop {body_shop_id: $body_shop_id})<-[:REPAIRED_AT]-(cl:Claim)
            MATCH (c:Claimant)-[:FILED]->(cl)
            
            RETURN 
                cl.claim_id as claim_id,
                cl.claim_number as claim_number,
                c.name as claimant_name,
                cl.accident_date as accident_date,
                cl.property_damage_amount as repair_amount,
                cl.total_claim_amount as total_amount,
                cl.risk_score as risk_score,
                cl.status as status
            ORDER BY cl.accident_date DESC
            """
            
            results = self.driver.execute_query(query, {'body_shop_id': body_shop_id})
            return results
            
        except Exception as e:
            logger.error(f"Error getting body shop claims: {e}", exc_info=True)
            return []
    
    def get_body_shop_statistics(self, body_shop_id: str) -> Dict:
        """Get statistics for a body shop"""
        try:
            query = """
            MATCH (b:BodyShop {body_shop_id: $body_shop_id})
            OPTIONAL MATCH (b)<-[:REPAIRED_AT]-(cl:Claim)
            OPTIONAL MATCH (c:Claimant)-[:FILED]->(cl)
            
            WITH b, cl, c
            
            RETURN 
                b.body_shop_id as body_shop_id,
                b.name as name,
                b.city as city,
                count(DISTINCT cl) as claim_count,
                count(DISTINCT c) as unique_claimants,
                sum(cl.property_damage_amount) as total_repairs,
                avg(cl.property_damage_amount) as avg_repair_amount,
                avg(cl.risk_score) as avg_risk_score,
                max(cl.accident_date) as last_repair_date
            """
            
            results = self.driver.execute_query(query, {'body_shop_id': body_shop_id})
            
            if results:
                return results[0]
            
            return {}
            
        except Exception as e:
            logger.error(f"Error getting body shop statistics: {e}", exc_info=True)
            return {}
    
    def get_high_volume_body_shops(self, min_claims: int = 20, limit: int = 50) -> List[Dict]:
        """Get body shops with high claim volume (potential fraud indicators)"""
        try:
            query = """
            MATCH (b:BodyShop)<-[:REPAIRED_AT]-(cl:Claim)
            MATCH (c:Claimant)-[:FILED]->(cl)
            
            WITH b,
                 count(DISTINCT cl) as claim_count,
                 count(DISTINCT c) as unique_claimants,
                 sum(cl.property_damage_amount) as total_repairs,
                 avg(cl.property_damage_amount) as avg_repair_amount,
                 avg(cl.risk_score) as avg_risk_score
            
            WHERE claim_count >= $min_claims
            
            // Check for repeat claimants
            MATCH (b)<-[:REPAIRED_AT]-(cl2:Claim)<-[:FILED]-(c2:Claimant)
            WITH b, claim_count, unique_claimants, total_repairs, avg_repair_amount, avg_risk_score,
                 c2.claimant_id as claimant_id, count(cl2) as claimant_visits
            WHERE claimant_visits >= 2
            
            WITH b, claim_count, unique_claimants, total_repairs, avg_repair_amount, avg_risk_score,
                 count(DISTINCT claimant_id) as repeat_claimants
            
            RETURN 
                b.body_shop_id as body_shop_id,
                b.name as name,
                b.city as city,
                claim_count,
                unique_claimants,
                total_repairs,
                avg_repair_amount,
                avg_risk_score,
                repeat_claimants,
                toFloat(repeat_claimants) / unique_claimants as repeat_ratio
            ORDER BY avg_risk_score DESC, claim_count DESC
            LIMIT $limit
            """
            
            results = self.driver.execute_query(query, {
                'min_claims': min_claims,
                'limit': limit
            })
            
            return results
            
        except Exception as e:
            logger.error(f"Error getting high volume body shops: {e}", exc_info=True)
            return []
    
    def find_body_shop_networks(self, body_shop_id: str) -> List[Dict]:
        """Find claimants and other entities connected to this body shop"""
        try:
            query = """
            MATCH (b:BodyShop {body_shop_id: $body_shop_id})<-[:REPAIRED_AT]-(cl:Claim)
            MATCH (c:Claimant)-[:FILED]->(cl)
            
            // Find attorneys connected to this body shop
            OPTIONAL MATCH (cl)-[:REPRESENTED_BY]->(a:Attorney)
            
            // Find medical providers connected to this body shop
            OPTIONAL MATCH (cl)-[:TREATED_BY]->(m:MedicalProvider)
            
            // Find tow companies connected to this body shop
            OPTIONAL MATCH (cl)-[:TOWED_BY]->(t:TowCompany)
            
            WITH b, c, 
                 collect(DISTINCT a.name) as attorneys,
                 collect(DISTINCT m.name) as medical_providers,
                 collect(DISTINCT t.name) as tow_companies,
                 count(cl) as claimant_claim_count
            
            RETURN 
                c.claimant_id as claimant_id,
                c.name as claimant_name,
                claimant_claim_count,
                attorneys,
                medical_providers,
                tow_companies
            ORDER BY claimant_claim_count DESC
            LIMIT 50
            """
            
            results = self.driver.execute_query(query, {'body_shop_id': body_shop_id})
            return results
            
        except Exception as e:
            logger.error(f"Error finding body shop networks: {e}", exc_info=True)
            return []
    
    def get_body_shop_referral_sources(self, body_shop_id: str) -> Dict:
        """Analyze how claims arrive at this body shop (tow companies, etc.)"""
        try:
            query = """
            MATCH (b:BodyShop {body_shop_id: $body_shop_id})<-[:REPAIRED_AT]-(cl:Claim)
            
            // Tow company referrals
            OPTIONAL MATCH (cl)-[:TOWED_BY]->(t:TowCompany)
            
            // Attorney referrals
            OPTIONAL MATCH (cl)-[:REPRESENTED_BY]->(a:Attorney)
            
            WITH b,
                 count(DISTINCT cl) as total_claims,
                 count(DISTINCT t) as towed_claims,
                 count(DISTINCT a) as attorney_claims,
                 collect(DISTINCT {name: t.name, count: size((t)<-[:TOWED_BY]-(:Claim)-[:REPAIRED_AT]->(b))}) as tow_sources,
                 collect(DISTINCT {name: a.name, count: size((a)<-[:REPRESENTED_BY]-(:Claim)-[:REPAIRED_AT]->(b))}) as attorney_sources
            
            RETURN 
                b.name as body_shop_name,
                total_claims,
                towed_claims,
                attorney_claims,
                toFloat(towed_claims) / total_claims as tow_referral_ratio,
                toFloat(attorney_claims) / total_claims as attorney_referral_ratio,
                tow_sources,
                attorney_sources
            """
            
            results = self.driver.execute_query(query, {'body_shop_id': body_shop_id})
            
            if results:
                return results[0]
            
            return {}
            
        except Exception as e:
            logger.error(f"Error getting body shop referral sources: {e}", exc_info=True)
            return {}
    
    # ==================== UPDATE OPERATIONS ====================
    
    def update_body_shop(self, body_shop_id: str, updates: Dict) -> bool:
        """Update body shop properties"""
        try:
            set_clauses = [f"b.{key} = ${key}" for key in updates.keys()]
            set_clause = ", ".join(set_clauses)
            
            query = f"""
            MATCH (b:BodyShop {{body_shop_id: $body_shop_id}})
            SET {set_clause}
            RETURN b.body_shop_id as body_shop_id
            """
            
            params = {'body_shop_id': body_shop_id}
            params.update(updates)
            
            result = self.driver.execute_write(query, params)
            
            if result:
                logger.info(f"Updated body shop: {body_shop_id}")
                return True
            
            return False
            
        except Exception as e:
            logger.error(f"Error updating body shop: {e}", exc_info=True)
            return False
    
    # ==================== DELETE OPERATIONS ====================
    
    def delete_body_shop(self, body_shop_id: str) -> bool:
        """Delete a body shop and its relationships"""
        try:
            query = """
            MATCH (b:BodyShop {body_shop_id: $body_shop_id})
            DETACH DELETE b
            """
            
            self.driver.execute_write(query, {'body_shop_id': body_shop_id})
            logger.info(f"Deleted body shop: {body_shop_id}")
            return True
            
        except Exception as e:
            logger.error(f"Error deleting body shop: {e}", exc_info=True)
            return False
    
    # ==================== SEARCH OPERATIONS ====================
    
    def search_body_shops(
        self,
        city: Optional[str] = None,
        state: Optional[str] = None,
        name: Optional[str] = None,
        limit: int = 100
    ) -> List[BodyShop]:
        """Search body shops by location or name"""
        try:
            where_clauses = []
            params = {'limit': limit}
            
            if city:
                where_clauses.append("b.city = $city")
                params['city'] = city
            
            if state:
                where_clauses.append("b.state = $state")
                params['state'] = state
            
            if name:
                where_clauses.append("b.name CONTAINS $name")
                params['name'] = name
            
            where_clause = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""
            
            query = f"""
            MATCH (b:BodyShop)
            {where_clause}
            RETURN 
                b.body_shop_id as body_shop_id,
                b.name as name,
                b.license_number as license_number,
                b.phone as phone,
                b.street as street,
                b.city as city,
                b.state as state,
                b.zip_code as zip_code
            LIMIT $limit
            """
            
            results = self.driver.execute_query(query, params)
            
            return [BodyShop.from_dict(row) for row in results]
            
        except Exception as e:
            logger.error(f"Error searching body shops: {e}", exc_info=True)
            return []
