"""
Tow Company Repository - Database operations for tow companies
Handles tow company CRUD and fraud detection queries
"""
import logging
from typing import Dict, List, Optional

from data.neo4j_driver import get_neo4j_driver
from data.models.claim import TowCompany
from utils.logger import setup_logger

logger = setup_logger(__name__)


class TowCompanyRepository:
    """
    Repository for tow company database operations
    """
    
    def __init__(self):
        self.driver = get_neo4j_driver()
    
    # ==================== CREATE OPERATIONS ====================
    
    def create_tow_company(self, tow_company: TowCompany) -> bool:
        """
        Create a new tow company in the database
        
        Args:
            tow_company: TowCompany object
            
        Returns:
            True if successful, False otherwise
        """
        try:
            query = """
            CREATE (t:TowCompany {
                tow_company_id: $tow_company_id,
                name: $name,
                license_number: $license_number,
                phone: $phone,
                city: $city,
                state: $state
            })
            RETURN t.tow_company_id as tow_company_id
            """
            
            result = self.driver.execute_write(query, tow_company.to_dict())
            
            if result:
                logger.info(f"Created tow company: {tow_company.tow_company_id}")
                return True
            
            return False
            
        except Exception as e:
            logger.error(f"Error creating tow company: {e}", exc_info=True)
            return False
    
    def create_or_update_tow_company(self, tow_company: TowCompany) -> bool:
        """Create tow company if not exists, update if exists"""
        try:
            query = """
            MERGE (t:TowCompany {tow_company_id: $tow_company_id})
            SET t.name = $name,
                t.license_number = $license_number,
                t.phone = $phone,
                t.city = $city,
                t.state = $state
            RETURN t.tow_company_id as tow_company_id
            """
            
            result = self.driver.execute_write(query, tow_company.to_dict())
            
            if result:
                logger.info(f"Created/Updated tow company: {tow_company.tow_company_id}")
                return True
            
            return False
            
        except Exception as e:
            logger.error(f"Error creating/updating tow company: {e}", exc_info=True)
            return False
    
    # ==================== READ OPERATIONS ====================
    
    def get_tow_company_by_id(self, tow_company_id: str) -> Optional[TowCompany]:
        """Get tow company by ID"""
        try:
            query = """
            MATCH (t:TowCompany {tow_company_id: $tow_company_id})
            RETURN 
                t.tow_company_id as tow_company_id,
                t.name as name,
                t.license_number as license_number,
                t.phone as phone,
                t.city as city,
                t.state as state
            """
            
            results = self.driver.execute_query(query, {'tow_company_id': tow_company_id})
            
            if results:
                return TowCompany.from_dict(results[0])
            
            return None
            
        except Exception as e:
            logger.error(f"Error getting tow company: {e}", exc_info=True)
            return None
    
    def get_tow_company_claims(self, tow_company_id: str) -> List[Dict]:
        """Get all claims towed by this company"""
        try:
            query = """
            MATCH (t:TowCompany {tow_company_id: $tow_company_id})<-[:TOWED_BY]-(cl:Claim)
            MATCH (c:Claimant)-[:FILED]->(cl)
            
            OPTIONAL MATCH (cl)-[:REPAIRED_AT]->(b:BodyShop)
            
            RETURN 
                cl.claim_id as claim_id,
                cl.claim_number as claim_number,
                c.name as claimant_name,
                cl.accident_date as accident_date,
                cl.total_claim_amount as total_amount,
                cl.risk_score as risk_score,
                b.name as body_shop,
                cl.status as status
            ORDER BY cl.accident_date DESC
            """
            
            results = self.driver.execute_query(query, {'tow_company_id': tow_company_id})
            return results
            
        except Exception as e:
            logger.error(f"Error getting tow company claims: {e}", exc_info=True)
            return []
    
    def get_tow_company_statistics(self, tow_company_id: str) -> Dict:
        """Get statistics for a tow company"""
        try:
            query = """
            MATCH (t:TowCompany {tow_company_id: $tow_company_id})
            OPTIONAL MATCH (t)<-[:TOWED_BY]-(cl:Claim)
            OPTIONAL MATCH (c:Claimant)-[:FILED]->(cl)
            
            WITH t, cl, c
            
            RETURN 
                t.tow_company_id as tow_company_id,
                t.name as name,
                t.city as city,
                count(DISTINCT cl) as tow_count,
                count(DISTINCT c) as unique_claimants,
                sum(cl.total_claim_amount) as total_claim_value,
                avg(cl.risk_score) as avg_risk_score,
                max(cl.accident_date) as last_tow_date
            """
            
            results = self.driver.execute_query(query, {'tow_company_id': tow_company_id})
            
            if results:
                return results[0]
            
            return {}
            
        except Exception as e:
            logger.error(f"Error getting tow company statistics: {e}", exc_info=True)
            return {}
    
    def get_tow_company_referral_patterns(self, tow_company_id: str) -> Dict:
        """Analyze body shop referral patterns (kickback detection)"""
        try:
            query = """
            MATCH (t:TowCompany {tow_company_id: $tow_company_id})<-[:TOWED_BY]-(cl:Claim)
            
            // Get body shop referrals
            OPTIONAL MATCH (cl)-[:REPAIRED_AT]->(b:BodyShop)
            
            WITH t,
                 count(DISTINCT cl) as total_tows,
                 count(DISTINCT b) as unique_body_shops,
                 collect({
                     body_shop_id: b.body_shop_id,
                     body_shop_name: b.name,
                     referral_count: size((b)<-[:REPAIRED_AT]-(:Claim)-[:TOWED_BY]->(t))
                 }) as body_shop_referrals
            
            // Calculate concentration
            WITH t, total_tows, unique_body_shops, body_shop_referrals,
                 head([ref IN body_shop_referrals ORDER BY ref.referral_count DESC | ref.referral_count]) as top_referral_count
            
            RETURN 
                t.name as tow_company_name,
                total_tows,
                unique_body_shops,
                toFloat(top_referral_count) / total_tows as concentration_ratio,
                body_shop_referrals
            """
            
            results = self.driver.execute_query(query, {'tow_company_id': tow_company_id})
            
            if results:
                return results[0]
            
            return {}
            
        except Exception as e:
            logger.error(f"Error getting tow company referral patterns: {e}", exc_info=True)
            return {}
    
    def get_suspicious_tow_companies(self, min_tows: int = 15, min_concentration: float = 0.7, limit: int = 50) -> List[Dict]:
        """Get tow companies with suspicious referral patterns (potential kickbacks)"""
        try:
            query = """
            MATCH (t:TowCompany)<-[:TOWED_BY]-(cl:Claim)-[:REPAIRED_AT]->(b:BodyShop)
            
            WITH t, b,
                 count(cl) as shared_claims
            
            WITH t,
                 collect({body_shop: b.name, body_shop_id: b.body_shop_id, shared_claims: shared_claims}) as body_shop_referrals,
                 sum(shared_claims) as total_tows
            
            WHERE total_tows >= $min_tows
            
            // Calculate concentration (how often they refer to top body shop)
            WITH t, body_shop_referrals, total_tows,
                 head([r IN body_shop_referrals ORDER BY r.shared_claims DESC | r.shared_claims]) as top_referral_count
            
            WITH t, body_shop_referrals, total_tows, top_referral_count,
                 toFloat(top_referral_count) / total_tows as concentration_ratio
            
            WHERE concentration_ratio >= $min_concentration
            
            // Get average risk score
            MATCH (t)<-[:TOWED_BY]-(cl2:Claim)
            WITH t, body_shop_referrals, total_tows, concentration_ratio,
                 avg(cl2.risk_score) as avg_risk_score
            
            RETURN 
                t.tow_company_id as tow_company_id,
                t.name as name,
                t.city as city,
                total_tows,
                concentration_ratio,
                avg_risk_score,
                body_shop_referrals[0] as top_body_shop
            ORDER BY concentration_ratio DESC, total_tows DESC
            LIMIT $limit
            """
            
            results = self.driver.execute_query(query, {
                'min_tows': min_tows,
                'min_concentration': min_concentration,
                'limit': limit
            })
            
            return results
            
        except Exception as e:
            logger.error(f"Error getting suspicious tow companies: {e}", exc_info=True)
            return []
    
    def find_tow_company_networks(self, tow_company_id: str) -> List[Dict]:
        """Find body shops and claimants frequently connected to this tow company"""
        try:
            query = """
            MATCH (t:TowCompany {tow_company_id: $tow_company_id})<-[:TOWED_BY]-(cl:Claim)
            MATCH (c:Claimant)-[:FILED]->(cl)
            MATCH (cl)-[:REPAIRED_AT]->(b:BodyShop)
            
            WITH b, 
                 count(cl) as referral_count,
                 collect(DISTINCT c.name) as claimants,
                 avg(cl.risk_score) as avg_risk
            
            RETURN 
                b.body_shop_id as body_shop_id,
                b.name as body_shop_name,
                b.city as body_shop_city,
                referral_count,
                avg_risk,
                claimants
            ORDER BY referral_count DESC
            LIMIT 20
            """
            
            results = self.driver.execute_query(query, {'tow_company_id': tow_company_id})
            return results
            
        except Exception as e:
            logger.error(f"Error finding tow company networks: {e}", exc_info=True)
            return []
    
    # ==================== UPDATE OPERATIONS ====================
    
    def update_tow_company(self, tow_company_id: str, updates: Dict) -> bool:
        """Update tow company properties"""
        try:
            set_clauses = [f"t.{key} = ${key}" for key in updates.keys()]
            set_clause = ", ".join(set_clauses)
            
            query = f"""
            MATCH (t:TowCompany {{tow_company_id: $tow_company_id}})
            SET {set_clause}
            RETURN t.tow_company_id as tow_company_id
            """
            
            params = {'tow_company_id': tow_company_id}
            params.update(updates)
            
            result = self.driver.execute_write(query, params)
            
            if result:
                logger.info(f"Updated tow company: {tow_company_id}")
                return True
            
            return False
            
        except Exception as e:
            logger.error(f"Error updating tow company: {e}", exc_info=True)
            return False
    
    # ==================== DELETE OPERATIONS ====================
    
    def delete_tow_company(self, tow_company_id: str) -> bool:
        """Delete a tow company and its relationships"""
        try:
            query = """
            MATCH (t:TowCompany {tow_company_id: $tow_company_id})
            DETACH DELETE t
            """
            
            self.driver.execute_write(query, {'tow_company_id': tow_company_id})
            logger.info(f"Deleted tow company: {tow_company_id}")
            return True
            
        except Exception as e:
            logger.error(f"Error deleting tow company: {e}", exc_info=True)
            return False
    
    # ==================== SEARCH OPERATIONS ====================
    
    def search_tow_companies(
        self,
        city: Optional[str] = None,
        state: Optional[str] = None,
        name: Optional[str] = None,
        limit: int = 100
    ) -> List[TowCompany]:
        """Search tow companies by location or name"""
        try:
            where_clauses = []
            params = {'limit': limit}
            
            if city:
                where_clauses.append("t.city = $city")
                params['city'] = city
            
            if state:
                where_clauses.append("t.state = $state")
                params['state'] = state
            
            if name:
                where_clauses.append("t.name CONTAINS $name")
                params['name'] = name
            
            where_clause = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""
            
            query = f"""
            MATCH (t:TowCompany)
            {where_clause}
            RETURN 
                t.tow_company_id as tow_company_id,
                t.name as name,
                t.license_number as license_number,
                t.phone as phone,
                t.city as city,
                t.state as state
            LIMIT $limit
            """
            
            results = self.driver.execute_query(query, params)
            
            return [TowCompany.from_dict(row) for row in results]
            
        except Exception as e:
            logger.error(f"Error searching tow companies: {e}", exc_info=True)
            return []
