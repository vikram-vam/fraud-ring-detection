"""
Medical Provider Repository - Database operations for medical providers
Handles medical provider CRUD and fraud detection queries
"""
import logging
from typing import Dict, List, Optional

from data.neo4j_driver import get_neo4j_driver
from data.models.claim import MedicalProvider
from utils.logger import setup_logger

logger = setup_logger(__name__)


class MedicalProviderRepository:
    """
    Repository for medical provider database operations
    """
    
    def __init__(self):
        self.driver = get_neo4j_driver()
    
    # ==================== CREATE OPERATIONS ====================
    
    def create_medical_provider(self, provider: MedicalProvider) -> bool:
        """
        Create a new medical provider in the database
        
        Args:
            provider: MedicalProvider object
            
        Returns:
            True if successful, False otherwise
        """
        try:
            query = """
            CREATE (m:MedicalProvider {
                provider_id: $provider_id,
                name: $name,
                provider_type: $provider_type,
                license_number: $license_number,
                phone: $phone,
                street: $street,
                city: $city,
                state: $state,
                zip_code: $zip_code
            })
            RETURN m.provider_id as provider_id
            """
            
            result = self.driver.execute_write(query, provider.to_dict())
            
            if result:
                logger.info(f"Created medical provider: {provider.provider_id}")
                return True
            
            return False
            
        except Exception as e:
            logger.error(f"Error creating medical provider: {e}", exc_info=True)
            return False
    
    def create_or_update_medical_provider(self, provider: MedicalProvider) -> bool:
        """Create medical provider if not exists, update if exists"""
        try:
            query = """
            MERGE (m:MedicalProvider {provider_id: $provider_id})
            SET m.name = $name,
                m.provider_type = $provider_type,
                m.license_number = $license_number,
                m.phone = $phone,
                m.street = $street,
                m.city = $city,
                m.state = $state,
                m.zip_code = $zip_code
            RETURN m.provider_id as provider_id
            """
            
            result = self.driver.execute_write(query, provider.to_dict())
            
            if result:
                logger.info(f"Created/Updated medical provider: {provider.provider_id}")
                return True
            
            return False
            
        except Exception as e:
            logger.error(f"Error creating/updating medical provider: {e}", exc_info=True)
            return False
    
    # ==================== READ OPERATIONS ====================
    
    def get_medical_provider_by_id(self, provider_id: str) -> Optional[MedicalProvider]:
        """Get medical provider by ID"""
        try:
            query = """
            MATCH (m:MedicalProvider {provider_id: $provider_id})
            RETURN 
                m.provider_id as provider_id,
                m.name as name,
                m.provider_type as provider_type,
                m.license_number as license_number,
                m.phone as phone,
                m.street as street,
                m.city as city,
                m.state as state,
                m.zip_code as zip_code
            """
            
            results = self.driver.execute_query(query, {'provider_id': provider_id})
            
            if results:
                return MedicalProvider.from_dict(results[0])
            
            return None
            
        except Exception as e:
            logger.error(f"Error getting medical provider: {e}", exc_info=True)
            return None
    
    def get_medical_provider_claims(self, provider_id: str) -> List[Dict]:
        """Get all claims treated by this medical provider"""
        try:
            query = """
            MATCH (m:MedicalProvider {provider_id: $provider_id})<-[:TREATED_BY]-(cl:Claim)
            MATCH (c:Claimant)-[:FILED]->(cl)
            
            RETURN 
                cl.claim_id as claim_id,
                cl.claim_number as claim_number,
                c.name as claimant_name,
                cl.accident_date as accident_date,
                cl.injury_type as injury_type,
                cl.bodily_injury_amount as treatment_amount,
                cl.total_claim_amount as total_amount,
                cl.risk_score as risk_score,
                cl.status as status
            ORDER BY cl.accident_date DESC
            """
            
            results = self.driver.execute_query(query, {'provider_id': provider_id})
            return results
            
        except Exception as e:
            logger.error(f"Error getting medical provider claims: {e}", exc_info=True)
            return []
    
    def get_medical_provider_statistics(self, provider_id: str) -> Dict:
        """Get statistics for a medical provider"""
        try:
            query = """
            MATCH (m:MedicalProvider {provider_id: $provider_id})
            OPTIONAL MATCH (m)<-[:TREATED_BY]-(cl:Claim)
            OPTIONAL MATCH (c:Claimant)-[:FILED]->(cl)
            
            WITH m, cl, c
            
            // Calculate soft tissue injury ratio
            WITH m,
                 count(DISTINCT cl) as claim_count,
                 count(DISTINCT c) as unique_patients,
                 sum(cl.bodily_injury_amount) as total_treatments,
                 avg(cl.bodily_injury_amount) as avg_treatment_amount,
                 avg(cl.risk_score) as avg_risk_score,
                 collect(cl.injury_type) as injury_types
            
            WITH m, claim_count, unique_patients, total_treatments, avg_treatment_amount, avg_risk_score,
                 size([i IN injury_types WHERE i IN ['Whiplash', 'Back Pain', 'Neck Pain', 'Soft Tissue Injury']]) as soft_tissue_count,
                 size(injury_types) as total_injuries
            
            RETURN 
                m.provider_id as provider_id,
                m.name as name,
                m.provider_type as provider_type,
                m.city as city,
                claim_count,
                unique_patients,
                total_treatments,
                avg_treatment_amount,
                avg_risk_score,
                toFloat(soft_tissue_count) / CASE WHEN total_injuries > 0 THEN total_injuries ELSE 1 END as soft_tissue_ratio
            """
            
            results = self.driver.execute_query(query, {'provider_id': provider_id})
            
            if results:
                return results[0]
            
            return {}
            
        except Exception as e:
            logger.error(f"Error getting medical provider statistics: {e}", exc_info=True)
            return {}
    
    def get_high_volume_medical_providers(self, min_claims: int = 20, limit: int = 50) -> List[Dict]:
        """Get medical providers with high claim volume (medical mill indicators)"""
        try:
            query = """
            MATCH (m:MedicalProvider)<-[:TREATED_BY]-(cl:Claim)
            WHERE cl.bodily_injury_amount > 0
            MATCH (c:Claimant)-[:FILED]->(cl)
            
            WITH m,
                 count(DISTINCT cl) as claim_count,
                 count(DISTINCT c) as unique_patients,
                 sum(cl.bodily_injury_amount) as total_treatments,
                 avg(cl.bodily_injury_amount) as avg_treatment_amount,
                 avg(cl.risk_score) as avg_risk_score,
                 collect(cl.injury_type) as injury_types
            
            WHERE claim_count >= $min_claims
            
            // Check for repeat patients
            MATCH (m)<-[:TREATED_BY]-(cl2:Claim)<-[:FILED]-(c2:Claimant)
            WITH m, claim_count, unique_patients, total_treatments, avg_treatment_amount, avg_risk_score, injury_types,
                 c2.claimant_id as patient_id, count(cl2) as patient_visits
            WHERE patient_visits >= 2
            
            WITH m, claim_count, unique_patients, total_treatments, avg_treatment_amount, avg_risk_score, injury_types,
                 count(DISTINCT patient_id) as repeat_patients
            
            // Calculate soft tissue ratio
            WITH m, claim_count, unique_patients, total_treatments, avg_treatment_amount, avg_risk_score, repeat_patients,
                 size([i IN injury_types WHERE i IN ['Whiplash', 'Back Pain', 'Neck Pain', 'Soft Tissue Injury']]) as soft_tissue_count,
                 size(injury_types) as total_injuries
            
            RETURN 
                m.provider_id as provider_id,
                m.name as name,
                m.provider_type as provider_type,
                m.city as city,
                claim_count,
                unique_patients,
                total_treatments,
                avg_treatment_amount,
                avg_risk_score,
                repeat_patients,
                toFloat(repeat_patients) / unique_patients as repeat_ratio,
                toFloat(soft_tissue_count) / total_injuries as soft_tissue_ratio
            ORDER BY avg_risk_score DESC, claim_count DESC
            LIMIT $limit
            """
            
            results = self.driver.execute_query(query, {
                'min_claims': min_claims,
                'limit': limit
            })
            
            return results
            
        except Exception as e:
            logger.error(f"Error getting high volume medical providers: {e}", exc_info=True)
            return []
    
    def find_medical_provider_networks(self, provider_id: str) -> List[Dict]:
        """Find attorneys and other entities frequently connected to this provider"""
        try:
            query = """
            MATCH (m:MedicalProvider {provider_id: $provider_id})<-[:TREATED_BY]-(cl:Claim)
            MATCH (c:Claimant)-[:FILED]->(cl)
            
            // Find attorneys
            OPTIONAL MATCH (cl)-[:REPRESENTED_BY]->(a:Attorney)
            
            // Find body shops
            OPTIONAL MATCH (cl)-[:REPAIRED_AT]->(b:BodyShop)
            
            WITH m, c, 
                 count(cl) as patient_claim_count,
                 collect(DISTINCT a.name) as attorneys,
                 collect(DISTINCT b.name) as body_shops
            
            RETURN 
                c.claimant_id as claimant_id,
                c.name as claimant_name,
                patient_claim_count,
                attorneys,
                body_shops
            ORDER BY patient_claim_count DESC
            LIMIT 50
            """
            
            results = self.driver.execute_query(query, {'provider_id': provider_id})
            return results
            
        except Exception as e:
            logger.error(f"Error finding medical provider networks: {e}", exc_info=True)
            return []
    
    def get_providers_by_type(self, provider_type: str, limit: int = 100) -> List[MedicalProvider]:
        """Get all medical providers of a specific type"""
        try:
            query = """
            MATCH (m:MedicalProvider {provider_type: $provider_type})
            RETURN 
                m.provider_id as provider_id,
                m.name as name,
                m.provider_type as provider_type,
                m.license_number as license_number,
                m.phone as phone,
                m.street as street,
                m.city as city,
                m.state as state,
                m.zip_code as zip_code
            LIMIT $limit
            """
            
            results = self.driver.execute_query(query, {
                'provider_type': provider_type,
                'limit': limit
            })
            
            return [MedicalProvider.from_dict(row) for row in results]
            
        except Exception as e:
            logger.error(f"Error getting providers by type: {e}", exc_info=True)
            return []
    
    # ==================== UPDATE OPERATIONS ====================
    
    def update_medical_provider(self, provider_id: str, updates: Dict) -> bool:
        """Update medical provider properties"""
        try:
            set_clauses = [f"m.{key} = ${key}" for key in updates.keys()]
            set_clause = ", ".join(set_clauses)
            
            query = f"""
            MATCH (m:MedicalProvider {{provider_id: $provider_id}})
            SET {set_clause}
            RETURN m.provider_id as provider_id
            """
            
            params = {'provider_id': provider_id}
            params.update(updates)
            
            result = self.driver.execute_write(query, params)
            
            if result:
                logger.info(f"Updated medical provider: {provider_id}")
                return True
            
            return False
            
        except Exception as e:
            logger.error(f"Error updating medical provider: {e}", exc_info=True)
            return False
    
    # ==================== DELETE OPERATIONS ====================
    
    def delete_medical_provider(self, provider_id: str) -> bool:
        """Delete a medical provider and its relationships"""
        try:
            query = """
            MATCH (m:MedicalProvider {provider_id: $provider_id})
            DETACH DELETE m
            """
            
            self.driver.execute_write(query, {'provider_id': provider_id})
            logger.info(f"Deleted medical provider: {provider_id}")
            return True
            
        except Exception as e:
            logger.error(f"Error deleting medical provider: {e}", exc_info=True)
            return False
    
    # ==================== SEARCH OPERATIONS ====================
    
    def search_medical_providers(
        self,
        city: Optional[str] = None,
        state: Optional[str] = None,
        provider_type: Optional[str] = None,
        name: Optional[str] = None,
        limit: int = 100
    ) -> List[MedicalProvider]:
        """Search medical providers by location, type, or name"""
        try:
            where_clauses = []
            params = {'limit': limit}
            
            if city:
                where_clauses.append("m.city = $city")
                params['city'] = city
            
            if state:
                where_clauses.append("m.state = $state")
                params['state'] = state
            
            if provider_type:
                where_clauses.append("m.provider_type = $provider_type")
                params['provider_type'] = provider_type
            
            if name:
                where_clauses.append("m.name CONTAINS $name")
                params['name'] = name
            
            where_clause = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""
            
            query = f"""
            MATCH (m:MedicalProvider)
            {where_clause}
            RETURN 
                m.provider_id as provider_id,
                m.name as name,
                m.provider_type as provider_type,
                m.license_number as license_number,
                m.phone as phone,
                m.street as street,
                m.city as city,
                m.state as state,
                m.zip_code as zip_code
            LIMIT $limit
            """
            
            results = self.driver.execute_query(query, params)
            
            return [MedicalProvider.from_dict(row) for row in results]
            
        except Exception as e:
            logger.error(f"Error searching medical providers: {e}", exc_info=True)
            return []
