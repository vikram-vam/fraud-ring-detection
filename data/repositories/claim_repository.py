"""
Claim Repository - Database operations for auto insurance claims
Handles CRUD operations and complex queries for claims and related entities
"""
import logging
from typing import Dict, List, Optional, Tuple
from datetime import datetime

from data.neo4j_driver import get_neo4j_driver
from data.models.claim import (
    Claim, Claimant, Vehicle, BodyShop, MedicalProvider,
    Attorney, TowCompany, AccidentLocation, Witness, FraudRing
)
from utils.logger import setup_logger

logger = setup_logger(__name__)


class ClaimRepository:
    """
    Repository for claim database operations
    """
    
    def __init__(self):
        self.driver = get_neo4j_driver()
    
    # ==================== CREATE OPERATIONS ====================
    
    def create_claim(self, claim: Claim) -> bool:
        """
        Create a new claim in the database
        
        Args:
            claim: Claim object
            
        Returns:
            True if successful, False otherwise
        """
        try:
            query = """
            CREATE (cl:Claim {
                claim_id: $claim_id,
                claim_number: $claim_number,
                claimant_id: $claimant_id,
                accident_date: date($accident_date),
                report_date: date($report_date),
                accident_type: $accident_type,
                injury_type: $injury_type,
                total_claim_amount: $total_claim_amount,
                property_damage_amount: $property_damage_amount,
                bodily_injury_amount: $bodily_injury_amount,
                status: $status,
                risk_score: $risk_score
            })
            RETURN cl.claim_id as claim_id
            """
            
            result = self.driver.execute_write(query, claim.to_dict())
            
            if result:
                logger.info(f"Created claim: {claim.claim_id}")
                return True
            
            return False
            
        except Exception as e:
            logger.error(f"Error creating claim: {e}", exc_info=True)
            return False
    
    def create_claimant(self, claimant: Claimant) -> bool:
        """Create a new claimant"""
        try:
            query = """
            CREATE (c:Claimant {
                claimant_id: $claimant_id,
                name: $name,
                email: $email,
                phone: $phone,
                drivers_license: $drivers_license,
                date_of_birth: date($date_of_birth),
                street: $street,
                city: $city,
                state: $state,
                zip_code: $zip_code
            })
            RETURN c.claimant_id as claimant_id
            """
            
            result = self.driver.execute_write(query, claimant.to_dict())
            
            if result:
                logger.info(f"Created claimant: {claimant.claimant_id}")
                return True
            
            return False
            
        except Exception as e:
            logger.error(f"Error creating claimant: {e}", exc_info=True)
            return False
    
    def create_vehicle(self, vehicle: Vehicle) -> bool:
        """Create a new vehicle"""
        try:
            query = """
            CREATE (v:Vehicle {
                vehicle_id: $vehicle_id,
                vin: $vin,
                license_plate: $license_plate,
                make: $make,
                model: $model,
                year: $year,
                color: $color
            })
            RETURN v.vehicle_id as vehicle_id
            """
            
            result = self.driver.execute_write(query, vehicle.to_dict())
            
            if result:
                logger.info(f"Created vehicle: {vehicle.vehicle_id}")
                return True
            
            return False
            
        except Exception as e:
            logger.error(f"Error creating vehicle: {e}", exc_info=True)
            return False
    
    # ==================== RELATIONSHIP OPERATIONS ====================
    
    def link_claimant_to_claim(self, claimant_id: str, claim_id: str) -> bool:
        """Create FILED relationship between claimant and claim"""
        try:
            query = """
            MATCH (c:Claimant {claimant_id: $claimant_id})
            MATCH (cl:Claim {claim_id: $claim_id})
            MERGE (c)-[:FILED]->(cl)
            RETURN c.claimant_id as claimant_id
            """
            
            result = self.driver.execute_write(query, {
                'claimant_id': claimant_id,
                'claim_id': claim_id
            })
            
            return result is not None
            
        except Exception as e:
            logger.error(f"Error linking claimant to claim: {e}", exc_info=True)
            return False
    
    def link_claim_to_vehicle(self, claim_id: str, vehicle_id: str) -> bool:
        """Create INVOLVES_VEHICLE relationship"""
        try:
            query = """
            MATCH (cl:Claim {claim_id: $claim_id})
            MATCH (v:Vehicle {vehicle_id: $vehicle_id})
            MERGE (cl)-[:INVOLVES_VEHICLE]->(v)
            RETURN cl.claim_id as claim_id
            """
            
            result = self.driver.execute_write(query, {
                'claim_id': claim_id,
                'vehicle_id': vehicle_id
            })
            
            return result is not None
            
        except Exception as e:
            logger.error(f"Error linking claim to vehicle: {e}", exc_info=True)
            return False
    
    def link_claim_to_body_shop(self, claim_id: str, body_shop_id: str) -> bool:
        """Create REPAIRED_AT relationship"""
        try:
            query = """
            MATCH (cl:Claim {claim_id: $claim_id})
            MATCH (b:BodyShop {body_shop_id: $body_shop_id})
            MERGE (cl)-[:REPAIRED_AT]->(b)
            RETURN cl.claim_id as claim_id
            """
            
            result = self.driver.execute_write(query, {
                'claim_id': claim_id,
                'body_shop_id': body_shop_id
            })
            
            return result is not None
            
        except Exception as e:
            logger.error(f"Error linking claim to body shop: {e}", exc_info=True)
            return False
    
    def link_claim_to_medical_provider(self, claim_id: str, provider_id: str) -> bool:
        """Create TREATED_BY relationship"""
        try:
            query = """
            MATCH (cl:Claim {claim_id: $claim_id})
            MATCH (m:MedicalProvider {provider_id: $provider_id})
            MERGE (cl)-[:TREATED_BY]->(m)
            RETURN cl.claim_id as claim_id
            """
            
            result = self.driver.execute_write(query, {
                'claim_id': claim_id,
                'provider_id': provider_id
            })
            
            return result is not None
            
        except Exception as e:
            logger.error(f"Error linking claim to medical provider: {e}", exc_info=True)
            return False
    
    def link_claim_to_attorney(self, claim_id: str, attorney_id: str) -> bool:
        """Create REPRESENTED_BY relationship"""
        try:
            query = """
            MATCH (cl:Claim {claim_id: $claim_id})
            MATCH (a:Attorney {attorney_id: $attorney_id})
            MERGE (cl)-[:REPRESENTED_BY]->(a)
            RETURN cl.claim_id as claim_id
            """
            
            result = self.driver.execute_write(query, {
                'claim_id': claim_id,
                'attorney_id': attorney_id
            })
            
            return result is not None
            
        except Exception as e:
            logger.error(f"Error linking claim to attorney: {e}", exc_info=True)
            return False
    
    def link_claim_to_location(self, claim_id: str, location_id: str) -> bool:
        """Create OCCURRED_AT relationship"""
        try:
            query = """
            MATCH (cl:Claim {claim_id: $claim_id})
            MATCH (l:AccidentLocation {location_id: $location_id})
            MERGE (cl)-[:OCCURRED_AT]->(l)
            RETURN cl.claim_id as claim_id
            """
            
            result = self.driver.execute_write(query, {
                'claim_id': claim_id,
                'location_id': location_id
            })
            
            return result is not None
            
        except Exception as e:
            logger.error(f"Error linking claim to location: {e}", exc_info=True)
            return False
    
    def link_witness_to_claim(self, witness_id: str, claim_id: str) -> bool:
        """Create WITNESSED relationship"""
        try:
            query = """
            MATCH (w:Witness {witness_id: $witness_id})
            MATCH (cl:Claim {claim_id: $claim_id})
            MERGE (w)-[:WITNESSED]->(cl)
            RETURN w.witness_id as witness_id
            """
            
            result = self.driver.execute_write(query, {
                'witness_id': witness_id,
                'claim_id': claim_id
            })
            
            return result is not None
            
        except Exception as e:
            logger.error(f"Error linking witness to claim: {e}", exc_info=True)
            return False
    
    # ==================== READ OPERATIONS ====================
    
    def get_claim_by_id(self, claim_id: str) -> Optional[Claim]:
        """Get claim by ID"""
        try:
            query = """
            MATCH (cl:Claim {claim_id: $claim_id})
            RETURN 
                cl.claim_id as claim_id,
                cl.claim_number as claim_number,
                cl.claimant_id as claimant_id,
                cl.accident_date as accident_date,
                cl.report_date as report_date,
                cl.accident_type as accident_type,
                cl.injury_type as injury_type,
                cl.total_claim_amount as total_claim_amount,
                cl.property_damage_amount as property_damage_amount,
                cl.bodily_injury_amount as bodily_injury_amount,
                cl.status as status,
                cl.risk_score as risk_score
            """
            
            results = self.driver.execute_query(query, {'claim_id': claim_id})
            
            if results:
                return Claim.from_dict(results[0])
            
            return None
            
        except Exception as e:
            logger.error(f"Error getting claim: {e}", exc_info=True)
            return None
    
    def get_claimant_by_id(self, claimant_id: str) -> Optional[Claimant]:
        """Get claimant by ID"""
        try:
            query = """
            MATCH (c:Claimant {claimant_id: $claimant_id})
            RETURN 
                c.claimant_id as claimant_id,
                c.name as name,
                c.email as email,
                c.phone as phone,
                c.drivers_license as drivers_license,
                c.date_of_birth as date_of_birth,
                c.street as street,
                c.city as city,
                c.state as state,
                c.zip_code as zip_code
            """
            
            results = self.driver.execute_query(query, {'claimant_id': claimant_id})
            
            if results:
                return Claimant.from_dict(results[0])
            
            return None
            
        except Exception as e:
            logger.error(f"Error getting claimant: {e}", exc_info=True)
            return None
    
    def get_claims_by_claimant(self, claimant_id: str) -> List[Claim]:
        """Get all claims for a claimant"""
        try:
            query = """
            MATCH (c:Claimant {claimant_id: $claimant_id})-[:FILED]->(cl:Claim)
            RETURN 
                cl.claim_id as claim_id,
                cl.claim_number as claim_number,
                cl.claimant_id as claimant_id,
                cl.accident_date as accident_date,
                cl.report_date as report_date,
                cl.accident_type as accident_type,
                cl.injury_type as injury_type,
                cl.total_claim_amount as total_claim_amount,
                cl.property_damage_amount as property_damage_amount,
                cl.bodily_injury_amount as bodily_injury_amount,
                cl.status as status,
                cl.risk_score as risk_score
            ORDER BY cl.accident_date DESC
            """
            
            results = self.driver.execute_query(query, {'claimant_id': claimant_id})
            
            return [Claim.from_dict(row) for row in results]
            
        except Exception as e:
            logger.error(f"Error getting claims by claimant: {e}", exc_info=True)
            return []
    
    def get_high_risk_claims(self, min_risk: float = 70, limit: int = 100) -> List[Dict]:
        """Get high risk claims with related entities"""
        try:
            query = """
            MATCH (c:Claimant)-[:FILED]->(cl:Claim)
            WHERE cl.risk_score >= $min_risk
            
            OPTIONAL MATCH (cl)-[:INVOLVES_VEHICLE]->(v:Vehicle)
            OPTIONAL MATCH (cl)-[:REPAIRED_AT]->(b:BodyShop)
            OPTIONAL MATCH (cl)-[:TREATED_BY]->(m:MedicalProvider)
            OPTIONAL MATCH (cl)-[:REPRESENTED_BY]->(a:Attorney)
            OPTIONAL MATCH (c)-[:MEMBER_OF]->(r:FraudRing)
            
            RETURN 
                cl.claim_id as claim_id,
                cl.claim_number as claim_number,
                c.name as claimant_name,
                cl.accident_type as accident_type,
                cl.accident_date as accident_date,
                cl.total_claim_amount as total_amount,
                cl.risk_score as risk_score,
                cl.status as status,
                v.make + ' ' + v.model as vehicle,
                b.name as body_shop,
                m.name as medical_provider,
                a.name as attorney,
                r.ring_id as ring_id
            ORDER BY cl.risk_score DESC, cl.accident_date DESC
            LIMIT $limit
            """
            
            results = self.driver.execute_query(query, {
                'min_risk': min_risk,
                'limit': limit
            })
            
            return results
            
        except Exception as e:
            logger.error(f"Error getting high risk claims: {e}", exc_info=True)
            return []
    
    def get_claim_network(self, claim_id: str) -> Dict:
        """Get complete network of entities related to a claim"""
        try:
            query = """
            MATCH (c:Claimant)-[:FILED]->(cl:Claim {claim_id: $claim_id})
            
            OPTIONAL MATCH (cl)-[:INVOLVES_VEHICLE]->(v:Vehicle)
            OPTIONAL MATCH (cl)-[:REPAIRED_AT]->(b:BodyShop)
            OPTIONAL MATCH (cl)-[:TREATED_BY]->(m:MedicalProvider)
            OPTIONAL MATCH (cl)-[:REPRESENTED_BY]->(a:Attorney)
            OPTIONAL MATCH (cl)-[:TOWED_BY]->(t:TowCompany)
            OPTIONAL MATCH (cl)-[:OCCURRED_AT]->(l:AccidentLocation)
            OPTIONAL MATCH (w:Witness)-[:WITNESSED]->(cl)
            OPTIONAL MATCH (c)-[:MEMBER_OF]->(r:FraudRing)
            
            RETURN 
                cl.claim_id as claim_id,
                cl.claim_number as claim_number,
                cl.accident_date as accident_date,
                cl.total_claim_amount as total_amount,
                cl.risk_score as risk_score,
                
                c.claimant_id as claimant_id,
                c.name as claimant_name,
                
                v.vehicle_id as vehicle_id,
                v.make + ' ' + v.model + ' ' + v.year as vehicle_info,
                
                b.body_shop_id as body_shop_id,
                b.name as body_shop_name,
                
                m.provider_id as medical_provider_id,
                m.name as medical_provider_name,
                
                a.attorney_id as attorney_id,
                a.name as attorney_name,
                
                t.tow_company_id as tow_company_id,
                t.name as tow_company_name,
                
                l.location_id as location_id,
                l.intersection as location_intersection,
                
                collect(w.name) as witnesses,
                
                r.ring_id as ring_id,
                r.pattern_type as ring_pattern
            """
            
            results = self.driver.execute_query(query, {'claim_id': claim_id})
            
            if results:
                return results[0]
            
            return {}
            
        except Exception as e:
            logger.error(f"Error getting claim network: {e}", exc_info=True)
            return {}
    
    # ==================== UPDATE OPERATIONS ====================
    
    def update_claim_risk_score(self, claim_id: str, risk_score: float) -> bool:
        """Update claim risk score"""
        try:
            query = """
            MATCH (cl:Claim {claim_id: $claim_id})
            SET cl.risk_score = $risk_score
            RETURN cl.claim_id as claim_id
            """
            
            result = self.driver.execute_write(query, {
                'claim_id': claim_id,
                'risk_score': risk_score
            })
            
            if result:
                logger.info(f"Updated risk score for claim {claim_id}: {risk_score}")
                return True
            
            return False
            
        except Exception as e:
            logger.error(f"Error updating claim risk score: {e}", exc_info=True)
            return False
    
    def update_claim_status(self, claim_id: str, status: str) -> bool:
        """Update claim status"""
        try:
            query = """
            MATCH (cl:Claim {claim_id: $claim_id})
            SET cl.status = $status
            RETURN cl.claim_id as claim_id
            """
            
            result = self.driver.execute_write(query, {
                'claim_id': claim_id,
                'status': status
            })
            
            if result:
                logger.info(f"Updated status for claim {claim_id}: {status}")
                return True
            
            return False
            
        except Exception as e:
            logger.error(f"Error updating claim status: {e}", exc_info=True)
            return False
    
    # ==================== DELETE OPERATIONS ====================
    
    def delete_claim(self, claim_id: str) -> bool:
        """Delete a claim and its relationships"""
        try:
            query = """
            MATCH (cl:Claim {claim_id: $claim_id})
            DETACH DELETE cl
            """
            
            self.driver.execute_write(query, {'claim_id': claim_id})
            logger.info(f"Deleted claim: {claim_id}")
            return True
            
        except Exception as e:
            logger.error(f"Error deleting claim: {e}", exc_info=True)
            return False
    
    # ==================== SEARCH OPERATIONS ====================
    
    def search_claims(
        self,
        filters: Optional[Dict] = None,
        limit: int = 100
    ) -> List[Dict]:
        """
        Search claims with flexible filters
        
        Args:
            filters: Dictionary of filter criteria
            limit: Maximum results to return
            
        Returns:
            List of claim dictionaries
        """
        try:
            # Build query dynamically based on filters
            where_clauses = []
            params = {'limit': limit}
            
            if filters:
                if 'min_risk' in filters:
                    where_clauses.append("cl.risk_score >= $min_risk")
                    params['min_risk'] = filters['min_risk']
                
                if 'max_risk' in filters:
                    where_clauses.append("cl.risk_score <= $max_risk")
                    params['max_risk'] = filters['max_risk']
                
                if 'accident_types' in filters and filters['accident_types']:
                    where_clauses.append("cl.accident_type IN $accident_types")
                    params['accident_types'] = filters['accident_types']
                
                if 'statuses' in filters and filters['statuses']:
                    where_clauses.append("cl.status IN $statuses")
                    params['statuses'] = filters['statuses']
                
                if 'min_amount' in filters:
                    where_clauses.append("cl.total_claim_amount >= $min_amount")
                    params['min_amount'] = filters['min_amount']
            
            where_clause = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""
            
            query = f"""
            MATCH (c:Claimant)-[:FILED]->(cl:Claim)
            {where_clause}
            
            OPTIONAL MATCH (cl)-[:INVOLVES_VEHICLE]->(v:Vehicle)
            OPTIONAL MATCH (c)-[:MEMBER_OF]->(r:FraudRing)
            
            RETURN 
                cl.claim_id as claim_id,
                cl.claim_number as claim_number,
                c.name as claimant_name,
                cl.accident_type as accident_type,
                cl.accident_date as accident_date,
                cl.total_claim_amount as total_amount,
                cl.risk_score as risk_score,
                cl.status as status,
                v.make + ' ' + v.model as vehicle,
                r.ring_id as ring_id
            ORDER BY cl.risk_score DESC, cl.accident_date DESC
            LIMIT $limit
            """
            
            results = self.driver.execute_query(query, params)
            return results
            
        except Exception as e:
            logger.error(f"Error searching claims: {e}", exc_info=True)
            return []
