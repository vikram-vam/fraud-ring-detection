"""
Vehicle Repository - Database operations for vehicles
Handles vehicle CRUD and fraud detection queries
"""
import logging
from typing import Dict, List, Optional
from datetime import datetime

from data.neo4j_driver import get_neo4j_driver
from data.models.claim import Vehicle
from utils.logger import setup_logger

logger = setup_logger(__name__)


class VehicleRepository:
    """
    Repository for vehicle database operations
    """
    
    def __init__(self):
        self.driver = get_neo4j_driver()
    
    # ==================== CREATE OPERATIONS ====================
    
    def create_vehicle(self, vehicle: Vehicle) -> bool:
        """
        Create a new vehicle in the database
        
        Args:
            vehicle: Vehicle object
            
        Returns:
            True if successful, False otherwise
        """
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
    
    def create_or_update_vehicle(self, vehicle: Vehicle) -> bool:
        """Create vehicle if not exists, update if exists"""
        try:
            query = """
            MERGE (v:Vehicle {vehicle_id: $vehicle_id})
            SET v.vin = $vin,
                v.license_plate = $license_plate,
                v.make = $make,
                v.model = $model,
                v.year = $year,
                v.color = $color
            RETURN v.vehicle_id as vehicle_id
            """
            
            result = self.driver.execute_write(query, vehicle.to_dict())
            
            if result:
                logger.info(f"Created/Updated vehicle: {vehicle.vehicle_id}")
                return True
            
            return False
            
        except Exception as e:
            logger.error(f"Error creating/updating vehicle: {e}", exc_info=True)
            return False
    
    # ==================== READ OPERATIONS ====================
    
    def get_vehicle_by_id(self, vehicle_id: str) -> Optional[Vehicle]:
        """Get vehicle by ID"""
        try:
            query = """
            MATCH (v:Vehicle {vehicle_id: $vehicle_id})
            RETURN 
                v.vehicle_id as vehicle_id,
                v.vin as vin,
                v.license_plate as license_plate,
                v.make as make,
                v.model as model,
                v.year as year,
                v.color as color
            """
            
            results = self.driver.execute_query(query, {'vehicle_id': vehicle_id})
            
            if results:
                return Vehicle.from_dict(results[0])
            
            return None
            
        except Exception as e:
            logger.error(f"Error getting vehicle: {e}", exc_info=True)
            return None
    
    def get_vehicle_by_vin(self, vin: str) -> Optional[Vehicle]:
        """Get vehicle by VIN"""
        try:
            query = """
            MATCH (v:Vehicle {vin: $vin})
            RETURN 
                v.vehicle_id as vehicle_id,
                v.vin as vin,
                v.license_plate as license_plate,
                v.make as make,
                v.model as model,
                v.year as year,
                v.color as color
            """
            
            results = self.driver.execute_query(query, {'vin': vin})
            
            if results:
                return Vehicle.from_dict(results[0])
            
            return None
            
        except Exception as e:
            logger.error(f"Error getting vehicle by VIN: {e}", exc_info=True)
            return None
    
    def get_vehicle_claims(self, vehicle_id: str) -> List[Dict]:
        """Get all claims involving this vehicle"""
        try:
            query = """
            MATCH (v:Vehicle {vehicle_id: $vehicle_id})<-[:INVOLVES_VEHICLE]-(cl:Claim)
            MATCH (c:Claimant)-[:FILED]->(cl)
            
            RETURN 
                cl.claim_id as claim_id,
                cl.claim_number as claim_number,
                c.name as claimant_name,
                cl.accident_date as accident_date,
                cl.accident_type as accident_type,
                cl.total_claim_amount as total_amount,
                cl.risk_score as risk_score,
                cl.status as status
            ORDER BY cl.accident_date DESC
            """
            
            results = self.driver.execute_query(query, {'vehicle_id': vehicle_id})
            return results
            
        except Exception as e:
            logger.error(f"Error getting vehicle claims: {e}", exc_info=True)
            return []
    
    def get_vehicle_statistics(self, vehicle_id: str) -> Dict:
        """Get statistics for a vehicle"""
        try:
            query = """
            MATCH (v:Vehicle {vehicle_id: $vehicle_id})
            OPTIONAL MATCH (v)<-[:INVOLVES_VEHICLE]-(cl:Claim)
            OPTIONAL MATCH (c:Claimant)-[:FILED]->(cl)
            
            WITH v, cl, c
            
            RETURN 
                v.vehicle_id as vehicle_id,
                v.make + ' ' + v.model + ' ' + v.year as vehicle_info,
                v.vin as vin,
                count(DISTINCT cl) as accident_count,
                count(DISTINCT c) as unique_claimants,
                sum(cl.total_claim_amount) as total_damages,
                avg(cl.risk_score) as avg_risk_score,
                max(cl.accident_date) as last_accident_date,
                min(cl.accident_date) as first_accident_date
            """
            
            results = self.driver.execute_query(query, {'vehicle_id': vehicle_id})
            
            if results:
                return results[0]
            
            return {}
            
        except Exception as e:
            logger.error(f"Error getting vehicle statistics: {e}", exc_info=True)
            return {}
    
    def get_high_risk_vehicles(self, min_accidents: int = 3, limit: int = 50) -> List[Dict]:
        """Get vehicles with multiple accidents (potential fraud)"""
        try:
            query = """
            MATCH (v:Vehicle)<-[:INVOLVES_VEHICLE]-(cl:Claim)
            MATCH (c:Claimant)-[:FILED]->(cl)
            
            WITH v,
                 count(DISTINCT cl) as accident_count,
                 count(DISTINCT c) as unique_claimants,
                 sum(cl.total_claim_amount) as total_damages,
                 avg(cl.risk_score) as avg_risk_score,
                 collect(DISTINCT c.name) as claimant_names
            
            WHERE accident_count >= $min_accidents
            
            RETURN 
                v.vehicle_id as vehicle_id,
                v.make + ' ' + v.model + ' ' + v.year as vehicle_info,
                v.vin as vin,
                v.license_plate as license_plate,
                accident_count,
                unique_claimants,
                total_damages,
                avg_risk_score,
                claimant_names
            ORDER BY accident_count DESC, unique_claimants DESC
            LIMIT $limit
            """
            
            results = self.driver.execute_query(query, {
                'min_accidents': min_accidents,
                'limit': limit
            })
            
            return results
            
        except Exception as e:
            logger.error(f"Error getting high risk vehicles: {e}", exc_info=True)
            return []
    
    def find_shared_vehicles(self, claimant_id: str) -> List[Dict]:
        """Find other claimants who share vehicles with this claimant"""
        try:
            query = """
            MATCH (c1:Claimant {claimant_id: $claimant_id})-[:FILED]->(cl1:Claim)-[:INVOLVES_VEHICLE]->(v:Vehicle)
            MATCH (c2:Claimant)-[:FILED]->(cl2:Claim)-[:INVOLVES_VEHICLE]->(v)
            WHERE c1.claimant_id <> c2.claimant_id
            
            WITH c2, v, count(DISTINCT cl2) as shared_claims
            
            RETURN 
                c2.claimant_id as claimant_id,
                c2.name as claimant_name,
                v.vehicle_id as vehicle_id,
                v.make + ' ' + v.model as vehicle_info,
                shared_claims
            ORDER BY shared_claims DESC
            """
            
            results = self.driver.execute_query(query, {'claimant_id': claimant_id})
            return results
            
        except Exception as e:
            logger.error(f"Error finding shared vehicles: {e}", exc_info=True)
            return []
    
    # ==================== UPDATE OPERATIONS ====================
    
    def update_vehicle(self, vehicle_id: str, updates: Dict) -> bool:
        """Update vehicle properties"""
        try:
            # Build SET clause dynamically
            set_clauses = [f"v.{key} = ${key}" for key in updates.keys()]
            set_clause = ", ".join(set_clauses)
            
            query = f"""
            MATCH (v:Vehicle {{vehicle_id: $vehicle_id}})
            SET {set_clause}
            RETURN v.vehicle_id as vehicle_id
            """
            
            params = {'vehicle_id': vehicle_id}
            params.update(updates)
            
            result = self.driver.execute_write(query, params)
            
            if result:
                logger.info(f"Updated vehicle: {vehicle_id}")
                return True
            
            return False
            
        except Exception as e:
            logger.error(f"Error updating vehicle: {e}", exc_info=True)
            return False
    
    # ==================== DELETE OPERATIONS ====================
    
    def delete_vehicle(self, vehicle_id: str) -> bool:
        """Delete a vehicle and its relationships"""
        try:
            query = """
            MATCH (v:Vehicle {vehicle_id: $vehicle_id})
            DETACH DELETE v
            """
            
            self.driver.execute_write(query, {'vehicle_id': vehicle_id})
            logger.info(f"Deleted vehicle: {vehicle_id}")
            return True
            
        except Exception as e:
            logger.error(f"Error deleting vehicle: {e}", exc_info=True)
            return False
    
    # ==================== SEARCH OPERATIONS ====================
    
    def search_vehicles(
        self,
        make: Optional[str] = None,
        model: Optional[str] = None,
        year: Optional[str] = None,
        limit: int = 100
    ) -> List[Vehicle]:
        """Search vehicles by make, model, year"""
        try:
            where_clauses = []
            params = {'limit': limit}
            
            if make:
                where_clauses.append("v.make = $make")
                params['make'] = make
            
            if model:
                where_clauses.append("v.model = $model")
                params['model'] = model
            
            if year:
                where_clauses.append("v.year = $year")
                params['year'] = year
            
            where_clause = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""
            
            query = f"""
            MATCH (v:Vehicle)
            {where_clause}
            RETURN 
                v.vehicle_id as vehicle_id,
                v.vin as vin,
                v.license_plate as license_plate,
                v.make as make,
                v.model as model,
                v.year as year,
                v.color as color
            LIMIT $limit
            """
            
            results = self.driver.execute_query(query, params)
            
            return [Vehicle.from_dict(row) for row in results]
            
        except Exception as e:
            logger.error(f"Error searching vehicles: {e}", exc_info=True)
            return []
