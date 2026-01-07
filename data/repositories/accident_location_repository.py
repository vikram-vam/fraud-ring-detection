"""
Accident Location Repository - Database operations for accident locations
Handles accident location CRUD and hotspot detection
"""
import logging
from typing import Dict, List, Optional

from data.neo4j_driver import get_neo4j_driver
from data.models.claim import AccidentLocation
from utils.logger import setup_logger

logger = setup_logger(__name__)


class AccidentLocationRepository:
    """
    Repository for accident location database operations
    """
    
    def __init__(self):
        self.driver = get_neo4j_driver()
    
    # ==================== CREATE OPERATIONS ====================
    
    def create_accident_location(self, location: AccidentLocation) -> bool:
        """
        Create a new accident location in the database
        
        Args:
            location: AccidentLocation object
            
        Returns:
            True if successful, False otherwise
        """
        try:
            query = """
            CREATE (l:AccidentLocation {
                location_id: $location_id,
                intersection: $intersection,
                city: $city,
                state: $state,
                latitude: $latitude,
                longitude: $longitude
            })
            RETURN l.location_id as location_id
            """
            
            result = self.driver.execute_write(query, location.to_dict())
            
            if result:
                logger.info(f"Created accident location: {location.location_id}")
                return True
            
            return False
            
        except Exception as e:
            logger.error(f"Error creating accident location: {e}", exc_info=True)
            return False
    
    def create_or_update_accident_location(self, location: AccidentLocation) -> bool:
        """Create accident location if not exists, update if exists"""
        try:
            query = """
            MERGE (l:AccidentLocation {location_id: $location_id})
            SET l.intersection = $intersection,
                l.city = $city,
                l.state = $state,
                l.latitude = $latitude,
                l.longitude = $longitude
            RETURN l.location_id as location_id
            """
            
            result = self.driver.execute_write(query, location.to_dict())
            
            if result:
                logger.info(f"Created/Updated accident location: {location.location_id}")
                return True
            
            return False
            
        except Exception as e:
            logger.error(f"Error creating/updating accident location: {e}", exc_info=True)
            return False
    
    # ==================== READ OPERATIONS ====================
    
    def get_accident_location_by_id(self, location_id: str) -> Optional[AccidentLocation]:
        """Get accident location by ID"""
        try:
            query = """
            MATCH (l:AccidentLocation {location_id: $location_id})
            RETURN 
                l.location_id as location_id,
                l.intersection as intersection,
                l.city as city,
                l.state as state,
                l.latitude as latitude,
                l.longitude as longitude
            """
            
            results = self.driver.execute_query(query, {'location_id': location_id})
            
            if results:
                return AccidentLocation.from_dict(results[0])
            
            return None
            
        except Exception as e:
            logger.error(f"Error getting accident location: {e}", exc_info=True)
            return None
    
    def get_location_accidents(self, location_id: str) -> List[Dict]:
        """Get all accidents at this location"""
        try:
            query = """
            MATCH (l:AccidentLocation {location_id: $location_id})<-[:OCCURRED_AT]-(cl:Claim)
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
            
            results = self.driver.execute_query(query, {'location_id': location_id})
            return results
            
        except Exception as e:
            logger.error(f"Error getting location accidents: {e}", exc_info=True)
            return []
    
    def get_location_statistics(self, location_id: str) -> Dict:
        """Get statistics for an accident location"""
        try:
            query = """
            MATCH (l:AccidentLocation {location_id: $location_id})
            OPTIONAL MATCH (l)<-[:OCCURRED_AT]-(cl:Claim)
            OPTIONAL MATCH (c:Claimant)-[:FILED]->(cl)
            OPTIONAL MATCH (w:Witness)-[:WITNESSED]->(cl)
            
            WITH l, cl, c, w
            
            RETURN 
                l.location_id as location_id,
                l.intersection as intersection,
                l.city as city,
                count(DISTINCT cl) as accident_count,
                count(DISTINCT c) as unique_claimants,
                sum(cl.total_claim_amount) as total_claim_value,
                avg(cl.risk_score) as avg_risk_score,
                count(DISTINCT w) as witness_count,
                max(cl.accident_date) as last_accident_date,
                min(cl.accident_date) as first_accident_date
            """
            
            results = self.driver.execute_query(query, {'location_id': location_id})
            
            if results:
                return results[0]
            
            return {}
            
        except Exception as e:
            logger.error(f"Error getting location statistics: {e}", exc_info=True)
            return {}
    
    def get_accident_hotspots(self, min_accidents: int = 5, limit: int = 50) -> List[Dict]:
        """Get accident hotspot locations (potential staged accident areas)"""
        try:
            query = """
            MATCH (l:AccidentLocation)<-[:OCCURRED_AT]-(cl:Claim)
            MATCH (c:Claimant)-[:FILED]->(cl)
            
            OPTIONAL MATCH (w:Witness)-[:WITNESSED]->(cl)
            
            WITH l,
                 count(DISTINCT cl) as accident_count,
                 count(DISTINCT c) as unique_claimants,
                 avg(cl.total_claim_amount) as avg_amount,
                 avg(cl.risk_score) as avg_risk,
                 collect(DISTINCT w.witness_id) as witness_ids,
                 max(cl.accident_date) as last_accident,
                 min(cl.accident_date) as first_accident
            
            WHERE accident_count >= $min_accidents
            
            // Calculate accident frequency (accidents per month)
            WITH l, accident_count, unique_claimants, avg_amount, avg_risk, witness_ids, last_accident, first_accident,
                 duration.between(date(first_accident), date(last_accident)).days as days_span
            
            WITH l, accident_count, unique_claimants, avg_amount, avg_risk, witness_ids,
                 CASE WHEN days_span > 0 THEN toFloat(accident_count) / (days_span / 30.0) ELSE accident_count END as accidents_per_month
            
            RETURN 
                l.location_id as location_id,
                l.intersection as intersection,
                l.city as city,
                accident_count,
                unique_claimants,
                avg_amount,
                avg_risk,
                size(witness_ids) as witness_count,
                accidents_per_month
            ORDER BY accident_count DESC, avg_risk DESC
            LIMIT $limit
            """
            
            results = self.driver.execute_query(query, {
                'min_accidents': min_accidents,
                'limit': limit
            })
            
            return results
            
        except Exception as e:
            logger.error(f"Error getting accident hotspots: {e}", exc_info=True)
            return []
    
    def find_location_patterns(self, location_id: str) -> Dict:
        """Analyze patterns at this location (claimants, witnesses, timing)"""
        try:
            query = """
            MATCH (l:AccidentLocation {location_id: $location_id})<-[:OCCURRED_AT]-(cl:Claim)
            MATCH (c:Claimant)-[:FILED]->(cl)
            
            OPTIONAL MATCH (w:Witness)-[:WITNESSED]->(cl)
            
            // Get accident types
            WITH l, cl, c, w,
                 collect(cl.accident_type) as accident_types,
                 collect(cl.accident_date) as accident_dates
            
            // Count repeat claimants
            WITH l, accident_types, accident_dates,
                 count(DISTINCT cl) as total_accidents,
                 count(DISTINCT c) as unique_claimants,
                 count(DISTINCT w) as unique_witnesses
            
            // Find most common accident type
            UNWIND accident_types as accident_type
            WITH l, total_accidents, unique_claimants, unique_witnesses, accident_dates,
                 accident_type, count(accident_type) as type_count
            ORDER BY type_count DESC
            
            WITH l, total_accidents, unique_claimants, unique_witnesses, accident_dates,
                 head(collect({type: accident_type, count: type_count})) as most_common_type
            
            // Analyze timing patterns
            UNWIND accident_dates as accident_date
            WITH l, total_accidents, unique_claimants, unique_witnesses, most_common_type,
                 date(accident_date).dayOfWeek as day_of_week
            
            WITH l, total_accidents, unique_claimants, unique_witnesses, most_common_type,
                 collect(day_of_week) as days_of_week
            
            // Count weekend accidents
            WITH l, total_accidents, unique_claimants, unique_witnesses, most_common_type,
                 size([d IN days_of_week WHERE d IN [6, 7]]) as weekend_accidents
            
            RETURN 
                l.intersection as intersection,
                total_accidents,
                unique_claimants,
                unique_witnesses,
                most_common_type,
                weekend_accidents,
                toFloat(weekend_accidents) / total_accidents as weekend_ratio
            """
            
            results = self.driver.execute_query(query, {'location_id': location_id})
            
            if results:
                return results[0]
            
            return {}
            
        except Exception as e:
            logger.error(f"Error finding location patterns: {e}", exc_info=True)
            return {}
    
    def find_nearby_locations(self, location_id: str, distance_km: float = 5.0) -> List[Dict]:
        """Find nearby accident locations (if coordinates are available)"""
        try:
            query = """
            MATCH (l1:AccidentLocation {location_id: $location_id})
            WHERE l1.latitude IS NOT NULL AND l1.longitude IS NOT NULL
            
            MATCH (l2:AccidentLocation)
            WHERE l2.location_id <> l1.location_id
              AND l2.latitude IS NOT NULL 
              AND l2.longitude IS NOT NULL
            
            // Calculate distance using Haversine formula (approximate)
            WITH l1, l2,
                 point({latitude: l1.latitude, longitude: l1.longitude}) as p1,
                 point({latitude: l2.latitude, longitude: l2.longitude}) as p2
            
            WITH l1, l2,
                 distance(p1, p2) / 1000.0 as distance_km
            
            WHERE distance_km <= $distance_km
            
            // Get accident counts
            OPTIONAL MATCH (l2)<-[:OCCURRED_AT]-(cl:Claim)
            
            WITH l2, distance_km,
                 count(cl) as accident_count
            
            RETURN 
                l2.location_id as location_id,
                l2.intersection as intersection,
                l2.city as city,
                distance_km,
                accident_count
            ORDER BY distance_km ASC
            LIMIT 20
            """
            
            results = self.driver.execute_query(query, {
                'location_id': location_id,
                'distance_km': distance_km
            })
            
            return results
            
        except Exception as e:
            logger.error(f"Error finding nearby locations: {e}", exc_info=True)
            return []
    
    # ==================== UPDATE OPERATIONS ====================
    
    def update_accident_location(self, location_id: str, updates: Dict) -> bool:
        """Update accident location properties"""
        try:
            set_clauses = [f"l.{key} = ${key}" for key in updates.keys()]
            set_clause = ", ".join(set_clauses)
            
            query = f"""
            MATCH (l:AccidentLocation {{location_id: $location_id}})
            SET {set_clause}
            RETURN l.location_id as location_id
            """
            
            params = {'location_id': location_id}
            params.update(updates)
            
            result = self.driver.execute_write(query, params)
            
            if result:
                logger.info(f"Updated accident location: {location_id}")
                return True
            
            return False
            
        except Exception as e:
            logger.error(f"Error updating accident location: {e}", exc_info=True)
            return False
    
    # ==================== DELETE OPERATIONS ====================
    
    def delete_accident_location(self, location_id: str) -> bool:
        """Delete an accident location and its relationships"""
        try:
            query = """
            MATCH (l:AccidentLocation {location_id: $location_id})
            DETACH DELETE l
            """
            
            self.driver.execute_write(query, {'location_id': location_id})
            logger.info(f"Deleted accident location: {location_id}")
            return True
            
        except Exception as e:
            logger.error(f"Error deleting accident location: {e}", exc_info=True)
            return False
    
    # ==================== SEARCH OPERATIONS ====================
    
    def search_accident_locations(
        self,
        city: Optional[str] = None,
        state: Optional[str] = None,
        intersection: Optional[str] = None,
        limit: int = 100
    ) -> List[AccidentLocation]:
        """Search accident locations by city, state, or intersection"""
        try:
            where_clauses = []
            params = {'limit': limit}
            
            if city:
                where_clauses.append("l.city = $city")
                params['city'] = city
            
            if state:
                where_clauses.append("l.state = $state")
                params['state'] = state
            
            if intersection:
                where_clauses.append("l.intersection CONTAINS $intersection")
                params['intersection'] = intersection
            
            where_clause = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""
            
            query = f"""
            MATCH (l:AccidentLocation)
            {where_clause}
            RETURN 
                l.location_id as location_id,
                l.intersection as intersection,
                l.city as city,
                l.state as state,
                l.latitude as latitude,
                l.longitude as longitude
            LIMIT $limit
            """
            
            results = self.driver.execute_query(query, params)
            
            return [AccidentLocation.from_dict(row) for row in results]
            
        except Exception as e:
            logger.error(f"Error searching accident locations: {e}", exc_info=True)
            return []
