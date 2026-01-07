"""
Neo4j Driver - Database connection and operations for auto insurance fraud detection
Handles connections, queries, constraints, and indexes
"""
from neo4j import GraphDatabase
from typing import List, Dict, Optional, Any
import os
from dotenv import load_dotenv
import logging

from utils.logger import setup_logger

# Load environment variables
load_dotenv()

logger = setup_logger(__name__)


class Neo4jDriver:
    """Neo4j database driver with connection management and query execution"""
    
    def __init__(self, uri: str = None, user: str = None, password: str = None):
        """
        Initialize Neo4j driver
        
        Args:
            uri: Neo4j connection URI (default: from env NEO4J_URI)
            user: Neo4j username (default: from env NEO4J_USER)
            password: Neo4j password (default: from env NEO4J_PASSWORD)
        """
        self.uri = uri or os.getenv('NEO4J_URI', 'bolt://localhost:7687')
        self.user = user or os.getenv('NEO4J_USER', 'neo4j')
        self.password = password or os.getenv('NEO4J_PASSWORD', 'password')
        
        try:
            self.driver = GraphDatabase.driver(self.uri, auth=(self.user, self.password))
            logger.info(f"Neo4j driver initialized for {self.uri}")
        except Exception as e:
            logger.error(f"Failed to initialize Neo4j driver: {e}", exc_info=True)
            raise
    
    def close(self):
        """Close the driver connection"""
        if self.driver:
            self.driver.close()
            logger.info("Neo4j driver closed")
    
    def test_connection(self) -> bool:
        """
        Test the database connection
        
        Returns:
            True if connection successful, False otherwise
        """
        try:
            with self.driver.session() as session:
                result = session.run("RETURN 1 as test")
                value = result.single()
                return value["test"] == 1
        except Exception as e:
            logger.error(f"Connection test failed: {e}", exc_info=True)
            return False
    
    def execute_query(self, query: str, parameters: Dict = None) -> List[Dict]:
        """
        Execute a read query and return results
        
        Args:
            query: Cypher query string
            parameters: Query parameters dictionary
            
        Returns:
            List of result dictionaries
        """
        try:
            with self.driver.session() as session:
                result = session.run(query, parameters or {})
                return [dict(record) for record in result]
        except Exception as e:
            logger.error(f"Query execution failed: {e}\nQuery: {query}", exc_info=True)
            raise
    
    def execute_write(self, query: str, parameters: Dict = None) -> Any:
        """
        Execute a write query
        
        Args:
            query: Cypher query string
            parameters: Query parameters dictionary
            
        Returns:
            Query result
        """
        try:
            with self.driver.session() as session:
                result = session.run(query, parameters or {})
                return result.single() if result.peek() else None
        except Exception as e:
            logger.error(f"Write query failed: {e}\nQuery: {query}", exc_info=True)
            raise
    
    def get_node_count(self, label: str) -> int:
        """
        Get count of nodes with specific label
        
        Args:
            label: Node label (e.g., 'Claimant', 'Vehicle', 'BodyShop')
            
        Returns:
            Count of nodes
        """
        query = f"MATCH (n:{label}) RETURN count(n) as count"
        try:
            result = self.execute_query(query)
            return result[0]['count'] if result else 0
        except Exception as e:
            logger.error(f"Failed to get node count for {label}: {e}")
            return 0
    
    def get_relationship_count(self, rel_type: str = None) -> int:
        """
        Get count of relationships
        
        Args:
            rel_type: Relationship type (optional, None for all)
            
        Returns:
            Count of relationships
        """
        if rel_type:
            query = f"MATCH ()-[r:{rel_type}]->() RETURN count(r) as count"
        else:
            query = "MATCH ()-[r]->() RETURN count(r) as count"
        
        try:
            result = self.execute_query(query)
            return result[0]['count'] if result else 0
        except Exception as e:
            logger.error(f"Failed to get relationship count: {e}")
            return 0
    
    def get_statistics(self) -> Dict:
        """
        Get comprehensive database statistics for auto insurance
        
        Returns:
            Dictionary with node and relationship counts
        """
        stats = {}
        
        # Core entities
        stats['claimants'] = self.get_node_count('Claimant')
        stats['claims'] = self.get_node_count('Claim')
        stats['fraud_rings'] = self.get_node_count('FraudRing')
        
        # Auto insurance specific entities
        stats['vehicles'] = self.get_node_count('Vehicle')
        stats['body_shops'] = self.get_node_count('BodyShop')
        stats['medical_providers'] = self.get_node_count('MedicalProvider')
        stats['attorneys'] = self.get_node_count('Attorney')
        stats['tow_companies'] = self.get_node_count('TowCompany')
        stats['accident_locations'] = self.get_node_count('AccidentLocation')
        stats['witnesses'] = self.get_node_count('Witness')
        
        # Relationship counts
        stats['total_relationships'] = self.get_relationship_count()
        stats['filed_relationships'] = self.get_relationship_count('FILED')
        stats['member_of_relationships'] = self.get_relationship_count('MEMBER_OF')
        stats['involves_vehicle'] = self.get_relationship_count('INVOLVES_VEHICLE')
        stats['repaired_at'] = self.get_relationship_count('REPAIRED_AT')
        stats['treated_by'] = self.get_relationship_count('TREATED_BY')
        stats['represented_by'] = self.get_relationship_count('REPRESENTED_BY')
        stats['towed_by'] = self.get_relationship_count('TOWED_BY')
        stats['witnessed'] = self.get_relationship_count('WITNESSED')
        stats['occurred_at'] = self.get_relationship_count('OCCURRED_AT')
        
        return stats
    
    def get_database_info(self) -> Dict:
        """
        Get database metadata
        
        Returns:
            Dictionary with database information
        """
        try:
            query = """
            CALL dbms.components() YIELD name, versions, edition
            RETURN name, versions[0] as version, edition
            """
            result = self.execute_query(query)
            
            if result:
                return {
                    'name': result[0].get('name', 'Unknown'),
                    'version': result[0].get('version', 'Unknown'),
                    'edition': result[0].get('edition', 'Unknown')
                }
            return {}
        except Exception as e:
            logger.error(f"Failed to get database info: {e}")
            return {}
    
    def create_constraints(self):
        """
        Create unique constraints for all entity types
        Ensures data integrity and improves query performance
        """
        constraints = [
            # Core entities
            "CREATE CONSTRAINT claimant_id IF NOT EXISTS FOR (c:Claimant) REQUIRE c.claimant_id IS UNIQUE",
            "CREATE CONSTRAINT claim_id IF NOT EXISTS FOR (cl:Claim) REQUIRE cl.claim_id IS UNIQUE",
            "CREATE CONSTRAINT claim_number IF NOT EXISTS FOR (cl:Claim) REQUIRE cl.claim_number IS UNIQUE",
            "CREATE CONSTRAINT ring_id IF NOT EXISTS FOR (r:FraudRing) REQUIRE r.ring_id IS UNIQUE",
            
            # Auto insurance entities
            "CREATE CONSTRAINT vehicle_id IF NOT EXISTS FOR (v:Vehicle) REQUIRE v.vehicle_id IS UNIQUE",
            "CREATE CONSTRAINT vin IF NOT EXISTS FOR (v:Vehicle) REQUIRE v.vin IS UNIQUE",
            "CREATE CONSTRAINT body_shop_id IF NOT EXISTS FOR (b:BodyShop) REQUIRE b.body_shop_id IS UNIQUE",
            "CREATE CONSTRAINT medical_provider_id IF NOT EXISTS FOR (m:MedicalProvider) REQUIRE m.provider_id IS UNIQUE",
            "CREATE CONSTRAINT attorney_id IF NOT EXISTS FOR (a:Attorney) REQUIRE a.attorney_id IS UNIQUE",
            "CREATE CONSTRAINT tow_company_id IF NOT EXISTS FOR (t:TowCompany) REQUIRE t.tow_company_id IS UNIQUE",
            "CREATE CONSTRAINT accident_location_id IF NOT EXISTS FOR (l:AccidentLocation) REQUIRE l.location_id IS UNIQUE",
            "CREATE CONSTRAINT witness_id IF NOT EXISTS FOR (w:Witness) REQUIRE w.witness_id IS UNIQUE"
        ]
        
        for constraint in constraints:
            try:
                self.execute_write(constraint)
                logger.info(f"Created constraint: {constraint.split('FOR')[0].strip()}")
            except Exception as e:
                logger.warning(f"Constraint creation warning: {e}")
    
    def create_indexes(self):
        """
        Create indexes for frequently queried properties
        Improves query performance significantly
        """
        indexes = [
            # Claimant indexes
            "CREATE INDEX claimant_name IF NOT EXISTS FOR (c:Claimant) ON (c.name)",
            "CREATE INDEX claimant_email IF NOT EXISTS FOR (c:Claimant) ON (c.email)",
            "CREATE INDEX claimant_phone IF NOT EXISTS FOR (c:Claimant) ON (c.phone)",
            "CREATE INDEX claimant_drivers_license IF NOT EXISTS FOR (c:Claimant) ON (c.drivers_license)",
            
            # Claim indexes
            "CREATE INDEX claim_status IF NOT EXISTS FOR (cl:Claim) ON (cl.status)",
            "CREATE INDEX claim_risk_score IF NOT EXISTS FOR (cl:Claim) ON (cl.risk_score)",
            "CREATE INDEX claim_accident_date IF NOT EXISTS FOR (cl:Claim) ON (cl.accident_date)",
            "CREATE INDEX claim_report_date IF NOT EXISTS FOR (cl:Claim) ON (cl.report_date)",
            "CREATE INDEX claim_accident_type IF NOT EXISTS FOR (cl:Claim) ON (cl.accident_type)",
            "CREATE INDEX claim_total_amount IF NOT EXISTS FOR (cl:Claim) ON (cl.total_claim_amount)",
            
            # Vehicle indexes
            "CREATE INDEX vehicle_make IF NOT EXISTS FOR (v:Vehicle) ON (v.make)",
            "CREATE INDEX vehicle_model IF NOT EXISTS FOR (v:Vehicle) ON (v.model)",
            "CREATE INDEX vehicle_year IF NOT EXISTS FOR (v:Vehicle) ON (v.year)",
            "CREATE INDEX vehicle_license_plate IF NOT EXISTS FOR (v:Vehicle) ON (v.license_plate)",
            
            # Body shop indexes
            "CREATE INDEX body_shop_name IF NOT EXISTS FOR (b:BodyShop) ON (b.name)",
            "CREATE INDEX body_shop_city IF NOT EXISTS FOR (b:BodyShop) ON (b.city)",
            "CREATE INDEX body_shop_license IF NOT EXISTS FOR (b:BodyShop) ON (b.license_number)",
            
            # Medical provider indexes
            "CREATE INDEX medical_provider_name IF NOT EXISTS FOR (m:MedicalProvider) ON (m.name)",
            "CREATE INDEX medical_provider_type IF NOT EXISTS FOR (m:MedicalProvider) ON (m.provider_type)",
            "CREATE INDEX medical_provider_city IF NOT EXISTS FOR (m:MedicalProvider) ON (m.city)",
            
            # Attorney indexes
            "CREATE INDEX attorney_name IF NOT EXISTS FOR (a:Attorney) ON (a.name)",
            "CREATE INDEX attorney_firm IF NOT EXISTS FOR (a:Attorney) ON (a.firm)",
            "CREATE INDEX attorney_bar_number IF NOT EXISTS FOR (a:Attorney) ON (a.bar_number)",
            
            # Tow company indexes
            "CREATE INDEX tow_company_name IF NOT EXISTS FOR (t:TowCompany) ON (t.name)",
            "CREATE INDEX tow_company_city IF NOT EXISTS FOR (t:TowCompany) ON (t.city)",
            
            # Accident location indexes
            "CREATE INDEX accident_location_intersection IF NOT EXISTS FOR (l:AccidentLocation) ON (l.intersection)",
            "CREATE INDEX accident_location_city IF NOT EXISTS FOR (l:AccidentLocation) ON (l.city)",
            
            # Witness indexes
            "CREATE INDEX witness_name IF NOT EXISTS FOR (w:Witness) ON (w.name)",
            "CREATE INDEX witness_phone IF NOT EXISTS FOR (w:Witness) ON (w.phone)",
            
            # Fraud ring indexes
            "CREATE INDEX fraud_ring_type IF NOT EXISTS FOR (r:FraudRing) ON (r.ring_type)",
            "CREATE INDEX fraud_ring_pattern IF NOT EXISTS FOR (r:FraudRing) ON (r.pattern_type)",
            "CREATE INDEX fraud_ring_status IF NOT EXISTS FOR (r:FraudRing) ON (r.status)",
            "CREATE INDEX fraud_ring_confidence IF NOT EXISTS FOR (r:FraudRing) ON (r.confidence_score)"
        ]
        
        for index in indexes:
            try:
                self.execute_write(index)
                logger.info(f"Created index: {index.split('FOR')[0].strip()}")
            except Exception as e:
                logger.warning(f"Index creation warning: {e}")
    
    def clear_database(self, confirm: bool = False):
        """
        Clear all nodes and relationships from database
        
        Args:
            confirm: Must be True to execute (safety check)
        """
        if not confirm:
            logger.warning("Database clear operation requires confirmation")
            return
        
        try:
            # Delete all relationships first
            query = "MATCH ()-[r]->() DELETE r"
            self.execute_write(query)
            logger.info("Deleted all relationships")
            
            # Delete all nodes
            query = "MATCH (n) DELETE n"
            self.execute_write(query)
            logger.info("Deleted all nodes")
            
            logger.info("Database cleared successfully")
        except Exception as e:
            logger.error(f"Failed to clear database: {e}", exc_info=True)
            raise
    
    def get_high_risk_claims(self, threshold: float = 70, limit: int = 100) -> List[Dict]:
        """
        Get high-risk claims for auto insurance
        
        Args:
            threshold: Minimum risk score
            limit: Maximum number of results
            
        Returns:
            List of high-risk claim dictionaries
        """
        query = """
        MATCH (c:Claimant)-[:FILED]->(cl:Claim)
        WHERE cl.risk_score >= $threshold
        
        OPTIONAL MATCH (cl)-[:INVOLVES_VEHICLE]->(v:Vehicle)
        OPTIONAL MATCH (cl)-[:REPAIRED_AT]->(b:BodyShop)
        OPTIONAL MATCH (cl)-[:TREATED_BY]->(m:MedicalProvider)
        OPTIONAL MATCH (cl)-[:REPRESENTED_BY]->(a:Attorney)
        OPTIONAL MATCH (cl)-[:OCCURRED_AT]->(l:AccidentLocation)
        OPTIONAL MATCH (c)-[:MEMBER_OF]->(r:FraudRing)
        
        RETURN 
            cl.claim_id as claim_id,
            cl.claim_number as claim_number,
            c.claimant_id as claimant_id,
            c.name as claimant_name,
            cl.accident_type as accident_type,
            cl.total_claim_amount as claim_amount,
            cl.accident_date as accident_date,
            cl.report_date as report_date,
            cl.status as status,
            cl.risk_score as risk_score,
            v.make + ' ' + v.model + ' ' + v.year as vehicle_info,
            b.name as body_shop,
            m.name as medical_provider,
            a.name as attorney,
            l.intersection as accident_location,
            r.ring_id as ring_id,
            r.pattern_type as ring_type
        ORDER BY cl.risk_score DESC
        LIMIT $limit
        """
        
        return self.execute_query(query, {'threshold': threshold, 'limit': limit})
    
    def get_entity_claim_history(
        self, 
        entity_type: str, 
        entity_id: str,
        limit: int = 50
    ) -> List[Dict]:
        """
        Get claim history for any entity (BodyShop, MedicalProvider, Attorney, etc.)
        
        Args:
            entity_type: Entity label (e.g., 'BodyShop', 'MedicalProvider')
            entity_id: Entity identifier
            limit: Maximum results
            
        Returns:
            List of claims associated with entity
        """
        # Map entity types to relationships
        relationship_map = {
            'BodyShop': 'REPAIRED_AT',
            'MedicalProvider': 'TREATED_BY',
            'Attorney': 'REPRESENTED_BY',
            'TowCompany': 'TOWED_BY',
            'AccidentLocation': 'OCCURRED_AT'
        }
        
        rel_type = relationship_map.get(entity_type)
        if not rel_type:
            logger.error(f"Unknown entity type: {entity_type}")
            return []
        
        # Determine ID field based on entity type
        id_field_map = {
            'BodyShop': 'body_shop_id',
            'MedicalProvider': 'provider_id',
            'Attorney': 'attorney_id',
            'TowCompany': 'tow_company_id',
            'AccidentLocation': 'location_id'
        }
        
        id_field = id_field_map.get(entity_type, 'id')
        
        query = f"""
        MATCH (e:{entity_type} {{{id_field}: $entity_id}})
        MATCH (cl:Claim)-[:{rel_type}]->(e)
        MATCH (c:Claimant)-[:FILED]->(cl)
        
        OPTIONAL MATCH (cl)-[:INVOLVES_VEHICLE]->(v:Vehicle)
        OPTIONAL MATCH (c)-[:MEMBER_OF]->(r:FraudRing)
        
        RETURN 
            cl.claim_id as claim_id,
            cl.claim_number as claim_number,
            c.name as claimant_name,
            cl.accident_date as accident_date,
            cl.total_claim_amount as claim_amount,
            cl.risk_score as risk_score,
            v.make + ' ' + v.model as vehicle,
            r.ring_id as ring_id
        ORDER BY cl.accident_date DESC
        LIMIT $limit
        """
        
        return self.execute_query(query, {'entity_id': entity_id, 'limit': limit})
    
    def get_repeat_witness_claims(self, min_appearances: int = 3) -> List[Dict]:
        """
        Find witnesses appearing in multiple claims (FRAUD INDICATOR)
        
        Args:
            min_appearances: Minimum number of claim appearances
            
        Returns:
            List of suspicious witnesses with claim counts
        """
        query = """
        MATCH (w:Witness)-[:WITNESSED]->(cl:Claim)
        WITH w, count(cl) as claim_count, collect(cl.claim_id) as claim_ids
        WHERE claim_count >= $min_appearances
        
        RETURN 
            w.witness_id as witness_id,
            w.name as witness_name,
            w.phone as witness_phone,
            claim_count,
            claim_ids
        ORDER BY claim_count DESC
        """
        
        return self.execute_query(query, {'min_appearances': min_appearances})
    
    def get_vehicle_accident_frequency(self, min_accidents: int = 2) -> List[Dict]:
        """
        Find vehicles involved in multiple accidents (FRAUD INDICATOR)
        
        Args:
            min_accidents: Minimum number of accidents
            
        Returns:
            List of vehicles with multiple accidents
        """
        query = """
        MATCH (v:Vehicle)<-[:INVOLVES_VEHICLE]-(cl:Claim)
        WITH v, count(cl) as accident_count, collect(cl.claim_id) as claim_ids
        WHERE accident_count >= $min_accidents
        
        RETURN 
            v.vehicle_id as vehicle_id,
            v.vin as vin,
            v.make + ' ' + v.model + ' ' + v.year as vehicle_info,
            v.license_plate as license_plate,
            accident_count,
            claim_ids
        ORDER BY accident_count DESC
        """
        
        return self.execute_query(query, {'min_accidents': min_accidents})
    
    def get_accident_location_hotspots(self, min_accidents: int = 5) -> List[Dict]:
        """
        Find accident location hotspots (potential staged accident locations)
        
        Args:
            min_accidents: Minimum accidents at location
            
        Returns:
            List of accident hotspot locations
        """
        query = """
        MATCH (l:AccidentLocation)<-[:OCCURRED_AT]-(cl:Claim)
        WITH l, count(cl) as accident_count, 
             collect(DISTINCT cl.claim_id) as claim_ids,
             avg(cl.risk_score) as avg_risk_score
        WHERE accident_count >= $min_accidents
        
        RETURN 
            l.location_id as location_id,
            l.intersection as intersection,
            l.city as city,
            accident_count,
            avg_risk_score,
            claim_ids
        ORDER BY accident_count DESC, avg_risk_score DESC
        """
        
        return self.execute_query(query, {'min_accidents': min_accidents})


# Singleton instance
_driver_instance = None


def get_neo4j_driver() -> Neo4jDriver:
    """
    Get singleton Neo4j driver instance
    
    Returns:
        Neo4jDriver instance
    """
    global _driver_instance
    
    if _driver_instance is None:
        _driver_instance = Neo4jDriver()
    
    return _driver_instance


def close_neo4j_driver():
    """Close the singleton driver instance"""
    global _driver_instance
    
    if _driver_instance:
        _driver_instance.close()
        _driver_instance = None
