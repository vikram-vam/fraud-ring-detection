"""
Neo4j Data Loader for Fraud Ring Detection System
Loads synthetic data into Neo4j AuraDB instance.
"""

import os
import logging
from datetime import datetime
import pandas as pd
from neo4j import GraphDatabase
from dotenv import load_dotenv
import ssl

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

class Neo4jLoader:
    def __init__(self):
        self.uri = os.getenv('NEO4J_URI')
        self.username = os.getenv('NEO4J_USERNAME')
        self.password = os.getenv('NEO4J_PASSWORD')
        
        if not all([self.uri, self.username, self.password]):
            raise ValueError("Missing Neo4j credentials in .env file")
            
        logger.info(f"Connecting to Neo4j at {self.uri}...")
        
        # Configure SSL context to handle self-signed certificates
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE
        
        # Handle different URI schemes
        # If using neo4j+s://, convert to neo4j:// to allow custom SSL context
        if self.uri.startswith("neo4j+s://"):
            working_uri = self.uri.replace("neo4j+s://", "neo4j://")
            logger.info("Converted URI to neo4j:// to allow custom SSL configuration")
        elif self.uri.startswith("bolt+s://"):
            working_uri = self.uri.replace("bolt+s://", "bolt://")
            logger.info("Converted URI to bolt:// to allow custom SSL configuration")
        else:
            working_uri = self.uri

        # Connect to Neo4j with custom SSL context
        self.driver = GraphDatabase.driver(
            working_uri, 
            auth=(self.username, self.password),
            encrypted=True,
            ssl_context=ssl_context
        )
        
        # Verify connection
        self.verify_connection()

    def verify_connection(self):
        """Verify database connection"""
        try:
            with self.driver.session() as session:
                result = session.run("RETURN 1 as num")
                record = result.single()
                if record['num'] == 1:
                    logger.info("✅ Successfully connected to Neo4j!")
        except Exception as e:
            logger.error(f"❌ Failed to connect to Neo4j: {e}")
            raise

    def close(self):
        """Close the driver connection"""
        if self.driver:
            self.driver.close()
            logger.info("Connection closed.")

    def clear_database(self):
        """Clear all data from the database"""
        logger.warning("⚠️  Clearing database...")
        with self.driver.session() as session:
            session.run("MATCH (n) DETACH DELETE n")
        logger.info("Database cleared.")

    def create_constraints(self):
        """Create uniqueness constraints and indexes"""
        logger.info("Creating constraints and indexes...")
        
        queries = [
            # Constraints
            "CREATE CONSTRAINT claimant_id IF NOT EXISTS FOR (c:Claimant) REQUIRE c.claimant_id IS UNIQUE",
            "CREATE CONSTRAINT policy_id IF NOT EXISTS FOR (p:Policy) REQUIRE p.policy_id IS UNIQUE",
            "CREATE CONSTRAINT vehicle_id IF NOT EXISTS FOR (v:Vehicle) REQUIRE v.vin IS UNIQUE",
            "CREATE CONSTRAINT claim_id IF NOT EXISTS FOR (cl:Claim) REQUIRE cl.claim_id IS UNIQUE",
            "CREATE CONSTRAINT shop_id IF NOT EXISTS FOR (s:RepairShop) REQUIRE s.shop_id IS UNIQUE",
            "CREATE CONSTRAINT provider_id IF NOT EXISTS FOR (m:MedicalProvider) REQUIRE m.provider_id IS UNIQUE",
            "CREATE CONSTRAINT lawyer_id IF NOT EXISTS FOR (l:Lawyer) REQUIRE l.lawyer_id IS UNIQUE",
            "CREATE CONSTRAINT witness_id IF NOT EXISTS FOR (w:Witness) REQUIRE w.witness_id IS UNIQUE",
            
            # Indexes
            "CREATE INDEX claimant_name IF NOT EXISTS FOR (c:Claimant) ON (c.name)",
            "CREATE INDEX claim_date IF NOT EXISTS FOR (cl:Claim) ON (cl.claim_date)",
            "CREATE INDEX fraud_ring IF NOT EXISTS FOR (c:Claimant) ON (c.fraud_ring_id)"
        ]
        
        with self.driver.session() as session:
            for query in queries:
                session.run(query)
        logger.info("Constraints and indexes created.")

    def load_data(self, filename, data_type, batch_size=1000):
        """Generic data loader function"""
        filepath = os.path.join('data', filename)
        if not os.path.exists(filepath):
            logger.error(f"File not found: {filepath}")
            return

        logger.info(f"Loading {data_type} from {filename}...")
        df = pd.read_csv(filepath)
        
        # Replace NaN with None for Cypher compatibility
        df = df.where(pd.notnull(df), None)
        
        total_rows = len(df)
        chunks = [df[i:i + batch_size] for i in range(0, total_rows, batch_size)]
        
        with self.driver.session() as session:
            for i, chunk in enumerate(chunks):
                data = chunk.to_dict('records')
                self._process_batch(session, data_type, data)
                
                if (i + 1) * batch_size % 5000 == 0:
                    logger.info(f"  Processed {min((i + 1) * batch_size, total_rows)}/{total_rows} records...")
                    
        logger.info(f"✅ Loaded {total_rows} {data_type}.")

    def _process_batch(self, session, data_type, data):
        """Process a batch of data based on type"""
        
        if data_type == 'claimants':
            query = """
            UNWIND $rows as row
            CREATE (c:Claimant {
                claimant_id: row.claimant_id,
                name: row.name,
                ssn: row.ssn,
                dob: row.dob,
                address: row.address,
                phone: row.phone,
                email: row.email,
                license_number: row.license_number,
                is_fraud_ring: row.is_fraud_ring,
                fraud_ring_id: row.fraud_ring_id
            })
            """
            
        elif data_type == 'policies':
            query = """
            UNWIND $rows as row
            MATCH (c:Claimant {claimant_id: row.claimant_id})
            CREATE (p:Policy {
                policy_id: row.policy_id,
                policy_number: row.policy_number,
                start_date: row.start_date,
                end_date: row.end_date,
                premium: row.premium,
                coverage_type: row.coverage_type
            })
            CREATE (c)-[:HAS_POLICY]->(p)
            """
            
        elif data_type == 'vehicles':
            query = """
            UNWIND $rows as row
            MATCH (p:Policy {policy_id: row.policy_id})
            CREATE (v:Vehicle {
                vehicle_id: row.vehicle_id,
                vin: row.vin,
                make: row.make,
                model: row.model,
                year: row.year,
                color: row.color,
                license_plate: row.license_plate
            })
            CREATE (v)-[:INSURED_BY]->(p)
            """
            
        elif data_type == 'repair_shops':
            query = """
            UNWIND $rows as row
            CREATE (s:RepairShop {
                shop_id: row.shop_id,
                name: row.name,
                address: row.address,
                phone: row.phone,
                license_number: row.license_number,
                is_fraud_involved: row.is_fraud_involved
            })
            """
            
        elif data_type == 'medical_providers':
            query = """
            UNWIND $rows as row
            CREATE (m:MedicalProvider {
                provider_id: row.provider_id,
                name: row.name,
                specialty: row.specialty,
                address: row.address,
                phone: row.phone,
                npi_number: row.npi_number,
                is_fraud_involved: row.is_fraud_involved
            })
            """
            
        elif data_type == 'lawyers':
            query = """
            UNWIND $rows as row
            CREATE (l:Lawyer {
                lawyer_id: row.lawyer_id,
                name: row.name,
                bar_number: row.bar_number,
                firm_name: row.firm_name,
                address: row.address,
                phone: row.phone,
                is_fraud_involved: row.is_fraud_involved
            })
            """
            
        elif data_type == 'witnesses':
            query = """
            UNWIND $rows as row
            CREATE (w:Witness {
                witness_id: row.witness_id,
                name: row.name,
                phone: row.phone,
                address: row.address,
                is_recurring: row.is_recurring
            })
            """
            
        elif data_type == 'claims':
            query = """
            UNWIND $rows as row
            MATCH (c:Claimant {claimant_id: row.claimant_id})
            MATCH (p:Policy {policy_id: row.policy_id})
            MATCH (v:Vehicle {vehicle_id: row.vehicle_id})
            MATCH (s:RepairShop {shop_id: row.repair_shop_id})
            
            CREATE (cl:Claim {
                claim_id: row.claim_id,
                claim_number: row.claim_number,
                claim_date: row.claim_date,
                incident_date: row.incident_date,
                claim_amount: row.claim_amount,
                claim_type: row.claim_type,
                status: row.status,
                description: row.description,
                location: row.location,
                weather_condition: row.weather_condition,
                is_fraud_ring: row.is_fraud_ring,
                fraud_ring_id: row.fraud_ring_id
            })
            CREATE (c)-[:FILED_CLAIM]->(cl)
            CREATE (cl)-[:UNDER_POLICY]->(p)
            CREATE (cl)-[:INVOLVES_VEHICLE]->(v)
            CREATE (cl)-[:REPAIRED_AT]->(s)
            
            WITH cl, row
            WHERE row.medical_provider_id IS NOT NULL 
            MATCH (m:MedicalProvider {provider_id: row.medical_provider_id})
            CREATE (cl)-[:TREATED_BY]->(m)
            
            WITH cl, row
            WHERE row.lawyer_id IS NOT NULL 
            MATCH (l:Lawyer {lawyer_id: row.lawyer_id})
            CREATE (cl)-[:REPRESENTED_BY]->(l)
            """
            
        elif data_type == 'claim_witnesses':
            query = """
            UNWIND $rows as row
            MATCH (cl:Claim {claim_id: row.claim_id})
            MATCH (w:Witness {witness_id: row.witness_id})
            CREATE (cl)-[:HAS_WITNESS {relationship: row.relationship}]->(w)
            """
            
        elif data_type == 'claimant_relationships':
            query = """
            UNWIND $rows as row
            MATCH (c1:Claimant {claimant_id: row.from_claimant})
            MATCH (c2:Claimant {claimant_id: row.to_claimant})
            CREATE (c1)-[:RELATED_TO {type: row.relationship_type, ring_id: row.ring_id}]->(c2)
            """
            
        else:
            return

        session.run(query, rows=data)

    def create_derived_relationships(self):
        """Create implicit relationships based on shared attributes"""
        logger.info("Creating derived relationships (Address/Phone sharing)...")
        with self.driver.session() as session:
            # Shared Address
            session.run("""
                MATCH (c1:Claimant), (c2:Claimant)
                WHERE c1.claimant_id < c2.claimant_id 
                  AND c1.address = c2.address
                MERGE (c1)-[:SHARES_ADDRESS]->(c2)
            """)
            
            # Shared Phone
            session.run("""
                MATCH (c1:Claimant), (c2:Claimant)
                WHERE c1.claimant_id < c2.claimant_id 
                  AND c1.phone = c2.phone
                MERGE (c1)-[:SHARES_PHONE]->(c2)
            """)
        logger.info("Derived relationships created.")

    def print_summary(self):
        """Print database statistics"""
        logger.info("Generating Final Summary...")
        with self.driver.session() as session:
            # Node counts
            result = session.run("CALL db.labels() YIELD label RETURN label")
            labels = [r['label'] for r in result]
            
            print("\n" + "="*40)
            print("DATABASE SUMMARY")
            print("="*40)
            print("Nodes:")
            total_nodes = 0
            for label in labels:
                count = session.run(f"MATCH (n:{label}) RETURN count(n) as c").single()['c']
                print(f"  {label:<20}: {count}")
                total_nodes += count
            print(f"  {'TOTAL':<20}: {total_nodes}")
            
            # Relationship counts
            print("\nRelationships:")
            result = session.run("MATCH ()-[r]->() RETURN count(r) as c")
            print(f"  Total Relationships : {result.single()['c']}")
            print("="*40 + "\n")

def main():
    loader = None
    try:
        loader = Neo4jLoader()
        
        # Clear existing data
        loader.clear_database()
        
        # Setup schema
        loader.create_constraints()
        
        # Load entities
        loader.load_data('claimants.csv', 'claimants')
        loader.load_data('policies.csv', 'policies')
        loader.load_data('vehicles.csv', 'vehicles')
        loader.load_data('repair_shops.csv', 'repair_shops')
        loader.load_data('medical_providers.csv', 'medical_providers')
        loader.load_data('lawyers.csv', 'lawyers')
        loader.load_data('witnesses.csv', 'witnesses')
        
        # Load main transactions and connections
        loader.load_data('claims.csv', 'claims')
        loader.load_data('claim_witnesses.csv', 'claim_witnesses')
        loader.load_data('claimant_relationships.csv', 'claimant_relationships')
        
        # derived
        loader.create_derived_relationships()
        
        # Summary
        loader.print_summary()
        
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        if loader:
            loader.close()

if __name__ == "__main__":
    main()
