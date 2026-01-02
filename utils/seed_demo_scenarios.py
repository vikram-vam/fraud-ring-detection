
import os
from neo4j import GraphDatabase
import ssl
from dotenv import load_dotenv
import random
from datetime import datetime, timedelta

# Load environment variables
load_dotenv()

def seed_demo_scenarios():
    uri = os.getenv('NEO4J_URI')
    username = os.getenv('NEO4J_USERNAME')
    password = os.getenv('NEO4J_PASSWORD')

    # Configure SSL
    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE

    working_uri = uri
    if uri.startswith("neo4j+s://"):
        working_uri = uri.replace("neo4j+s://", "neo4j://")
    elif uri.startswith("bolt+s://"):
        working_uri = uri.replace("bolt+s://", "bolt://")

    driver = GraphDatabase.driver(
        working_uri, 
        auth=(username, password),
        encrypted=True,
        ssl_context=ssl_context
    )

    with driver.session() as session:
        print("Seeding Demo Scenarios...")

        # 1. Ensure High-Risk Provider "Dr. Roy Cook" exists and is bad
        print("- Enhancing Dr. Roy Cook (The 'Trap')...")
        session.run("""
            MERGE (m:MedicalProvider {name: 'Dr. Roy Cook'})
            ON CREATE SET 
                m.provider_id = 'MD_COOK_99',
                m.city = 'Metro City',
                m.specialty = 'Chiropractor'
        """)
        
        # Link him to existing active fraud rings if not already heavily linked
        # We find a ring and force a link to make him validly high risk
        session.run("""
            MATCH (m:MedicalProvider {name: 'Dr. Roy Cook'})
            MATCH (c:Claim)
            WHERE c.is_fraud_ring = true
            WITH m, c ORDER BY rand() LIMIT 5
            MERGE (c)-[:TREATED_BY]->(m)
        """)

        # 2. Scenario: "The Staged Accident" (Ring with Lawyer, Doctor, shared address)
        print("- creating 'Staged Accident' scenario...")
        session.run("""
            MERGE (r:FraudRing {ring_id: 'RING_STAGED_01'})
            SET r.type = 'Staged Accident'
            
            // Lawyer
            MERGE (l:Lawyer {lawyer_id: 'LAW_SG_01'})
            SET l.name = 'Saul Goodman'
            
            // 3 Claimants sharing address
            MERGE (c1:Claimant {claimant_id: 'CLM_SA_01'})
            SET c1.name = 'Gary Plotter', c1.age = 34, c1.address = '123 Fake St'
            
            MERGE (c2:Claimant {claimant_id: 'CLM_SA_02'})
            SET c2.name = 'Harry Rotter', c2.age = 36, c2.address = '123 Fake St'
            
            MERGE (c3:Claimant {claimant_id: 'CLM_SA_03'})
            SET c3.name = 'Mary Totter', c3.age = 29, c3.address = '123 Fake St'
            
            // Link Claimants
            MERGE (c1)-[:SHARES_ADDRESS]->(c2)
            MERGE (c2)-[:SHARES_ADDRESS]->(c3)
            
            // Claims
            MERGE (cl1:Claim {claim_id: 'CLM_ID_SA_01'})
            SET cl1.claim_amount = 15000, cl1.claim_date = '2025-01-15', cl1.is_fraud_ring = true, cl1.description = 'Staged Intersection Collision'
            
            MERGE (cl2:Claim {claim_id: 'CLM_ID_SA_02'})
            SET cl2.claim_amount = 18000, cl2.claim_date = '2025-01-15', cl2.is_fraud_ring = true, cl2.description = 'Staged Intersection Collision'
            
            MERGE (c1)-[:FILED_CLAIM]->(cl1)
            MERGE (c2)-[:FILED_CLAIM]->(cl2)
            
            MERGE (cl1)-[:REPRESENTED_BY]->(l)
            MERGE (cl2)-[:REPRESENTED_BY]->(l)
            
            // Mark potential fraud ring info
            SET c1.fraud_ring_id = 'RING_0' // Assigning to existing ring bucket for visibility
            SET c2.fraud_ring_id = 'RING_0'
        """)

        # 3. Scenario: "The Paper Accident" (Shops & Invoices only)
        print("- Creating 'Paper Accident' scenario...")
        session.run("""
            MERGE (s:RepairShop {shop_id: 'SHOP_SC_01'})
            SET s.name = 'Shady Customs'
            
            MERGE (c4:Claimant {claimant_id: 'CLM_PA_01'})
            SET c4.name = 'Invisible Man', c4.age = 45
            
            MERGE (cl3:Claim {claim_id: 'CLM_ID_PA_01'})
            SET cl3.claim_amount = 4500, cl3.claim_date = '2025-02-01', cl3.description = 'Bumper Repair Invoice', cl3.is_fraud_ring = true
            
            MERGE (c4)-[:FILED_CLAIM]->(cl3)
            MERGE (cl3)-[:REPAIRED_AT]->(s)
            
            SET c4.fraud_ring_id = 'RING_0'
        """)
        
        # 4. Scenario: "VIN Swapping Ring" (Shared Engine Number)
        print("- Creating 'VIN Swap' scenario...")
        session.run("""
            MERGE (v1:Vehicle {vehicle_id: 'VEH_VS_01'})
            SET v1.vin = 'VIN_123456789', v1.engine_no = 'ENG_99999', v1.make = 'Honda', v1.model = 'Accord'
            
            MERGE (v2:Vehicle {vehicle_id: 'VEH_VS_02'})
            SET v2.vin = 'VIN_987654321', v2.engine_no = 'ENG_99999', v2.make = 'Honda', v2.model = 'Civic'
            
            // Link to different policies/claimants to look "independent"
            MERGE (c_vs1:Claimant {claimant_id: 'CLM_VS_01'})
            SET c_vs1.name = 'Slick Rick'
            
            MERGE (c_vs2:Claimant {claimant_id: 'CLM_VS_02'})
            SET c_vs2.name = 'Fast Eddie'
            
            MERGE (cl_vs1:Claim {claim_id: 'CLM_ID_VS_01'})
            SET cl_vs1.claim_amount = 8000, cl_vs1.claim_date = '2025-02-10', cl_vs1.is_fraud_ring = true
            
            MERGE (cl_vs2:Claim {claim_id: 'CLM_ID_VS_02'})
            SET cl_vs2.claim_amount = 8000, cl_vs2.claim_date = '2025-02-12', cl_vs2.is_fraud_ring = true
            
            MERGE (c_vs1)-[:FILED_CLAIM]->(cl_vs1)
            MERGE (c_vs2)-[:FILED_CLAIM]->(cl_vs2)
            
            MERGE (cl_vs1)-[:INVOLVES_VEHICLE]->(v1)
            MERGE (cl_vs2)-[:INVOLVES_VEHICLE]->(v2)
            
            // Mark for ring detection
            SET c_vs1.fraud_ring_id = 'RING_VIN_SWAP'
            SET c_vs2.fraud_ring_id = 'RING_VIN_SWAP'
        """)
        
        # 5. Enhance Existing Vehicles with VINs (for visual richness)
        print("- Enhancing existing vehicles with VINs...")
        session.run("""
            MATCH (v:Vehicle)
            WHERE v.vin IS NULL
            WITH v, rand() as r
            SET v.vin = 'VIN_' + toString(toInteger(r * 1000000)),
                v.engine_no = 'ENG_' + toString(toInteger(r * 1000000))
        """)

        print("Seeding Complete.")

    driver.close()

if __name__ == "__main__":
    seed_demo_scenarios()
