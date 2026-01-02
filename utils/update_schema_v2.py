
import os
from neo4j import GraphDatabase
import ssl
from dotenv import load_dotenv
import random
from datetime import datetime, timedelta

# Load environment variables
load_dotenv()

def update_schema_v2():
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
        print("Updating Schema for V2...")

        # 1. Initialize Status and Disposition on Claims
        print("- Initializing Claim statuses...")
        session.run("""
            MATCH (c:Claim)
            WHERE c.status IS NULL
            SET c.status = 'New', 
                c.disposition = 'Pending',
                c.notes = []
        """)

        # 2. Simulate Date Data for Trend Analysis (if missing)
        # We'll distribute claim dates over the last 6 months
        print("- Simulating historical data for trends...")
        
        # specific query to set dates if they don't look like dates or are missing
        # For this demo, let's just ensure every claim has a valid ISO date
        # We will iterate and set random dates for visualization purposes
        result = session.run("MATCH (c:Claim) RETURN elementId(c) as id")
        ids = [rec['id'] for rec in result]
        
        end_date = datetime.now()
        start_date = end_date - timedelta(days=180)
        
        for eid in ids:
            random_days = random.randint(0, 180)
            fake_date = (start_date + timedelta(days=random_days)).isoformat()
            
            # Using elementId for AuraDB compatibility (or ID())
            session.run("""
                MATCH (c:Claim)
                WHERE elementId(c) = $id
                SET c.claim_date = $date
            """, id=eid, date=fake_date)

        print("Schema Update Complete.")

    driver.close()

if __name__ == "__main__":
    update_schema_v2()
