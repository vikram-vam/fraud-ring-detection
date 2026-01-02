
import os
from neo4j import GraphDatabase
import ssl
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def get_db_schema():
    uri = os.getenv('NEO4J_URI')
    username = os.getenv('NEO4J_USERNAME')
    password = os.getenv('NEO4J_PASSWORD')

    # Configure SSL context
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
        print("\n--- NODES (Entities) ---")
        result = session.run("CALL db.labels()")
        labels = [rec['label'] for rec in result]
        for label in labels:
            count = session.run(f"MATCH (n:{label}) RETURN count(n) as c").single()['c']
            print(f"- {label} (Count: {count})")

        print("\n--- RELATIONSHIPS ---")
        result = session.run("CALL db.relationshipTypes()")
        rels = [rec['relationshipType'] for rec in result]
        for rel in rels:
            count = session.run(f"MATCH ()-[r:{rel}]->() RETURN count(r) as c").single()['c']
            print(f"- {rel} (Count: {count})")

        print("\n--- CONNECTION PATTERNS (StartNode -> Relationship -> EndNode) ---")
        result = session.run("""
            MATCH (a)-[r]->(b)
            RETURN distinct labels(a) as start_labels, type(r) as rel_type, labels(b) as end_labels
            ORDER BY rel_type
        """)
        for rec in result:
            start = rec['start_labels'][0] if rec['start_labels'] else 'Unknown'
            end = rec['end_labels'][0] if rec['end_labels'] else 'Unknown'
            print(f"- {start} --[{rec['rel_type']}]--> {end}")

    driver.close()

if __name__ == "__main__":
    get_db_schema()
