"""
Test Neo4j connection with detailed debugging
"""
from neo4j import GraphDatabase
import os
from dotenv import load_dotenv
import ssl

load_dotenv()

uri = os.getenv('NEO4J_URI')
username = os.getenv('NEO4J_USERNAME')
password = os.getenv('NEO4J_PASSWORD')

print("=" * 60)
print("NEO4J CONNECTION TEST")
print("=" * 60)
print(f"\nURI: {uri}")
print(f"Username: {username}")
print(f"Password length: {len(password)} chars")

# Convert to standard scheme
working_uri = uri.replace("neo4j+s://", "neo4j://")

# Create SSL context
ssl_context = ssl.create_default_context()
ssl_context.check_hostname = False
ssl_context.verify_mode = ssl.CERT_NONE

print(f"\nConverted URI: {working_uri}")
print("Attempting connection...\n")

try:
    driver = GraphDatabase.driver(
        working_uri, 
        auth=(username, password),
        encrypted=True,
        ssl_context=ssl_context
    )
    
    print("[SUCCESS] Driver created!")
    
    # Test actual query
    with driver.session() as session:
        result = session.run("RETURN 1 as num")
        record = result.single()
        print(f"[SUCCESS] Query executed: {record['num']}")
        
    driver.close()
    print("[SUCCESS] Connection test passed!")
    
except Exception as e:
    print(f"[ERROR] Connection failed!")
    print(f"Error type: {type(e).__name__}")
    print(f"Error message: {e}")
    
    # Try to get more details
    if hasattr(e, 'code'):
        print(f"Error code: {e.code}")
    if hasattr(e, 'message'):
        print(f"Detailed message: {e.message}")

print("\n" + "=" * 60)
