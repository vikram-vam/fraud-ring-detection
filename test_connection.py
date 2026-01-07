"""Test Neo4j AuraDB connection"""
import warnings
warnings.filterwarnings("ignore")

from data.neo4j_driver import get_neo4j_driver

def test_connection():
    try:
        driver = get_neo4j_driver()
        print("✓ Successfully connected to Neo4j AuraDB!")
        
        result = driver.execute_query("RETURN 'Hello from AuraDB!' as message")
        if result:
            print(f"✓ Test query successful: {result[0]['message']}")
            return True
        
    except Exception as e:
        print(f"✗ Connection failed: {e}")
        return False

if __name__ == "__main__":
    import sys
    success = test_connection()
    # Clean exit
    sys.exit(0 if success else 1)
