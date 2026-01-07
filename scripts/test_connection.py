"""
Test Connection Script - Verify Neo4j database connectivity
"""
import sys
import os

# Add parent directory to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from data.neo4j_driver import get_neo4j_driver
from utils.logger import setup_logger

logger = setup_logger(__name__)


def test_connection():
    """Test Neo4j database connection"""
    
    print("=" * 60)
    print("Neo4j Connection Test")
    print("=" * 60)
    print()
    
    try:
        # Get driver instance
        print("1. Initializing Neo4j driver...")
        driver = get_neo4j_driver()
        print("   ✓ Driver initialized")
        print()
        
        # Test connectivity
        print("2. Testing connectivity...")
        if driver.test_connection():
            print("   ✓ Connection successful!")
        else:
            print("   ✗ Connection failed!")
            return False
        print()
        
        # Get database info
        print("3. Retrieving database information...")
        db_info = driver.get_database_info()
        
        if db_info:
            print(f"   Database: {db_info.get('name', 'Unknown')}")
            print(f"   Version: {db_info.get('version', 'Unknown')}")
            print(f"   Edition: {db_info.get('edition', 'Unknown')}")
        else:
            print("   Could not retrieve database info")
        print()
        
        # Get statistics
        print("4. Retrieving database statistics...")
        stats = driver.get_statistics()
        
        if stats:
            print(f"   Claimants: {stats.get('claimants', 0)}")
            print(f"   Claims: {stats.get('claims', 0)}")
            print(f"   Providers: {stats.get('providers', 0)}")
            print(f"   Attorneys: {stats.get('attorneys', 0)}")
            print(f"   Addresses: {stats.get('addresses', 0)}")
            print(f"   Fraud Rings: {stats.get('fraud_rings', 0)}")
            print(f"   Total Relationships: {stats.get('total_relationships', 0)}")
        else:
            print("   Could not retrieve statistics")
        print()
        
        print("=" * 60)
        print("✓ All tests passed!")
        print("=" * 60)
        
        return True
        
    except Exception as e:
        print()
        print("=" * 60)
        print(f"✗ Connection test failed: {str(e)}")
        print("=" * 60)
        logger.error(f"Connection test failed: {e}", exc_info=True)
        return False


if __name__ == "__main__":
    success = test_connection()
    sys.exit(0 if success else 1)
