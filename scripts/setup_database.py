"""
Setup Database Script - Initialize Neo4j database for Auto Insurance Fraud Detection
Creates constraints, indexes, and verifies database structure
"""
import sys
import os
import argparse
from datetime import datetime

# Add parent directory to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from data.neo4j_driver import get_neo4j_driver
from utils.logger import setup_logger

logger = setup_logger(__name__)


def print_section_header(title: str):
    """Print formatted section header"""
    print("\n" + "=" * 80)
    print(f"  {title}")
    print("=" * 80)


def print_step(step_num: int, description: str):
    """Print formatted step"""
    print(f"\n{step_num}. {description}")
    print("-" * 80)


def verify_connection(driver) -> bool:
    """Verify database connection"""
    print_step(1, "Verifying Database Connection")
    
    if not driver.test_connection():
        print("   ✗ Could not connect to database")
        print("\n   Please check:")
        print("   - Neo4j is running")
        print("   - Connection credentials in .env file")
        print("   - NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD are set correctly")
        return False
    
    print("   ✓ Database connection successful")
    
    # Get database info
    db_info = driver.get_database_info()
    if db_info:
        print(f"   Database: {db_info.get('name', 'Unknown')}")
        print(f"   Version: {db_info.get('version', 'Unknown')}")
        print(f"   Edition: {db_info.get('edition', 'Unknown')}")
    
    return True


def clear_database(driver, force: bool = False):
    """Clear existing database"""
    print_step(2, "Clearing Existing Database")
    
    if not force:
        print("   ⚠️  WARNING: This will DELETE ALL DATA in the database!")
        print("   This action cannot be undone.")
        print()
        
        confirm = input("   Type 'DELETE ALL DATA' to confirm: ")
        
        if confirm != "DELETE ALL DATA":
            print("   ✗ Confirmation failed. Database not cleared.")
            return False
    
    try:
        # Get current counts before clearing
        stats = driver.get_statistics()
        total_nodes = sum([
            stats.get('claimants', 0),
            stats.get('claims', 0),
            stats.get('vehicles', 0),
            stats.get('body_shops', 0),
            stats.get('medical_providers', 0),
            stats.get('attorneys', 0),
            stats.get('tow_companies', 0),
            stats.get('accident_locations', 0),
            stats.get('witnesses', 0),
            stats.get('fraud_rings', 0)
        ])
        total_relationships = stats.get('total_relationships', 0)
        
        print(f"   Current database size:")
        print(f"   - Nodes: {total_nodes:,}")
        print(f"   - Relationships: {total_relationships:,}")
        print()
        
        # Clear database
        driver.clear_database(confirm=True)
        
        print("   ✓ Database cleared successfully")
        return True
        
    except Exception as e:
        print(f"   ✗ Failed to clear database: {str(e)}")
        logger.error(f"Database clear failed: {e}", exc_info=True)
        return False


def create_constraints(driver):
    """Create unique constraints"""
    print_step(3, "Creating Unique Constraints")
    
    try:
        print("   Creating constraints for entity uniqueness...")
        driver.create_constraints()
        print("   ✓ All constraints created successfully")
        
        # Verify constraints
        print("\n   Verifying constraints:")
        
        constraint_query = """
        SHOW CONSTRAINTS
        YIELD name, type, entityType, labelsOrTypes
        RETURN name, type, entityType, labelsOrTypes
        ORDER BY entityType, labelsOrTypes
        """
        
        try:
            results = driver.execute_query(constraint_query)
            
            if results:
                print(f"   ✓ {len(results)} constraints active")
                
                # Group by entity
                entity_constraints = {}
                for row in results:
                    entity = row.get('labelsOrTypes', ['Unknown'])[0] if row.get('labelsOrTypes') else 'Unknown'
                    if entity not in entity_constraints:
                        entity_constraints[entity] = []
                    entity_constraints[entity].append(row.get('name', 'Unknown'))
                
                # Display by entity
                for entity, constraints in sorted(entity_constraints.items()):
                    print(f"     {entity}: {len(constraints)} constraint(s)")
            else:
                print("   ⚠ Could not verify constraints")
        
        except Exception as e:
            print(f"   ⚠ Could not verify constraints: {str(e)}")
        
        return True
        
    except Exception as e:
        print(f"   ✗ Failed to create constraints: {str(e)}")
        logger.error(f"Constraint creation failed: {e}", exc_info=True)
        return False


def create_indexes(driver):
    """Create performance indexes"""
    print_step(4, "Creating Performance Indexes")
    
    try:
        print("   Creating indexes for query optimization...")
        driver.create_indexes()
        print("   ✓ All indexes created successfully")
        
        # Verify indexes
        print("\n   Verifying indexes:")
        
        index_query = """
        SHOW INDEXES
        YIELD name, type, entityType, labelsOrTypes, properties, state
        WHERE type = 'RANGE'
        RETURN name, entityType, labelsOrTypes, properties, state
        ORDER BY entityType, labelsOrTypes
        """
        
        try:
            results = driver.execute_query(index_query)
            
            if results:
                # Count by state
                online_count = sum(1 for r in results if r.get('state') == 'ONLINE')
                total_count = len(results)
                
                print(f"   ✓ {online_count}/{total_count} indexes online")
                
                # Group by entity
                entity_indexes = {}
                for row in results:
                    entity = row.get('labelsOrTypes', ['Unknown'])[0] if row.get('labelsOrTypes') else 'Unknown'
                    if entity not in entity_indexes:
                        entity_indexes[entity] = 0
                    entity_indexes[entity] += 1
                
                # Display by entity
                for entity, count in sorted(entity_indexes.items()):
                    print(f"     {entity}: {count} index(es)")
            else:
                print("   ⚠ Could not verify indexes")
        
        except Exception as e:
            print(f"   ⚠ Could not verify indexes: {str(e)}")
        
        return True
        
    except Exception as e:
        print(f"   ✗ Failed to create indexes: {str(e)}")
        logger.error(f"Index creation failed: {e}", exc_info=True)
        return False


def verify_setup(driver):
    """Verify database setup"""
    print_step(5, "Verifying Database Setup")
    
    try:
        stats = driver.get_statistics()
        
        print("   Current database state:")
        print()
        print("   📊 Node Counts:")
        print(f"      Claimants:          {stats.get('claimants', 0):>6,}")
        print(f"      Claims:             {stats.get('claims', 0):>6,}")
        print(f"      Vehicles:           {stats.get('vehicles', 0):>6,}")
        print(f"      Body Shops:         {stats.get('body_shops', 0):>6,}")
        print(f"      Medical Providers:  {stats.get('medical_providers', 0):>6,}")
        print(f"      Attorneys:          {stats.get('attorneys', 0):>6,}")
        print(f"      Tow Companies:      {stats.get('tow_companies', 0):>6,}")
        print(f"      Accident Locations: {stats.get('accident_locations', 0):>6,}")
        print(f"      Witnesses:          {stats.get('witnesses', 0):>6,}")
        print(f"      Fraud Rings:        {stats.get('fraud_rings', 0):>6,}")
        print()
        print("   🔗 Relationship Counts:")
        print(f"      Total:              {stats.get('total_relationships', 0):>6,}")
        print(f"      FILED:              {stats.get('filed_relationships', 0):>6,}")
        print(f"      MEMBER_OF:          {stats.get('member_of_relationships', 0):>6,}")
        print(f"      INVOLVES_VEHICLE:   {stats.get('involves_vehicle', 0):>6,}")
        print(f"      REPAIRED_AT:        {stats.get('repaired_at', 0):>6,}")
        print(f"      TREATED_BY:         {stats.get('treated_by', 0):>6,}")
        print(f"      REPRESENTED_BY:     {stats.get('represented_by', 0):>6,}")
        print(f"      TOWED_BY:           {stats.get('towed_by', 0):>6,}")
        print(f"      WITNESSED:          {stats.get('witnessed', 0):>6,}")
        print(f"      OCCURRED_AT:        {stats.get('occurred_at', 0):>6,}")
        print()
        
        # Check if database is empty
        total_nodes = sum([
            stats.get('claimants', 0),
            stats.get('claims', 0),
            stats.get('vehicles', 0),
            stats.get('body_shops', 0),
            stats.get('medical_providers', 0),
            stats.get('attorneys', 0),
            stats.get('tow_companies', 0),
            stats.get('accident_locations', 0),
            stats.get('witnesses', 0),
            stats.get('fraud_rings', 0)
        ])
        
        if total_nodes == 0:
            print("   ℹ️  Database is empty (ready for data load)")
            print()
            print("   Next step: Load sample data using:")
            print("   python scripts/load_sample_data.py")
        else:
            print("   ✓ Database contains data")
        
        return True
        
    except Exception as e:
        print(f"   ✗ Failed to verify setup: {str(e)}")
        logger.error(f"Verification failed: {e}", exc_info=True)
        return False


def display_summary(success: bool, clear_performed: bool):
    """Display setup summary"""
    print_section_header("SETUP SUMMARY")
    
    if success:
        print("\n✅ Database setup completed successfully!")
        print()
        print("Setup performed:")
        if clear_performed:
            print("  ✓ Database cleared")
        print("  ✓ Constraints created")
        print("  ✓ Indexes created")
        print("  ✓ Setup verified")
        print()
        print("Your Auto Insurance Fraud Detection database is ready!")
        print()
        print("Next steps:")
        print("  1. Load sample data:")
        print("     python scripts/load_sample_data.py")
        print()
        print("  2. Start the application:")
        print("     streamlit run app.py")
        print()
    else:
        print("\n❌ Database setup failed!")
        print()
        print("Please check the error messages above and:")
        print("  - Ensure Neo4j is running")
        print("  - Verify connection credentials")
        print("  - Check Neo4j logs for errors")
        print()


def setup_database(clear_existing: bool = False, force_clear: bool = False):
    """
    Main setup function for Auto Insurance Fraud Detection database
    
    Args:
        clear_existing: If True, clear all existing data before setup
        force_clear: If True, skip confirmation prompt for clearing
    
    Returns:
        True if setup successful, False otherwise
    """
    
    print_section_header("AUTO INSURANCE FRAUD DETECTION - DATABASE SETUP")
    print(f"\nStarted: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    success = True
    clear_performed = False
    
    try:
        # Get driver instance
        driver = get_neo4j_driver()
        
        # Step 1: Verify connection
        if not verify_connection(driver):
            return False
        
        # Step 2: Clear database if requested
        if clear_existing:
            if not clear_database(driver, force=force_clear):
                return False
            clear_performed = True
        
        # Step 3: Create constraints
        step_num = 3 if clear_existing else 2
        if not create_constraints(driver):
            success = False
        
        # Step 4: Create indexes
        step_num += 1
        if not create_indexes(driver):
            success = False
        
        # Step 5: Verify setup
        step_num += 1
        if not verify_setup(driver):
            success = False
        
        # Display summary
        display_summary(success, clear_performed)
        
        return success
        
    except KeyboardInterrupt:
        print("\n\n⚠️  Setup interrupted by user")
        return False
    
    except Exception as e:
        print(f"\n\n❌ Setup failed with error: {str(e)}")
        logger.error(f"Database setup failed: {e}", exc_info=True)
        return False


def main():
    """Main function with command-line argument handling"""
    
    parser = argparse.ArgumentParser(
        description="Setup Neo4j database for Auto Insurance Fraud Detection",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Basic setup (create constraints and indexes)
  python scripts/setup_database.py

  # Clear database and setup
  python scripts/setup_database.py --clear

  # Force clear without confirmation prompt
  python scripts/setup_database.py --clear --force

  # Show this help message
  python scripts/setup_database.py --help
        """
    )
    
    parser.add_argument(
        '--clear',
        action='store_true',
        help='Clear all existing data before setup (DANGEROUS!)'
    )
    
    parser.add_argument(
        '--force',
        action='store_true',
        help='Force clear without confirmation prompt (use with --clear)'
    )
    
    args = parser.parse_args()
    
    # Validate arguments
    if args.force and not args.clear:
        print("❌ Error: --force can only be used with --clear")
        print()
        parser.print_help()
        sys.exit(1)
    
    # Run setup
    success = setup_database(
        clear_existing=args.clear,
        force_clear=args.force
    )
    
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
