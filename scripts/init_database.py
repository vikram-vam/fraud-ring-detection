"""
Database Initialization Script
Creates all necessary indexes, constraints, and verifies database setup
"""
import sys
import time
from pathlib import Path
import os

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data.neo4j_driver import get_neo4j_driver
from utils.config import config
from utils.logger import setup_logger, configure_app_logging

logger = setup_logger(__name__)


def create_constraints(driver):
    """Create all unique constraints"""
    constraints = [
        "claim_id_unique",
        "claimant_id_unique",
        "vehicle_id_unique",
        "body_shop_id_unique",
        "provider_id_unique",
        "attorney_id_unique",
        "tow_company_id_unique",
        "location_id_unique",
        "witness_id_unique",
        "ring_id_unique",
        "alert_id_unique",
        "case_id_unique"
    ]
    
    created_constraints = 0
    
    for constraint_name in constraints:
        try:
            query = f"""
            CREATE CONSTRAINT {constraint_name} IF NOT EXISTS
            FOR (n:{get_label_from_constraint(constraint_name)}) REQUIRE n.{get_id_field_from_constraint(constraint_name)} IS UNIQUE
            """
            
            driver.execute_write(query)
            logger.info(f"✓ Created constraint: {constraint_name}")
            created_constraints += 1
            
        except Exception as e:
            logger.warning(f"Constraint {constraint_name} already exists or failed: {e}")
    
    return created_constraints


def create_indexes(driver):
    """Create performance indexes"""
    indexes = [
        "claim_risk_score",
        "claim_status",
        "claim_accident_date",
        "claim_amount",
        "claimant_email",
        "vehicle_vin",
        "alert_status",
        "alert_severity",
        "case_status",
        "fraud_ring_type"
    ]
    
    created_indexes = 0
    
    for index_name in indexes:
        try:
            query = get_index_query(index_name)
            driver.execute_write(query)
            logger.info(f"✓ Created index: {index_name}")
            created_indexes += 1
            
        except Exception as e:
            logger.warning(f"Index {index_name} already exists or failed: {e}")
    
    return created_indexes


def create_fulltext_indexes(driver):
    """Create full-text search indexes"""
    ft_indexes = [
        ("claimant_search", ["Claimant"], ["name", "email"]),
        ("body_shop_search", ["BodyShop"], ["name", "city"]),
        ("medical_provider_search", ["MedicalProvider"], ["name", "provider_type"])
    ]
    
    created_ft_indexes = 0
    
    for index_name, labels, properties in ft_indexes:
        try:
            query = f"""
            CALL db.index.fulltext.createNodeIndex('{index_name}', {labels}, {properties})
            """
            driver.execute_write(query)
            logger.info(f"✓ Created full-text index: {index_name}")
            created_ft_indexes += 1
            
        except Exception as e:
            logger.warning(f"Full-text index {index_name} already exists or failed: {e}")
    
    return created_ft_indexes


def verify_database_setup(driver):
    """Verify database constraints and indexes"""
    logger.info("\n🔍 Verifying database setup...")
    
    # Check constraints
    constraints_result = driver.execute_query("SHOW CONSTRAINTS")
    constraint_count = len(constraints_result)
    
    # Check indexes
    indexes_result = driver.execute_query("SHOW INDEXES")
    index_count = len(indexes_result)
    
    logger.info(f"📊 Found {constraint_count} constraints")
    logger.info(f"📊 Found {index_count} indexes")
    
    return constraint_count, index_count


def get_label_from_constraint(constraint_name):
    """Map constraint name to node label"""
    mapping = {
        "claim_id_unique": "Claim",
        "claimant_id_unique": "Claimant",
        "vehicle_id_unique": "Vehicle",
        "body_shop_id_unique": "BodyShop",
        "provider_id_unique": "MedicalProvider",
        "attorney_id_unique": "Attorney",
        "tow_company_id_unique": "TowCompany",
        "location_id_unique": "AccidentLocation",
        "witness_id_unique": "Witness",
        "ring_id_unique": "FraudRing",
        "alert_id_unique": "Alert",
        "case_id_unique": "Case"
    }
    return mapping.get(constraint_name, "Entity")


def get_id_field_from_constraint(constraint_name):
    """Map constraint name to ID field"""
    mapping = {
        "claim_id_unique": "claim_id",
        "claimant_id_unique": "claimant_id",
        "vehicle_id_unique": "vehicle_id",
        "body_shop_id_unique": "body_shop_id",
        "provider_id_unique": "provider_id",
        "attorney_id_unique": "attorney_id",
        "tow_company_id_unique": "tow_company_id",
        "location_id_unique": "location_id",
        "witness_id_unique": "witness_id",
        "ring_id_unique": "ring_id",
        "alert_id_unique": "alert_id",
        "case_id_unique": "case_id"
    }
    return mapping.get(constraint_name, "id")


def get_index_query(index_name):
    """Generate index creation query"""
    queries = {
        "claim_risk_score": "CREATE INDEX claim_risk_score IF NOT EXISTS FOR (c:Claim) ON (c.risk_score)",
        "claim_status": "CREATE INDEX claim_status IF NOT EXISTS FOR (c:Claim) ON (c.status)",
        "claim_accident_date": "CREATE INDEX claim_accident_date IF NOT EXISTS FOR (c:Claim) ON (c.accident_date)",
        "claim_amount": "CREATE INDEX claim_amount IF NOT EXISTS FOR (c:Claim) ON (c.total_claim_amount)",
        "claimant_email": "CREATE INDEX claimant_email IF NOT EXISTS FOR (c:Claimant) ON (c.email)",
        "vehicle_vin": "CREATE INDEX vehicle_vin IF NOT EXISTS FOR (v:Vehicle) ON (v.vin)",
        "alert_status": "CREATE INDEX alert_status IF NOT EXISTS FOR (a:Alert) ON (a.status)",
        "alert_severity": "CREATE INDEX alert_severity IF NOT EXISTS FOR (a:Alert) ON (a.severity)",
        "case_status": "CREATE INDEX case_status IF NOT EXISTS FOR (c:Case) ON (c.status)",
        "fraud_ring_type": "CREATE INDEX fraud_ring_type IF NOT EXISTS FOR (r:FraudRing) ON (r.ring_type)"
    }
    return queries.get(index_name, f"CREATE INDEX {index_name} IF NOT EXISTS")


def main():
    """Main initialization function"""
    print("\n" + "="*80)
    print("🚀 Neo4j Database Initialization")
    print("="*80)
    
    # Configure logging
    configure_app_logging('INFO')
    
    try:
        # Get driver
        driver = get_neo4j_driver()
        logger.info("Connected to Neo4j database")
        
        print("\n📋 Step 1: Creating constraints...")
        constraint_count = create_constraints(driver)
        print(f"✅ Created/verified {constraint_count} constraints")
        
        print("\n📋 Step 2: Creating indexes...")
        index_count = create_indexes(driver)
        print(f"✅ Created/verified {index_count} indexes")
        
        print("\n📋 Step 3: Creating full-text indexes...")
        ft_index_count = create_fulltext_indexes(driver)
        print(f"✅ Created/verified {ft_index_count} full-text indexes")
        
        print("\n📋 Step 4: Verifying setup...")
        constraints_found, indexes_found = verify_database_setup(driver)
        print(f"✅ Verification complete: {constraints_found} constraints, {indexes_found} indexes")
        
        print("\n🎉 Database initialization completed successfully!")
        print("="*80)
        
        # Close driver
        driver.close()
        
    except Exception as e:
        logger.error(f"Database initialization failed: {e}", exc_info=True)
        print(f"\n❌ Database initialization failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
