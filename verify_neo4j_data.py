"""
VERIFY NEO4J DATA
Query the database directly to confirm data was loaded successfully
"""

from neo4j import GraphDatabase
import os
from dotenv import load_dotenv
import pandas as pd
import ssl

load_dotenv()

def connect_to_neo4j():
    """Connect to Neo4j with error handling"""
    uri = os.getenv('NEO4J_URI')
    username = os.getenv('NEO4J_USERNAME')
    password = os.getenv('NEO4J_PASSWORD')
    
    print("="*70)
    print("CONNECTING TO NEO4J")
    print("="*70)
    print(f"URI: {uri}")
    
    try:
        # Configure SSL context to handle self-signed certificates
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE
        
        # Handle different URI schemes
        if uri.startswith("neo4j+s://"):
            working_uri = uri.replace("neo4j+s://", "neo4j://")
            print("Converted URI to neo4j:// to allow custom SSL configuration")
        elif uri.startswith("bolt+s://"):
            working_uri = uri.replace("bolt+s://", "bolt://")
            print("Converted URI to bolt:// to allow custom SSL configuration")
        else:
            working_uri = uri

        driver = GraphDatabase.driver(
            working_uri, 
            auth=(username, password),
            encrypted=True,
            ssl_context=ssl_context
        )
        
        # Test connection
        with driver.session() as session:
            result = session.run("RETURN 1 as test")
            record = result.single()
            if record['test'] == 1:
                print("[OK] Connection successful!")
        
        return driver
        
    except Exception as e:
        print(f"[ERROR] Connection failed: {e}")
        return None

def verify_database(driver):
    """Comprehensive database verification"""
    
    if not driver:
        print("❌ No database connection available")
        return
    
    with driver.session() as session:
        
        # 1. Total Node Count
        print("\n" + "="*70)
        print("1. TOTAL NODE COUNT")
        print("="*70)
        
        result = session.run("MATCH (n) RETURN count(n) as total")
        total_nodes = result.single()['total']
        print(f"Total nodes in database: {total_nodes:,}")
        
        if total_nodes == 0:
            print("\n[WARNING] Database is EMPTY!")
            print("   The data loading may have failed silently.")
            print("   Run neo4j_loader_verbose.py to reload data with detailed logging.")
            return
        
        # 2. Nodes by Label
        print("\n" + "="*70)
        print("2. NODES BY LABEL")
        print("="*70)
        
        result = session.run("""
            CALL db.labels() YIELD label
            CALL (label) {
                MATCH (n)
                WHERE label IN labels(n)
                RETURN count(n) as count
            }
            RETURN label, count
            ORDER BY count DESC
        """)
        
        for record in result:
            print(f"  {record['label']:20s}: {record['count']:6,d} nodes")
        
        # 3. Total Relationship Count
        print("\n" + "="*70)
        print("3. TOTAL RELATIONSHIP COUNT")
        print("="*70)
        
        result = session.run("MATCH ()-[r]->() RETURN count(r) as total")
        total_rels = result.single()['total']
        print(f"Total relationships in database: {total_rels:,}")
        
        # 4. Relationships by Type
        print("\n" + "="*70)
        print("4. RELATIONSHIPS BY TYPE")
        print("="*70)
        
        result = session.run("""
            CALL db.relationshipTypes() YIELD relationshipType
            CALL (relationshipType) {
                MATCH ()-[r]->()
                WHERE type(r) = relationshipType
                RETURN count(r) as count
            }
            RETURN relationshipType, count
            ORDER BY count DESC
        """)
        
        for record in result:
            print(f"  {record['relationshipType']:20s}: {record['count']:6,d} relationships")
        
        # 5. Fraud Ring Statistics
        print("\n" + "="*70)
        print("5. FRAUD RING STATISTICS")
        print("="*70)
        
        result = session.run("""
            MATCH (c:Claimant)
            WHERE c.is_fraud_ring = true
            RETURN count(DISTINCT c.fraud_ring_id) as num_rings,
                   count(c) as total_members
        """)
        
        record = result.single()
        print(f"Number of fraud rings: {record['num_rings']}")
        print(f"Total fraud ring members: {record['total_members']}")
        
        # 6. Fraud Ring Details
        print("\n" + "="*70)
        print("6. FRAUD RING DETAILS")
        print("="*70)
        
        result = session.run("""
            MATCH (c:Claimant)
            WHERE c.is_fraud_ring = true
            RETURN c.fraud_ring_id as ring, count(c) as members
            ORDER BY ring
        """)
        
        rings_found = False
        for record in result:
            rings_found = True
            print(f"  {record['ring']:15s}: {record['members']:3d} members")
        
        if not rings_found:
            print("  [WARNING] No fraud rings found!")
        
        # 7. Sample Claimants
        print("\n" + "="*70)
        print("7. SAMPLE CLAIMANTS (First 5)")
        print("="*70)
        
        result = session.run("""
            MATCH (c:Claimant)
            RETURN c.claimant_id as id, c.name as name, c.is_fraud_ring as fraud
            LIMIT 5
        """)
        
        for record in result:
            fraud_emoji = "[FRAUD]" if record['fraud'] else "[OK]"
            print(f"  {fraud_emoji} {record['name']:30s} ({record['id']})")
        
        # 8. Sample Claims
        print("\n" + "="*70)
        print("8. SAMPLE CLAIMS (First 5)")
        print("="*70)
        
        result = session.run("""
            MATCH (c:Claimant)-[:FILED_CLAIM]->(cl:Claim)
            RETURN cl.claim_id as id, 
                   c.name as claimant, 
                   cl.claim_amount as amount,
                   cl.claim_type as type
            LIMIT 5
        """)
        
        for record in result:
            print(f"  {record['id']:15s} | {record['claimant']:25s} | ${record['amount']:>10,.2f} | {record['type']}")
        
        # 9. Sample Graph Connections
        print("\n" + "="*70)
        print("9. SAMPLE GRAPH CONNECTIONS")
        print("="*70)
        
        result = session.run("""
            MATCH (c:Claimant)-[:FILED_CLAIM]->(cl:Claim)-[:REPAIRED_AT]->(s:RepairShop)
            RETURN c.name as claimant, cl.claim_amount as amount, s.name as shop
            LIMIT 3
        """)
        
        print("Sample claim chain: Claimant -> Claim -> Repair Shop")
        for record in result:
            print(f"  {record['claimant']:25s} -> ${record['amount']:>8,.0f} -> {record['shop']}")
        
        # 10. Fraud Indicators
        print("\n" + "="*70)
        print("10. FRAUD INDICATORS")
        print("="*70)
        
        # Shared addresses
        result = session.run("""
            MATCH (c1:Claimant)-[:SHARES_ADDRESS]-(c2:Claimant)
            RETURN count(DISTINCT c1) as count
        """)
        shared_address = result.single()['count']
        print(f"  Claimants sharing addresses: {shared_address}")
        
        # Shared phones
        result = session.run("""
            MATCH (c1:Claimant)-[:SHARES_PHONE]-(c2:Claimant)
            RETURN count(DISTINCT c1) as count
        """)
        shared_phone = result.single()['count']
        print(f"  Claimants sharing phones: {shared_phone}")
        
        # High-volume repair shops
        result = session.run("""
            MATCH (cl:Claim)-[:REPAIRED_AT]->(s:RepairShop)
            WITH s, count(cl) as claim_count
            WHERE claim_count >= 5
            RETURN count(s) as shops, max(claim_count) as max_claims
        """)
        record = result.single()
        print(f"  Repair shops with 5+ claims: {record['shops']}")
        print(f"  Max claims at single shop: {record['max_claims']}")
        
        # 11. Test a Complex Query
        print("\n" + "="*70)
        print("11. COMPLEX QUERY TEST (Fraud Ring Network)")
        print("="*70)
        
        result = session.run("""
            MATCH (c:Claimant {fraud_ring_id: 'RING_0'})
            OPTIONAL MATCH (c)-[:FILED_CLAIM]->(cl:Claim)
            OPTIONAL MATCH (c)-[r:RELATED_TO|SHARES_ADDRESS|SHARES_PHONE]-(other:Claimant)
            WHERE other.fraud_ring_id = 'RING_0'
            RETURN 
                count(DISTINCT c) as members,
                count(DISTINCT cl) as claims,
                count(DISTINCT r) as connections
        """)
        
        record = result.single()
        if record['members'] > 0:
            print(f"  [OK] RING_0 Analysis:")
            print(f"     Members: {record['members']}")
            print(f"     Claims: {record['claims']}")
            print(f"     Internal connections: {record['connections']}")
        else:
            print("  [WARNING] RING_0 not found or has no data")
        
        # 12. Database Health Check
        print("\n" + "="*70)
        print("12. DATABASE HEALTH CHECK")
        print("="*70)
        
        # Check for orphaned nodes
        result = session.run("""
            MATCH (n)
            WHERE NOT (n)--()
            RETURN count(n) as orphans
        """)
        orphans = result.single()['orphans']
        
        if orphans > 0:
            print(f"  [WARNING] Found {orphans} orphaned nodes (nodes with no relationships)")
        else:
            print(f"  [OK] No orphaned nodes found")
        
        # Check for expected node types
        expected_labels = ['Claimant', 'Policy', 'Vehicle', 'Claim', 'RepairShop', 'MedicalProvider', 'Lawyer', 'Witness']
        result = session.run("CALL db.labels() YIELD label RETURN collect(label) as labels")
        actual_labels = result.single()['labels']
        
        missing_labels = set(expected_labels) - set(actual_labels)
        if missing_labels:
            print(f"  [WARNING] Missing expected node types: {missing_labels}")
        else:
            print(f"  [OK] All expected node types present")
        
        # Summary
        print("\n" + "="*70)
        print("VERIFICATION SUMMARY")
        print("="*70)
        
        if total_nodes > 0 and total_rels > 0:
            print("[OK] Database is populated with data")
            print(f"[OK] {total_nodes:,} nodes and {total_rels:,} relationships found")
            print("[OK] Data structure looks correct")
            print("\n[INFO] You can now:")
            print("   1. Open Neo4j Browser and run queries")
            print("   2. Run: streamlit run app.py")
            print("   3. Start exploring fraud rings!")
        else:
            print("[ERROR] Database appears to be empty or incomplete")
            print("[WARNING] Please run: python neo4j_loader_verbose.py")
        
        print("="*70 + "\n")

def main():
    """Main verification function"""
    driver = connect_to_neo4j()
    
    if driver:
        verify_database(driver)
        driver.close()
        print("[OK] Verification complete. Connection closed.\n")
    else:
        print("[ERROR] Could not establish database connection\n")

if __name__ == "__main__":
    main()