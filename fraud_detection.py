"""
FRAUD RING DETECTION ALGORITHMS
Implements multiple algorithms for detecting insurance fraud rings
"""

from neo4j import GraphDatabase
import networkx as nx
import pandas as pd
import numpy as np
from typing import List, Dict, Tuple
from collections import defaultdict
import logging
import ssl
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class FraudDetector:
    def __init__(self, driver):
        """Initialize fraud detector with Neo4j driver"""
        self.driver = driver
    
    def get_subgraph_for_claimant(self, claimant_id: str, depth: int = 2) -> Dict:
        """Extract subgraph around a claimant"""
        with self.driver.session() as session:
            result = session.run("""
                MATCH path = (c:Claimant {claimant_id: $claimant_id})-[*1..$depth]-(connected)
                WITH c, collect(distinct connected) as nodes, collect(distinct relationships(path)) as rels
                UNWIND nodes as node
                UNWIND rels as relList
                UNWIND relList as rel
                RETURN 
                    c,
                    collect(distinct node) as connected_nodes,
                    collect(distinct {
                        type: type(rel),
                        start: startNode(rel),
                        end: endNode(rel)
                    }) as relationships
            """, claimant_id=claimant_id, depth=depth)
            
            record = result.single()
            if record:
                return {
                    'center_claimant': dict(record['c']),
                    'connected_nodes': [dict(node) for node in record['connected_nodes']],
                    'relationships': record['relationships']
                }
            return None
    
    def calculate_louvain_communities(self) -> Dict[str, int]:
        """
        Use Louvain algorithm to detect communities (potential fraud rings)
        Returns mapping of claimant_id to community_id
        """
        logger.info("Running Louvain community detection...")
        
        with self.driver.session() as session:
            # Get all claimants and their connections
            result = session.run("""
                MATCH (c1:Claimant)-[r]-(c2:Claimant)
                WHERE type(r) IN ['RELATED_TO', 'SHARES_ADDRESS', 'SHARES_PHONE']
                RETURN c1.claimant_id as source, c2.claimant_id as target, type(r) as rel_type
            """)
            
            # Build NetworkX graph
            G = nx.Graph()
            for record in result:
                weight = 2.0 if record['rel_type'] == 'RELATED_TO' else 1.5
                G.add_edge(record['source'], record['target'], weight=weight)
            
            # Run Louvain
            if len(G.nodes()) > 0:
                communities = nx.community.louvain_communities(G, weight='weight', seed=42)
                
                # Create mapping
                community_map = {}
                for idx, community in enumerate(communities):
                    for claimant_id in community:
                        community_map[claimant_id] = idx
                
                logger.info(f"Found {len(communities)} communities")
                return community_map
            
            return {}
    
    def detect_fraud_rings_by_patterns(self) -> List[Dict]:
        """
        Detect fraud rings based on suspicious patterns:
        - Shared repair shops
        - Shared medical providers
        - Shared lawyers
        - Shared addresses/phones
        - Claim timing patterns
        """
        logger.info("Detecting fraud rings by patterns...")
        
        fraud_rings = []
        
        with self.driver.session() as session:
            # Pattern 1: Shared service provider clusters
            result = session.run("""
                // Find claimants using same repair shop
                MATCH (c:Claimant)-[:FILED_CLAIM]->(cl:Claim)-[:REPAIRED_AT]->(s:RepairShop)
                WITH s, collect(distinct c) as claimants, count(distinct cl) as claim_count
                WHERE claim_count >= 5
                
                // Find connections among these claimants
                UNWIND claimants as c1
                UNWIND claimants as c2
                WITH s, claimants, claim_count, c1, c2
                WHERE c1.claimant_id < c2.claimant_id
                MATCH path = (c1)-[r:RELATED_TO|SHARES_ADDRESS|SHARES_PHONE]-(c2)
                
                WITH s, claimants, claim_count, count(distinct path) as connections
                WHERE connections >= 3
                
                RETURN 
                    'shared_repair_shop' as pattern_type,
                    s.shop_id as entity_id,
                    s.name as entity_name,
                    [c in claimants | c.claimant_id] as claimant_ids,
                    claim_count,
                    connections,
                    claim_count * 0.3 + connections * 0.5 as suspicion_score
                ORDER BY suspicion_score DESC
                LIMIT 10
            """)
            
            for record in result:
                # Raw Score Calculation
                # Base risk: claim count (volume)
                # Multiplier: Connection to known ring members
                # Ratio: Claims per Claimant (High ratio = Suspicious)
                
                claim_count = record['claim_count']
                connections = record['connections']
                unique_claimants = len(record['claimant_ids'])
                
                # Ratio of Claims to People (e.g., 20 claims from 2 people is BAD)
                claims_per_person = claim_count / max(1, unique_claimants)
                
                raw_score = (claims_per_person * 2.0) + (connections * 1.5)
                
                # Sigmoid Normalization (0-10)
                # Midpoint approx 10 (score 5), Steepness 0.3
                import math
                normalized_score = 10 / (1 + math.exp(-0.3 * (raw_score - 8)))
                
                fraud_rings.append({
                    'pattern_type': record['pattern_type'],
                    'entity_id': record['entity_id'],
                    'entity_name': record['entity_name'],
                    'claimant_ids': record['claimant_ids'],
                    'claim_count': claim_count,
                    'connections': connections,
                    'suspicion_score': round(normalized_score, 1) # 1 decimal place
                })
            
            # Pattern 2: Shared medical provider clusters
            result = session.run("""
                MATCH (c:Claimant)-[:FILED_CLAIM]->(cl:Claim)-[:TREATED_BY]->(m:MedicalProvider)
                WITH m, collect(distinct c) as claimants, count(distinct cl) as claim_count
                WHERE claim_count >= 4
                
                UNWIND claimants as c1
                UNWIND claimants as c2
                WITH m, claimants, claim_count, c1, c2
                WHERE c1.claimant_id < c2.claimant_id
                MATCH path = (c1)-[r:RELATED_TO|SHARES_ADDRESS|SHARES_PHONE]-(c2)
                
                WITH m, claimants, claim_count, count(distinct path) as connections
                WHERE connections >= 2
                
                RETURN 
                    'shared_medical_provider' as pattern_type,
                    m.provider_id as entity_id,
                    m.name as entity_name,
                    [c in claimants | c.claimant_id] as claimant_ids,
                    claim_count,
                    connections,
                    claim_count * 0.4 + connections * 0.6 as suspicion_score
                ORDER BY suspicion_score DESC
                LIMIT 10
            """)
            
            for record in result:
                claim_count = record['claim_count']
                connections = record['connections']
                unique_claimants = len(record['claimant_ids'])
                
                # Medical often has higher volume, so we tolerate slightly higher ratios
                claims_per_person = claim_count / max(1, unique_claimants)
                
                raw_score = (claims_per_person * 1.5) + (connections * 2.0)
                
                # Sigmoid Normalization
                import math
                normalized_score = 10 / (1 + math.exp(-0.3 * (raw_score - 10)))
                
                fraud_rings.append({
                    'pattern_type': record['pattern_type'],
                    'entity_id': record['entity_id'],
                    'entity_name': record['entity_name'],
                    'claimant_ids': record['claimant_ids'],
                    'claim_count': claim_count,
                    'connections': connections,
                    'suspicion_score': round(normalized_score, 1)
                })
            
            # Pattern 3: Recurring witness patterns
            result = session.run("""
                MATCH (w:Witness)<-[:HAS_WITNESS]-(cl:Claim)<-[:FILED_CLAIM]-(c:Claimant)
                WHERE w.is_recurring = true
                WITH w, collect(distinct c) as claimants, count(distinct cl) as claim_count
                WHERE claim_count >= 3
                
                RETURN 
                    'recurring_witness' as pattern_type,
                    w.witness_id as entity_id,
                    w.name as entity_name,
                    [c in claimants | c.claimant_id] as claimant_ids,
                    claim_count,
                    0 as connections,
                    claim_count * 0.7 as suspicion_score
                ORDER BY suspicion_score DESC
                LIMIT 10
            """)
            
            for record in result:
                fraud_rings.append({
                    'pattern_type': record['pattern_type'],
                    'entity_id': record['entity_id'],
                    'entity_name': record['entity_name'],
                    'claimant_ids': record['claimant_ids'],
                    'claim_count': record['claim_count'],
                    'connections': record['connections'],
                    'suspicion_score': round(record['suspicion_score'], 2)
                })
        
        logger.info(f"Detected {len(fraud_rings)} suspicious patterns")
        return fraud_rings
    
    def calculate_fraud_propensity_for_claim(self, claim_id: str) -> Dict:
        """
        Calculate fraud propensity score for a specific claim
        Returns score (0-100) and contributing factors
        """
        logger.info(f"Calculating fraud propensity for claim {claim_id}...")
        
        factors = {}
        total_score = 0.0
        
        with self.driver.session() as session:
            # Get claim details
            claim_result = session.run("""
                MATCH (c:Claimant)-[:FILED_CLAIM]->(cl:Claim {claim_id: $claim_id})
                OPTIONAL MATCH (cl)-[:REPAIRED_AT]->(s:RepairShop)
                OPTIONAL MATCH (cl)-[:TREATED_BY]->(m:MedicalProvider)
                OPTIONAL MATCH (cl)-[:REPRESENTED_BY]->(l:Lawyer)
                RETURN c, cl, s, m, l
            """, claim_id=claim_id)
            
            record = claim_result.single()
            if not record:
                return {'error': 'Claim not found'}
            
            claimant = dict(record['c'])
            claim = dict(record['cl'])
            shop = dict(record['s']) if record['s'] else None
            provider = dict(record['m']) if record['m'] else None
            lawyer = dict(record['l']) if record['l'] else None
            
            # Factor 1: Claimant has connections to known fraud ring members
            ring_connection_result = session.run("""
                MATCH (c:Claimant {claimant_id: $claimant_id})
                OPTIONAL MATCH (c)-[r:RELATED_TO|SHARES_ADDRESS|SHARES_PHONE]-(other:Claimant)
                WHERE other.is_fraud_ring = true
                RETURN count(distinct other) as fraud_connections
            """, claimant_id=claimant['claimant_id'])
            
            fraud_connections = ring_connection_result.single()['fraud_connections']
            if fraud_connections > 0:
                connection_score = min(fraud_connections * 15, 40)
                factors['fraud_ring_connections'] = {
                    'score': connection_score,
                    'description': f'Connected to {fraud_connections} known fraud ring member(s)',
                    'severity': 'high' if fraud_connections >= 3 else 'medium'
                }
                total_score += connection_score
            
            # Factor 2: Repair shop involvement
            if shop:
                shop_claims_result = session.run("""
                    MATCH (cl:Claim)-[:REPAIRED_AT]->(s:RepairShop {shop_id: $shop_id})
                    RETURN count(cl) as total_claims
                """, shop_id=shop['shop_id'])
                
                shop_claims = shop_claims_result.single()['total_claims']
                if shop_claims >= 10:
                    shop_score = min((shop_claims - 9) * 2, 20)
                    factors['suspicious_repair_shop'] = {
                        'score': shop_score,
                        'description': f'Repair shop has {shop_claims} claims in system',
                        'severity': 'high' if shop_claims >= 20 else 'medium'
                    }
                    total_score += shop_score
            
            # Factor 3: Medical provider patterns
            if provider:
                provider_claims_result = session.run("""
                    MATCH (cl:Claim)-[:TREATED_BY]->(m:MedicalProvider {provider_id: $provider_id})
                    RETURN count(cl) as total_claims
                """, provider_id=provider['provider_id'])
                
                provider_claims = provider_claims_result.single()['total_claims']
                if provider_claims >= 8:
                    provider_score = min((provider_claims - 7) * 2.5, 20)
                    factors['suspicious_medical_provider'] = {
                        'score': provider_score,
                        'description': f'Medical provider has {provider_claims} claims in system',
                        'severity': 'high' if provider_claims >= 15 else 'medium'
                    }
                    total_score += provider_score
            
            # Factor 4: Lawyer representation patterns
            if lawyer:
                lawyer_clients_result = session.run("""
                    MATCH (cl:Claim)-[:REPRESENTED_BY]->(l:Lawyer {lawyer_id: $lawyer_id})
                    RETURN count(distinct cl) as client_count
                """, lawyer_id=lawyer['lawyer_id'])
                
                client_count = lawyer_clients_result.single()['client_count']
                if client_count >= 10:
                    lawyer_score = min((client_count - 9) * 1.5, 15)
                    factors['suspicious_lawyer'] = {
                        'score': lawyer_score,
                        'description': f'Lawyer represents {client_count} claimants in system',
                        'severity': 'medium'
                    }
                    total_score += lawyer_score
            
            # Factor 5: Claimant's claim history
            claim_history_result = session.run("""
                MATCH (c:Claimant {claimant_id: $claimant_id})-[:FILED_CLAIM]->(cl:Claim)
                RETURN count(cl) as total_claims, sum(cl.claim_amount) as total_amount
            """, claimant_id=claimant['claimant_id'])
            
            history = claim_history_result.single()
            if history['total_claims'] >= 3:
                history_score = min((history['total_claims'] - 2) * 5, 15)
                factors['multiple_claims'] = {
                    'score': history_score,
                    'description': f'Claimant has filed {history["total_claims"]} claims',
                    'severity': 'medium' if history['total_claims'] < 5 else 'high'
                }
                total_score += history_score
            
            # Factor 6: Shared address with multiple claimants
            address_result = session.run("""
                MATCH (c:Claimant {claimant_id: $claimant_id})
                MATCH (other:Claimant)
                WHERE other.claimant_id <> c.claimant_id AND other.address = c.address
                RETURN count(other) as shared_address_count
            """, claimant_id=claimant['claimant_id'])
            
            shared_address = address_result.single()['shared_address_count']
            if shared_address >= 2:
                address_score = min(shared_address * 5, 15)
                factors['shared_address'] = {
                    'score': address_score,
                    'description': f'Address shared with {shared_address} other claimant(s)',
                    'severity': 'high' if shared_address >= 4 else 'medium'
                }
                total_score += address_score
            
            # Factor 7: Claim amount analysis
            claim_amount = claim['claim_amount']
            avg_amount_result = session.run("""
                MATCH (cl:Claim)
                WHERE cl.claim_type = $claim_type
                RETURN avg(cl.claim_amount) as avg_amount, stdev(cl.claim_amount) as std_amount
            """, claim_type=claim['claim_type'])
            
            avg_stats = avg_amount_result.single()
            if avg_stats['avg_amount']:
                z_score = (claim_amount - avg_stats['avg_amount']) / (avg_stats['std_amount'] or 1)
                if z_score > 2:
                    amount_score = min((z_score - 2) * 5, 10)
                    factors['high_claim_amount'] = {
                        'score': amount_score,
                        'description': f'Claim amount ${claim_amount:,.2f} is {z_score:.1f} sigma above average',
                        'severity': 'medium'
                    }
                    total_score += amount_score
        
        # Normalize score to 0-100
        final_score = min(total_score, 100)
        
        # Determine risk level
        if final_score >= 70:
            risk_level = 'High'
        elif final_score >= 40:
            risk_level = 'Medium'
        else:
            risk_level = 'Low'
        
        return {
            'claim_id': claim_id,
            'fraud_propensity_score': round(final_score, 2),
            'risk_level': risk_level,
            'contributing_factors': factors,
            'total_factors': len(factors),
            'recommendation': 'Flag for investigation' if final_score >= 40 else 'Standard processing'
        }
    
    def get_fraud_ring_statistics(self) -> Dict:
        """Get overall fraud ring statistics"""
        with self.driver.session() as session:
            stats = {}
            
            # Total fraud rings
            ring_result = session.run("""
                MATCH (c:Claimant)
                WHERE c.is_fraud_ring = true
                RETURN count(distinct c.fraud_ring_id) as num_rings,
                       count(c) as fraud_members,
                       sum(CASE WHEN exists((c)-[:FILED_CLAIM]->()) THEN 1 ELSE 0 END) as members_with_claims
            """)
            
            ring_stats = ring_result.single()
            stats['num_fraud_rings'] = ring_stats['num_rings']
            stats['fraud_ring_members'] = ring_stats['fraud_members']
            stats['members_with_claims'] = ring_stats['members_with_claims']
            
            # Fraud claims
            claim_result = session.run("""
                MATCH (cl:Claim)
                WHERE cl.is_fraud_ring = true
                RETURN count(cl) as fraud_claims, sum(cl.claim_amount) as total_fraud_amount
            """)
            
            claim_stats = claim_result.single()
            stats['fraud_claims'] = claim_stats['fraud_claims']
            stats['total_fraud_amount'] = round(claim_stats['total_fraud_amount'], 2)
            
            return stats

def main():
    """Test fraud detection algorithms"""
    from dotenv import load_dotenv
    
    load_dotenv()
    
    uri = os.getenv('NEO4J_URI')
    username = os.getenv('NEO4J_USERNAME')
    password = os.getenv('NEO4J_PASSWORD')

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
    
    detector = FraudDetector(driver)
    
    # Test community detection
    communities = detector.calculate_louvain_communities()
    print(f"Found {len(set(communities.values()))} communities")
    
    # Test pattern detection
    fraud_rings = detector.detect_fraud_rings_by_patterns()
    print(f"Detected {len(fraud_rings)} suspicious patterns")
    
    # Test statistics
    stats = detector.get_fraud_ring_statistics()
    print("Fraud Ring Statistics:", stats)
    
    driver.close()

if __name__ == "__main__":
    main()