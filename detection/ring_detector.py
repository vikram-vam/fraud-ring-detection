"""
Ring Detector - Detect and analyze auto insurance fraud rings
Uses graph algorithms to identify connected fraud networks
"""
import logging
from typing import Dict, List, Optional, Set, Tuple
from datetime import datetime
from collections import defaultdict
import uuid

from data.neo4j_driver import get_neo4j_driver
from utils.logger import setup_logger

logger = setup_logger(__name__)


class RingDetector:
    """
    Fraud ring detection engine for auto insurance
    Identifies networks of connected claimants, entities, and claims
    """
    
    def __init__(self):
        self.driver = get_neo4j_driver()
        
        # Detection thresholds
        self.thresholds = {
            'min_ring_members': 3,
            'min_shared_connections': 2,
            'min_confidence': 0.6,
            'min_total_claims': 5
        }
    
    def detect_fraud_rings(self) -> List[Dict]:
        """
        Detect all fraud rings using multiple algorithms
        
        Returns:
            List of detected fraud rings
        """
        logger.info("Starting fraud ring detection")
        
        all_rings = []
        
        # 1. Detect rings based on shared entities
        shared_entity_rings = self.detect_shared_entity_rings()
        all_rings.extend(shared_entity_rings)
        logger.info(f"Found {len(shared_entity_rings)} shared entity rings")
        
        # 2. Detect rings based on accident patterns
        accident_pattern_rings = self.detect_accident_pattern_rings()
        all_rings.extend(accident_pattern_rings)
        logger.info(f"Found {len(accident_pattern_rings)} accident pattern rings")
        
        # 3. Detect rings based on witness networks
        witness_network_rings = self.detect_witness_network_rings()
        all_rings.extend(witness_network_rings)
        logger.info(f"Found {len(witness_network_rings)} witness network rings")
        
        # 4. Detect rings based on vehicle sharing
        vehicle_sharing_rings = self.detect_vehicle_sharing_rings()
        all_rings.extend(vehicle_sharing_rings)
        logger.info(f"Found {len(vehicle_sharing_rings)} vehicle sharing rings")
        
        # Deduplicate and merge overlapping rings
        merged_rings = self._merge_overlapping_rings(all_rings)
        logger.info(f"After merging: {len(merged_rings)} unique rings")
        
        # Calculate final confidence scores
        for ring in merged_rings:
            ring['confidence_score'] = self._calculate_ring_confidence(ring)
        
        # Filter by confidence threshold
        high_confidence_rings = [
            r for r in merged_rings 
            if r['confidence_score'] >= self.thresholds['min_confidence']
        ]
        
        logger.info(f"Fraud ring detection complete. Found {len(high_confidence_rings)} high-confidence rings")
        
        return high_confidence_rings
    
    def detect_shared_entity_rings(self) -> List[Dict]:
        """
        Detect rings where claimants share multiple entities
        (body shops, medical providers, attorneys)
        """
        logger.info("Detecting shared entity rings")
        
        query = """
        // Find claimants who share body shops
        MATCH (c1:Claimant)-[:FILED]->(cl1:Claim)-[:REPAIRED_AT]->(b:BodyShop)
        MATCH (c2:Claimant)-[:FILED]->(cl2:Claim)-[:REPAIRED_AT]->(b)
        WHERE c1.claimant_id < c2.claimant_id
        
        WITH c1, c2, collect(DISTINCT b.body_shop_id) as shared_body_shops
        
        // Find shared medical providers
        OPTIONAL MATCH (c1)-[:FILED]->(cl3:Claim)-[:TREATED_BY]->(m:MedicalProvider)
        OPTIONAL MATCH (c2)-[:FILED]->(cl4:Claim)-[:TREATED_BY]->(m)
        
        WITH c1, c2, shared_body_shops, collect(DISTINCT m.provider_id) as shared_medical_providers
        
        // Find shared attorneys
        OPTIONAL MATCH (c1)-[:FILED]->(cl5:Claim)-[:REPRESENTED_BY]->(a:Attorney)
        OPTIONAL MATCH (c2)-[:FILED]->(cl6:Claim)-[:REPRESENTED_BY]->(a)
        
        WITH c1, c2, shared_body_shops, shared_medical_providers, 
             collect(DISTINCT a.attorney_id) as shared_attorneys
        
        // Calculate total shared connections
        WITH c1, c2,
             size(shared_body_shops) + size(shared_medical_providers) + size(shared_attorneys) as shared_count,
             shared_body_shops, shared_medical_providers, shared_attorneys
        
        WHERE shared_count >= $min_shared
        
        // Get claim counts
        MATCH (c1)-[:FILED]->(cl_c1:Claim)
        MATCH (c2)-[:FILED]->(cl_c2:Claim)
        
        WITH c1, c2, shared_count, shared_body_shops, shared_medical_providers, shared_attorneys,
             count(DISTINCT cl_c1) as c1_claims, count(DISTINCT cl_c2) as c2_claims
        
        RETURN 
            c1.claimant_id as claimant1_id,
            c1.name as claimant1_name,
            c2.claimant_id as claimant2_id,
            c2.name as claimant2_name,
            shared_count,
            shared_body_shops,
            shared_medical_providers,
            shared_attorneys,
            c1_claims,
            c2_claims
        ORDER BY shared_count DESC
        LIMIT 100
        """
        
        try:
            results = self.driver.execute_query(query, {
                'min_shared': self.thresholds['min_shared_connections']
            })
            
            # Build claimant groups based on shared connections
            claimant_groups = self._build_claimant_groups(results)
            
            # Create ring objects
            rings = []
            for group_id, claimants in enumerate(claimant_groups):
                if len(claimants) >= self.thresholds['min_ring_members']:
                    ring = self._create_ring_from_claimants(
                        claimants,
                        'shared_entities',
                        f"SHARED_ENTITY_RING_{group_id}"
                    )
                    if ring:
                        rings.append(ring)
            
            return rings
            
        except Exception as e:
            logger.error(f"Error detecting shared entity rings: {e}", exc_info=True)
            return []
    
    def detect_accident_pattern_rings(self) -> List[Dict]:
        """
        Detect rings based on suspicious accident patterns
        (same locations, dates, circumstances)
        """
        logger.info("Detecting accident pattern rings")
        
        query = """
        // Find claimants with accidents at same locations
        MATCH (c1:Claimant)-[:FILED]->(cl1:Claim)-[:OCCURRED_AT]->(l:AccidentLocation)
        MATCH (c2:Claimant)-[:FILED]->(cl2:Claim)-[:OCCURRED_AT]->(l)
        WHERE c1.claimant_id < c2.claimant_id
        
        // Check for temporal proximity (within 30 days)
        WITH c1, c2, l, cl1, cl2
        WHERE duration.between(date(cl1.accident_date), date(cl2.accident_date)).days <= 30
        
        // Count shared locations
        WITH c1, c2, count(DISTINCT l) as shared_locations
        WHERE shared_locations >= 1
        
        // Get additional connection info
        MATCH (c1)-[:FILED]->(cl_c1:Claim)
        MATCH (c2)-[:FILED]->(cl_c2:Claim)
        
        WITH c1, c2, shared_locations,
             avg(cl_c1.risk_score) as c1_avg_risk,
             avg(cl_c2.risk_score) as c2_avg_risk,
             count(DISTINCT cl_c1) as c1_claims,
             count(DISTINCT cl_c2) as c2_claims
        
        WHERE c1_avg_risk >= 50 AND c2_avg_risk >= 50
        
        RETURN 
            c1.claimant_id as claimant1_id,
            c1.name as claimant1_name,
            c2.claimant_id as claimant2_id,
            c2.name as claimant2_name,
            shared_locations,
            c1_avg_risk,
            c2_avg_risk,
            c1_claims,
            c2_claims
        ORDER BY shared_locations DESC
        LIMIT 100
        """
        
        try:
            results = self.driver.execute_query(query, {})
            
            # Build claimant groups
            claimant_groups = self._build_claimant_groups(results)
            
            # Create ring objects
            rings = []
            for group_id, claimants in enumerate(claimant_groups):
                if len(claimants) >= self.thresholds['min_ring_members']:
                    ring = self._create_ring_from_claimants(
                        claimants,
                        'accident_pattern',
                        f"ACCIDENT_PATTERN_RING_{group_id}"
                    )
                    if ring:
                        rings.append(ring)
            
            return rings
            
        except Exception as e:
            logger.error(f"Error detecting accident pattern rings: {e}", exc_info=True)
            return []
    
    def detect_witness_network_rings(self) -> List[Dict]:
        """
        Detect rings based on shared witnesses across multiple claims
        """
        logger.info("Detecting witness network rings")
        
        query = """
        // Find claimants sharing witnesses
        MATCH (c1:Claimant)-[:FILED]->(cl1:Claim)<-[:WITNESSED]-(w:Witness)
        MATCH (c2:Claimant)-[:FILED]->(cl2:Claim)<-[:WITNESSED]-(w)
        WHERE c1.claimant_id < c2.claimant_id
        
        // Count shared witnesses
        WITH c1, c2, collect(DISTINCT w.witness_id) as shared_witnesses
        WHERE size(shared_witnesses) >= 1
        
        // Get claim info
        MATCH (c1)-[:FILED]->(cl_c1:Claim)
        MATCH (c2)-[:FILED]->(cl_c2:Claim)
        
        WITH c1, c2, shared_witnesses,
             count(DISTINCT cl_c1) as c1_claims,
             count(DISTINCT cl_c2) as c2_claims,
             avg(cl_c1.risk_score) as c1_avg_risk,
             avg(cl_c2.risk_score) as c2_avg_risk
        
        RETURN 
            c1.claimant_id as claimant1_id,
            c1.name as claimant1_name,
            c2.claimant_id as claimant2_id,
            c2.name as claimant2_name,
            size(shared_witnesses) as shared_witness_count,
            c1_claims,
            c2_claims,
            c1_avg_risk,
            c2_avg_risk
        ORDER BY shared_witness_count DESC
        LIMIT 100
        """
        
        try:
            results = self.driver.execute_query(query, {})
            
            # Build claimant groups
            claimant_groups = self._build_claimant_groups(results)
            
            # Create ring objects
            rings = []
            for group_id, claimants in enumerate(claimant_groups):
                if len(claimants) >= self.thresholds['min_ring_members']:
                    ring = self._create_ring_from_claimants(
                        claimants,
                        'witness_network',
                        f"WITNESS_NETWORK_RING_{group_id}"
                    )
                    if ring:
                        rings.append(ring)
            
            return rings
            
        except Exception as e:
            logger.error(f"Error detecting witness network rings: {e}", exc_info=True)
            return []
    
    def detect_vehicle_sharing_rings(self) -> List[Dict]:
        """
        Detect rings where multiple claimants use the same vehicles
        """
        logger.info("Detecting vehicle sharing rings")
        
        query = """
        // Find claimants sharing vehicles
        MATCH (c1:Claimant)-[:FILED]->(cl1:Claim)-[:INVOLVES_VEHICLE]->(v:Vehicle)
        MATCH (c2:Claimant)-[:FILED]->(cl2:Claim)-[:INVOLVES_VEHICLE]->(v)
        WHERE c1.claimant_id < c2.claimant_id
        
        // Count shared vehicles
        WITH c1, c2, collect(DISTINCT v.vehicle_id) as shared_vehicles
        WHERE size(shared_vehicles) >= 1
        
        // Get claim info
        MATCH (c1)-[:FILED]->(cl_c1:Claim)
        MATCH (c2)-[:FILED]->(cl_c2:Claim)
        
        WITH c1, c2, shared_vehicles,
             count(DISTINCT cl_c1) as c1_claims,
             count(DISTINCT cl_c2) as c2_claims,
             avg(cl_c1.risk_score) as c1_avg_risk,
             avg(cl_c2.risk_score) as c2_avg_risk
        
        WHERE c1_avg_risk >= 50 OR c2_avg_risk >= 50
        
        RETURN 
            c1.claimant_id as claimant1_id,
            c1.name as claimant1_name,
            c2.claimant_id as claimant2_id,
            c2.name as claimant2_name,
            size(shared_vehicles) as shared_vehicle_count,
            c1_claims,
            c2_claims,
            c1_avg_risk,
            c2_avg_risk
        ORDER BY shared_vehicle_count DESC
        LIMIT 100
        """
        
        try:
            results = self.driver.execute_query(query, {})
            
            # Build claimant groups
            claimant_groups = self._build_claimant_groups(results)
            
            # Create ring objects
            rings = []
            for group_id, claimants in enumerate(claimant_groups):
                if len(claimants) >= self.thresholds['min_ring_members']:
                    ring = self._create_ring_from_claimants(
                        claimants,
                        'vehicle_sharing',
                        f"VEHICLE_SHARING_RING_{group_id}"
                    )
                    if ring:
                        rings.append(ring)
            
            return rings
            
        except Exception as e:
            logger.error(f"Error detecting vehicle sharing rings: {e}", exc_info=True)
            return []
    
    def create_fraud_ring_nodes(self, rings: List[Dict]) -> int:
        """
        Create FraudRing nodes in Neo4j and link members
        
        Args:
            rings: List of detected fraud rings
            
        Returns:
            Number of rings created
        """
        logger.info(f"Creating {len(rings)} fraud ring nodes in database")
        
        created_count = 0
        
        for ring in rings:
            try:
                # Create FraudRing node
                create_query = """
                CREATE (r:FraudRing {
                    ring_id: $ring_id,
                    ring_type: $ring_type,
                    pattern_type: $pattern_type,
                    status: $status,
                    confidence_score: $confidence_score,
                    member_count: $member_count,
                    estimated_fraud_amount: $estimated_fraud_amount,
                    discovered_date: date($discovered_date),
                    discovered_by: $discovered_by
                })
                RETURN r.ring_id as ring_id
                """
                
                result = self.driver.execute_write(create_query, {
                    'ring_id': ring['ring_id'],
                    'ring_type': ring.get('ring_type', 'DISCOVERED'),
                    'pattern_type': ring['pattern_type'],
                    'status': ring.get('status', 'UNDER_REVIEW'),
                    'confidence_score': ring['confidence_score'],
                    'member_count': ring['member_count'],
                    'estimated_fraud_amount': ring.get('estimated_fraud_amount', 0),
                    'discovered_date': datetime.now().strftime('%Y-%m-%d'),
                    'discovered_by': 'RingDetector'
                })
                
                if result:
                    # Link members to ring
                    for member_id in ring['member_ids']:
                        link_query = """
                        MATCH (c:Claimant {claimant_id: $claimant_id})
                        MATCH (r:FraudRing {ring_id: $ring_id})
                        MERGE (c)-[:MEMBER_OF]->(r)
                        """
                        
                        self.driver.execute_write(link_query, {
                            'claimant_id': member_id,
                            'ring_id': ring['ring_id']
                        })
                    
                    created_count += 1
                    logger.debug(f"Created fraud ring: {ring['ring_id']}")
                
            except Exception as e:
                logger.error(f"Error creating fraud ring {ring.get('ring_id')}: {e}")
                continue
        
        logger.info(f"Successfully created {created_count} fraud ring nodes")
        return created_count
    
    # Helper methods
    
    def _build_claimant_groups(self, results: List[Dict]) -> List[Set[str]]:
        """
        Build groups of connected claimants from pairwise connections
        Uses union-find algorithm
        """
        # Build adjacency list
        connections = defaultdict(set)
        
        for result in results:
            c1_id = result['claimant1_id']
            c2_id = result['claimant2_id']
            connections[c1_id].add(c2_id)
            connections[c2_id].add(c1_id)
        
        # Find connected components
        visited = set()
        groups = []
        
        def dfs(node, group):
            visited.add(node)
            group.add(node)
            for neighbor in connections[node]:
                if neighbor not in visited:
                    dfs(neighbor, group)
        
        for node in connections:
            if node not in visited:
                group = set()
                dfs(node, group)
                groups.append(group)
        
        return groups
    
    def _create_ring_from_claimants(
        self,
        claimant_ids: Set[str],
        pattern_type: str,
        ring_id_prefix: str
    ) -> Optional[Dict]:
        """
        Create a fraud ring object from a set of claimants
        """
        try:
            # Get claimant details and calculate ring metrics
            claimant_list = list(claimant_ids)
            
            query = """
            MATCH (c:Claimant)
            WHERE c.claimant_id IN $claimant_ids
            
            OPTIONAL MATCH (c)-[:FILED]->(cl:Claim)
            
            WITH c, cl
            
            RETURN 
                collect(DISTINCT c.claimant_id) as member_ids,
                collect(DISTINCT c.name) as member_names,
                count(DISTINCT cl) as total_claims,
                sum(cl.total_claim_amount) as total_amount,
                avg(cl.risk_score) as avg_risk
            """
            
            results = self.driver.execute_query(query, {'claimant_ids': claimant_list})
            
            if not results:
                return None
            
            data = results[0]
            
            # Generate ring ID
            ring_id = f"{ring_id_prefix}_{uuid.uuid4().hex[:8]}"
            
            ring = {
                'ring_id': ring_id,
                'pattern_type': pattern_type,
                'ring_type': 'DISCOVERED',
                'status': 'UNDER_REVIEW',
                'member_count': len(claimant_list),
                'member_ids': data['member_ids'],
                'member_names': data['member_names'],
                'total_claims': data.get('total_claims', 0),
                'estimated_fraud_amount': data.get('total_amount', 0),
                'avg_risk_score': round(data.get('avg_risk', 0), 2)
            }
            
            return ring
            
        except Exception as e:
            logger.error(f"Error creating ring from claimants: {e}", exc_info=True)
            return None
    
    def _merge_overlapping_rings(self, rings: List[Dict]) -> List[Dict]:
        """
        Merge rings that have significant member overlap
        """
        if not rings:
            return []
        
        # Build member sets
        ring_members = [set(ring['member_ids']) for ring in rings]
        
        # Find overlapping rings (>50% overlap)
        merged = []
        used = set()
        
        for i, ring1 in enumerate(rings):
            if i in used:
                continue
            
            members1 = ring_members[i]
            merged_members = members1.copy()
            merged_ring = ring1.copy()
            
            for j in range(i + 1, len(rings)):
                if j in used:
                    continue
                
                members2 = ring_members[j]
                overlap = len(members1 & members2)
                overlap_ratio = overlap / min(len(members1), len(members2))
                
                if overlap_ratio > 0.5:
                    # Merge rings
                    merged_members.update(members2)
                    used.add(j)
                    
                    # Combine pattern types
                    if merged_ring['pattern_type'] != rings[j]['pattern_type']:
                        merged_ring['pattern_type'] = 'mixed'
            
            # Update merged ring
            merged_ring['member_ids'] = list(merged_members)
            merged_ring['member_count'] = len(merged_members)
            merged.append(merged_ring)
            used.add(i)
        
        return merged
    
    def _calculate_ring_confidence(self, ring: Dict) -> float:
        """
        Calculate confidence score for fraud ring
        """
        confidence = 0.5
        
        # Member count factor
        member_count = ring.get('member_count', 0)
        if member_count >= 10:
            confidence += 0.2
        elif member_count >= 5:
            confidence += 0.15
        elif member_count >= 3:
            confidence += 0.1
        
        # Average risk factor
        avg_risk = ring.get('avg_risk_score', 0)
        if avg_risk >= 70:
            confidence += 0.2
        elif avg_risk >= 60:
            confidence += 0.15
        elif avg_risk >= 50:
            confidence += 0.1
        
        # Total claims factor
        total_claims = ring.get('total_claims', 0)
        if total_claims >= 15:
            confidence += 0.1
        elif total_claims >= 10:
            confidence += 0.05
        
        return min(confidence, 1.0)
