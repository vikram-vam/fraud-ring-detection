"""
Ring Repository - Data access for fraud rings
"""
from typing import List, Dict, Optional
import logging

from data.neo4j_driver import Neo4jDriver
from data.models.fraud_ring import FraudRing

logger = logging.getLogger(__name__)


class RingRepository:
    """Repository for FraudRing entity operations"""
    
    def __init__(self):
        self.driver = Neo4jDriver()
    
    def get_all_rings(
        self,
        ring_type: Optional[str] = None,
        status: Optional[str] = None,
        min_confidence: float = 0.0,
        limit: int = 100
    ) -> List[FraudRing]:
        """
        Get all fraud rings with optional filters
        
        Args:
            ring_type: Filter by ring type (KNOWN, DISCOVERED, etc.)
            status: Filter by status (CONFIRMED, UNDER_REVIEW, DISMISSED)
            min_confidence: Minimum confidence score
            limit: Maximum number to return
            
        Returns:
            List of FraudRing objects
        """
        where_clauses = ["r.confidence_score >= $min_confidence"]
        
        if ring_type:
            where_clauses.append("r.ring_type = $ring_type")
        
        if status:
            where_clauses.append("r.status = $status")
        
        where_clause = " AND ".join(where_clauses)
        
        query = f"""
        MATCH (r:FraudRing)
        WHERE {where_clause}
        ORDER BY r.confidence_score DESC
        LIMIT $limit
        RETURN 
            r.ring_id as ring_id,
            r.ring_type as ring_type,
            r.pattern_type as pattern_type,
            r.status as status,
            r.confidence_score as confidence_score,
            r.member_count as member_count,
            r.estimated_fraud_amount as estimated_fraud_amount,
            r.discovered_date as discovered_date,
            r.discovered_by as discovered_by,
            r.confirmed_date as confirmed_date,
            r.confirmed_by as confirmed_by,
            r.dismissed_date as dismissed_date,
            r.dismissed_by as dismissed_by,
            r.dismissal_reason as dismissal_reason
        """
        
        results = self.driver.execute_query(query, {
            'ring_type': ring_type,
            'status': status,
            'min_confidence': min_confidence,
            'limit': limit
        })
        
        return [FraudRing.from_dict(r) for r in results] if results else []
    
    def get_ring_by_id(self, ring_id: str) -> Optional[FraudRing]:
        """
        Get fraud ring by ID
        
        Args:
            ring_id: Ring ID
            
        Returns:
            FraudRing object or None
        """
        query = """
        MATCH (r:FraudRing {ring_id: $ring_id})
        RETURN 
            r.ring_id as ring_id,
            r.ring_type as ring_type,
            r.pattern_type as pattern_type,
            r.status as status,
            r.confidence_score as confidence_score,
            r.member_count as member_count,
            r.estimated_fraud_amount as estimated_fraud_amount,
            r.discovered_date as discovered_date,
            r.discovered_by as discovered_by,
            r.confirmed_date as confirmed_date,
            r.confirmed_by as confirmed_by,
            r.dismissed_date as dismissed_date,
            r.dismissed_by as dismissed_by,
            r.dismissal_reason as dismissal_reason
        """
        
        results = self.driver.execute_query(query, {'ring_id': ring_id})
        
        return FraudRing.from_dict(results[0]) if results else None
    
    def get_ring_members(self, ring_id: str) -> List[Dict]:
        """
        Get all members of a fraud ring
        
        Args:
            ring_id: Ring ID
            
        Returns:
            List of member dictionaries
        """
        query = """
        MATCH (c:Claimant)-[:MEMBER_OF]->(r:FraudRing {ring_id: $ring_id})
        OPTIONAL MATCH (c)-[:FILED]->(cl:Claim)
        WITH c, 
             count(cl) as claim_count,
             sum(cl.claim_amount) as total_amount,
             avg(cl.risk_score) as avg_risk_score
        RETURN 
            c.claimant_id as claimant_id,
            c.name as name,
            c.email as email,
            claim_count,
            total_amount,
            avg_risk_score
        ORDER BY claim_count DESC
        """
        
        results = self.driver.execute_query(query, {'ring_id': ring_id})
        
        return results if results else []
    
    def get_ring_network(self, ring_id: str) -> Dict:
        """
        Get network graph data for a ring
        
        Args:
            ring_id: Ring ID
            
        Returns:
            Dictionary with nodes and edges
        """
        # Get nodes
        node_query = """
        MATCH (r:FraudRing {ring_id: $ring_id})
        MATCH (c:Claimant)-[:MEMBER_OF]->(r)
        
        OPTIONAL MATCH (c)-[:LIVES_AT]->(a:Address)
        OPTIONAL MATCH (c)-[:FILED]->(:Claim)-[:TREATED_BY]->(p:Provider)
        OPTIONAL MATCH (c)-[:FILED]->(:Claim)-[:REPRESENTED_BY]->(att:Attorney)
        OPTIONAL MATCH (c)-[:HAS_PHONE]->(ph:Phone)
        
        WITH r, c, 
             collect(DISTINCT a) as addresses,
             collect(DISTINCT p) as providers,
             collect(DISTINCT att) as attorneys,
             collect(DISTINCT ph) as phones
        
        WITH r, 
             collect({
                 id: c.claimant_id, 
                 label: c.name, 
                 type: 'Claimant',
                 ring_member: true
             }) as claimant_nodes,
             collect(DISTINCT {id: a.address_id, label: a.street, type: 'Address'}) as address_nodes,
             collect(DISTINCT {id: p.provider_id, label: p.name, type: 'Provider'}) as provider_nodes,
             collect(DISTINCT {id: att.attorney_id, label: att.name, type: 'Attorney'}) as attorney_nodes,
             collect(DISTINCT {id: ph.phone_number, label: ph.phone_number, type: 'Phone'}) as phone_nodes
        
        RETURN 
            claimant_nodes + address_nodes + provider_nodes + attorney_nodes + phone_nodes as nodes
        """
        
        node_results = self.driver.execute_query(node_query, {'ring_id': ring_id})
        
        nodes = []
        if node_results and node_results[0].get('nodes'):
            for node in node_results[0]['nodes']:
                if node and node.get('id'):
                    nodes.append(node)
        
        # Get edges
        edge_query = """
        MATCH (r:FraudRing {ring_id: $ring_id})
        MATCH (c:Claimant)-[:MEMBER_OF]->(r)
        
        OPTIONAL MATCH (c)-[:LIVES_AT]->(a:Address)
        WITH c, collect(DISTINCT {source: c.claimant_id, target: a.address_id, type: 'LIVES_AT'}) as addr_edges
        
        OPTIONAL MATCH (c)-[:FILED]->(:Claim)-[:TREATED_BY]->(p:Provider)
        WITH c, addr_edges, collect(DISTINCT {source: c.claimant_id, target: p.provider_id, type: 'TREATED_BY'}) as prov_edges
        
        OPTIONAL MATCH (c)-[:FILED]->(:Claim)-[:REPRESENTED_BY]->(att:Attorney)
        WITH c, addr_edges, prov_edges, collect(DISTINCT {source: c.claimant_id, target: att.attorney_id, type: 'REPRESENTED_BY'}) as att_edges
        
        OPTIONAL MATCH (c)-[:HAS_PHONE]->(ph:Phone)
        WITH addr_edges, prov_edges, att_edges, collect(DISTINCT {source: c.claimant_id, target: ph.phone_number, type: 'HAS_PHONE'}) as phone_edges
        
        RETURN addr_edges + prov_edges + att_edges + phone_edges as edges
        """
        
        edge_results = self.driver.execute_query(edge_query, {'ring_id': ring_id})
        
        edges = []
        if edge_results and edge_results[0].get('edges'):
            for edge in edge_results[0]['edges']:
                if edge and edge.get('source') and edge.get('target'):
                    edges.append(edge)
        
        return {
            'nodes': nodes,
            'edges': edges
        }
    
    def confirm_ring(self, ring_id: str, confirmed_by: str) -> bool:
        """
        Confirm a discovered ring as fraud
        
        Args:
            ring_id: Ring ID
            confirmed_by: User/investigator ID
            
        Returns:
            True if successful
        """
        query = """
        MATCH (r:FraudRing {ring_id: $ring_id})
        SET r.status = 'CONFIRMED',
            r.confirmed_date = datetime(),
            r.confirmed_by = $confirmed_by
        RETURN r.ring_id as ring_id
        """
        
        try:
            results = self.driver.execute_write(query, {
                'ring_id': ring_id,
                'confirmed_by': confirmed_by
            })
            return len(results) > 0
        except Exception as e:
            logger.error(f"Error confirming ring: {e}", exc_info=True)
            return False
    
    def dismiss_ring(self, ring_id: str, reason: str, dismissed_by: str) -> bool:
        """
        Dismiss a discovered ring as false positive
        
        Args:
            ring_id: Ring ID
            reason: Reason for dismissal
            dismissed_by: User/investigator ID
            
        Returns:
            True if successful
        """
        query = """
        MATCH (r:FraudRing {ring_id: $ring_id})
        SET r.status = 'DISMISSED',
            r.dismissed_date = datetime(),
            r.dismissed_by = $dismissed_by,
            r.dismissal_reason = $reason
        RETURN r.ring_id as ring_id
        """
        
        try:
            results = self.driver.execute_write(query, {
                'ring_id': ring_id,
                'reason': reason,
                'dismissed_by': dismissed_by
            })
            return len(results) > 0
        except Exception as e:
            logger.error(f"Error dismissing ring: {e}", exc_info=True)
            return False
