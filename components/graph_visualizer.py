"""
GRAPH VISUALIZATION COMPONENT
Handles all graph visualization using Pyvis
"""

from pyvis.network import Network
import networkx as nx
from typing import Dict, List, Any
import logging
import tempfile
import os

logger = logging.getLogger(__name__)

class GraphVisualizer:
    def __init__(self, driver):
        self.driver = driver
    
    def visualize_fraud_ring(self, ring_id: str):
        """Visualize a specific fraud ring including 2-hop connections"""
        
        with self.driver.session() as session:
            # Expanded query to get internal connections + claims + providers/vehicles
            result = session.run("""
                MATCH (c:Claimant {fraud_ring_id: $ring_id})
                WITH collect(c) as ring_members
                
                // 1. Internal Ring Connections (Direct)
                OPTIONAL MATCH (c1:Claimant)-[r1]-(c2:Claimant)
                WHERE c1 IN ring_members AND c2 IN ring_members AND elementId(c1) < elementId(c2)
                
                // 2. Claims filed by members
                OPTIONAL MATCH (c3:Claimant)-[r2:FILED_CLAIM]->(cl:Claim)
                WHERE c3 IN ring_members
                
                // 3. Linked Entities (Providers, Vehicles, Witnesses) from these Claims
                OPTIONAL MATCH (cl)-[r3]->(linked)
                WHERE type(r3) IN ['REPAIRED_AT', 'TREATED_BY', 'REPRESENTED_BY', 'HAS_WITNESS', 'INVOLVES_VEHICLE']
                
                RETURN 
                    collect(distinct {start: startNode(r1), rel: r1, end: endNode(r1)}) as internal_rels,
                    collect(distinct {start: startNode(r2), rel: r2, end: endNode(r2)}) as claim_rels,
                    collect(distinct {start: startNode(r3), rel: r3, end: endNode(r3)}) as linked_rels
            """, ring_id=ring_id)
            
            record = result.single()
            
            # Initialize Pyvis network
            net = Network(height="600px", width="100%", bgcolor="#ffffff", font_color="black", cdn_resources='remote')
            net.force_atlas_2based()
            
            seen_nodes = set()
            
            def add_graph_element(start_node, relation, end_node):
                if start_node is None or end_node is None:
                    return

                # Process Start Node
                start_dict = dict(start_node)
                start_id = self._get_node_id(start_dict)
                start_type = list(start_node.labels)[0] if hasattr(start_node, 'labels') else 'Unknown'
                
                if start_id not in seen_nodes:
                    net.add_node(
                        start_id,
                        label=self._get_node_label(start_dict, start_type),
                        title=self._get_node_tooltip(start_dict, start_type),
                        color=self._get_node_color(start_type),
                        shape=self._get_node_shape(start_type),
                        size=25 if start_type == 'Claimant' else 20
                    )
                    seen_nodes.add(start_id)

                # Process End Node
                end_dict = dict(end_node)
                end_id = self._get_node_id(end_dict)
                end_type = list(end_node.labels)[0] if hasattr(end_node, 'labels') else 'Unknown'
                
                if end_id not in seen_nodes:
                    net.add_node(
                        end_id,
                        label=self._get_node_label(end_dict, end_type),
                        title=self._get_node_tooltip(end_dict, end_type),
                        color=self._get_node_color(end_type),
                        shape=self._get_node_shape(end_type),
                        size=25 if end_type == 'Claimant' else 20
                    )
                    seen_nodes.add(end_id)
                
                # Add Edge
                if relation is not None:
                    try:
                        edge_label = relation.type.replace('_', ' ').title()
                        net.add_edge(
                            start_id,
                            end_id,
                            label=edge_label,
                            arrows='to',
                            title=f"Relationship: {edge_label}"
                        )
                    except Exception:
                        pass

            # Process all collected relationships
            for group in ['internal_rels', 'claim_rels', 'linked_rels']:
                if record[group]:
                    for item in record[group]:
                        if item['rel'] is not None:
                            add_graph_element(item['start'], item['rel'], item['end'])
            
            # Generate temporary HTML file
            try:
                temp_dir = tempfile.mkdtemp()
                path = os.path.join(temp_dir, "fraud_ring_network.html")
                net.save_graph(path)
                
                with open(path, 'r', encoding='utf-8') as f:
                    html_content = f.read()
                return html_content
            except Exception as e:
                logger.error(f"Error generating graph HTML: {e}")
                return None

    def visualize_filtered_fraud_ring(self, ring_id: str, exclude_types: List[str] = None):
        """Visualize a specific fraud ring with filtering"""
        if exclude_types is None:
            exclude_types = []
            
        with self.driver.session() as session:
            # Build where clause for filtering 3rd hop
            # If a type is excluded, we don't fetch relationships pointing to it
            
            # Expanded query to get internal connections + claims + providers/vehicles
            result = session.run("""
                MATCH (c:Claimant {fraud_ring_id: $ring_id})
                WITH collect(c) as ring_members
                
                // 1. Internal Ring Connections (Direct)
                OPTIONAL MATCH (c1:Claimant)-[r1]-(c2:Claimant)
                WHERE c1 IN ring_members AND c2 IN ring_members AND elementId(c1) < elementId(c2)
                
                // 2. Claims filed by members
                OPTIONAL MATCH (c3:Claimant)-[r2:FILED_CLAIM]->(cl:Claim)
                WHERE c3 IN ring_members
                
                // 3. Linked Entities (Providers, Vehicles, Witnesses) from these Claims
                OPTIONAL MATCH (cl)-[r3]->(linked)
                WHERE type(r3) IN ['REPAIRED_AT', 'TREATED_BY', 'REPRESENTED_BY', 'HAS_WITNESS', 'INVOLVES_VEHICLE']
                
                RETURN 
                    collect(distinct {start: startNode(r1), rel: r1, end: endNode(r1)}) as internal_rels,
                    collect(distinct {start: startNode(r2), rel: r2, end: endNode(r2)}) as claim_rels,
                    collect(distinct {start: startNode(r3), rel: r3, end: endNode(r3)}) as linked_rels
            """, ring_id=ring_id)
            
            record = result.single()
            
            # Initialize Pyvis network
            net = Network(height="600px", width="100%", bgcolor="#ffffff", font_color="black", cdn_resources='remote')
            net.force_atlas_2based()
            
            seen_nodes = set()
            
            def add_graph_element(start_node, relation, end_node):
                if start_node is None or end_node is None:
                    return

                # Process Start Node
                start_type = list(start_node.labels)[0] if hasattr(start_node, 'labels') else 'Unknown'
                if start_type in exclude_types:
                    return

                start_dict = dict(start_node)
                start_id = self._get_node_id(start_dict)
                
                if start_id not in seen_nodes:
                    net.add_node(
                        start_id,
                        label=self._get_node_label(start_dict, start_type),
                        title=self._get_node_tooltip(start_dict, start_type),
                        color=self._get_node_color(start_type),
                        shape=self._get_node_shape(start_type),
                        size=25 if start_type == 'Claimant' else 20
                    )
                    seen_nodes.add(start_id)

                # Process End Node
                end_type = list(end_node.labels)[0] if hasattr(end_node, 'labels') else 'Unknown'
                if end_type in exclude_types:
                    return

                end_dict = dict(end_node)
                end_id = self._get_node_id(end_dict)
                
                if end_id not in seen_nodes:
                    net.add_node(
                        end_id,
                        label=self._get_node_label(end_dict, end_type),
                        title=self._get_node_tooltip(end_dict, end_type),
                        color=self._get_node_color(end_type),
                        shape=self._get_node_shape(end_type),
                        size=25 if end_type == 'Claimant' else 20
                    )
                    seen_nodes.add(end_id)
                
                # Add Edge
                if relation is not None:
                    try:
                        edge_label = relation.type.replace('_', ' ').title()
                        net.add_edge(
                            start_id,
                            end_id,
                            label=edge_label,
                            arrows='to',
                            title=f"Relationship: {edge_label}"
                        )
                    except Exception:
                        pass

            # Process all collected relationships
            for group in ['internal_rels', 'claim_rels', 'linked_rels']:
                if record[group]:
                    for item in record[group]:
                        if item['rel'] is not None:
                            add_graph_element(item['start'], item['rel'], item['end'])
            
            # Generate temporary HTML file
            try:
                temp_dir = tempfile.mkdtemp()
                path = os.path.join(temp_dir, "fraud_ring_network.html")
                net.save_graph(path)
                
                with open(path, 'r', encoding='utf-8') as f:
                    html_content = f.read()
                return html_content
            except Exception as e:
                logger.error(f"Error generating graph HTML: {e}")
                return None

    def visualize_claim_connections(self, claim_id: str, depth: int = 2):
        """Visualize connections around a specific claim using Pyvis"""
        
        with self.driver.session() as session:
            # We use f-string injection for depth because it can't be a parameter in the path pattern
            result = session.run(f"""
                MATCH (cl:Claim {{claim_id: $claim_id}})
                OPTIONAL MATCH path = (cl)-[*1..{depth}]-(connected)
                WITH cl, collect(distinct connected) as nodes, collect(distinct relationships(path)) as rels
                UNWIND nodes as node
                UNWIND rels as relList
                UNWIND relList as rel
                RETURN 
                    cl,
                    collect(distinct node) as connected_nodes,
                    collect(distinct {{
                        type: type(rel),
                        start: startNode(rel),
                        end: endNode(rel)
                    }}) as relationships
            """, claim_id=claim_id)
            
            record = result.single()
            if not record:
                return None
            
            # Initialize Pyvis network
            net = Network(height="600px", width="100%", bgcolor="#ffffff", font_color="black", cdn_resources='remote')
            net.force_atlas_2based()
            
            seen_nodes = set()
            
            # Add central claim
            claim = dict(record['cl'])
            
            net.add_node(
                claim_id,
                label=self._get_node_label(claim, 'Claim'),
                title=self._get_node_tooltip(claim, 'Claim'),
                color='orange',
                shape='diamond',
                size=25  # Slightly larger central node
            )
            seen_nodes.add(claim_id)
            
            # Add connected nodes
            for node in record['connected_nodes']:
                if node is not None:
                    node_dict = dict(node)
                    node_id = self._get_node_id(node_dict)
                    node_type = list(node.labels)[0] if hasattr(node, 'labels') else 'Unknown'
                    
                    if node_id not in seen_nodes:
                        net.add_node(
                            node_id,
                            label=self._get_node_label(node_dict, node_type),
                            title=self._get_node_tooltip(node_dict, node_type),
                            color=self._get_node_color(node_type),
                            shape=self._get_node_shape(node_type)
                        )
                        seen_nodes.add(node_id)
            
            # Add edges
            for rel in record['relationships']:
                start_id = self._get_node_id(dict(rel['start']))
                end_id = self._get_node_id(dict(rel['end']))
                
                if start_id in seen_nodes and end_id in seen_nodes:
                    try:
                        edge_label = rel['type'].replace('_', ' ').title()
                        net.add_edge(
                            start_id, 
                            end_id,
                            label=edge_label,
                            arrows='to',
                            title=f"Relationship: {edge_label}"
                        )
                    except Exception:
                        pass

            # Generate HTML
            try:
                temp_dir = tempfile.mkdtemp()
                path = os.path.join(temp_dir, "claim_network.html")
                net.save_graph(path)
                
                with open(path, 'r', encoding='utf-8') as f:
                    html_content = f.read()
                return html_content
            except Exception as e:
                logger.error(f"Error generating graph HTML: {e}")
                return None

    def visualize_provider_network(self, entity_id: str, pattern_type: str):
        """
        Visualize ego network for a specific provider/shop/witness.
        Shows the central entity, claims linked to it, and claimants linked to those claims.
        Also shows internal connections between the claimants to highlight the 'ring' nature.
        """
        
        # Determine relationship type based on pattern
        rel_type_filter = ""
        if 'repair' in pattern_type:
            rel_type_filter = "[:REPAIRED_AT]"
        elif 'medical' in pattern_type:
            rel_type_filter = "[:TREATED_BY]"
        elif 'witness' in pattern_type:
            rel_type_filter = "[:HAS_WITNESS]"
        else:
            rel_type_filter = "--" # Generic Fallback

        with self.driver.session() as session:
            result = session.run(f"""
                // 1. Find Central Entity
                MATCH (center) WHERE center.shop_id = $id OR center.provider_id = $id OR center.witness_id = $id
                
                // 2. Find Claims linked to this entity
                MATCH (center)<-[r1]-(cl:Claim)
                
                // 3. Find Claimants who filed these claims
                MATCH (cl)<-[r2:FILED_CLAIM]-(c:Claimant)
                
                WITH center, collect(distinct cl) as claims, collect(distinct c) as claimants, 
                     collect(distinct {{start: cl, rel: r1, end: center}}) as rels1,
                     collect(distinct {{start: c, rel: r2, end: cl}}) as rels2
                
                // 4. Find Connections between these Claimants (The "Ring" element)
                OPTIONAL MATCH (c1:Claimant)-[r3:RELATED_TO|SHARES_ADDRESS|SHARES_PHONE]-(c2:Claimant)
                WHERE c1 IN claimants AND c2 IN claimants AND elementId(c1) < elementId(c2)
                
                RETURN 
                    center,
                    claims, 
                    claimants,
                    rels1 + rels2 as primary_rels,
                    collect(distinct {{start: startNode(r3), rel: r3, end: endNode(r3)}}) as internal_rels
            """, id=entity_id)
            
            record = result.single()
            if not record:
                return None
            
            # Initialize Pyvis network
            net = Network(height="500px", width="100%", bgcolor="#ffffff", font_color="black", cdn_resources='remote')
            net.force_atlas_2based()
            
            seen_nodes = set()
            
            # Helper to add node safely
            def add_node_safe(node):
                if node is None: return
                node_dict = dict(node)
                node_id = self._get_node_id(node_dict)
                node_type = list(node.labels)[0] if hasattr(node, 'labels') else 'Unknown'
                
                if node_id not in seen_nodes:
                    # Central entity highlight
                    size = 40 if node_id == entity_id else (25 if node_type == 'Claimant' else 20)
                    color = '#ff0000' if node_id == entity_id else self._get_node_color(node_type)
                    
                    net.add_node(
                        node_id,
                        label=self._get_node_label(node_dict, node_type),
                        title=self._get_node_tooltip(node_dict, node_type),
                        color=color,
                        shape=self._get_node_shape(node_type),
                        size=size
                    )
                    seen_nodes.add(node_id)
                return node_id

            # Helper to add edge
            def add_edge_safe(rel_item):
                if rel_item['rel'] is None: return
                start_id = add_node_safe(rel_item['start'])
                end_id = add_node_safe(rel_item['end'])
                
                if start_id and end_id:
                    try:
                        lbl = rel_item['rel'].type.replace('_', ' ').title()
                        net.add_edge(start_id, end_id, title=lbl, label=lbl, arrows='to')
                    except: pass

            # Process Data
            center_id = add_node_safe(record['center'])
            
            for item in record['primary_rels']:
                add_edge_safe(item)
                
            for item in record['internal_rels']:
                add_edge_safe(item)
                
            # Generate HTML
            try:
                temp_dir = tempfile.mkdtemp()
                path = os.path.join(temp_dir, f"entity_network_{entity_id}.html")
                net.save_graph(path)
                with open(path, 'r', encoding='utf-8') as f:
                    return f.read()
            except Exception as e:
                logger.error(f"Error generating graph: {e}")
                return None

    def _get_node_id(self, node_dict: Dict) -> str:
        """Extract unique ID from node"""
        for key in ['claimant_id', 'claim_id', 'policy_id', 'vehicle_id', 
                    'shop_id', 'provider_id', 'lawyer_id', 'witness_id']:
            if key in node_dict:
                return node_dict[key]
        return str(node_dict)

    def _get_node_label(self, node_dict: Dict, node_type: str) -> str:
        """Get label for node"""
        if node_type == 'Claimant':
            return node_dict.get('name', 'Unknown')
        elif node_type in ['RepairShop', 'MedicalProvider', 'Lawyer', 'Witness']:
            return node_dict.get('name', 'Unknown')
        elif node_type == 'Claim':
            cid = node_dict.get('claim_id', 'Unknown')
            amt = node_dict.get('claim_amount', 0)
            return f"{cid}\n${amt:,.0f}"
        else:
            return node_type

    def _get_node_tooltip(self, node_dict: Dict, node_type: str) -> str:
        """Generate friendly text tooltip"""
        lines = []
        lines.append(f"Role: {node_type}")
        
        if node_type == 'Claimant':
            lines.append(f"Name: {node_dict.get('name', 'N/A')}")
            lines.append(f"Age: {node_dict.get('age', 'N/A')}")
            if node_dict.get('is_fraud_ring'):
                lines.append("⚠️ FLAGGED: Suspected Fraud Ring Member")
                
        elif node_type == 'Claim':
            lines.append(f"Claim ID: {node_dict.get('claim_id', 'N/A')}")
            lines.append(f"Amount: ${node_dict.get('claim_amount', 0):,.2f}")
            lines.append(f"Date: {node_dict.get('claim_date', '')[:10]}")
            lines.append(f"Status: {node_dict.get('status', 'Open')}")
            desc = node_dict.get('description', '')
            if desc:
                lines.append(f"Details: {desc}")
        
        elif node_type == 'Vehicle':
             lines.append(f"Make: {node_dict.get('make', 'N/A')} {node_dict.get('model', '')}")
             lines.append(f"Year: {node_dict.get('year', 'N/A')}")
             lines.append(f"VIN: {node_dict.get('vin', 'N/A')}")
             lines.append(f"Engine #: {node_dict.get('engine_no', 'N/A')}")

        elif node_type in ['RepairShop', 'MedicalProvider', 'Lawyer']:
             lines.append(f"Name: {node_dict.get('name', 'N/A')}")
             lines.append(f"Location: {node_dict.get('city', 'N/A')}")
             if node_type == 'RepairShop':
                 lines.append(f"ID: {node_dict.get('shop_id', 'N/A')}")
             if node_type == 'MedicalProvider':
                 lines.append(f"Specialty: {node_dict.get('specialty', 'N/A')}")
        
        return "\n".join(lines)

    def _get_node_color(self, node_type: str) -> str:
        colors = {
            'Claimant': '#ff0000',      # Red
            'Claim': '#ffa500',         # Orange
            'Policy': '#0000ff',        # Blue
            'Vehicle': '#008000',       # Green
            'RepairShop': '#800080',    # Purple
            'MedicalProvider': '#00ffff', # Cyan
            'Lawyer': '#ffc0cb',        # Pink
            'Witness': '#ffff00'        # Yellow
        }
        return colors.get(node_type, '#808080')

    def _get_node_shape(self, node_type: str) -> str:
        shapes = {
            'Claimant': 'dot',
            'Claim': 'diamond',
            'Policy': 'box',
            'Vehicle': 'triangle',
            'RepairShop': 'square',
            'MedicalProvider': 'star',
            'Lawyer': 'hexagon',
            'Witness': 'ellipse'
        }
        return shapes.get(node_type, 'dot')

    def _get_node_label(self, node_dict: Dict, node_type: str) -> str:
        """Get label for node"""
        if node_type == 'Claimant':
            return node_dict.get('name', 'Unknown')
        elif node_type in ['RepairShop', 'MedicalProvider', 'Lawyer', 'Witness']:
            return node_dict.get('name', 'Unknown')
        elif node_type == 'Claim':
            # Show Claim ID and Amount
            cid = node_dict.get('claim_id', 'Unknown')
            amt = node_dict.get('claim_amount', 0)
            return f"{cid}\n${amt:,.0f}"
        else:
            return node_type

    def _get_node_tooltip(self, node_dict: Dict, node_type: str) -> str:
        """Generate friendly text tooltip"""
        # We use plain text with newlines which is robust and readable
        lines = []
        
        # Role & ID
        lines.append(f"Role: {node_type}")
        
        # Specific Business Info
        if node_type == 'Claimant':
            lines.append(f"Name: {node_dict.get('name', 'N/A')}")
            lines.append(f"Age: {node_dict.get('age', 'N/A')}")
            if node_dict.get('is_fraud_ring'):
                lines.append("⚠️ FLAGGED: Suspected Fraud Ring Member")
                
        elif node_type == 'Claim':
            lines.append(f"Claim ID: {node_dict.get('claim_id', 'N/A')}")
            lines.append(f"Amount: ${node_dict.get('claim_amount', 0):,.2f}")
            lines.append(f"Date: {node_dict.get('claim_date', '')[:10]}")
            lines.append(f"Status: {node_dict.get('status', 'Open')}")
            desc = node_dict.get('description', '')
            if desc:
                lines.append(f"Details: {desc}")
        
        elif node_type == 'Vehicle':
             lines.append(f"Make: {node_dict.get('make', 'N/A')} {node_dict.get('model', '')}")
             lines.append(f"Year: {node_dict.get('year', 'N/A')}")
             lines.append(f"VIN: {node_dict.get('vin', 'N/A')}")
             lines.append(f"Engine #: {node_dict.get('engine_no', 'N/A')}")

        elif node_type in ['RepairShop', 'MedicalProvider', 'Lawyer']:
             lines.append(f"Name: {node_dict.get('name', 'N/A')}")
             lines.append(f"Location: {node_dict.get('city', 'N/A')}")
             if node_type == 'RepairShop':
                 lines.append(f"ID: {node_dict.get('shop_id', 'N/A')}")
             if node_type == 'MedicalProvider':
                 lines.append(f"Specialty: {node_dict.get('specialty', 'N/A')}")
        
        return "\n".join(lines)

    def _get_node_color(self, node_type: str) -> str:
        colors = {
            'Claimant': '#ff0000',      # Red
            'Claim': '#ffa500',         # Orange
            'Policy': '#0000ff',        # Blue
            'Vehicle': '#008000',       # Green
            'RepairShop': '#800080',    # Purple
            'MedicalProvider': '#00ffff', # Cyan
            'Lawyer': '#ffc0cb',        # Pink
            'Witness': '#ffff00'        # Yellow
        }
        return colors.get(node_type, '#808080') # Gray

    def _get_node_shape(self, node_type: str) -> str:
        shapes = {
            'Claimant': 'dot',
            'Claim': 'diamond',
            'Policy': 'box',
            'Vehicle': 'triangle',
            'RepairShop': 'square',
            'MedicalProvider': 'star',
            'Lawyer': 'hexagon',
            'Witness': 'ellipse'
        }
        return shapes.get(node_type, 'dot')
