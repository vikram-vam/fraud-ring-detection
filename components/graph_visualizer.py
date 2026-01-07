"""
Graph Visualizer - Network graph visualization component
Creates interactive network graphs using Plotly
"""
import streamlit as st
import plotly.graph_objects as go
import networkx as nx
from typing import List, Dict, Optional
import logging

logger = logging.getLogger(__name__)


class GraphVisualizer:
    """
    Visualize entity relationships as interactive network graphs
    """
    
    def __init__(self):
        self.default_colors = {
            'Claimant': '#E74C3C',      # Red
            'Provider': '#3498DB',       # Blue
            'Attorney': '#9B59B6',       # Purple
            'Address': '#F39C12',        # Orange
            'Phone': '#1ABC9C',          # Teal
            'FraudRing': '#E67E22'       # Dark Orange
        }
    
    def render_network_graph(
        self,
        nodes: List[Dict],
        edges: List[Dict],
        title: str = "Entity Network",
        height: int = 600,
        highlight_nodes: Optional[List[str]] = None
    ):
        """
        Render an interactive network graph
        
        Args:
            nodes: List of node dictionaries with 'id', 'label', 'type'
            edges: List of edge dictionaries with 'source', 'target', 'type'
            title: Graph title
            height: Graph height in pixels
            highlight_nodes: List of node IDs to highlight
        """
        if not nodes:
            st.warning("No nodes to display in graph")
            return
        
        try:
            # Create NetworkX graph
            G = nx.Graph()
            
            # Add nodes
            for node in nodes:
                G.add_node(
                    node['id'],
                    label=node.get('label', node['id']),
                    node_type=node.get('type', 'Unknown')
                )
            
            # Add edges
            for edge in edges:
                if edge.get('source') and edge.get('target'):
                    G.add_edge(
                        edge['source'],
                        edge['target'],
                        edge_type=edge.get('type', 'CONNECTED')
                    )
            
            # Calculate layout
            pos = nx.spring_layout(G, k=1, iterations=50)
            
            # Create edge traces
            edge_trace = self._create_edge_trace(G, pos)
            
            # Create node traces (one per type for legend)
            node_traces = self._create_node_traces(G, pos, highlight_nodes)
            
            # Create figure
            fig = go.Figure(
                data=[edge_trace] + node_traces,
                layout=go.Layout(
                    title=title,
                    titlefont_size=16,
                    showlegend=True,
                    hovermode='closest',
                    margin=dict(b=20, l=5, r=5, t=40),
                    xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
                    yaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
                    height=height,
                    plot_bgcolor='rgba(0,0,0,0)',
                    paper_bgcolor='rgba(0,0,0,0)'
                )
            )
            
            # Display in Streamlit
            st.plotly_chart(fig, use_container_width=True)
            
            # Display graph statistics
            self._display_graph_stats(G)
            
        except Exception as e:
            logger.error(f"Error rendering network graph: {e}", exc_info=True)
            st.error(f"Error rendering graph: {str(e)}")
    
    def _create_edge_trace(self, G: nx.Graph, pos: Dict) -> go.Scatter:
        """Create edge trace for Plotly"""
        edge_x = []
        edge_y = []
        
        for edge in G.edges():
            x0, y0 = pos[edge[0]]
            x1, y1 = pos[edge[1]]
            edge_x.extend([x0, x1, None])
            edge_y.extend([y0, y1, None])
        
        return go.Scatter(
            x=edge_x,
            y=edge_y,
            line=dict(width=0.5, color='#888'),
            hoverinfo='none',
            mode='lines',
            showlegend=False
        )
    
    def _create_node_traces(
        self,
        G: nx.Graph,
        pos: Dict,
        highlight_nodes: Optional[List[str]] = None
    ) -> List[go.Scatter]:
        """Create node traces grouped by type"""
        highlight_nodes = highlight_nodes or []
        
        # Group nodes by type
        nodes_by_type = {}
        for node in G.nodes():
            node_type = G.nodes[node].get('node_type', 'Unknown')
            if node_type not in nodes_by_type:
                nodes_by_type[node_type] = []
            nodes_by_type[node_type].append(node)
        
        traces = []
        
        for node_type, node_list in nodes_by_type.items():
            node_x = []
            node_y = []
            node_text = []
            node_sizes = []
            
            for node in node_list:
                x, y = pos[node]
                node_x.append(x)
                node_y.append(y)
                
                label = G.nodes[node].get('label', node)
                degree = G.degree(node)
                node_text.append(f"{label}<br>Connections: {degree}")
                
                # Highlight specific nodes
                if node in highlight_nodes:
                    node_sizes.append(20)
                else:
                    node_sizes.append(15)
            
            color = self.default_colors.get(node_type, '#95A5A6')
            
            trace = go.Scatter(
                x=node_x,
                y=node_y,
                mode='markers',
                name=node_type,
                hoverinfo='text',
                text=node_text,
                marker=dict(
                    size=node_sizes,
                    color=color,
                    line=dict(width=2, color='white')
                )
            )
            
            traces.append(trace)
        
        return traces
    
    def _display_graph_stats(self, G: nx.Graph):
        """Display graph statistics"""
        with st.expander("📊 Graph Statistics"):
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                st.metric("Nodes", G.number_of_nodes())
            
            with col2:
                st.metric("Edges", G.number_of_edges())
            
            with col3:
                density = nx.density(G)
                st.metric("Density", f"{density:.3f}")
            
            with col4:
                if G.number_of_nodes() > 0:
                    avg_degree = sum(dict(G.degree()).values()) / G.number_of_nodes()
                    st.metric("Avg Connections", f"{avg_degree:.1f}")
    
    def render_ring_network(self, ring_data: Dict, height: int = 700):
        """
        Render network graph specifically for a fraud ring
        
        Args:
            ring_data: Dictionary with 'nodes' and 'edges' from ring repository
            height: Graph height
        """
        nodes = ring_data.get('nodes', [])
        edges = ring_data.get('edges', [])
        
        if not nodes:
            st.warning("No network data available for this ring")
            return
        
        # Identify ring members to highlight
        ring_members = [n['id'] for n in nodes if n.get('ring_member', False)]
        
        self.render_network_graph(
            nodes=nodes,
            edges=edges,
            title="Fraud Ring Network",
            height=height,
            highlight_nodes=ring_members
        )
    
    def render_ego_network(
        self,
        center_node_id: str,
        center_node_label: str,
        neighbors: List[Dict],
        title: str = "Entity Network"
    ):
        """
        Render ego network (centered on one entity)
        
        Args:
            center_node_id: Center node ID
            center_node_label: Center node label
            neighbors: List of neighbor dictionaries
            title: Graph title
        """
        nodes = [
            {
                'id': center_node_id,
                'label': center_node_label,
                'type': 'Center',
                'ring_member': True
            }
        ]
        
        edges = []
        
        for neighbor in neighbors:
            nodes.append({
                'id': neighbor.get('id'),
                'label': neighbor.get('label'),
                'type': neighbor.get('type', 'Unknown')
            })
            
            edges.append({
                'source': center_node_id,
                'target': neighbor.get('id'),
                'type': neighbor.get('relationship', 'CONNECTED')
            })
        
        self.render_network_graph(
            nodes=nodes,
            edges=edges,
            title=title,
            highlight_nodes=[center_node_id]
        )
