"""
Discovered Rings Page - Review and manage auto insurance fraud rings
"""
import streamlit as st
import pandas as pd
from streamlit_agraph import agraph, Node, Edge, Config

from data.neo4j_driver import get_neo4j_driver
from utils.logger import setup_logger

# ------------------------------------------------------------------
# Setup
# ------------------------------------------------------------------
logger = setup_logger(__name__)
st.set_page_config(page_title="Discovered Rings", page_icon="🕸️", layout="wide")
driver = get_neo4j_driver()

# ------------------------------------------------------------------
# Network color palette (single source of truth)
# ------------------------------------------------------------------
COLORS = {
    "Claimant": "#5DADE2",
    "Claim_High": "#EC7063",
    "Claim_Low": "#F4D03F",
    "Vehicle": "#AAB7B8",
    "BodyShop": "#E67E22",
    "Medical": "#58D68D",
    "Attorney": "#AF7AC5",
    "Witness": "#F5B041",
    "Location": "#85929E",
}

# ------------------------------------------------------------------
# Page entry guard (prevents overlay opening by default)
# ------------------------------------------------------------------
PAGE_KEY = "discovered_rings_page_loaded"
if PAGE_KEY not in st.session_state:
    st.session_state.active_ring_id = None
    st.session_state.selected_node_id = None
    st.session_state[PAGE_KEY] = True


# ------------------------------------------------------------------
# Legend with integrated entity filters (stateful)
# ------------------------------------------------------------------
def render_network_legend():
    legend_items = [
        ("Claimant", "Claimants", COLORS["Claimant"]),
        ("Claim", "Claims", COLORS["Claim_High"]),
        ("Vehicle", "Vehicles", COLORS["Vehicle"]),
        ("BodyShop", "Body Shops", COLORS["BodyShop"]),
        ("Medical", "Medical Providers", COLORS["Medical"]),
        ("Attorney", "Attorneys", COLORS["Attorney"]),
        ("Witness", "Witnesses", COLORS["Witness"]),
        ("Location", "Accident Locations", COLORS["Location"]),
    ]

    entity_filters = {}

    for key, label, color in legend_items:
        cols = st.columns([0.25, 0.75])

        with cols[0]:
            entity_filters[key] = st.checkbox(
                "",
                value=st.session_state.get(f"legend_toggle_{key}", True),
                key=f"legend_toggle_{key}",
            )

        with cols[1]:
            st.markdown(
                f"""
                <div style="display:flex;align-items:center;">
                    <div style="
                        width:14px;
                        height:14px;
                        border-radius:50%;
                        background-color:{color};
                        margin-right:8px;
                    "></div>
                    <span>{label}</span>
                </div>
                """,
                unsafe_allow_html=True,
            )

    return entity_filters


# ------------------------------------------------------------------
# Overlay dialog
# ------------------------------------------------------------------
@st.dialog("🕸️ Fraud Ring Network", width="large")
def ring_network_dialog(ring_id: str):
    render_ring_graph(ring_id)


# ------------------------------------------------------------------
# Graph renderer
# ------------------------------------------------------------------
def render_ring_graph(ring_id: str):
    st.markdown(f"### 🕸️ Network Visualization — {ring_id}")

    try:
        col_legend, col_graph, col_inspector = st.columns([1.2, 4.6, 1.8])

        # ---------------- LEFT: Controls + Legend ----------------
        with col_legend:
            st.markdown("### 📏 Network Depth")
            network_depth = st.slider(
                "Expand Network",
                min_value=1,
                max_value=4,
                value=3,
                help="Controls how far the network expands from the ring",
            )

            st.markdown("---")
            st.markdown("### 🗺️ Legend")
            entity_filters = render_network_legend()

        # ---------------- CENTER: Graph ----------------
        with col_graph:
            query = """
            MATCH (r:FraudRing {ring_id: $ring_id})<-[:MEMBER_OF]-(c:Claimant)
            OPTIONAL MATCH (c)-[:FILED]->(cl:Claim)
            OPTIONAL MATCH (cl)-[:REPAIRED_AT]->(b:BodyShop)
            OPTIONAL MATCH (cl)-[:TREATED_BY]->(m:MedicalProvider)
            OPTIONAL MATCH (cl)-[:REPRESENTED_BY]->(a:Attorney)
            OPTIONAL MATCH (cl)-[:INVOLVES_VEHICLE]->(v:Vehicle)
            OPTIONAL MATCH (cl)-[:OCCURRED_AT]->(l:AccidentLocation)
            OPTIONAL MATCH (w:Witness)-[:WITNESSED]->(cl)
            RETURN r, c, cl, b, m, a, v, l, w
            """

            results = driver.execute_query(query, {"ring_id": ring_id})

            nodes, edges = [], []
            added_node_ids = set()

            def add_node(node_id, label, color, icon, title):
                if node_id not in added_node_ids:
                    nodes.append(
                        Node(
                            id=node_id,
                            label=label,
                            size=25,
                            color=color,
                            symbolType=icon,
                            title=title,
                        )
                    )
                    added_node_ids.add(node_id)

            for record in results:
                c = record["c"]
                cl = record["cl"]
                v = record["v"]
                b = record["b"]
                m = record["m"]
                a = record["a"]
                w = record["w"]
                l = record["l"]

                # Claimant (Depth ≥ 1)
                if c and network_depth >= 1 and entity_filters["Claimant"]:
                    add_node(
                        c["claimant_id"],
                        c["name"],
                        COLORS["Claimant"],
                        "person",
                        f"<b>Claimant</b><br>{c['name']}",
                    )

                # Claim (Depth ≥ 2)
                if cl and network_depth >= 2 and entity_filters["Claim"]:
                    risk = cl.get("risk_score", 0)
                    color = COLORS["Claim_High"] if risk >= 70 else COLORS["Claim_Low"]
                    add_node(
                        cl["claim_id"],
                        cl["claim_number"],
                        color,
                        "file-text",
                        f"<b>Claim</b><br>{cl['claim_number']}<br>Risk: {risk}",
                    )
                    if c and entity_filters["Claimant"]:
                        edges.append(
                            Edge(c["claimant_id"], cl["claim_id"], color="#BDC3C7")
                        )

                # Vehicle
                if v and cl and network_depth >= 3 and entity_filters["Vehicle"]:
                    label = f"{v['make']} {v['model']}"
                    add_node(
                        v["vehicle_id"],
                        label,
                        COLORS["Vehicle"],
                        "car",
                        f"<b>Vehicle</b><br>{label}<br>{v['vin']}",
                    )
                    edges.append(Edge(cl["claim_id"], v["vehicle_id"], "#BDC3C7"))

                # Body Shop
                if b and cl and network_depth >= 3 and entity_filters["BodyShop"]:
                    add_node(
                        b["body_shop_id"],
                        b["name"],
                        COLORS["BodyShop"],
                        "wrench",
                        f"<b>Body Shop</b><br>{b['name']}",
                    )
                    edges.append(Edge(cl["claim_id"], b["body_shop_id"], "#BDC3C7"))

                # Medical Provider
                if m and cl and network_depth >= 3 and entity_filters["Medical"]:
                    add_node(
                        m["provider_id"],
                        m["name"],
                        COLORS["Medical"],
                        "medkit",
                        f"<b>Medical Provider</b><br>{m['name']}",
                    )
                    edges.append(Edge(cl["claim_id"], m["provider_id"], "#BDC3C7"))

                # Attorney
                if a and cl and network_depth >= 3 and entity_filters["Attorney"]:
                    add_node(
                        a["attorney_id"],
                        a["name"],
                        COLORS["Attorney"],
                        "briefcase",
                        f"<b>Attorney</b><br>{a['name']}",
                    )
                    edges.append(Edge(cl["claim_id"], a["attorney_id"], "#BDC3C7"))

                # Witness
                if w and cl and network_depth >= 3 and entity_filters["Witness"]:
                    add_node(
                        w["witness_id"],
                        w["name"],
                        COLORS["Witness"],
                        "eye",
                        f"<b>Witness</b><br>{w['name']}",
                    )
                    edges.append(Edge(w["witness_id"], cl["claim_id"], "#BDC3C7"))

                # Accident Location
                if l and cl and network_depth >= 3 and entity_filters["Location"]:
                    label = l.get("intersection") or l.get("location_id", "Location")
                    tooltip = "<br>".join(
                        [str(v) for v in [l.get("intersection"), l.get("city")] if v]
                    )
                    add_node(
                        l["location_id"],
                        label,
                        COLORS["Location"],
                        "map-marker",
                        f"<b>Accident Location</b><br>{tooltip}",
                    )
                    edges.append(Edge(cl["claim_id"], l["location_id"], "#BDC3C7"))

            config = Config(
                height=600,
                directed=True,
                physics={
                    "enabled": True,
                    "solver": "forceAtlas2Based",
                    "stabilization": {"enabled": True, "iterations": 200},
                },
            )

            selected = agraph(nodes=nodes, edges=edges, config=config)
            if selected:
                st.session_state.selected_node_id = selected
                st.session_state.active_ring_id = ring_id

        # ---------------- RIGHT: Inspector ----------------
        with col_inspector:
            st.markdown("### 🕵️ Inspector")
            if st.session_state.selected_node_id:
                render_node_details(st.session_state.selected_node_id)
            else:
                st.info("Select a node to view details.")

    except Exception as e:
        st.error(f"Error rendering graph: {e}")
        logger.exception(e)


# ------------------------------------------------------------------
# Inspector
# ------------------------------------------------------------------
def render_node_details(node_id: str):
    query = """
    MATCH (n)
    WHERE n.claimant_id = $id
       OR n.claim_id = $id
       OR n.vehicle_id = $id
       OR n.body_shop_id = $id
       OR n.provider_id = $id
       OR n.attorney_id = $id
       OR n.witness_id = $id
       OR n.location_id = $id
    RETURN n, labels(n) AS labels LIMIT 1
    """
    results = driver.execute_query(query, {"id": node_id})
    if not results:
        st.warning("No details found.")
        return

    node = dict(results[0]["n"])
    label = results[0]["labels"][0]
    st.subheader(label)

    for k in ["created_at", "embedding"]:
        node.pop(k, None)

    if "risk_score" in node:
        st.metric("Risk Score", node.pop("risk_score"))

    if "total_claim_amount" in node:
        st.metric("Claim Amount", f"${node.pop('total_claim_amount'):,.2f}")

    st.dataframe(pd.DataFrame(node.items(), columns=["Property", "Value"]),
                 hide_index=True, use_container_width=True)


# ------------------------------------------------------------------
# MAIN PAGE (unchanged logic)
# ------------------------------------------------------------------
def main():
    st.title("🕸️ Discovered Auto Insurance Fraud Rings")
    st.markdown("**Review, analyze, and manage detected fraud networks**")

    if st.session_state.active_ring_id:
        ring_network_dialog(st.session_state.active_ring_id)

    with st.spinner("Loading fraud rings..."):
        rings = load_rings()

    display_summary_metrics(rings)
    st.markdown("---")
    display_rings(rings)


def load_rings(ring_type=None, pattern_type=None, status=None, min_confidence=0.0, min_members=2):
    """Load fraud rings with filters"""
    try:
        query = """
        MATCH (r:FraudRing)
        WHERE r.confidence_score >= $min_confidence
          AND r.member_count >= $min_members
        """
        
        params = {
            'min_confidence': min_confidence,
            'min_members': min_members
        }
        
        # Add filters
        if ring_type:
            query += " AND r.ring_type IN $ring_types"
            params['ring_types'] = ring_type
        
        if pattern_type:
            query += " AND r.pattern_type IN $pattern_types"
            params['pattern_types'] = pattern_type
        
        if status:
            query += " AND r.status IN $statuses"
            params['statuses'] = status
        
        query += """
        RETURN 
            r.ring_id as ring_id,
            r.ring_type as ring_type,
            r.pattern_type as pattern_type,
            r.status as status,
            r.confidence_score as confidence_score,
            r.member_count as member_count,
            r.estimated_fraud_amount as estimated_fraud_amount
        ORDER BY r.confidence_score DESC, r.member_count DESC
        LIMIT 100
        """
        
        return driver.execute_query(query, params)
        
    except Exception as e:
        logger.error(f"Error loading rings: {e}", exc_info=True)
        st.error(f"Error loading rings: {str(e)}")
        return []


def display_summary_metrics(rings):
    """Display summary metrics for rings"""
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(
            "Total Rings",
            len(rings)
        )
    
    with col2:
        total_members = sum(r['member_count'] for r in rings)
        st.metric(
            "Total Members",
            total_members
        )
    
    with col3:
        total_fraud = sum(r.get('estimated_fraud_amount', 0) for r in rings)
        st.metric(
            "Estimated Fraud",
            f"${total_fraud:,.0f}"
        )
    
    with col4:
        confirmed = sum(1 for r in rings if r['status'] == 'CONFIRMED')
        st.metric(
            "Confirmed Rings",
            confirmed
        )


def display_rings(rings):
    """Display list of rings"""
    
    if not rings:
        return
    
    st.subheader(f"📋 Fraud Rings ({len(rings)})")
    
    # Pattern type mapping for display
    pattern_names = {
        'staged_accident': '🎭 Staged Accidents',
        'body_shop_fraud': '🔧 Body Shop Fraud',
        'medical_mill': '🏥 Medical Mill',
        'attorney_organized': '⚖️ Attorney Organized',
        'phantom_passenger': '👥 Phantom Passengers',
        'tow_truck_kickback': '🚛 Tow Truck Kickback',
        'mixed': '🔀 Mixed Patterns'
    }
    
    # Sort options
    sort_by = st.selectbox(
        "Sort by",
        options=['Confidence', 'Member Count', 'Fraud Amount'],
        index=0
    )
    
    # Sort rings
    if sort_by == 'Confidence':
        rings_sorted = sorted(rings, key=lambda x: x['confidence_score'], reverse=True)
    elif sort_by == 'Member Count':
        rings_sorted = sorted(rings, key=lambda x: x['member_count'], reverse=True)
    else:
        rings_sorted = sorted(rings, key=lambda x: x.get('estimated_fraud_amount', 0), reverse=True)
    
    # Display rings as cards
    for ring in rings_sorted:
        pattern_display = pattern_names.get(ring['pattern_type'], ring['pattern_type'])
        confidence = ring['confidence_score']
        
        # Color based on confidence
        if confidence >= 0.9:
            border_color = "#C0392B"  # Dark red - very high confidence
        elif confidence >= 0.8:
            border_color = "#E74C3C"  # Red - high confidence
        elif confidence >= 0.7:
            border_color = "#E67E22"  # Orange - medium-high
        else:
            border_color = "#F39C12"  # Yellow - medium
        
        with st.container():
            st.markdown(
                f"""
                <div style="
                    border-left: 6px solid {border_color};
                    padding: 5px;
                    margin-bottom: 10px;
                ">
                </div>
                """,
                unsafe_allow_html=True
            )
            
            with st.expander(
                f"{pattern_display} - **{ring['ring_id']}** ({ring['member_count']} members) - Confidence: {confidence:.1%}",
                expanded=False
            ):
                # Ring details
                col1, col2, col3, col4 = st.columns(4)
                
                with col1:
                    st.markdown("**Pattern Type**")
                    st.info(pattern_display)
                
                with col2:
                    st.markdown("**Status**")
                    status_color = "🟢" if ring['status'] == 'CONFIRMED' else "🟡" if ring['status'] == 'UNDER_REVIEW' else "🔴"
                    st.info(f"{status_color} {ring['status']}")
                
                with col3:
                    st.markdown("**Confidence Score**")
                    st.metric("", f"{confidence:.1%}")
                
                with col4:
                    st.markdown("**Estimated Fraud**")
                    st.metric("", f"${ring.get('estimated_fraud_amount', 0):,.0f}")
                
                # Get ring members
                st.markdown("---")
                st.markdown("**Ring Members:**")
                
                members_query = """
                MATCH (c:Claimant)-[:MEMBER_OF]->(r:FraudRing {ring_id: $ring_id})
                OPTIONAL MATCH (c)-[:FILED]->(cl:Claim)
                RETURN 
                    c.name as name,
                    c.claimant_id as claimant_id,
                    count(cl) as claim_count,
                    sum(cl.total_claim_amount) as total_claimed,
                    avg(cl.risk_score) as avg_risk
                ORDER BY total_claimed DESC
                """
                
                members = driver.execute_query(members_query, {'ring_id': ring['ring_id']})
                
                if members:
                    df = pd.DataFrame(members)
                    df['total_claimed'] = df['total_claimed'].apply(lambda x: f"${x:,.0f}")
                    df['avg_risk'] = df['avg_risk'].apply(lambda x: f"{x:.1f}")
                    st.dataframe(df, use_container_width=True, hide_index=True)
                
                # Action buttons
                st.markdown("---")
                
                col1, col2, col3 = st.columns(3)
                
                with col1:
                    if st.button("👁️ View Network", key=f"view_{ring['ring_id']}", use_container_width=True):
                        st.session_state['active_ring_id'] = ring['ring_id']
                        st.rerun()
                
                with col2:
                    if ring['status'] == 'UNDER_REVIEW':
                        if st.button("✅ Confirm", key=f"confirm_{ring['ring_id']}", use_container_width=True):
                            confirm_ring(ring['ring_id'])
                
                with col3:
                    if ring['status'] == 'UNDER_REVIEW':
                        if st.button("❌ Dismiss", key=f"dismiss_{ring['ring_id']}", use_container_width=True):
                            st.session_state[f'dismiss_ring_{ring["ring_id"]}'] = True
                
                # Show dismiss dialog if triggered
                if st.session_state.get(f'dismiss_ring_{ring["ring_id"]}'):
                    show_dismiss_dialog(ring['ring_id'])


def confirm_ring(ring_id: str):
    """Confirm a ring as fraud"""
    try:
        query = """
        MATCH (r:FraudRing {ring_id: $ring_id})
        SET r.status = 'CONFIRMED'
        RETURN r.ring_id as ring_id
        """
        
        driver.execute_write(query, {'ring_id': ring_id})
        st.success(f"Ring {ring_id} confirmed as fraud")
        st.rerun()
        
    except Exception as e:
        logger.error(f"Error confirming ring: {e}")
        st.error(f"Failed to confirm ring: {str(e)}")


def show_dismiss_dialog(ring_id: str):
    """Show dialog for dismissing ring"""
    
    st.markdown(f"### ❌ Dismiss Ring {ring_id}")
    
    reason = st.text_area(
        "Reason for dismissal",
        placeholder="Enter reason why this is not a fraud ring...",
        key=f"reason_{ring_id}"
    )
    
    col1, col2 = st.columns(2)
    
    with col1:
        if st.button("Confirm Dismissal", use_container_width=True, key=f"confirm_dismiss_{ring_id}"):
            if reason:
                dismiss_ring(ring_id, reason)
            else:
                st.error("Please provide a reason for dismissal")
    
    with col2:
        if st.button("Cancel", use_container_width=True, key=f"cancel_dismiss_{ring_id}"):
            del st.session_state[f'dismiss_ring_{ring_id}']
            st.rerun()


def dismiss_ring(ring_id: str, reason: str):
    """Dismiss a ring"""
    try:
        query = """
        MATCH (r:FraudRing {ring_id: $ring_id})
        SET r.status = 'DISMISSED',
            r.dismissal_reason = $reason
        RETURN r.ring_id as ring_id
        """
        
        driver.execute_write(query, {'ring_id': ring_id, 'reason': reason})
        st.success(f"Ring {ring_id} dismissed")
        
        if f'dismiss_ring_{ring_id}' in st.session_state:
            del st.session_state[f'dismiss_ring_{ring_id}']
        
        st.rerun()
        
    except Exception as e:
        logger.error(f"Error dismissing ring: {e}")
        st.error(f"Failed to dismiss ring: {str(e)}")


if __name__ == "__main__":
    main()