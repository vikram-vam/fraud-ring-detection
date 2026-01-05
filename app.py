"""
AUTO INSURANCE FRAUD RING DETECTION - STREAMLIT APPLICATION
Main entry point for the demo application
"""

import streamlit as st
from neo4j import GraphDatabase
import os
from dotenv import load_dotenv
from components.graph_visualizer import GraphVisualizer
from components.claim_form import ClaimForm
from components.fraud_scorer import FraudScorer
from fraud_detection import FraudDetector
import logging
import ssl
import streamlit.components.v1 as components

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Page configuration
st.set_page_config(
    page_title="Fraud Ring Detection System",
    page_icon="🔍",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: 700;
        color: #1f77b4;
        text-align: center;
        margin-bottom: 2rem;
    }
    .metric-card {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 0.5rem;
        border-left: 4px solid #1f77b4;
    }
    .high-risk {
        background-color: #ffe6e6;
        border-left-color: #d32f2f;
    }
    .medium-risk {
        background-color: #fff4e6;
        border-left-color: #f57c00;
    }
    .low-risk {
        background-color: #e6f7e6;
        border-left-color: #388e3c;
    }
    .stButton>button {
        width: 100%;
    }
</style>
""", unsafe_allow_html=True)

@st.cache_resource
def init_neo4j_connection():
    """Initialize Neo4j connection - works both locally (.env) and on Streamlit Cloud (secrets)"""
    # Try Streamlit secrets first (for Cloud deployment)
    try:
        uri = st.secrets["NEO4J_URI"]
        username = st.secrets["NEO4J_USERNAME"]
        password = st.secrets["NEO4J_PASSWORD"]
    except (KeyError, FileNotFoundError):
        # Fall back to .env file (for local development)
        uri = os.getenv('NEO4J_URI')
        username = os.getenv('NEO4J_USERNAME')
        password = os.getenv('NEO4J_PASSWORD')
    
    if not all([uri, username, password]):
        st.error("⚠️ Neo4j credentials not found. Set in .env (local) or Streamlit secrets (cloud).")
        st.stop()
    
    # Configure SSL context to handle self-signed certificates (AuraDB fix)
    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE
    
    # Handle different URI schemes for custom SSL context
    working_uri = uri
    if uri.startswith("neo4j+s://"):
        working_uri = uri.replace("neo4j+s://", "neo4j://")
    elif uri.startswith("bolt+s://"):
        working_uri = uri.replace("bolt+s://", "bolt://")
    
    try:
        driver = GraphDatabase.driver(
            working_uri, 
            auth=(username, password),
            encrypted=True,
            ssl_context=ssl_context
        )
        # Test connection
        with driver.session() as session:
            session.run("RETURN 1")
        logger.info("Neo4j connection established")
        return driver
    except Exception as e:
        st.error(f"❌ Failed to connect to Neo4j: {e}")
        st.stop()

def display_statistics(driver):
    """Display key business volume statistics in sidebar"""
    
    st.sidebar.markdown("### 📊 Business Volume")
    
    with driver.session() as session:
        # Business Scale Metrics
        stats = session.run("""
            MATCH (c:Claim)
            WITH count(c) as total_claims, avg(c.claim_amount) as avg_cost
            
            MATCH (u:Claimant)
            WITH total_claims, avg_cost, count(u) as total_customers
            
            MATCH (p) WHERE p:MedicalProvider OR p:RepairShop
            RETURN 
                total_claims, 
                avg_cost, 
                total_customers, 
                count(p) as provider_network
        """).single()
    
    col1, col2 = st.sidebar.columns(2)
    with col1:
        st.metric("Customers", f"{stats['total_customers']:,}", help="Total unique policyholders and claimants tracked")
        st.metric("Claims Vol", f"{stats['total_claims']:,}", help="Total number of claims processed in the system")
    
    with col2:
        st.metric("Providers", f"{stats['provider_network']:,}", help="Network size of Medical Providers and Repair Shops")
        st.metric("Avg Cost", f"${stats['avg_cost']:,.0f}", help="Average payout amount per claim across all types")



def main():
    """Main application function"""
    
    # Header
    st.markdown('<h1 class="main-header">🔍 Auto Insurance Fraud Ring Detection System</h1>', unsafe_allow_html=True)
    
    # Initialize connection
    driver = init_neo4j_connection()
    
    # Sidebar navigation
    st.sidebar.title("Navigation")
    page = st.sidebar.radio(
        "Select View",
        ["🏠 Dashboard", "🕸️ Fraud Ring Visualization", "📝 Add New Claim", "🔎 Claim Investigation"]
    )
    
    # Display statistics
    display_statistics(driver)
    
    # Sidebar info
    st.sidebar.markdown("---")
    st.sidebar.markdown("""
    ### About
    This system demonstrates advanced fraud ring detection using graph analytics.
    
    **Features:**
    - Real-time fraud ring visualization
    - Community detection algorithms
    - Pattern-based fraud identification
    - Dynamic fraud propensity scoring
    """)
    
    # Page routing
    if page == "🏠 Dashboard":
        show_dashboard(driver)
    elif page == "🕸️ Fraud Ring Visualization":
        show_fraud_rings(driver)
    elif page == "📝 Add New Claim":
        show_new_claim_form(driver)
    elif page == "🔎 Claim Investigation":
        show_claim_investigation(driver)

def show_dashboard(driver):
    """Display dashboard with overview"""
    st.header("Dashboard Overview")
    
    detector = FraudDetector(driver)
    
    # Key metrics
    col1, col2, col3, col4 = st.columns(4)
    
    # Fetch key metrics
    with driver.session() as session:
        # Basic Counts and Fraud Amounts
        basic_stats = session.run("""
            MATCH (c:Claim)
            RETURN 
                count(c) as total_claims,
                sum(c.claim_amount) as total_amount,
                count(CASE WHEN c.is_fraud_ring = true THEN 1 END) as fraud_claims,
                sum(CASE WHEN c.is_fraud_ring = true THEN c.claim_amount END) as fraud_amount
        """).single()
        
        # Fraud Centric Metrics: Active Rings
        fraud_stats = session.run("""
             MATCH (c:Claimant) WHERE c.is_fraud_ring = true 
             RETURN count(distinct c.fraud_ring_id) as active_rings
        """).single()
        
        # High Risk Providers (Connected to multiple fraud rings or high volume fraud)
        # We explicitly look for providers linked to multiple distinct rings OR high volume of flagged claims
        risk_providers = session.run("""
            MATCH (p)-[:TREATED_BY|REPAIRED_AT|REPRESENTED_BY]-(c:Claim)-[:FILED_CLAIM]-(u:Claimant)
            WHERE u.is_fraud_ring = true
            WITH p, count(distinct u.fraud_ring_id) as linked_rings, count(distinct c) as fraud_claims
            WHERE linked_rings >= 1 OR fraud_claims >= 3
            RETURN count(p) as high_risk_count
        """).single()

    # METRICS ROW
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        amount = basic_stats['fraud_amount'] if basic_stats['fraud_amount'] else 0
        st.metric("Total Detected Fraud", f"${amount:,.0f}", 
                 help="Total value of claims linked to confirmed fraud rings")
        
    with col2:
        st.metric("Active Fraud Rings", fraud_stats['active_rings'], 
                 help="Number of distinct organized fraud groups identified")
        
    with col3:
        st.metric("High Risk Providers", risk_providers['high_risk_count'], 
                 help="Service providers (Doctors, Shops) linked to suspicious activity")
                 
    with col4:
        total = basic_stats['total_claims']
        fraud = basic_stats['fraud_claims']
        fraud_pct = (fraud / total * 100) if total > 0 else 0
        st.metric("Suspicious Volume", f"{fraud_pct:.1f}%", help="% of Claims flagged as suspicious")
    
    st.markdown("---")
    
    # Fraud patterns detected
    st.subheader("🎯 Detected Fraud Patterns")
    
    fraud_rings = detector.detect_fraud_rings_by_patterns()
    visualizer = GraphVisualizer(driver) # Initialize here for use in loop
    
    
    # Session state for visualization modal
    if 'active_viz_html' not in st.session_state:
        st.session_state.active_viz_html = None
    if 'active_viz_title' not in st.session_state:
        st.session_state.active_viz_title = None

    # Function to clear viz
    def close_viz():
        st.session_state.active_viz_html = None
        st.session_state.active_viz_title = None

    # Modal Overlay (Custom implementation using expander at top if active)
    if st.session_state.active_viz_html:
        col_title, col_close = st.columns([8, 1])
        with col_title:
             st.markdown(f"### 🕸️ Network Analysis: {st.session_state.active_viz_title}")
        with col_close:
            # Subtle close button on the right
            st.button("✕", on_click=close_viz, help="Close Visualization")
        
        components.html(st.session_state.active_viz_html, height=600, scrolling=True)
        st.markdown("---") # Separator

    if fraud_rings:
        # Group by pattern type
        pattern_tabs = st.tabs(["Repair Shop Patterns", "Medical Provider Patterns", "Witness Patterns"])
        
        repair_patterns = [fr for fr in fraud_rings if fr['pattern_type'] == 'shared_repair_shop']
        medical_patterns = [fr for fr in fraud_rings if fr['pattern_type'] == 'shared_medical_provider']
        witness_patterns = [fr for fr in fraud_rings if fr['pattern_type'] == 'recurring_witness']
        
        with pattern_tabs[0]:
            if repair_patterns:
                for idx, pattern in enumerate(repair_patterns[:5]):
                    with st.expander(f"🔧 {pattern['entity_name']} - Score: {pattern['suspicion_score']}"):
                        col1, col2 = st.columns([3, 1])
                        with col1:
                            st.write(f"**Claimants Involved:** {len(pattern['claimant_ids'])}")
                            st.write(f"**Total Claims:** {pattern['claim_count']}")
                            st.write(f"**Internal Connections:** {pattern['connections']}")
                            st.write(f"**Suspicion Score:** {pattern['suspicion_score']}/10")
                        
                        # Visualization Button
                        if st.button(f"🕸️ View Network", key=f"btn_rep_{idx}"):
                             st.session_state.active_viz_title = pattern['entity_name']
                             st.session_state.active_viz_html = visualizer.visualize_provider_network(pattern['entity_id'], 'repair')
                             st.rerun()
            else:
                st.info("No suspicious repair shop patterns detected")
        
        with pattern_tabs[1]:
            if medical_patterns:
                for idx, pattern in enumerate(medical_patterns[:5]):
                    with st.expander(f"🏥 {pattern['entity_name']} - Score: {pattern['suspicion_score']}"):
                        col1, col2 = st.columns([3, 1])
                        with col1:
                            st.write(f"**Claimants Involved:** {len(pattern['claimant_ids'])}")
                            st.write(f"**Total Claims:** {pattern['claim_count']}")
                            st.write(f"**Internal Connections:** {pattern['connections']}")
                            st.write(f"**Suspicion Score:** {pattern['suspicion_score']}/10")
                        
                        if st.button(f"🕸️ View Network", key=f"btn_med_{idx}"):
                             st.session_state.active_viz_title = pattern['entity_name']
                             st.session_state.active_viz_html = visualizer.visualize_provider_network(pattern['entity_id'], 'medical')
                             st.rerun()
            else:
                st.info("No suspicious medical provider patterns detected")
        
        with pattern_tabs[2]:
            if witness_patterns:
                for idx, pattern in enumerate(witness_patterns[:5]):
                    with st.expander(f"👤 {pattern['entity_name']} - Score: {pattern['suspicion_score']}"):
                        col1, col2 = st.columns([3, 1])
                        with col1:
                            st.write(f"**Claims Witnessed:** {pattern['claim_count']}")
                            st.write(f"**Claimants Involved:** {len(pattern['claimant_ids'])}")
                            st.write(f"**Suspicion Score:** {pattern['suspicion_score']}/10")
                        
                        if st.button(f"🕸️ View Network", key=f"btn_wit_{idx}"):
                             st.session_state.active_viz_title = pattern['entity_name']
                             st.session_state.active_viz_html = visualizer.visualize_provider_network(pattern['entity_id'], 'witness')
                             st.rerun()
            else:
                st.info("No suspicious witness patterns detected")
    
    st.markdown("---")
    
    # Recent claims
    st.subheader("📋 Recent Claims")
    
    with driver.session() as session:
        result = session.run("""
            MATCH (c:Claimant)-[:FILED_CLAIM]->(cl:Claim)
            RETURN 
                cl.claim_id as claim_id,
                cl.claim_number as claim_number,
                c.name as claimant_name,
                cl.claim_date as claim_date,
                cl.claim_amount as amount,
                cl.claim_type as type,
                cl.status as status,
                c.is_fraud_ring as is_suspicious
            ORDER BY cl.claim_date DESC
            LIMIT 10
        """)
        
        claims_data = []
        for record in result:
            claims_data.append({
                'Claim ID': record['claim_id'],
                'Claim #': record['claim_number'],
                'Claimant': record['claimant_name'],
                'Date': record['claim_date'][:10],
                'Amount': f"${record['amount']:,.2f}",
                'Type': record['type'],
                'Status': record['status'],
                'Suspicious': '🚨' if record['is_suspicious'] else '✅'
            })
        
        import pandas as pd
        df = pd.DataFrame(claims_data)
        st.dataframe(df, use_container_width=True, hide_index=True)

def show_fraud_rings(driver):
    """Display fraud ring visualization"""
    st.header("Fraud Ring Network Visualization")
    
    # Fraud ring selector
    with driver.session() as session:
        result = session.run("""
            MATCH (c:Claimant)
            WHERE c.is_fraud_ring = true
            RETURN distinct c.fraud_ring_id as ring_id
            ORDER BY ring_id
        """)
        
        fraud_rings = [record['ring_id'] for record in result]
    
    if not fraud_rings:
        st.warning("No fraud rings found in the database")
        return
    
    # Two-column layout: Controls | Legend
    col_controls, col_legend = st.columns([2, 1])
    
    with col_controls:
        selected_ring = st.selectbox("Select Fraud Ring", fraud_rings)
        visualize_clicked = st.button("🔍 Visualize Network")
    
    with col_legend:
        # Compact HTML Legend with exact colors from GraphVisualizer
        st.markdown("""
        <div style="font-size:12px; line-height:1.6;">
        <b>Legend</b><br>
        <span style="color:#ff0000;">●</span> Claimant &nbsp;
        <span style="color:#ffa500;">◆</span> Claim &nbsp;
        <span style="color:#0000ff;">■</span> Policy<br>
        <span style="color:#008000;">▲</span> Vehicle &nbsp;
        <span style="color:#800080;">■</span> Repair Shop<br>
        <span style="color:#00ffff;">★</span> Medical Provider &nbsp;
        <span style="color:#ffc0cb;">⬢</span> Lawyer<br>
        <span style="color:#ffff00;">⬬</span> Witness
        </div>
        """, unsafe_allow_html=True)
    
    if visualize_clicked:
        with st.spinner("Generating network visualization..."):
            visualizer = GraphVisualizer(driver)
            fig = visualizer.visualize_fraud_ring(selected_ring)
            
            if fig:
                components.html(fig, height=600, scrolling=True)
                
                # Ring statistics
                st.subheader("Ring Statistics")
                
                with driver.session() as session:
                    result = session.run("""
                        MATCH (c:Claimant {fraud_ring_id: $ring_id})
                        OPTIONAL MATCH (c)-[:FILED_CLAIM]->(cl:Claim)
                        OPTIONAL MATCH (c)-[r:RELATED_TO|SHARES_ADDRESS|SHARES_PHONE]-(other:Claimant)
                        WHERE other.fraud_ring_id = $ring_id
                        RETURN 
                            count(distinct c) as members,
                            count(distinct cl) as claims,
                            sum(cl.claim_amount) as total_amount,
                            count(distinct r) as connections
                    """, ring_id=selected_ring)
                    
                    stats = result.single()
                    
                    col1, col2, col3, col4 = st.columns(4)
                    col1.metric("Members", stats['members'])
                    col2.metric("Claims Filed", stats['claims'] or 0)
                    col3.metric("Total Amount", f"${stats['total_amount'] or 0:,.2f}")
                    col4.metric("Internal Connections", stats['connections'] or 0)

def show_new_claim_form(driver):
    """Display form for adding new claim"""
    st.header("Add New Insurance Claim")
    
    claim_form = ClaimForm(driver)
    new_claim_data = claim_form.render()
    
    if new_claim_data:
        st.success(f"✅ Claim {new_claim_data['claim_id']} added successfully!")
        
        # Show fraud propensity
        st.subheader("🎯 Fraud Propensity Analysis")
        
        scorer = FraudScorer(driver)
        score_result = scorer.score_claim(new_claim_data['claim_id'])
        
        if 'error' not in score_result:
            # Display score with color coding
            risk_level = score_result['risk_level']
            score = score_result['fraud_propensity_score']
            
            risk_class = f"{risk_level.lower()}-risk"
            
            st.markdown(f"""
            <div class="metric-card {risk_class}">
                <h3 style="margin-top: 0;">Risk Level: {risk_level}</h3>
                <h1 style="margin: 1rem 0;">{score}/100</h1>
                <p><strong>Recommendation:</strong> {score_result['recommendation']}</p>
            </div>
            """, unsafe_allow_html=True)
            
            # Contributing factors
            if score_result['contributing_factors']:
                st.subheader("Contributing Factors")
                
                for factor_name, factor_data in score_result['contributing_factors'].items():
                    severity_emoji = '🔴' if factor_data['severity'] == 'high' else '🟡'
                    
                    with st.expander(f"{severity_emoji} {factor_name.replace('_', ' ').title()} (+{factor_data['score']} points)"):
                        st.write(factor_data['description'])
                        st.progress(factor_data['score'] / 40)  # Normalize to reasonable max
        
        # Visualize claim in graph
        st.subheader("📊 Claim Network Position")
        
        visualizer = GraphVisualizer(driver)
        claim_graph = visualizer.visualize_claim_connections(new_claim_data['claim_id'])
        
        if claim_graph:
            components.html(claim_graph, height=600, scrolling=True)

def show_claim_investigation(driver):
    """Display claim investigation tool"""
    
    # Two-column layout: Header/Selector | Legend
    col_main, col_legend = st.columns([3, 1])
    
    with col_main:
        st.header("Claim Investigation Tool")
        
        # Claim search
        with driver.session() as session:
            result = session.run("""
                MATCH (cl:Claim)
                RETURN cl.claim_id as id, cl.claim_number as number
                ORDER BY cl.claim_date DESC
                LIMIT 100
            """)
            
            claims = [(record['id'], record['number']) for record in result]
        
        if not claims:
            st.warning("No claims found in database")
            return
        
        claim_options = [f"{claim_num} ({claim_id})" for claim_id, claim_num in claims]
        selected_claim = st.selectbox("Select Claim to Investigate", claim_options)
    
    with col_legend:
        st.markdown("""
        <div style="font-size:12px; line-height:1.6; margin-top:2rem;">
        <b>Legend</b><br>
        <span style="color:#ff0000;">●</span> Claimant &nbsp;
        <span style="color:#ffa500;">◆</span> Claim &nbsp;
        <span style="color:#0000ff;">■</span> Policy<br>
        <span style="color:#008000;">▲</span> Vehicle &nbsp;
        <span style="color:#800080;">■</span> Repair Shop<br>
        <span style="color:#00ffff;">★</span> Medical Provider &nbsp;
        <span style="color:#ffc0cb;">⬢</span> Lawyer<br>
        <span style="color:#ffff00;">⬬</span> Witness
        </div>
        """, unsafe_allow_html=True)
    
    if not claims:
        return
        
    if selected_claim:
        claim_id = selected_claim.split('(')[1].strip(')')
        
        # Initialize session state for investigation outputs
        if 'investigation_claim_id' not in st.session_state:
            st.session_state.investigation_claim_id = None
        if 'investigation_analysis' not in st.session_state:
            st.session_state.investigation_analysis = None
        if 'investigation_network' not in st.session_state:
            st.session_state.investigation_network = None
        
        # Reset if claim changed
        if st.session_state.investigation_claim_id != claim_id:
            st.session_state.investigation_claim_id = claim_id
            st.session_state.investigation_analysis = None
            st.session_state.investigation_network = None
        
        # Button row (30/70 split to match output)
        btn_col1, btn_col2 = st.columns([3, 7])
        with btn_col1:
            if st.button("🔍 Analyze Claim", use_container_width=True):
                with st.spinner("Analyzing..."):
                    scorer = FraudScorer(driver)
                    st.session_state.investigation_analysis = scorer.score_claim(claim_id)
        with btn_col2:
            if st.button("🕸️ View Network", use_container_width=True):
                with st.spinner("Loading..."):
                    visualizer = GraphVisualizer(driver)
                    st.session_state.investigation_network = visualizer.visualize_claim_connections(claim_id)
        
        # Output row (30/70 split)
        out_col1, out_col2 = st.columns([3, 7])
        
        with out_col1:
            if st.session_state.investigation_analysis and 'error' not in st.session_state.investigation_analysis:
                score_result = st.session_state.investigation_analysis
                st.subheader("Fraud Propensity Score")
                
                risk_level = score_result['risk_level']
                score = score_result['fraud_propensity_score']
                risk_class = f"{risk_level.lower()}-risk"
                
                st.markdown(f"""
                <div class="metric-card {risk_class}">
                    <h3 style="margin-top: 0;">Risk Level: {risk_level}</h3>
                    <h1 style="margin: 1rem 0;">{score}/100</h1>
                </div>
                """, unsafe_allow_html=True)
                
                if score_result['contributing_factors']:
                    st.write("**Contributing Factors:**")
                    for factor_name, factor_data in score_result['contributing_factors'].items():
                        st.write(f"- {factor_data['description']} (+{factor_data['score']} points)")
        
        with out_col2:
            if st.session_state.investigation_network:
                components.html(st.session_state.investigation_network, height=600, scrolling=True)

if __name__ == "__main__":
    main()
