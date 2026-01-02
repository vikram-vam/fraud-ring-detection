
import streamlit as st
import pandas as pd
from neo4j import GraphDatabase
import os
from dotenv import load_dotenv
import ssl
import streamlit.components.v1 as components
from components.graph_visualizer import GraphVisualizer
from components.claim_form import ClaimForm
from components.fraud_scorer import FraudScorer
import plotly.express as px
from datetime import datetime
import time

# Load environment variables
load_dotenv()

# --- CONFIGURATION ---
st.set_page_config(layout="wide", page_title="Fraud Ring Detection System V2", page_icon="🕵️")

# Custom CSS for V2 styling
st.markdown("""
<style>
    .metric-card {
        background-color: #f8f9fa;
        padding: 20px;
        border-radius: 10px;
        border: 1px solid #e9ecef;
        text-align: center;
        margin-bottom: 20px;
    }
    .metric-label {
        font-size: 14px;
        color: #6c757d;
        font-weight: bold;
        text-transform: uppercase;
    }
    .metric-value {
        font-size: 32px;
        font-weight: bold;
        color: #212529;
    }
    .status-badge {
        padding: 5px 10px;
        border-radius: 15px;
        font-size: 12px;
        font-weight: bold;
        color: white;
    }
    .status-new { background-color: #007bff; }
    .status-review { background-color: #ffc107; color: black !important; }
    .status-closed { background-color: #28a745; }
    .status-denied { background-color: #dc3545; }
</style>
""", unsafe_allow_html=True)

# --- DATABASE CONNECTION ---
@st.cache_resource
def get_driver():
    uri = os.getenv('NEO4J_URI')
    username = os.getenv('NEO4J_USERNAME')
    password = os.getenv('NEO4J_PASSWORD')
    
    # SSL Context for AuraDB
    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE
    
    working_uri = uri
    if uri.startswith("neo4j+s://"):
        working_uri = uri.replace("neo4j+s://", "neo4j://")
    elif uri.startswith("bolt+s://"):
        working_uri = uri.replace("bolt+s://", "bolt://")
        
    return GraphDatabase.driver(
        working_uri, 
        auth=(username, password), 
        encrypted=True,
        ssl_context=ssl_context
    )

driver = get_driver()

# --- V2 COMPONENTS ---

def render_dashboard_v2():
    st.title("🛡️ Executive Dashboard (V2)")
    
    with driver.session() as session:
        # Fetch Metrics
        stats = session.run("""
            MATCH (c:Claim)
            RETURN 
                count(c) as total_claims,
                sum(c.claim_amount) as total_amount,
                count(CASE WHEN c.is_fraud_ring = true THEN 1 END) as fraud_claims,
                sum(CASE WHEN c.is_fraud_ring = true THEN c.claim_amount END) as fraud_amount,
                count(CASE WHEN c.status = 'New' OR c.status IS NULL THEN 1 END) as backlog_count
        """).single()
        
        active_rings = session.run("MATCH (c:Claim) WHERE c.is_fraud_ring = true RETURN count(distinct c.fraud_ring_id) as rings").single()['rings']

    # KPI ROW
    kpi1, kpi2, kpi3, kpi4 = st.columns(4)
    
    with kpi1:
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-label">Fraud Exposure</div>
            <div class="metric-value">${stats['fraud_amount']:,.0f}</div>
            <small class="text-muted">Total Potential Loss</small>
        </div>
        """, unsafe_allow_html=True)
        
    with kpi2:
        loss_ratio_impact = (stats['fraud_amount'] / stats['total_amount'] * 100) if stats['total_amount'] > 0 else 0
        st.markdown(f"""
        <div class="metric-card" title="Calculated as (Fraud Claims Sum / Total Claims Amount). Represents direct profitability impact.">
            <div class="metric-label">Loss Ratio Impact ℹ️</div>
            <div class="metric-value">{loss_ratio_impact:.1f}%</div>
            <small class="text-muted">Profitaly Hit</small>
        </div>
        """, unsafe_allow_html=True)
        
    with kpi3:
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-label">Active Rings</div>
            <div class="metric-value">{active_rings}</div>
            <small class="text-muted">Network Targets</small>
        </div>
        """, unsafe_allow_html=True)

    with kpi4:
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-label">Investigator Backlog</div>
            <div class="metric-value">{stats['backlog_count']}</div>
            <small class="text-muted">Cases 'New' / 'Pending'</small>
        </div>
        """, unsafe_allow_html=True)

    # TREND ANALYSIS & WORKLOAD
    st.markdown("---")
    c1, c2 = st.columns([2, 1])
    
    with c1:
        st.subheader("Ring Detection Trends (Last 6 Months)")
        # Simulating trend data for demo since our data is static
        dates = pd.date_range(end=datetime.now(), periods=180).tolist()
        import random
        trend_data = pd.DataFrame({
            'Date': dates,
            'New Rings': [random.choice([0, 0, 0, 1]) for _ in range(180)]
        })
        trend_data['Cumulative Rings'] = trend_data['New Rings'].cumsum()
        
        fig = px.area(trend_data, x='Date', y='Cumulative Rings', title="Total Identified Rings Over Time")
        st.plotly_chart(fig, use_container_width=True)
        
    with c2:
        st.subheader("Operational Status")
        # Placeholder for status breakdown
        status_data = pd.DataFrame({
            'Status': ['New', 'In Review', 'Sent to SIU', 'Closed', 'Denied'],
            'Count': [stats['backlog_count'], 12, 5, 45, 8] # Mixed real and fake for demo
        })
        fig2 = px.pie(status_data, values='Count', names='Status', hole=0.4)
        st.plotly_chart(fig2, use_container_width=True)

def render_visualization_v2():
    st.title("🕸️ Graph Investigation Workbench")
    
    with driver.session() as session:
        result = session.run("""
            MATCH (c:Claimant) 
            WHERE c.is_fraud_ring = true
            RETURN distinct c.fraud_ring_id as ring_id
            ORDER BY ring_id
        """)
        fraud_rings = [record['ring_id'] for record in result]
    
    col1, col2 = st.columns([1, 4])
    
    with col1:
        st.markdown("### Controls")
        selected_ring = st.selectbox("Select Target Ring", fraud_rings)
        
        st.markdown("#### Filters")
        show_medical = st.checkbox("🏥 Medical Providers", value=True)
        show_repair = st.checkbox("🔧 Repair Shops", value=True)
        show_witness = st.checkbox("👁️ Witnesses", value=False) # Default hidden to declutter
        show_vehicle = st.checkbox("🚗 Vehicles", value=False)   # Default hidden to declutter
        
        exclude_types = []
        if not show_medical: exclude_types.append('MedicalProvider')
        if not show_repair: exclude_types.append('RepairShop')
        if not show_witness: exclude_types.append('Witness')
        if not show_vehicle: exclude_types.append('Vehicle')

        visualize_btn = st.button("🚀 Load Graph")
        
        st.info("💡 **Tip:** Hide Witnesses and Vehicles to see the core collusion hubs clearly.")

    with col2:
        if visualize_btn:
             with st.spinner("Analyzing Network Topology..."):
                visualizer = GraphVisualizer(driver)
                # Use filtered visualization
                html = visualizer.visualize_filtered_fraud_ring(selected_ring, exclude_types=exclude_types)
                if html:
                    components.html(html, height=700, scrolling=True)
                else:
                    st.error("Failed to generate visualization")

def render_workflow_v2():
    st.title("🕵️ Case Management & Disposition")
    
    # Search for Claim
    claim_id_input = st.text_input("Search Claim ID (e.g. CLM_123)", "")
    
    if claim_id_input:
        with driver.session() as session:
            # Fetch Claim Details
            query = """
                MATCH (c:Claim {claim_id: $cid})
                RETURN c
            """
            result = session.run(query, cid=claim_id_input).single()
            
            if result:
                claim = dict(result['c'])
                st.write(f"### Claim: {claim_id_input}")
                
                # Header Stats
                s1, s2, s3, s4 = st.columns(4)
                s1.metric("Amount", f"${claim.get('claim_amount', 0):,.2f}")
                s2.metric("Date", claim.get('claim_date', 'N/A')[:10])
                
                status = claim.get('status', 'New')
                s3.metric("Status", status)
                
                disposition = claim.get('disposition', 'Pending')
                s4.metric("Disposition", disposition)
                
                st.markdown("---")
                
                # WORKFLOW ACTIONS
                w1, w2 = st.columns([2, 1])
                
                with w1:
                    st.subheader("📝 Investigation Notes")
                    # In a real app, we'd fetch existing notes. For now, simulating a log.
                    existing_notes = claim.get('notes', [])
                    if existing_notes:
                        for note in existing_notes:
                            st.text(f"• {note}")
                    else:
                        st.info("No prior notes.")
                        
                    new_note = st.text_area("Add Finding", placeholder="e.g. Spoke to garage owner, he denied knowing claimant...")
                    if st.button("Save Note"):
                         session.run("""
                            MATCH (c:Claim {claim_id: $cid})
                            SET c.notes = coalesce(c.notes, []) + $note
                         """, cid=claim_id_input, note=f"{datetime.now().strftime('%Y-%m-%d %H:%M')}: {new_note}")
                         st.success("Note saved.")
                         st.rerun()

                with w2:
                    st.subheader("⚡ Decision")
                    st.write("Determine final outcome:")
                    
                    if st.button("✅ Approve Payment", type="primary"):
                        session.run("MATCH (c:Claim {claim_id: $cid}) SET c.status='Closed', c.disposition='Approved'", cid=claim_id_input)
                        st.success("Claim Approved.")
                        time.sleep(1)
                        st.rerun()
                        
                    if st.button("🚫 Deny Claim (Fraud)", type="primary"):
                        session.run("MATCH (c:Claim {claim_id: $cid}) SET c.status='Denied', c.disposition='Fraud Confirmed'", cid=claim_id_input)
                        st.error("Claim Denied as Fraud.")
                        time.sleep(1)
                        st.rerun()
                        
                    if st.button("⚠️ Refer to SIU"):
                        session.run("MATCH (c:Claim {claim_id: $cid}) SET c.status='In Review', c.disposition='SIU Referral'", cid=claim_id_input)
                        st.warning("Referred to SIU Team.")
                        time.sleep(1)
                        st.rerun()

            else:
                st.error("Claim not found.")


# --- MAIN LAYOUT ---
def main():
    st.sidebar.title("🚨 SIU Workbench v2.0")
    
    page = st.sidebar.radio("Module", ["Executive Dashboard", "Graph Investigation", "Case Management"])
    
    if page == "Executive Dashboard":
        render_dashboard_v2()
    elif page == "Graph Investigation":
        render_visualization_v2()
    elif page == "Case Management":
        render_workflow_v2()
    
    st.sidebar.markdown("---")
    st.sidebar.info("Logged in as: **Senior Investigator**")

if __name__ == "__main__":
    main()
