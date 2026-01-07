"""
Auto Insurance Fraud Detection System
Main Streamlit Application - Landing Page & Dashboard
"""
import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
import logging

from data.neo4j_driver import get_neo4j_driver
from analytics.risk_scorer import RiskScorer
from utils.logger import setup_logger

# Page configuration
st.set_page_config(
    page_title="Auto Insurance Fraud Detection",
    page_icon="🚗",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Setup logging
logger = setup_logger(__name__)

# Initialize components
try:
    driver = get_neo4j_driver()
    risk_scorer = RiskScorer()
except Exception as e:
    st.error(f"Failed to initialize application: {str(e)}")
    st.stop()


def load_dashboard_metrics():
    """Load key metrics for dashboard"""
    try:
        stats = driver.get_statistics()
        
        # Get high-risk claim count
        high_risk_query = """
        MATCH (cl:Claim)
        WHERE cl.risk_score >= 70
        RETURN count(cl) as high_risk_count
        """
        high_risk_result = driver.execute_query(high_risk_query)
        high_risk_count = high_risk_result[0]['high_risk_count'] if high_risk_result else 0
        
        # Get total claim amount at risk
        risk_amount_query = """
        MATCH (cl:Claim)
        WHERE cl.risk_score >= 70
        RETURN sum(cl.total_claim_amount) as total_at_risk
        """
        risk_amount_result = driver.execute_query(risk_amount_query)
        total_at_risk = risk_amount_result[0]['total_at_risk'] if risk_amount_result else 0
        
        # Get recent claims (last 30 days)
        recent_claims_query = """
        MATCH (cl:Claim)
        WHERE cl.report_date >= date() - duration({days: 30})
        RETURN count(cl) as recent_count
        """
        recent_result = driver.execute_query(recent_claims_query)
        recent_claims = recent_result[0]['recent_count'] if recent_result else 0
        
        # Get active investigations
        active_investigations_query = """
        MATCH (cl:Claim)
        WHERE cl.status IN ['Under Investigation', 'Under Review']
        RETURN count(cl) as active_count
        """
        active_result = driver.execute_query(active_investigations_query)
        active_investigations = active_result[0]['active_count'] if active_result else 0
        
        return {
            'stats': stats,
            'high_risk_count': high_risk_count,
            'total_at_risk': total_at_risk,
            'recent_claims': recent_claims,
            'active_investigations': active_investigations
        }
        
    except Exception as e:
        logger.error(f"Error loading dashboard metrics: {e}", exc_info=True)
        return None


def render_sidebar():
    """Render sidebar with navigation and info"""
    
    with st.sidebar:
        # App header
        st.markdown("## 🚗 Auto Insurance")
        st.markdown("### Fraud Detection System")
        st.markdown("---")
        
        # Connection status
        if driver.test_connection():
            st.success("🟢 Database Connected")
        else:
            st.error("🔴 Database Disconnected")
        
        st.markdown("---")
        
        # Navigation info
        st.markdown("### 📍 Navigation")
        st.markdown("""
        **Main Pages:**
        - 🏠 **Dashboard** - Overview & metrics
        - 🔥 **Hot Queue** - High-risk claims
        - 🔍 **Entity Profile** - Deep-dive analysis
        - 🕸️ **Discovered Rings** - Fraud networks
        
        **Quick Stats:**
        """)
        
        # Quick stats
        try:
            stats = driver.get_statistics()
            st.metric("Total Claims", f"{stats.get('claims', 0):,}")
            st.metric("Fraud Rings", f"{stats.get('fraud_rings', 0):,}")
            st.metric("Vehicles", f"{stats.get('vehicles', 0):,}")
        except:
            st.info("Stats loading...")
        
        st.markdown("---")
        
        # System info
        st.markdown("### ℹ️ System Info")
        st.caption(f"Version: 1.0.0")
        st.caption(f"Updated: {datetime.now().strftime('%Y-%m-%d')}")
        
        # Fraud pattern legend
        with st.expander("🕵️ Fraud Patterns"):
            st.markdown("""
            **Pattern Types:**
            - 🎭 Staged Accidents
            - 🔧 Body Shop Fraud
            - 🏥 Medical Mills
            - ⚖️ Attorney Organized
            - 👥 Phantom Passengers
            - 🚛 Tow Truck Kickbacks
            """)


def render_header():
    """Render main page header"""
    
    col1, col2 = st.columns([3, 1])
    
    with col1:
        st.title("🚗 Auto Insurance Fraud Detection System")
        st.markdown("**Real-time fraud detection and network analysis for auto insurance claims**")
    
    with col2:
        st.markdown("### 🎯 System Status")
        if driver.test_connection():
            st.success("System Online")
        else:
            st.error("System Offline")


def render_key_metrics(metrics_data):
    """Render key performance metrics"""
    
    st.markdown("## 📊 Key Metrics")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(
            "Total Claims",
            f"{metrics_data['stats'].get('claims', 0):,}",
            help="Total number of auto insurance claims in system"
        )
    
    with col2:
        st.metric(
            "High-Risk Claims",
            f"{metrics_data['high_risk_count']:,}",
            delta=f"{(metrics_data['high_risk_count'] / max(metrics_data['stats'].get('claims', 1), 1) * 100):.1f}%",
            help="Claims with risk score >= 70"
        )
    
    with col3:
        total_at_risk = metrics_data['total_at_risk'] or 0
        st.metric(
            "Amount at Risk",
            f"${total_at_risk:,.0f}",
            help="Total claim amount for high-risk claims"
        )
    
    with col4:
        st.metric(
            "Fraud Rings Detected",
            f"{metrics_data['stats'].get('fraud_rings', 0):,}",
            help="Number of detected fraud networks"
        )
    
    st.markdown("---")
    
    # Secondary metrics
    col5, col6, col7, col8 = st.columns(4)
    
    with col5:
        st.metric(
            "Recent Claims (30d)",
            f"{metrics_data['recent_claims']:,}",
            help="Claims filed in last 30 days"
        )
    
    with col6:
        st.metric(
            "Active Investigations",
            f"{metrics_data['active_investigations']:,}",
            help="Claims currently under investigation"
        )
    
    with col7:
        st.metric(
            "Body Shops",
            f"{metrics_data['stats'].get('body_shops', 0):,}",
            help="Registered body shops in system"
        )
    
    with col8:
        st.metric(
            "Medical Providers",
            f"{metrics_data['stats'].get('medical_providers', 0):,}",
            help="Medical providers in network"
        )


def render_fraud_pattern_summary():
    """Render fraud pattern breakdown"""
    
    st.markdown("## 🕸️ Fraud Pattern Distribution")
    
    try:
        query = """
        MATCH (r:FraudRing)
        WITH r.pattern_type as pattern, count(r) as ring_count, 
             sum(r.member_count) as total_members,
             avg(r.confidence_score) as avg_confidence
        RETURN pattern, ring_count, total_members, avg_confidence
        ORDER BY ring_count DESC
        """
        
        results = driver.execute_query(query)
        
        if results:
            df = pd.DataFrame(results)
            
            # Rename patterns for display
            pattern_names = {
                'staged_accident': '🎭 Staged Accidents',
                'body_shop_fraud': '🔧 Body Shop Fraud',
                'medical_mill': '🏥 Medical Mills',
                'attorney_organized': '⚖️ Attorney Organized',
                'phantom_passenger': '👥 Phantom Passengers',
                'tow_truck_kickback': '🚛 Tow Truck Kickbacks',
                'mixed': '🔀 Mixed Patterns'
            }
            
            df['pattern_display'] = df['pattern'].map(pattern_names)
            df['avg_confidence'] = (df['avg_confidence'] * 100).round(1)
            
            # Display as columns
            col1, col2 = st.columns(2)
            
            with col1:
                st.markdown("### Pattern Breakdown")
                for _, row in df.iterrows():
                    st.markdown(
                        f"**{row['pattern_display']}**  \n"
                        f"Rings: {row['ring_count']} | "
                        f"Members: {row['total_members']} | "
                        f"Confidence: {row['avg_confidence']}%"
                    )
            
            with col2:
                st.markdown("### Top Patterns")
                # Create a simple bar chart representation
                st.dataframe(
                    df[['pattern_display', 'ring_count', 'total_members', 'avg_confidence']].rename(columns={
                        'pattern_display': 'Pattern Type',
                        'ring_count': 'Rings',
                        'total_members': 'Members',
                        'avg_confidence': 'Confidence %'
                    }),
                    use_container_width=True,
                    hide_index=True
                )
        else:
            st.info("No fraud rings detected yet. Run detection algorithms to identify patterns.")
    
    except Exception as e:
        logger.error(f"Error rendering fraud patterns: {e}", exc_info=True)
        st.error("Could not load fraud pattern data")


def render_recent_high_risk_claims():
    """Render recent high-risk claims table"""
    
    st.markdown("## 🔥 Recent High-Risk Claims")
    
    try:
        query = """
        MATCH (c:Claimant)-[:FILED]->(cl:Claim)
        WHERE cl.risk_score >= 70
        
        OPTIONAL MATCH (cl)-[:INVOLVES_VEHICLE]->(v:Vehicle)
        OPTIONAL MATCH (cl)-[:OCCURRED_AT]->(l:AccidentLocation)
        OPTIONAL MATCH (c)-[:MEMBER_OF]->(r:FraudRing)
        
        RETURN 
            cl.claim_number as claim_number,
            c.name as claimant,
            cl.accident_type as accident_type,
            cl.total_claim_amount as amount,
            cl.accident_date as accident_date,
            cl.risk_score as risk_score,
            v.make + ' ' + v.model as vehicle,
            l.intersection as location,
            r.pattern_type as ring_type
        ORDER BY cl.report_date DESC
        LIMIT 10
        """
        
        results = driver.execute_query(query)
        
        if results:
            df = pd.DataFrame(results)
            
            # Format columns
            df['amount'] = df['amount'].apply(lambda x: f"${x:,.0f}")
            df['risk_score'] = df['risk_score'].apply(lambda x: f"{x:.1f}")
            df['ring_indicator'] = df['ring_type'].apply(lambda x: '🕸️' if pd.notna(x) else '')
            
            # Reorder columns
            display_cols = ['ring_indicator', 'claim_number', 'claimant', 'accident_type', 
                           'amount', 'risk_score', 'vehicle', 'location']
            
            display_cols = [col for col in display_cols if col in df.columns]
            
            st.dataframe(
                df[display_cols],
                use_container_width=True,
                hide_index=True
            )
            
            st.caption("🕸️ indicates claim is linked to a fraud ring")
        else:
            st.info("No high-risk claims found")
    
    except Exception as e:
        logger.error(f"Error loading recent claims: {e}", exc_info=True)
        st.error("Could not load recent claims data")


def render_entity_statistics():
    """Render entity-specific statistics"""
    
    st.markdown("## 🏢 Entity Statistics")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown("### 🔧 Body Shops")
        try:
            # Top body shops by claim count
            query = """
            MATCH (b:BodyShop)<-[:REPAIRED_AT]-(cl:Claim)
            WITH b, count(cl) as claim_count, avg(cl.risk_score) as avg_risk
            ORDER BY claim_count DESC
            LIMIT 5
            RETURN b.name as name, claim_count, round(avg_risk, 1) as avg_risk
            """
            results = driver.execute_query(query)
            
            if results:
                for row in results:
                    risk_color = "🔴" if row['avg_risk'] >= 70 else "🟡" if row['avg_risk'] >= 40 else "🟢"
                    st.markdown(f"{risk_color} **{row['name']}**")
                    st.caption(f"Claims: {row['claim_count']} | Avg Risk: {row['avg_risk']}")
            else:
                st.info("No body shop data")
        except Exception as e:
            st.error("Could not load body shop stats")
    
    with col2:
        st.markdown("### 🏥 Medical Providers")
        try:
            # Top medical providers
            query = """
            MATCH (m:MedicalProvider)<-[:TREATED_BY]-(cl:Claim)
            WITH m, count(cl) as claim_count, avg(cl.risk_score) as avg_risk
            ORDER BY claim_count DESC
            LIMIT 5
            RETURN m.name as name, claim_count, round(avg_risk, 1) as avg_risk
            """
            results = driver.execute_query(query)
            
            if results:
                for row in results:
                    risk_color = "🔴" if row['avg_risk'] >= 70 else "🟡" if row['avg_risk'] >= 40 else "🟢"
                    st.markdown(f"{risk_color} **{row['name']}**")
                    st.caption(f"Patients: {row['claim_count']} | Avg Risk: {row['avg_risk']}")
            else:
                st.info("No medical provider data")
        except Exception as e:
            st.error("Could not load medical provider stats")
    
    with col3:
        st.markdown("### ⚖️ Attorneys")
        try:
            # Top attorneys by client count
            query = """
            MATCH (a:Attorney)<-[:REPRESENTED_BY]-(cl:Claim)
            WITH a, count(cl) as claim_count, avg(cl.risk_score) as avg_risk
            ORDER BY claim_count DESC
            LIMIT 5
            RETURN a.name as name, claim_count, round(avg_risk, 1) as avg_risk
            """
            results = driver.execute_query(query)
            
            if results:
                for row in results:
                    risk_color = "🔴" if row['avg_risk'] >= 70 else "🟡" if row['avg_risk'] >= 40 else "🟢"
                    st.markdown(f"{risk_color} **{row['name']}**")
                    st.caption(f"Clients: {row['claim_count']} | Avg Risk: {row['avg_risk']}")
            else:
                st.info("No attorney data")
        except Exception as e:
            st.error("Could not load attorney stats")


def render_quick_actions():
    """Render quick action buttons"""
    
    st.markdown("## ⚡ Quick Actions")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        if st.button("🔥 View Hot Queue", use_container_width=True):
            st.switch_page("pages/01_🔥_Hot_Queue.py")
    
    with col2:
        if st.button("🕸️ View Fraud Rings", use_container_width=True):
            st.switch_page("pages/08_🕸️_Discovered_Rings.py")
    
    with col3:
        if st.button("🔍 Search Entities", use_container_width=True):
            st.switch_page("pages/03_🔍_Entity_Profile.py")
    
    with col4:
        if st.button("🔄 Refresh Dashboard", use_container_width=True):
            st.rerun()


def render_alerts():
    """Render system alerts and warnings"""
    
    try:
        # Check for repeat witnesses
        witness_query = """
        MATCH (w:Witness)-[:WITNESSED]->(cl:Claim)
        WITH w, count(cl) as claim_count
        WHERE claim_count >= 3
        RETURN count(w) as suspicious_witnesses
        """
        witness_result = driver.execute_query(witness_query)
        suspicious_witnesses = witness_result[0]['suspicious_witnesses'] if witness_result else 0
        
        # Check for vehicles in multiple accidents
        vehicle_query = """
        MATCH (v:Vehicle)<-[:INVOLVES_VEHICLE]-(cl:Claim)
        WITH v, count(cl) as accident_count
        WHERE accident_count >= 3
        RETURN count(v) as suspicious_vehicles
        """
        vehicle_result = driver.execute_query(vehicle_query)
        suspicious_vehicles = vehicle_result[0]['suspicious_vehicles'] if vehicle_result else 0
        
        # Check for location hotspots
        location_query = """
        MATCH (l:AccidentLocation)<-[:OCCURRED_AT]-(cl:Claim)
        WITH l, count(cl) as accident_count
        WHERE accident_count >= 5
        RETURN count(l) as hotspot_locations
        """
        location_result = driver.execute_query(location_query)
        hotspot_locations = location_result[0]['hotspot_locations'] if location_result else 0
        
        # Display alerts if any
        if suspicious_witnesses > 0 or suspicious_vehicles > 0 or hotspot_locations > 0:
            st.markdown("## ⚠️ System Alerts")
            
            if suspicious_witnesses > 0:
                st.warning(f"🚨 {suspicious_witnesses} witnesses appear in 3+ claims")
            
            if suspicious_vehicles > 0:
                st.warning(f"🚨 {suspicious_vehicles} vehicles involved in 3+ accidents")
            
            if hotspot_locations > 0:
                st.warning(f"🚨 {hotspot_locations} accident hotspot locations detected")
    
    except Exception as e:
        logger.error(f"Error loading alerts: {e}")


def main():
    """Main application function"""
    
    # Render sidebar
    render_sidebar()
    
    # Render main header
    render_header()
    
    st.markdown("---")
    
    # Load dashboard data
    with st.spinner("Loading dashboard data..."):
        metrics_data = load_dashboard_metrics()
    
    if not metrics_data:
        st.error("Failed to load dashboard data. Please check database connection.")
        return
    
    # Render key metrics
    render_key_metrics(metrics_data)
    
    # Render alerts
    render_alerts()
    
    st.markdown("---")
    
    # Render fraud pattern summary
    render_fraud_pattern_summary()
    
    st.markdown("---")
    
    # Render recent high-risk claims
    render_recent_high_risk_claims()
    
    st.markdown("---")
    
    # Render entity statistics
    render_entity_statistics()
    
    st.markdown("---")
    
    # Render quick actions
    render_quick_actions()
    
    # Footer
    st.markdown("---")
    st.caption("© 2026 Auto Insurance Fraud Detection System | Powered by Neo4j & Streamlit")


if __name__ == "__main__":
    main()
