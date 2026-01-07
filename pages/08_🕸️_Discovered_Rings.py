"""
Discovered Rings Page - Review and manage auto insurance fraud rings
"""
import streamlit as st
import pandas as pd
import logging

from data.neo4j_driver import get_neo4j_driver
from utils.logger import setup_logger

# Setup
logger = setup_logger(__name__)
st.set_page_config(page_title="Discovered Rings", page_icon="🕸️", layout="wide")

# Initialize
driver = get_neo4j_driver()


def main():
    """Main function for Discovered Rings page"""
    
    # Header
    st.title("🕸️ Discovered Auto Insurance Fraud Rings")
    st.markdown("**Review, analyze, and manage detected fraud networks**")
    
    # Action buttons
    col1, col2, col3 = st.columns([2, 1, 1])
    
    with col1:
        st.markdown("")  # Spacer
    
    with col2:
        if st.button("🔍 Run Detection", use_container_width=True):
            st.info("Detection algorithms would run here. Use load_sample_data.py to create rings.")
    
    with col3:
        if st.button("🔄 Refresh", use_container_width=True):
            st.rerun()
    
    # Sidebar filters
    with st.sidebar:
        st.header("🔍 Filters")
        
        ring_type = st.multiselect(
            "Ring Type",
            options=['KNOWN', 'DISCOVERED', 'SUSPICIOUS', 'EMERGING'],
            default=['DISCOVERED', 'SUSPICIOUS']
        )
        
        pattern_type = st.multiselect(
            "Pattern Type",
            options=[
                'staged_accident',
                'body_shop_fraud',
                'medical_mill',
                'attorney_organized',
                'phantom_passenger',
                'tow_truck_kickback',
                'mixed'
            ],
            default=None
        )
        
        status = st.multiselect(
            "Status",
            options=['CONFIRMED', 'UNDER_REVIEW', 'DISMISSED'],
            default=['CONFIRMED', 'UNDER_REVIEW']
        )
        
        min_confidence = st.slider(
            "Minimum Confidence",
            min_value=0.0,
            max_value=1.0,
            value=0.6,
            step=0.05
        )
        
        min_members = st.number_input(
            "Minimum Members",
            min_value=2,
            value=3,
            step=1
        )
    
    # Load rings
    with st.spinner("Loading fraud rings..."):
        rings = load_rings(
            ring_type=ring_type,
            pattern_type=pattern_type,
            status=status,
            min_confidence=min_confidence,
            min_members=min_members
        )
    
    if not rings:
        st.info("No fraud rings found matching your filters.")
        st.markdown("Rings are created when loading sample data with fraud patterns.")
        return
    
    # Summary metrics
    display_summary_metrics(rings)
    
    st.markdown("---")
    
    # Display rings
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
                        st.info("Network visualization would appear here")
                
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
