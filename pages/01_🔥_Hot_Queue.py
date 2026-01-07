"""
Hot Queue Page - High-risk auto insurance claims dashboard
Shows claims requiring immediate investigation
"""
import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
import logging

from data.neo4j_driver import get_neo4j_driver
from utils.logger import setup_logger

# Setup
logger = setup_logger(__name__)
st.set_page_config(page_title="Hot Queue", page_icon="🔥", layout="wide")

# Initialize components
driver = get_neo4j_driver()


def load_high_risk_claims(filters: dict) -> pd.DataFrame:
    """Load high-risk auto insurance claims with filters"""
    try:
        query = """
        MATCH (c:Claimant)-[:FILED]->(cl:Claim)
        WHERE cl.risk_score >= $min_risk
        
        OPTIONAL MATCH (cl)-[:INVOLVES_VEHICLE]->(v:Vehicle)
        OPTIONAL MATCH (cl)-[:REPAIRED_AT]->(b:BodyShop)
        OPTIONAL MATCH (cl)-[:TREATED_BY]->(m:MedicalProvider)
        OPTIONAL MATCH (cl)-[:REPRESENTED_BY]->(a:Attorney)
        OPTIONAL MATCH (cl)-[:OCCURRED_AT]->(l:AccidentLocation)
        OPTIONAL MATCH (cl)-[:TOWED_BY]->(t:TowCompany)
        OPTIONAL MATCH (c)-[:MEMBER_OF]->(r:FraudRing)
        
        RETURN 
            cl.claim_id as claim_id,
            cl.claim_number as claim_number,
            c.claimant_id as claimant_id,
            c.name as claimant_name,
            cl.accident_type as accident_type,
            cl.injury_type as injury_type,
            cl.accident_date as accident_date,
            cl.report_date as report_date,
            cl.property_damage_amount as property_damage,
            cl.bodily_injury_amount as bodily_injury,
            cl.total_claim_amount as total_amount,
            cl.status as status,
            cl.risk_score as risk_score,
            v.make + ' ' + v.model + ' (' + v.year + ')' as vehicle_info,
            v.license_plate as license_plate,
            b.name as body_shop,
            m.name as medical_provider,
            a.name as attorney,
            l.intersection as accident_location,
            t.name as tow_company,
            r.ring_id as ring_id,
            r.pattern_type as ring_type
        ORDER BY cl.risk_score DESC, cl.report_date DESC
        LIMIT $limit
        """
        
        results = driver.execute_query(query, {
            'min_risk': filters.get('min_risk', 70),
            'limit': filters.get('limit', 100)
        })
        
        if not results:
            return pd.DataFrame()
        
        df = pd.DataFrame(results)
        
        # Apply additional filters
        if filters.get('accident_types'):
            df = df[df['accident_type'].isin(filters['accident_types'])]
        
        if filters.get('statuses'):
            df = df[df['status'].isin(filters['statuses'])]
        
        if filters.get('has_ring_only'):
            df = df[df['ring_id'].notna()]
        
        if filters.get('min_amount'):
            df = df[df['total_amount'] >= filters['min_amount']]
        
        return df
        
    except Exception as e:
        logger.error(f"Error loading high-risk claims: {e}", exc_info=True)
        st.error(f"Error loading claims: {str(e)}")
        return pd.DataFrame()


def main():
    """Main function for Hot Queue page"""
    
    # Header
    st.title("🔥 Hot Queue - High-Risk Claims")
    st.markdown("**Auto insurance claims requiring immediate investigation**")
    
    # Sidebar filters
    with st.sidebar:
        st.header("🔍 Filters")
        
        filters = {}
        
        # Risk score filter
        with st.expander("Risk Score", expanded=True):
            filters['min_risk'] = st.slider(
                "Minimum Risk Score",
                min_value=0,
                max_value=100,
                value=70,
                step=5,
                help="Filter claims by minimum risk score"
            )
        
        # Accident type filter
        with st.expander("Accident Type", expanded=True):
            accident_types = [
                "Rear-End Collision",
                "Side-Impact Collision",
                "Head-On Collision",
                "Hit and Run",
                "Single Vehicle Accident",
                "Parking Lot Collision",
                "Intersection Collision",
                "Multi-Vehicle Pileup"
            ]
            filters['accident_types'] = st.multiselect(
                "Select Accident Types",
                options=accident_types,
                default=None,
                help="Filter by accident type"
            )
        
        # Status filter
        with st.expander("Status"):
            statuses = ["Open", "Under Investigation", "Under Review", "Closed", "Pending Payment"]
            filters['statuses'] = st.multiselect(
                "Select Statuses",
                options=statuses,
                default=["Open", "Under Investigation", "Under Review"],
                help="Filter by claim status"
            )
        
        # Date filter
        with st.expander("Date Range"):
            col1, col2 = st.columns(2)
            with col1:
                filters['start_date'] = st.date_input(
                    "From",
                    value=datetime.now() - timedelta(days=180)
                )
            with col2:
                filters['end_date'] = st.date_input(
                    "To",
                    value=datetime.now()
                )
        
        # Amount filter
        with st.expander("Claim Amount"):
            filters['min_amount'] = st.number_input(
                "Minimum Amount ($)",
                min_value=0,
                value=0,
                step=5000,
                help="Filter by minimum claim amount"
            )
        
        # Fraud ring filter
        filters['has_ring_only'] = st.checkbox(
            "🕸️ Ring Members Only",
            value=False,
            help="Show only claims linked to fraud rings"
        )
        
        # Result limit
        filters['limit'] = st.selectbox(
            "Results Limit",
            options=[50, 100, 200, 500],
            index=1
        )
        
        # Refresh button
        if st.button("🔄 Refresh Data", use_container_width=True):
            st.rerun()
    
    # Load data
    with st.spinner("Loading high-risk claims..."):
        df = load_high_risk_claims(filters)
    
    if df.empty:
        st.info("No high-risk claims found matching your filters.")
        return
    
    # Summary metrics
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(
            "Total High-Risk Claims",
            len(df),
            help="Claims above risk threshold"
        )
    
    with col2:
        total_amount = df['total_amount'].sum()
        st.metric(
            "Total Amount at Risk",
            f"${total_amount:,.0f}",
            help="Sum of all high-risk claim amounts"
        )
    
    with col3:
        avg_risk = df['risk_score'].mean()
        st.metric(
            "Average Risk Score",
            f"{avg_risk:.1f}",
            help="Mean risk score across claims"
        )
    
    with col4:
        ring_claims = df[df['ring_id'].notna()]['ring_id'].nunique()
        st.metric(
            "Ring-Associated Claims",
            ring_claims,
            help="Claims linked to fraud rings"
        )
    
    st.markdown("---")
    
    # Secondary metrics
    col5, col6, col7, col8 = st.columns(4)
    
    with col5:
        avg_property = df['property_damage'].mean()
        st.metric(
            "Avg Property Damage",
            f"${avg_property:,.0f}",
            help="Average property damage amount"
        )
    
    with col6:
        avg_injury = df['bodily_injury'].mean()
        st.metric(
            "Avg Bodily Injury",
            f"${avg_injury:,.0f}",
            help="Average bodily injury amount"
        )
    
    with col7:
        with_attorney = df[df['attorney'].notna()].shape[0]
        st.metric(
            "With Attorney",
            f"{with_attorney} ({with_attorney/len(df)*100:.1f}%)",
            help="Claims represented by attorney"
        )
    
    with col8:
        unique_vehicles = df[df['vehicle_info'].notna()]['vehicle_info'].nunique()
        st.metric(
            "Unique Vehicles",
            unique_vehicles,
            help="Number of unique vehicles involved"
        )
    
    st.markdown("---")
    
    # Display mode selector
    display_mode = st.radio(
        "Display Mode",
        options=["Table View", "Card View"],
        horizontal=True
    )
    
    if display_mode == "Table View":
        render_table_view(df)
    else:
        render_card_view(df)


def render_table_view(df: pd.DataFrame):
    """Render claims in table format"""
    
    st.subheader("📊 High-Risk Claims Table")
    
    # Prepare display dataframe
    display_df = df.copy()
    
    # Format columns
    if 'accident_date' in display_df.columns:
        display_df['accident_date'] = pd.to_datetime(display_df['accident_date']).dt.strftime('%Y-%m-%d')
    
    if 'total_amount' in display_df.columns:
        display_df['amount'] = display_df['total_amount'].apply(lambda x: f"${x:,.0f}")
    
    # Add ring indicator
    if 'ring_id' in display_df.columns:
        display_df['ring'] = display_df['ring_id'].apply(
            lambda x: '🕸️' if pd.notna(x) else ''
        )
    
    # Select columns to display
    columns_to_show = [
        'ring', 'claim_number', 'claimant_name', 'accident_type', 
        'vehicle_info', 'amount', 'accident_date', 'risk_score', 'status'
    ]
    
    # Filter columns that exist
    columns_to_show = [col for col in columns_to_show if col in display_df.columns]
    
    # Display table with selection
    st.dataframe(
        display_df[columns_to_show],
        use_container_width=True,
        hide_index=True,
        height=500
    )
    
    # Claim selection for detailed view
    st.markdown("---")
    st.subheader("🔍 Claim Details")
    
    claim_ids = df['claim_id'].tolist()
    claim_options = [f"{row['claim_number']} - {row['claimant_name']} - {row['accident_type']}" 
                     for _, row in df.iterrows()]
    
    selected_claim = st.selectbox(
        "Select claim to view details",
        options=range(len(claim_options)),
        format_func=lambda x: claim_options[x]
    )
    
    if selected_claim is not None:
        claim_id = claim_ids[selected_claim]
        render_claim_details(df.iloc[selected_claim])


def render_card_view(df: pd.DataFrame):
    """Render claims in card format"""
    
    st.subheader("📋 High-Risk Claims Cards")
    
    # Sort by risk score
    df_sorted = df.sort_values('risk_score', ascending=False)
    
    # Pagination
    items_per_page = 10
    total_pages = (len(df_sorted) + items_per_page - 1) // items_per_page
    
    page = st.number_input(
        "Page",
        min_value=1,
        max_value=total_pages,
        value=1,
        step=1
    )
    
    start_idx = (page - 1) * items_per_page
    end_idx = min(start_idx + items_per_page, len(df_sorted))
    
    st.caption(f"Showing {start_idx + 1}-{end_idx} of {len(df_sorted)} claims")
    
    # Display cards
    for idx in range(start_idx, end_idx):
        row = df_sorted.iloc[idx]
        
        with st.container():
            # Card border based on ring membership
            border_color = "#E74C3C" if pd.notna(row.get('ring_id')) else "#3498DB"
            
            st.markdown(
                f"""
                <div style="
                    border-left: 5px solid {border_color};
                    padding: 15px;
                    margin-bottom: 15px;
                    background-color: #f8f9fa;
                    border-radius: 5px;
                ">
                </div>
                """,
                unsafe_allow_html=True
            )
            
            col1, col2, col3 = st.columns([2, 2, 1])
            
            with col1:
                # Claim header
                ring_badge = "🕸️ " if pd.notna(row.get('ring_id')) else ""
                st.markdown(f"### {ring_badge}{row['claim_number']}")
                
                st.markdown(f"**Claimant:** {row['claimant_name']}")
                st.markdown(f"**Accident:** {row.get('accident_type', 'Unknown')}")
                st.markdown(f"**Date:** {row.get('accident_date', 'Unknown')}")
            
            with col2:
                st.markdown(f"**Vehicle:** {row.get('vehicle_info', 'Unknown')}")
                st.markdown(f"**Location:** {row.get('accident_location', 'Unknown')}")
                
                property_dmg = row.get('property_damage', 0)
                bodily_inj = row.get('bodily_injury', 0)
                st.markdown(f"**Property Damage:** ${property_dmg:,.0f}")
                st.markdown(f"**Bodily Injury:** ${bodily_inj:,.0f}")
            
            with col3:
                # Risk score
                risk_score = row.get('risk_score', 0)
                risk_level = 'HIGH' if risk_score >= 70 else 'MEDIUM' if risk_score >= 40 else 'LOW'
                
                color = '#E74C3C' if risk_level == 'HIGH' else '#F39C12' if risk_level == 'MEDIUM' else '#27AE60'
                
                st.markdown(
                    f"""
                    <div style="
                        background-color: {color};
                        color: white;
                        padding: 20px;
                        border-radius: 10px;
                        text-align: center;
                    ">
                        <h1 style="margin: 0; color: white;">{risk_score:.0f}</h1>
                        <p style="margin: 5px 0 0 0; font-size: 14px;">{risk_level} RISK</p>
                    </div>
                    """,
                    unsafe_allow_html=True
                )
                
                # View details button
                if st.button(f"View Details", key=f"view_{row['claim_id']}", use_container_width=True):
                    st.session_state['selected_claim_details'] = row
            
            # Additional info
            info_items = []
            if pd.notna(row.get('body_shop')):
                info_items.append(f"🔧 {row['body_shop']}")
            if pd.notna(row.get('medical_provider')):
                info_items.append(f"🏥 {row['medical_provider']}")
            if pd.notna(row.get('attorney')):
                info_items.append(f"⚖️ {row['attorney']}")
            if pd.notna(row.get('tow_company')):
                info_items.append(f"🚛 {row['tow_company']}")
            
            if info_items:
                st.caption(" | ".join(info_items))
            
            st.markdown("---")
    
    # Show selected claim details
    if 'selected_claim_details' in st.session_state:
        render_claim_details(st.session_state['selected_claim_details'])


def render_claim_details(claim_row):
    """Render detailed view of selected claim"""
    
    st.markdown("### 📄 Claim Details")
    
    # Main claim info
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown("#### Basic Information")
        st.markdown(f"**Claim Number:** {claim_row['claim_number']}")
        st.markdown(f"**Claimant:** {claim_row['claimant_name']}")
        st.markdown(f"**Status:** {claim_row.get('status', 'Unknown')}")
        st.markdown(f"**Risk Score:** {claim_row.get('risk_score', 0):.1f}")
    
    with col2:
        st.markdown("#### Accident Details")
        st.markdown(f"**Type:** {claim_row.get('accident_type', 'Unknown')}")
        st.markdown(f"**Injury:** {claim_row.get('injury_type', 'Unknown')}")
        st.markdown(f"**Date:** {claim_row.get('accident_date', 'Unknown')}")
        st.markdown(f"**Reported:** {claim_row.get('report_date', 'Unknown')}")
    
    with col3:
        st.markdown("#### Financial Details")
        property_dmg = claim_row.get('property_damage', 0)
        bodily_inj = claim_row.get('bodily_injury', 0)
        total = claim_row.get('total_amount', 0)
        
        st.markdown(f"**Property Damage:** ${property_dmg:,.2f}")
        st.markdown(f"**Bodily Injury:** ${bodily_inj:,.2f}")
        st.markdown(f"**Total Claim:** ${total:,.2f}")
    
    st.markdown("---")
    
    # Related entities
    st.markdown("#### 🔗 Related Entities")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        if pd.notna(claim_row.get('vehicle_info')):
            st.markdown("**🚗 Vehicle**")
            st.info(f"{claim_row['vehicle_info']}\n\n{claim_row.get('license_plate', '')}")
    
    with col2:
        if pd.notna(claim_row.get('body_shop')):
            st.markdown("**🔧 Body Shop**")
            st.info(claim_row['body_shop'])
    
    with col3:
        if pd.notna(claim_row.get('medical_provider')):
            st.markdown("**🏥 Medical Provider**")
            st.info(claim_row['medical_provider'])
    
    with col4:
        if pd.notna(claim_row.get('attorney')):
            st.markdown("**⚖️ Attorney**")
            st.info(claim_row['attorney'])
    
    # Fraud ring info
    if pd.notna(claim_row.get('ring_id')):
        st.markdown("---")
        st.markdown("#### 🕸️ Fraud Ring Association")
        
        st.warning(
            f"**This claim is linked to a fraud ring!**\n\n"
            f"Ring ID: {claim_row['ring_id']}\n\n"
            f"Pattern Type: {claim_row.get('ring_type', 'Unknown')}"
        )
    
    # Actions
    st.markdown("---")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        if st.button("🔍 Investigate Further", use_container_width=True):
            st.info("Investigation workflow would be triggered here")
    
    with col2:
        if st.button("👁️ View Network", use_container_width=True):
            st.info("Network visualization would appear here")
    
    with col3:
        if st.button("📊 View Full Profile", use_container_width=True):
            st.switch_page("pages/03_🔍_Entity_Profile.py")


if __name__ == "__main__":
    main()
