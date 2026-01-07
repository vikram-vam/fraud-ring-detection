"""
Entity Profile Page - Deep-dive analysis of auto insurance entities
Claimants, vehicles, body shops, medical providers, attorneys, etc.
"""
import streamlit as st
import pandas as pd
import logging

from data.neo4j_driver import get_neo4j_driver
from utils.logger import setup_logger

# Setup
logger = setup_logger(__name__)
st.set_page_config(page_title="Entity Profile", page_icon="🔍", layout="wide")

# Initialize
driver = get_neo4j_driver()


def main():
    """Main function for Entity Profile page"""
    
    # Header
    st.title("🔍 Entity Profile")
    st.markdown("**Deep-dive analysis of entities in the auto insurance fraud network**")
    
    # Entity type selector
    entity_type = st.selectbox(
        "Select Entity Type",
        options=[
            "Claimant",
            "Vehicle", 
            "Body Shop",
            "Medical Provider",
            "Attorney",
            "Tow Company",
            "Accident Location",
            "Witness"
        ],
        help="Choose the type of entity to analyze"
    )
    
    # Search interface
    search_entity(entity_type)


def search_entity(entity_type: str):
    """Search and select entity"""
    
    col1, col2 = st.columns([3, 1])
    
    with col1:
        search_term = st.text_input(
            f"Search {entity_type}",
            placeholder=f"Enter {entity_type.lower()} name, ID, or other identifier...",
            help=f"Search for {entity_type.lower()} by name, ID, or other identifiers"
        )
    
    with col2:
        search_button = st.button("🔍 Search", use_container_width=True)
    
    if search_button and search_term:
        with st.spinner(f"Searching for {entity_type.lower()}s..."):
            results = search_by_type(entity_type, search_term)
        
        if not results:
            st.warning(f"No {entity_type.lower()}s found matching '{search_term}'")
            return
        
        # Display search results
        st.subheader(f"Search Results ({len(results)} found)")
        
        # Create selection options based on entity type
        if entity_type == "Claimant":
            options = [f"{r['name']} ({r['claimant_id']})" for r in results]
            ids = [r['claimant_id'] for r in results]
        
        elif entity_type == "Vehicle":
            options = [f"{r['make']} {r['model']} {r['year']} - {r['license_plate']} ({r['vehicle_id']})" for r in results]
            ids = [r['vehicle_id'] for r in results]
        
        elif entity_type == "Body Shop":
            options = [f"{r['name']} - {r['city']} ({r['body_shop_id']})" for r in results]
            ids = [r['body_shop_id'] for r in results]
        
        elif entity_type == "Medical Provider":
            options = [f"{r['name']} - {r['provider_type']} ({r['provider_id']})" for r in results]
            ids = [r['provider_id'] for r in results]
        
        elif entity_type == "Attorney":
            options = [f"{r['name']} - {r['firm']} ({r['attorney_id']})" for r in results]
            ids = [r['attorney_id'] for r in results]
        
        elif entity_type == "Tow Company":
            options = [f"{r['name']} - {r['city']} ({r['tow_company_id']})" for r in results]
            ids = [r['tow_company_id'] for r in results]
        
        elif entity_type == "Accident Location":
            options = [f"{r['intersection']} - {r['city']} ({r['location_id']})" for r in results]
            ids = [r['location_id'] for r in results]
        
        elif entity_type == "Witness":
            options = [f"{r['name']} - {r['phone']} ({r['witness_id']})" for r in results]
            ids = [r['witness_id'] for r in results]
        
        selected_idx = st.selectbox(
            f"Select {entity_type}",
            options=range(len(options)),
            format_func=lambda x: options[x]
        )
        
        if selected_idx is not None:
            entity_id = ids[selected_idx]
            display_entity_profile(entity_type, entity_id, results[selected_idx])


def search_by_type(entity_type: str, search_term: str):
    """Search entities by type"""
    try:
        search_pattern = f"(?i).*{search_term}.*"
        
        if entity_type == "Claimant":
            query = """
            MATCH (c:Claimant)
            WHERE c.name =~ $pattern OR c.claimant_id =~ $pattern OR c.email =~ $pattern
            RETURN c.claimant_id as claimant_id, c.name as name, c.email as email, c.phone as phone
            LIMIT 20
            """
        
        elif entity_type == "Vehicle":
            query = """
            MATCH (v:Vehicle)
            WHERE v.make =~ $pattern OR v.model =~ $pattern OR v.vin =~ $pattern OR v.license_plate =~ $pattern
            RETURN v.vehicle_id as vehicle_id, v.make as make, v.model as model, v.year as year, 
                   v.license_plate as license_plate, v.vin as vin
            LIMIT 20
            """
        
        elif entity_type == "Body Shop":
            query = """
            MATCH (b:BodyShop)
            WHERE b.name =~ $pattern OR b.city =~ $pattern
            RETURN b.body_shop_id as body_shop_id, b.name as name, b.city as city, b.phone as phone
            LIMIT 20
            """
        
        elif entity_type == "Medical Provider":
            query = """
            MATCH (m:MedicalProvider)
            WHERE m.name =~ $pattern OR m.provider_type =~ $pattern OR m.city =~ $pattern
            RETURN m.provider_id as provider_id, m.name as name, m.provider_type as provider_type, 
                   m.city as city, m.phone as phone
            LIMIT 20
            """
        
        elif entity_type == "Attorney":
            query = """
            MATCH (a:Attorney)
            WHERE a.name =~ $pattern OR a.firm =~ $pattern
            RETURN a.attorney_id as attorney_id, a.name as name, a.firm as firm, 
                   a.city as city, a.phone as phone
            LIMIT 20
            """
        
        elif entity_type == "Tow Company":
            query = """
            MATCH (t:TowCompany)
            WHERE t.name =~ $pattern OR t.city =~ $pattern
            RETURN t.tow_company_id as tow_company_id, t.name as name, t.city as city, t.phone as phone
            LIMIT 20
            """
        
        elif entity_type == "Accident Location":
            query = """
            MATCH (l:AccidentLocation)
            WHERE l.intersection =~ $pattern OR l.city =~ $pattern
            RETURN l.location_id as location_id, l.intersection as intersection, l.city as city
            LIMIT 20
            """
        
        elif entity_type == "Witness":
            query = """
            MATCH (w:Witness)
            WHERE w.name =~ $pattern OR w.phone =~ $pattern
            RETURN w.witness_id as witness_id, w.name as name, w.phone as phone
            LIMIT 20
            """
        
        else:
            return []
        
        return driver.execute_query(query, {'pattern': search_pattern})
        
    except Exception as e:
        logger.error(f"Error searching {entity_type}: {e}", exc_info=True)
        st.error(f"Error searching: {str(e)}")
        return []


def display_entity_profile(entity_type: str, entity_id: str, entity_data: dict):
    """Display comprehensive entity profile"""
    
    st.markdown("---")
    st.header(f"{entity_type} Profile")
    
    # Dispatch to specific profile renderer
    if entity_type == "Claimant":
        display_claimant_profile(entity_id, entity_data)
    elif entity_type == "Vehicle":
        display_vehicle_profile(entity_id, entity_data)
    elif entity_type == "Body Shop":
        display_body_shop_profile(entity_id, entity_data)
    elif entity_type == "Medical Provider":
        display_medical_provider_profile(entity_id, entity_data)
    elif entity_type == "Attorney":
        display_attorney_profile(entity_id, entity_data)
    elif entity_type == "Tow Company":
        display_tow_company_profile(entity_id, entity_data)
    elif entity_type == "Accident Location":
        display_accident_location_profile(entity_id, entity_data)
    elif entity_type == "Witness":
        display_witness_profile(entity_id, entity_data)


def display_claimant_profile(claimant_id: str, entity_data: dict):
    """Display claimant profile"""
    
    # Basic info
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown("### 👤 Basic Information")
        st.markdown(f"**Name:** {entity_data.get('name', 'Unknown')}")
        st.markdown(f"**ID:** {claimant_id}")
        st.markdown(f"**Email:** {entity_data.get('email', 'N/A')}")
        st.markdown(f"**Phone:** {entity_data.get('phone', 'N/A')}")
    
    # Get claim statistics
    stats_query = """
    MATCH (c:Claimant {claimant_id: $claimant_id})-[:FILED]->(cl:Claim)
    OPTIONAL MATCH (c)-[:MEMBER_OF]->(r:FraudRing)
    RETURN 
        count(cl) as claim_count,
        sum(cl.total_claim_amount) as total_claimed,
        avg(cl.risk_score) as avg_risk,
        collect(DISTINCT r.ring_id) as rings
    """
    stats = driver.execute_query(stats_query, {'claimant_id': claimant_id})
    
    if stats:
        stats_data = stats[0]
        
        with col2:
            st.markdown("### 📊 Claim Statistics")
            st.metric("Total Claims", stats_data.get('claim_count', 0))
            st.metric("Total Claimed", f"${stats_data.get('total_claimed', 0):,.0f}")
            st.metric("Avg Risk Score", f"{stats_data.get('avg_risk', 0):.1f}")
        
        with col3:
            st.markdown("### ⚠️ Risk Indicators")
            rings = [r for r in stats_data.get('rings', []) if r]
            if rings:
                st.error(f"🕸️ Member of {len(rings)} fraud ring(s)")
                for ring_id in rings:
                    st.caption(f"- {ring_id}")
            else:
                st.success("✓ Not linked to fraud rings")
    
    # Claims table
    st.markdown("---")
    st.markdown("### 💰 Claim History")
    
    claims_query = """
    MATCH (c:Claimant {claimant_id: $claimant_id})-[:FILED]->(cl:Claim)
    OPTIONAL MATCH (cl)-[:INVOLVES_VEHICLE]->(v:Vehicle)
    RETURN 
        cl.claim_number as claim_number,
        cl.accident_type as accident_type,
        cl.accident_date as accident_date,
        cl.total_claim_amount as amount,
        cl.risk_score as risk_score,
        v.make + ' ' + v.model as vehicle
    ORDER BY cl.accident_date DESC
    """
    
    claims = driver.execute_query(claims_query, {'claimant_id': claimant_id})
    
    if claims:
        df = pd.DataFrame(claims)
        df['amount'] = df['amount'].apply(lambda x: f"${x:,.0f}")
        df['risk_score'] = df['risk_score'].apply(lambda x: f"{x:.1f}")
        st.dataframe(df, use_container_width=True, hide_index=True)
    else:
        st.info("No claims found")


def display_vehicle_profile(vehicle_id: str, entity_data: dict):
    """Display vehicle profile"""
    
    st.markdown(f"### 🚗 {entity_data['make']} {entity_data['model']} ({entity_data['year']})")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("**Vehicle Details:**")
        st.markdown(f"- **VIN:** {entity_data.get('vin', 'N/A')}")
        st.markdown(f"- **License Plate:** {entity_data.get('license_plate', 'N/A')}")
        st.markdown(f"- **Make/Model/Year:** {entity_data['make']} {entity_data['model']} {entity_data['year']}")
    
    # Get accident history
    accident_query = """
    MATCH (v:Vehicle {vehicle_id: $vehicle_id})<-[:INVOLVES_VEHICLE]-(cl:Claim)
    MATCH (c:Claimant)-[:FILED]->(cl)
    RETURN 
        count(cl) as accident_count,
        collect(cl.claim_number) as claims,
        collect(c.name) as claimants,
        sum(cl.total_claim_amount) as total_amount,
        avg(cl.risk_score) as avg_risk
    """
    
    accidents = driver.execute_query(accident_query, {'vehicle_id': vehicle_id})
    
    if accidents:
        acc_data = accidents[0]
        
        with col2:
            st.markdown("**Accident History:**")
            accident_count = acc_data.get('accident_count', 0)
            
            if accident_count >= 3:
                st.error(f"⚠️ **{accident_count} accidents** - SUSPICIOUS!")
            else:
                st.metric("Accidents", accident_count)
            
            st.metric("Total Claimed", f"${acc_data.get('total_amount', 0):,.0f}")
            st.metric("Avg Risk Score", f"{acc_data.get('avg_risk', 0):.1f}")
    
    # Claims table
    st.markdown("---")
    st.markdown("### 🚗 Accident History")
    
    claims_query = """
    MATCH (v:Vehicle {vehicle_id: $vehicle_id})<-[:INVOLVES_VEHICLE]-(cl:Claim)
    MATCH (c:Claimant)-[:FILED]->(cl)
    RETURN 
        cl.claim_number as claim_number,
        c.name as claimant,
        cl.accident_type as accident_type,
        cl.accident_date as date,
        cl.total_claim_amount as amount,
        cl.risk_score as risk_score
    ORDER BY cl.accident_date DESC
    """
    
    claims = driver.execute_query(claims_query, {'vehicle_id': vehicle_id})
    
    if claims:
        df = pd.DataFrame(claims)
        df['amount'] = df['amount'].apply(lambda x: f"${x:,.0f}")
        st.dataframe(df, use_container_width=True, hide_index=True)


def display_body_shop_profile(body_shop_id: str, entity_data: dict):
    """Display body shop profile"""
    
    st.markdown(f"### 🔧 {entity_data['name']}")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown("**Contact Information:**")
        st.markdown(f"- **Location:** {entity_data.get('city', 'Unknown')}")
        st.markdown(f"- **Phone:** {entity_data.get('phone', 'N/A')}")
    
    # Get repair statistics
    stats_query = """
    MATCH (b:BodyShop {body_shop_id: $body_shop_id})<-[:REPAIRED_AT]-(cl:Claim)
    MATCH (c:Claimant)-[:FILED]->(cl)
    OPTIONAL MATCH (c)-[:MEMBER_OF]->(r:FraudRing)
    RETURN 
        count(cl) as repair_count,
        count(DISTINCT c) as unique_claimants,
        sum(cl.property_damage_amount) as total_repairs,
        avg(cl.risk_score) as avg_risk,
        count(DISTINCT r) as ring_count
    """
    
    stats = driver.execute_query(stats_query, {'body_shop_id': body_shop_id})
    
    if stats:
        stats_data = stats[0]
        
        with col2:
            st.markdown("**Repair Statistics:**")
            st.metric("Total Repairs", stats_data.get('repair_count', 0))
            st.metric("Unique Claimants", stats_data.get('unique_claimants', 0))
            st.metric("Total Amount", f"${stats_data.get('total_repairs', 0):,.0f}")
        
        with col3:
            st.markdown("**Risk Assessment:**")
            avg_risk = stats_data.get('avg_risk', 0)
            st.metric("Avg Risk Score", f"{avg_risk:.1f}")
            
            ring_count = stats_data.get('ring_count', 0)
            if ring_count > 0:
                st.error(f"⚠️ Linked to {ring_count} fraud ring(s)")
            else:
                st.success("✓ No fraud ring links")
    
    # Repair history
    st.markdown("---")
    st.markdown("### 🔧 Repair History")
    
    repairs_query = """
    MATCH (b:BodyShop {body_shop_id: $body_shop_id})<-[:REPAIRED_AT]-(cl:Claim)
    MATCH (c:Claimant)-[:FILED]->(cl)
    RETURN 
        cl.claim_number as claim_number,
        c.name as claimant,
        cl.accident_date as date,
        cl.property_damage_amount as amount,
        cl.risk_score as risk_score
    ORDER BY cl.accident_date DESC
    LIMIT 50
    """
    
    repairs = driver.execute_query(repairs_query, {'body_shop_id': body_shop_id})
    
    if repairs:
        df = pd.DataFrame(repairs)
        df['amount'] = df['amount'].apply(lambda x: f"${x:,.0f}")
        st.dataframe(df, use_container_width=True, hide_index=True)


def display_medical_provider_profile(provider_id: str, entity_data: dict):
    """Display medical provider profile"""
    
    st.markdown(f"### 🏥 {entity_data['name']}")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown("**Provider Information:**")
        st.markdown(f"- **Type:** {entity_data.get('provider_type', 'Unknown')}")
        st.markdown(f"- **Location:** {entity_data.get('city', 'Unknown')}")
        st.markdown(f"- **Phone:** {entity_data.get('phone', 'N/A')}")
    
    # Get treatment statistics
    stats_query = """
    MATCH (m:MedicalProvider {provider_id: $provider_id})<-[:TREATED_BY]-(cl:Claim)
    MATCH (c:Claimant)-[:FILED]->(cl)
    OPTIONAL MATCH (c)-[:MEMBER_OF]->(r:FraudRing)
    RETURN 
        count(cl) as treatment_count,
        count(DISTINCT c) as unique_patients,
        sum(cl.bodily_injury_amount) as total_treatments,
        avg(cl.risk_score) as avg_risk,
        count(DISTINCT r) as ring_count
    """
    
    stats = driver.execute_query(stats_query, {'provider_id': provider_id})
    
    if stats:
        stats_data = stats[0]
        
        with col2:
            st.markdown("**Treatment Statistics:**")
            st.metric("Total Treatments", stats_data.get('treatment_count', 0))
            st.metric("Unique Patients", stats_data.get('unique_patients', 0))
            st.metric("Total Amount", f"${stats_data.get('total_treatments', 0):,.0f}")
        
        with col3:
            st.markdown("**Risk Assessment:**")
            avg_risk = stats_data.get('avg_risk', 0)
            st.metric("Avg Risk Score", f"{avg_risk:.1f}")
            
            ring_count = stats_data.get('ring_count', 0)
            if ring_count > 0:
                st.error(f"⚠️ Linked to {ring_count} fraud ring(s)")
            else:
                st.success("✓ No fraud ring links")
    
    # Treatment history
    st.markdown("---")
    st.markdown("### 🏥 Treatment History")
    
    treatments_query = """
    MATCH (m:MedicalProvider {provider_id: $provider_id})<-[:TREATED_BY]-(cl:Claim)
    MATCH (c:Claimant)-[:FILED]->(cl)
    RETURN 
        cl.claim_number as claim_number,
        c.name as patient,
        cl.injury_type as injury,
        cl.accident_date as date,
        cl.bodily_injury_amount as amount,
        cl.risk_score as risk_score
    ORDER BY cl.accident_date DESC
    LIMIT 50
    """
    
    treatments = driver.execute_query(treatments_query, {'provider_id': provider_id})
    
    if treatments:
        df = pd.DataFrame(treatments)
        df['amount'] = df['amount'].apply(lambda x: f"${x:,.0f}")
        st.dataframe(df, use_container_width=True, hide_index=True)


def display_attorney_profile(attorney_id: str, entity_data: dict):
    """Display attorney profile"""
    
    st.markdown(f"### ⚖️ {entity_data['name']}")
    st.markdown(f"**{entity_data['firm']}**")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown("**Contact Information:**")
        st.markdown(f"- **Location:** {entity_data.get('city', 'Unknown')}")
        st.markdown(f"- **Phone:** {entity_data.get('phone', 'N/A')}")
    
    # Get client statistics
    stats_query = """
    MATCH (a:Attorney {attorney_id: $attorney_id})<-[:REPRESENTED_BY]-(cl:Claim)
    MATCH (c:Claimant)-[:FILED]->(cl)
    OPTIONAL MATCH (c)-[:MEMBER_OF]->(r:FraudRing)
    RETURN 
        count(cl) as case_count,
        count(DISTINCT c) as unique_clients,
        sum(cl.total_claim_amount) as total_represented,
        avg(cl.risk_score) as avg_risk,
        count(DISTINCT r) as ring_count
    """
    
    stats = driver.execute_query(stats_query, {'attorney_id': attorney_id})
    
    if stats:
        stats_data = stats[0]
        
        with col2:
            st.markdown("**Case Statistics:**")
            st.metric("Total Cases", stats_data.get('case_count', 0))
            st.metric("Unique Clients", stats_data.get('unique_clients', 0))
            st.metric("Total Represented", f"${stats_data.get('total_represented', 0):,.0f}")
        
        with col3:
            st.markdown("**Risk Assessment:**")
            avg_risk = stats_data.get('avg_risk', 0)
            st.metric("Avg Risk Score", f"{avg_risk:.1f}")
            
            ring_count = stats_data.get('ring_count', 0)
            if ring_count > 0:
                st.error(f"⚠️ Linked to {ring_count} fraud ring(s)")
            else:
                st.success("✓ No fraud ring links")
    
    # Case history
    st.markdown("---")
    st.markdown("### ⚖️ Case History")
    
    cases_query = """
    MATCH (a:Attorney {attorney_id: $attorney_id})<-[:REPRESENTED_BY]-(cl:Claim)
    MATCH (c:Claimant)-[:FILED]->(cl)
    RETURN 
        cl.claim_number as claim_number,
        c.name as client,
        cl.accident_type as accident_type,
        cl.accident_date as date,
        cl.total_claim_amount as amount,
        cl.risk_score as risk_score
    ORDER BY cl.accident_date DESC
    LIMIT 50
    """
    
    cases = driver.execute_query(cases_query, {'attorney_id': attorney_id})
    
    if cases:
        df = pd.DataFrame(cases)
        df['amount'] = df['amount'].apply(lambda x: f"${x:,.0f}")
        st.dataframe(df, use_container_width=True, hide_index=True)


def display_tow_company_profile(tow_company_id: str, entity_data: dict):
    """Display tow company profile"""
    
    st.markdown(f"### 🚛 {entity_data['name']}")
    
    st.markdown(f"**Location:** {entity_data.get('city', 'Unknown')}")
    st.markdown(f"**Phone:** {entity_data.get('phone', 'N/A')}")
    
    # Get tow statistics
    stats_query = """
    MATCH (t:TowCompany {tow_company_id: $tow_company_id})<-[:TOWED_BY]-(cl:Claim)
    RETURN 
        count(cl) as tow_count,
        avg(cl.risk_score) as avg_risk
    """
    
    stats = driver.execute_query(stats_query, {'tow_company_id': tow_company_id})
    
    if stats:
        stats_data = stats[0]
        st.metric("Total Tows", stats_data.get('tow_count', 0))
        st.metric("Avg Risk Score", f"{stats_data.get('avg_risk', 0):.1f}")


def display_accident_location_profile(location_id: str, entity_data: dict):
    """Display accident location profile"""
    
    st.markdown(f"### 📍 {entity_data['intersection']}")
    st.markdown(f"**City:** {entity_data.get('city', 'Unknown')}")
    
    # Get accident statistics
    stats_query = """
    MATCH (l:AccidentLocation {location_id: $location_id})<-[:OCCURRED_AT]-(cl:Claim)
    RETURN 
        count(cl) as accident_count,
        avg(cl.risk_score) as avg_risk,
        sum(cl.total_claim_amount) as total_amount
    """
    
    stats = driver.execute_query(stats_query, {'location_id': location_id})
    
    if stats:
        stats_data = stats[0]
        accident_count = stats_data.get('accident_count', 0)
        
        if accident_count >= 5:
            st.error(f"⚠️ HOTSPOT: {accident_count} accidents at this location!")
        else:
            st.metric("Accidents at Location", accident_count)
        
        st.metric("Avg Risk Score", f"{stats_data.get('avg_risk', 0):.1f}")
        st.metric("Total Claims", f"${stats_data.get('total_amount', 0):,.0f}")


def display_witness_profile(witness_id: str, entity_data: dict):
    """Display witness profile"""
    
    st.markdown(f"### 👁️ {entity_data['name']}")
    st.markdown(f"**Phone:** {entity_data.get('phone', 'N/A')}")
    
    # Get witness statistics
    stats_query = """
    MATCH (w:Witness {witness_id: $witness_id})-[:WITNESSED]->(cl:Claim)
    RETURN 
        count(cl) as witnessed_count,
        avg(cl.risk_score) as avg_risk
    """
    
    stats = driver.execute_query(stats_query, {'witness_id': witness_id})
    
    if stats:
        stats_data = stats[0]
        witnessed_count = stats_data.get('witnessed_count', 0)
        
        if witnessed_count >= 3:
            st.error(f"⚠️ SUSPICIOUS: Witness appeared in {witnessed_count} accidents!")
        else:
            st.metric("Accidents Witnessed", witnessed_count)
        
        st.metric("Avg Risk Score", f"{stats_data.get('avg_risk', 0):.1f}")
    
    # Claims witnessed
    st.markdown("---")
    st.markdown("### 👁️ Accidents Witnessed")
    
    claims_query = """
    MATCH (w:Witness {witness_id: $witness_id})-[:WITNESSED]->(cl:Claim)
    MATCH (c:Claimant)-[:FILED]->(cl)
    RETURN 
        cl.claim_number as claim_number,
        c.name as claimant,
        cl.accident_type as accident_type,
        cl.accident_date as date,
        cl.risk_score as risk_score
    ORDER BY cl.accident_date DESC
    """
    
    claims = driver.execute_query(claims_query, {'witness_id': witness_id})
    
    if claims:
        df = pd.DataFrame(claims)
        st.dataframe(df, use_container_width=True, hide_index=True)


if __name__ == "__main__":
    main()
