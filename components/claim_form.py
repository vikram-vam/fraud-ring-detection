"""
CLAIM FORM COMPONENT
Form for adding new claims to the database
"""

import streamlit as st
from datetime import datetime, timedelta
import random

class ClaimForm:
    def __init__(self, driver):
        self.driver = driver
    
    def render(self):
        """Render the claim form and handle submission"""
        
        # Get available claimants
        with self.driver.session() as session:
            result = session.run("""
                MATCH (c:Claimant)
                RETURN c.claimant_id as id, c.name as name
                ORDER BY c.name
                LIMIT 200
            """)
            claimants = [(record['id'], record['name']) for record in result]
        
        if not claimants:
            st.error("No claimants found in database")
            return None
        
        # Claimant Source Selection - OUTSIDE form for dynamic updates
        claimant_source = st.radio("Claimant Source", ["Existing Customer", "New Applicant"], horizontal=True, key="claimant_source_radio")
        is_new_claimant = (claimant_source == "New Applicant")
        
        # Conditional fields based on source - also outside form for visibility
        if is_new_claimant:
            new_col1, new_col2, new_col3 = st.columns(3)
            with new_col1:
                new_claimant_name = st.text_input("Name", key="new_name")
            with new_col2:
                new_claimant_age = st.number_input("Age", 18, 100, 30, key="new_age")
            with new_col3:
                new_claimant_address = st.text_input("Address", key="new_addr")
            claimant_id_to_use = None
            selected_claimant = None
        else:
            claimant_options = [f"{name} ({id})" for id, name in claimants]
            selected_claimant = st.selectbox("Select Claimant", claimant_options)
            claimant_id_to_use = selected_claimant.split('(')[1].strip(')')
            new_claimant_name = None
            new_claimant_age = None
            new_claimant_address = None
        
        with st.form("new_claim_form"):
            st.subheader("Claim Details")
            
            col1, col2 = st.columns(2)
            
            with col1:
                
                # Claim type
                claim_type = st.selectbox("Claim Type", [
                    'Collision',
                    'Comprehensive',
                    'Liability',
                    'Personal Injury Protection',
                    'Uninsured Motorist'
                ])
                
                # Claim amount
                claim_amount = st.number_input("Claim Amount ($)", min_value=500.0, max_value=100000.0, value=5000.0, step=500.0)
                
                # Incident date
                incident_date = st.date_input("Incident Date", value=datetime.now() - timedelta(days=7))
            
            with col2:
                # Location
                location = st.text_input("Incident Location", value="Downtown")
                
                # Weather
                weather = st.selectbox("Weather Condition", ['Clear', 'Rain', 'Snow', 'Fog'])
                
                # Description
                description = st.text_area("Description", value=f"{claim_type} incident")
                
                # Status
                status = st.selectbox("Status", ['Open', 'Under Investigation', 'Approved'])
            
            # Service providers
            st.subheader("Service Providers")
            st.info("Select providers involved in this claim. (Hint: Linking to 'Dr. Roy Cook' often triggers fraud rules)")
            
            col3, col4, col5 = st.columns(3)
            
            with col3:
                # Get repair shops
                with self.driver.session() as session:
                    result = session.run("""
                        MATCH (s:RepairShop)
                        RETURN s.shop_id as id, s.name as name
                        ORDER BY s.name
                    """)
                    shops = [(record['id'], record['name']) for record in result]
                
                shop_options = ["None"] + [f"{name} ({id})" for id, name in shops]
                selected_shop = st.selectbox("Repair Shop", shop_options)
            
            with col4:
                # Get medical providers
                with self.driver.session() as session:
                    result = session.run("""
                        MATCH (m:MedicalProvider)
                        RETURN m.provider_id as id, m.name as name
                        ORDER BY m.name
                    """)
                    providers = [(record['id'], record['name']) for record in result]
                
                # Highlight Dr. Roy Cook if present for demo convenience (optional, but good for user)
                provider_desc = lambda n: f"⚠️ {n}" if "Roy Cook" in n else n
                
                provider_options = ["None"] + [f"{provider_desc(name)} ({id})" for id, name in providers]
                selected_provider = st.selectbox("Medical Provider", provider_options)
            
            with col5:
                # Get lawyers
                with self.driver.session() as session:
                    result = session.run("""
                        MATCH (l:Lawyer)
                        RETURN l.lawyer_id as id, l.name as name
                        ORDER BY l.name
                    """)
                    lawyers = [(record['id'], record['name']) for record in result]
                
                lawyer_options = ["None"] + [f"{name} ({id})" for id, name in lawyers]
                selected_lawyer = st.selectbox("Lawyer", lawyer_options)
            
            # Submit button
            submitted = st.form_submit_button("Submit Claim", use_container_width=True)
            
            if submitted:
                # Validate New Claimant
                if is_new_claimant and not new_claimant_name:
                    st.error("Please enter a name for the new claimant.")
                    return None

                # Generate IDs
                claim_id = f"CLM_NEW_{random.randint(10000, 99999)}"
                claim_number = f"CN{random.randint(1000000000, 9999999999)}"
                
                # Handle New Claimant Creation
                if is_new_claimant:
                    claimant_id_to_use = f"CLMNT_NEW_{random.randint(1000,9999)}"
                    try:
                        self._create_new_claimant(claimant_id_to_use, new_claimant_name, new_claimant_age, new_claimant_address)
                        st.success(f"Created new claimant profile: {new_claimant_name}")
                    except Exception as e:
                        st.error(f"Failed to create claimant: {e}")
                        return None

                # Extract shop/provider/lawyer IDs
                shop_id = selected_shop.split('(')[1].strip(')') if selected_shop != "None" else None
                provider_id = selected_provider.split('(')[1].strip(')') if selected_provider != "None" else None
                lawyer_id = selected_lawyer.split('(')[1].strip(')') if selected_lawyer != "None" else None
                
                # Insert claim into database
                try:
                    self._insert_claim(
                        claim_id, claim_number, claimant_id_to_use,
                        incident_date.isoformat(), datetime.now().isoformat(),
                        claim_amount, claim_type, status, description,
                        location, weather, shop_id, provider_id, lawyer_id
                    )
                    
                    return {
                        'claim_id': claim_id,
                        'claim_number': claim_number,
                        'claimant_id': claimant_id_to_use,
                        'success': True
                    }
                    
                except Exception as e:
                    st.error(f"Error submitting claim: {e}")
                    return None
        
        return None

    def _create_new_claimant(self, claimant_id, name, age, address):
        """Create a new claimant node and a default policy/vehicle for them"""
        with self.driver.session() as session:
            # Create Claimant
            session.run("""
                CREATE (c:Claimant {
                    claimant_id: $id,
                    name: $name,
                    age: $age,
                    address: $address,
                    risk_score: 0
                })
            """, id=claimant_id, name=name, age=age, address=address)
            
            # Create Dummy Policy & Vehicle (Required for Schema consistency)
            session.run("""
                MATCH (c:Claimant {claimant_id: $id})
                CREATE (p:Policy {policy_id: 'POL_' + $id, type: 'Personal Auto'})
                CREATE (v:Vehicle {vehicle_id: 'VEH_' + $id, make: 'Generic', model: 'Sedan', year: 2020})
                CREATE (c)-[:HAS_POLICY]->(p)
                CREATE (v)-[:INSURED_BY]->(p)
            """, id=claimant_id)
    
    def _insert_claim(self, claim_id, claim_number, claimant_id, incident_date, 
                      claim_date, claim_amount, claim_type, status, description,
                      location, weather, shop_id, provider_id, lawyer_id):
        """Insert new claim into Neo4j"""
        
        with self.driver.session() as session:
            # Get claimant's policy and vehicle
            policy_result = session.run("""
                MATCH (c:Claimant {claimant_id: $claimant_id})-[:HAS_POLICY]->(p:Policy)
                MATCH (v:Vehicle)-[:INSURED_BY]->(p)
                RETURN p.policy_id as policy_id, v.vehicle_id as vehicle_id
                LIMIT 1
            """, claimant_id=claimant_id)
            
            policy_record = policy_result.single()
            if not policy_record:
                raise Exception("No policy/vehicle found for claimant")
            
            policy_id = policy_record['policy_id']
            vehicle_id = policy_record['vehicle_id']
            
            # Create claim node and relationships
            query = """
                MATCH (c:Claimant {claimant_id: $claimant_id})
                MATCH (p:Policy {policy_id: $policy_id})
                MATCH (v:Vehicle {vehicle_id: $vehicle_id})
                CREATE (cl:Claim {
                    claim_id: $claim_id,
                    claim_number: $claim_number,
                    claim_date: $claim_date,
                    incident_date: $incident_date,
                    claim_amount: $claim_amount,
                    claim_type: $claim_type,
                    status: $status,
                    description: $description,
                    location: $location,
                    weather_condition: $weather,
                    is_fraud_ring: false
                })
                CREATE (c)-[:FILED_CLAIM]->(cl)
                CREATE (cl)-[:UNDER_POLICY]->(p)
                CREATE (cl)-[:INVOLVES_VEHICLE]->(v)
            """
            
            params = {
                'claim_id': claim_id,
                'claim_number': claim_number,
                'claimant_id': claimant_id,
                'policy_id': policy_id,
                'vehicle_id': vehicle_id,
                'claim_date': claim_date,
                'incident_date': incident_date,
                'claim_amount': claim_amount,
                'claim_type': claim_type,
                'status': status,
                'description': description,
                'location': location,
                'weather': weather
            }
            
            session.run(query, **params)
            
            # Add service provider relationships if selected
            if shop_id:
                session.run("""
                    MATCH (cl:Claim {claim_id: $claim_id})
                    MATCH (s:RepairShop {shop_id: $shop_id})
                    CREATE (cl)-[:REPAIRED_AT]->(s)
                """, claim_id=claim_id, shop_id=shop_id)
            
            if provider_id:
                session.run("""
                    MATCH (cl:Claim {claim_id: $claim_id})
                    MATCH (m:MedicalProvider {provider_id: $provider_id})
                    CREATE (cl)-[:TREATED_BY]->(m)
                """, claim_id=claim_id, provider_id=provider_id)
            
            if lawyer_id:
                session.run("""
                    MATCH (cl:Claim {claim_id: $claim_id})
                    MATCH (l:Lawyer {lawyer_id: $lawyer_id})
                    CREATE (cl)-[:REPRESENTED_BY]->(l)
                """, claim_id=claim_id, lawyer_id=lawyer_id)
