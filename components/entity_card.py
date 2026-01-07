"""
Entity Card Component - Render entity information cards
Displays formatted cards for all auto insurance entities
"""
import streamlit as st
import pandas as pd
from typing import Dict, List, Optional
from datetime import datetime


class EntityCard:
    """Component for rendering entity information cards"""
    
    def __init__(self):
        pass
    
    def render_claimant_card(self, claimant_data: Dict):
        """
        Render claimant information card
        
        Args:
            claimant_data: Dictionary with claimant information
        """
        st.markdown("### 👤 Claimant Information")
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.markdown("**Personal Details**")
            st.markdown(f"**Name:** {claimant_data.get('name', 'Unknown')}")
            st.markdown(f"**ID:** {claimant_data.get('claimant_id', 'N/A')}")
            st.markdown(f"**Email:** {claimant_data.get('email', 'N/A')}")
            st.markdown(f"**Phone:** {claimant_data.get('phone', 'N/A')}")
        
        with col2:
            st.markdown("**License Information**")
            st.markdown(f"**Driver's License:** {claimant_data.get('drivers_license', 'N/A')}")
            
            dob = claimant_data.get('date_of_birth', 'N/A')
            if dob and dob != 'N/A':
                try:
                    if isinstance(dob, str):
                        dob_date = datetime.strptime(dob, '%Y-%m-%d')
                    else:
                        dob_date = dob
                    age = (datetime.now() - dob_date).days // 365
                    st.markdown(f"**Date of Birth:** {dob} (Age: {age})")
                except:
                    st.markdown(f"**Date of Birth:** {dob}")
            else:
                st.markdown(f"**Date of Birth:** N/A")
        
        with col3:
            st.markdown("**Activity Summary**")
            st.metric("Total Claims", claimant_data.get('total_claims', 0))
            st.metric("Total Claimed", f"${claimant_data.get('total_claimed', 0):,.0f}")
            
            # Fraud ring indicator
            if claimant_data.get('ring_id'):
                st.error(f"🕸️ Fraud Ring Member")
            else:
                st.success("✓ No Ring Association")
    
    def render_claim_card(self, claim_data: Dict):
        """
        Render auto insurance claim information card
        
        Args:
            claim_data: Dictionary with claim information
        """
        st.markdown("### 🚗 Claim Information")
        
        # Header with risk score
        col1, col2 = st.columns([3, 1])
        
        with col1:
            st.markdown(f"**Claim Number:** {claim_data.get('claim_number', 'Unknown')}")
            st.markdown(f"**Status:** {claim_data.get('status', 'Unknown')}")
        
        with col2:
            risk_score = claim_data.get('risk_score', 0)
            risk_level = 'HIGH' if risk_score >= 70 else 'MEDIUM' if risk_score >= 40 else 'LOW'
            color = '#E74C3C' if risk_level == 'HIGH' else '#F39C12' if risk_level == 'MEDIUM' else '#27AE60'
            
            st.markdown(
                f"""
                <div style="
                    background-color: {color};
                    color: white;
                    padding: 15px;
                    border-radius: 10px;
                    text-align: center;
                ">
                    <h2 style="margin: 0; color: white;">{risk_score:.1f}</h2>
                    <p style="margin: 5px 0 0 0; font-size: 12px;">{risk_level} RISK</p>
                </div>
                """,
                unsafe_allow_html=True
            )
        
        st.markdown("---")
        
        # Accident details
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.markdown("**Accident Details**")
            st.markdown(f"**Type:** {claim_data.get('accident_type', 'Unknown')}")
            st.markdown(f"**Date:** {claim_data.get('accident_date', 'Unknown')}")
            st.markdown(f"**Reported:** {claim_data.get('report_date', 'Unknown')}")
            
            # Calculate days to report
            try:
                if claim_data.get('accident_date') and claim_data.get('report_date'):
                    acc_date = datetime.strptime(str(claim_data['accident_date']), '%Y-%m-%d')
                    rep_date = datetime.strptime(str(claim_data['report_date']), '%Y-%m-%d')
                    days_diff = (rep_date - acc_date).days
                    
                    if days_diff == 0:
                        st.warning(f"⚠️ Reported same day")
                    elif days_diff > 30:
                        st.warning(f"⚠️ Reported {days_diff} days later")
                    else:
                        st.info(f"Reported {days_diff} days later")
            except:
                pass
        
        with col2:
            st.markdown("**Injury Information**")
            st.markdown(f"**Injury Type:** {claim_data.get('injury_type', 'None')}")
            
            bodily_injury = claim_data.get('bodily_injury_amount', 0)
            if bodily_injury > 0:
                st.markdown(f"**Bodily Injury:** ${bodily_injury:,.2f}")
                
                if bodily_injury > 50000:
                    st.warning("⚠️ High bodily injury amount")
            else:
                st.markdown("**Bodily Injury:** No injury claim")
        
        with col3:
            st.markdown("**Financial Details**")
            property_damage = claim_data.get('property_damage_amount', 0)
            total_amount = claim_data.get('total_claim_amount', 0)
            
            st.markdown(f"**Property Damage:** ${property_damage:,.2f}")
            st.markdown(f"**Total Claim:** ${total_amount:,.2f}")
            
            if total_amount > 75000:
                st.error("🚨 High value claim")
            elif total_amount > 40000:
                st.warning("⚠️ Above average claim")
    
    def render_vehicle_card(self, vehicle_data: Dict):
        """
        Render vehicle information card
        
        Args:
            vehicle_data: Dictionary with vehicle information
        """
        st.markdown("### 🚗 Vehicle Information")
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.markdown("**Vehicle Details**")
            st.markdown(f"**Make:** {vehicle_data.get('make', 'Unknown')}")
            st.markdown(f"**Model:** {vehicle_data.get('model', 'Unknown')}")
            st.markdown(f"**Year:** {vehicle_data.get('year', 'Unknown')}")
            st.markdown(f"**Color:** {vehicle_data.get('color', 'Unknown')}")
        
        with col2:
            st.markdown("**Identification**")
            st.markdown(f"**VIN:** {vehicle_data.get('vin', 'N/A')}")
            st.markdown(f"**License Plate:** {vehicle_data.get('license_plate', 'N/A')}")
            st.markdown(f"**Vehicle ID:** {vehicle_data.get('vehicle_id', 'N/A')}")
        
        with col3:
            st.markdown("**Accident History**")
            accident_count = vehicle_data.get('accident_count', 0)
            
            if accident_count >= 3:
                st.error(f"🚨 {accident_count} accidents - SUSPICIOUS")
            elif accident_count >= 2:
                st.warning(f"⚠️ {accident_count} accidents")
            else:
                st.metric("Accidents", accident_count)
            
            if accident_count > 0:
                st.metric("Total Damage", f"${vehicle_data.get('total_damage', 0):,.0f}")
    
    def render_body_shop_card(self, body_shop_data: Dict):
        """
        Render body shop information card
        
        Args:
            body_shop_data: Dictionary with body shop information
        """
        st.markdown("### 🔧 Body Shop Information")
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.markdown("**Business Details**")
            st.markdown(f"**Name:** {body_shop_data.get('name', 'Unknown')}")
            st.markdown(f"**License:** {body_shop_data.get('license_number', 'N/A')}")
            st.markdown(f"**ID:** {body_shop_data.get('body_shop_id', 'N/A')}")
        
        with col2:
            st.markdown("**Location**")
            st.markdown(f"**Address:** {body_shop_data.get('street', 'N/A')}")
            st.markdown(f"**City:** {body_shop_data.get('city', 'Unknown')}")
            st.markdown(f"**State:** {body_shop_data.get('state', 'Unknown')}")
            st.markdown(f"**Zip:** {body_shop_data.get('zip_code', 'N/A')}")
            st.markdown(f"**Phone:** {body_shop_data.get('phone', 'N/A')}")
        
        with col3:
            st.markdown("**Repair Statistics**")
            repair_count = body_shop_data.get('repair_count', 0)
            total_repairs = body_shop_data.get('total_repairs', 0)
            avg_risk = body_shop_data.get('avg_risk_score', 0)
            
            st.metric("Total Repairs", repair_count)
            st.metric("Total Amount", f"${total_repairs:,.0f}")
            
            if avg_risk >= 70:
                st.error(f"🚨 Avg Risk: {avg_risk:.1f}")
            elif avg_risk >= 50:
                st.warning(f"⚠️ Avg Risk: {avg_risk:.1f}")
            else:
                st.info(f"Avg Risk: {avg_risk:.1f}")
    
    def render_medical_provider_card(self, provider_data: Dict):
        """
        Render medical provider information card
        
        Args:
            provider_data: Dictionary with medical provider information
        """
        st.markdown("### 🏥 Medical Provider Information")
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.markdown("**Provider Details**")
            st.markdown(f"**Name:** {provider_data.get('name', 'Unknown')}")
            st.markdown(f"**Type:** {provider_data.get('provider_type', 'Unknown')}")
            st.markdown(f"**License:** {provider_data.get('license_number', 'N/A')}")
            st.markdown(f"**ID:** {provider_data.get('provider_id', 'N/A')}")
        
        with col2:
            st.markdown("**Location**")
            st.markdown(f"**Address:** {provider_data.get('street', 'N/A')}")
            st.markdown(f"**City:** {provider_data.get('city', 'Unknown')}")
            st.markdown(f"**State:** {provider_data.get('state', 'Unknown')}")
            st.markdown(f"**Zip:** {provider_data.get('zip_code', 'N/A')}")
            st.markdown(f"**Phone:** {provider_data.get('phone', 'N/A')}")
        
        with col3:
            st.markdown("**Treatment Statistics**")
            treatment_count = provider_data.get('treatment_count', 0)
            total_treatments = provider_data.get('total_treatments', 0)
            avg_risk = provider_data.get('avg_risk_score', 0)
            
            st.metric("Total Treatments", treatment_count)
            st.metric("Total Amount", f"${total_treatments:,.0f}")
            
            if avg_risk >= 70:
                st.error(f"🚨 Avg Risk: {avg_risk:.1f}")
            elif avg_risk >= 50:
                st.warning(f"⚠️ Avg Risk: {avg_risk:.1f}")
            else:
                st.info(f"Avg Risk: {avg_risk:.1f}")
    
    def render_attorney_card(self, attorney_data: Dict):
        """
        Render attorney information card
        
        Args:
            attorney_data: Dictionary with attorney information
        """
        st.markdown("### ⚖️ Attorney Information")
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.markdown("**Attorney Details**")
            st.markdown(f"**Name:** {attorney_data.get('name', 'Unknown')}")
            st.markdown(f"**Firm:** {attorney_data.get('firm', 'Unknown')}")
            st.markdown(f"**Bar Number:** {attorney_data.get('bar_number', 'N/A')}")
            st.markdown(f"**ID:** {attorney_data.get('attorney_id', 'N/A')}")
        
        with col2:
            st.markdown("**Contact Information**")
            st.markdown(f"**Address:** {attorney_data.get('street', 'N/A')}")
            st.markdown(f"**City:** {attorney_data.get('city', 'Unknown')}")
            st.markdown(f"**State:** {attorney_data.get('state', 'Unknown')}")
            st.markdown(f"**Zip:** {attorney_data.get('zip_code', 'N/A')}")
            st.markdown(f"**Phone:** {attorney_data.get('phone', 'N/A')}")
            st.markdown(f"**Email:** {attorney_data.get('email', 'N/A')}")
        
        with col3:
            st.markdown("**Practice Statistics**")
            client_count = attorney_data.get('client_count', 0)
            case_count = attorney_data.get('case_count', 0)
            total_represented = attorney_data.get('total_represented', 0)
            avg_risk = attorney_data.get('avg_risk_score', 0)
            
            st.metric("Clients", client_count)
            st.metric("Cases", case_count)
            st.metric("Total Represented", f"${total_represented:,.0f}")
            
            if avg_risk >= 70:
                st.error(f"🚨 Avg Risk: {avg_risk:.1f}")
            elif avg_risk >= 50:
                st.warning(f"⚠️ Avg Risk: {avg_risk:.1f}")
            else:
                st.info(f"Avg Risk: {avg_risk:.1f}")
    
    def render_tow_company_card(self, tow_data: Dict):
        """
        Render tow company information card
        
        Args:
            tow_data: Dictionary with tow company information
        """
        st.markdown("### 🚛 Tow Company Information")
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.markdown("**Company Details**")
            st.markdown(f"**Name:** {tow_data.get('name', 'Unknown')}")
            st.markdown(f"**License:** {tow_data.get('license_number', 'N/A')}")
            st.markdown(f"**ID:** {tow_data.get('tow_company_id', 'N/A')}")
        
        with col2:
            st.markdown("**Contact Information**")
            st.markdown(f"**City:** {tow_data.get('city', 'Unknown')}")
            st.markdown(f"**State:** {tow_data.get('state', 'Unknown')}")
            st.markdown(f"**Phone:** {tow_data.get('phone', 'N/A')}")
        
        with col3:
            st.markdown("**Service Statistics**")
            tow_count = tow_data.get('tow_count', 0)
            avg_risk = tow_data.get('avg_risk_score', 0)
            
            st.metric("Total Tows", tow_count)
            
            if avg_risk >= 70:
                st.error(f"🚨 Avg Risk: {avg_risk:.1f}")
            elif avg_risk >= 50:
                st.warning(f"⚠️ Avg Risk: {avg_risk:.1f}")
            else:
                st.info(f"Avg Risk: {avg_risk:.1f}")
    
    def render_accident_location_card(self, location_data: Dict):
        """
        Render accident location information card
        
        Args:
            location_data: Dictionary with location information
        """
        st.markdown("### 📍 Accident Location Information")
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.markdown("**Location Details**")
            st.markdown(f"**Intersection:** {location_data.get('intersection', 'Unknown')}")
            st.markdown(f"**City:** {location_data.get('city', 'Unknown')}")
            st.markdown(f"**State:** {location_data.get('state', 'Unknown')}")
            st.markdown(f"**ID:** {location_data.get('location_id', 'N/A')}")
        
        with col2:
            st.markdown("**Coordinates**")
            st.markdown(f"**Latitude:** {location_data.get('latitude', 'N/A')}")
            st.markdown(f"**Longitude:** {location_data.get('longitude', 'N/A')}")
        
        with col3:
            st.markdown("**Accident Statistics**")
            accident_count = location_data.get('accident_count', 0)
            total_amount = location_data.get('total_amount', 0)
            avg_risk = location_data.get('avg_risk_score', 0)
            
            if accident_count >= 5:
                st.error(f"🚨 HOTSPOT: {accident_count} accidents")
            elif accident_count >= 3:
                st.warning(f"⚠️ {accident_count} accidents")
            else:
                st.metric("Accidents", accident_count)
            
            st.metric("Total Claims", f"${total_amount:,.0f}")
            st.info(f"Avg Risk: {avg_risk:.1f}")
    
    def render_witness_card(self, witness_data: Dict):
        """
        Render witness information card
        
        Args:
            witness_data: Dictionary with witness information
        """
        st.markdown("### 👁️ Witness Information")
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.markdown("**Witness Details**")
            st.markdown(f"**Name:** {witness_data.get('name', 'Unknown')}")
            st.markdown(f"**Phone:** {witness_data.get('phone', 'N/A')}")
            st.markdown(f"**ID:** {witness_data.get('witness_id', 'N/A')}")
        
        with col2:
            st.markdown("**Witness Activity**")
            witnessed_count = witness_data.get('witnessed_count', 0)
            
            if witnessed_count >= 3:
                st.error(f"🚨 SUSPICIOUS: {witnessed_count} accidents")
                st.warning("⚠️ Professional witness indicator")
            elif witnessed_count >= 2:
                st.warning(f"⚠️ {witnessed_count} accidents witnessed")
            else:
                st.metric("Accidents Witnessed", witnessed_count)
        
        with col3:
            st.markdown("**Risk Assessment**")
            avg_risk = witness_data.get('avg_risk_score', 0)
            
            if avg_risk >= 70:
                st.error(f"🚨 Avg Risk: {avg_risk:.1f}")
            elif avg_risk >= 50:
                st.warning(f"⚠️ Avg Risk: {avg_risk:.1f}")
            else:
                st.info(f"Avg Risk: {avg_risk:.1f}")
    
    def render_fraud_ring_card(self, ring_data: Dict):
        """
        Render fraud ring information card
        
        Args:
            ring_data: Dictionary with fraud ring information
        """
        st.markdown("### 🕸️ Fraud Ring Information")
        
        # Pattern type mapping
        pattern_names = {
            'staged_accident': '🎭 Staged Accidents',
            'body_shop_fraud': '🔧 Body Shop Fraud',
            'medical_mill': '🏥 Medical Mill',
            'attorney_organized': '⚖️ Attorney Organized',
            'phantom_passenger': '👥 Phantom Passengers',
            'tow_truck_kickback': '🚛 Tow Truck Kickback',
            'mixed': '🔀 Mixed Patterns'
        }
        
        pattern_display = pattern_names.get(ring_data.get('pattern_type', ''), ring_data.get('pattern_type', 'Unknown'))
        
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.markdown("**Ring Details**")
            st.markdown(f"**Ring ID:** {ring_data.get('ring_id', 'Unknown')}")
            st.markdown(f"**Pattern:** {pattern_display}")
            st.markdown(f"**Type:** {ring_data.get('ring_type', 'Unknown')}")
        
        with col2:
            st.markdown("**Status**")
            status = ring_data.get('status', 'Unknown')
            status_icon = "✅" if status == 'CONFIRMED' else "⏳" if status == 'UNDER_REVIEW' else "❌"
            st.markdown(f"**Status:** {status_icon} {status}")
            
            confidence = ring_data.get('confidence_score', 0)
            st.metric("Confidence", f"{confidence:.1%}")
        
        with col3:
            st.markdown("**Ring Metrics**")
            st.metric("Members", ring_data.get('member_count', 0))
            st.metric("Estimated Fraud", f"${ring_data.get('estimated_fraud_amount', 0):,.0f}")
        
        with col4:
            st.markdown("**Discovery Info**")
            st.markdown(f"**Discovered:** {ring_data.get('discovered_date', 'Unknown')}")
            st.markdown(f"**By:** {ring_data.get('discovered_by', 'System')}")
