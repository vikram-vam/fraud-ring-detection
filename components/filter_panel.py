"""
Filter Panel Component - Reusable filter controls for auto insurance
Provides consistent filtering across different pages
"""
import streamlit as st
from datetime import datetime, timedelta
from typing import Dict, List, Optional


class FilterPanel:
    """Component for rendering filter controls"""
    
    def __init__(self):
        self.accident_types = [
            "Rear-End Collision",
            "Side-Impact Collision",
            "Head-On Collision",
            "Hit and Run",
            "Single Vehicle Accident",
            "Parking Lot Collision",
            "Intersection Collision",
            "Multi-Vehicle Pileup"
        ]
        
        self.injury_types = [
            "Whiplash",
            "Back Pain",
            "Neck Pain",
            "Soft Tissue Injury",
            "Headaches",
            "Shoulder Pain",
            "Knee Injury",
            "Hip Injury",
            "No Injury"
        ]
        
        self.claim_statuses = [
            "Open",
            "Under Investigation",
            "Under Review",
            "Closed",
            "Pending Payment",
            "Denied"
        ]
        
        self.vehicle_makes = [
            "Toyota", "Honda", "Ford", "Chevrolet", "Nissan",
            "BMW", "Mercedes", "Audi", "Lexus", "Hyundai",
            "Kia", "Mazda", "Subaru", "Volkswagen"
        ]
        
        self.fraud_patterns = [
            'staged_accident',
            'body_shop_fraud',
            'medical_mill',
            'attorney_organized',
            'phantom_passenger',
            'tow_truck_kickback',
            'mixed'
        ]
    
    def render_claim_filters(self, prefix: str = "") -> Dict:
        """
        Render standard claim filters
        
        Args:
            prefix: Prefix for session state keys to avoid conflicts
            
        Returns:
            Dictionary of filter values
        """
        filters = {}
        
        st.markdown("### 🔍 Claim Filters")
        
        # Risk score filter
        with st.expander("Risk Score", expanded=True):
            filters['min_risk'] = st.slider(
                "Minimum Risk Score",
                min_value=0,
                max_value=100,
                value=70,
                step=5,
                key=f"{prefix}_min_risk",
                help="Filter claims by minimum risk score"
            )
            
            filters['max_risk'] = st.slider(
                "Maximum Risk Score",
                min_value=0,
                max_value=100,
                value=100,
                step=5,
                key=f"{prefix}_max_risk",
                help="Filter claims by maximum risk score"
            )
        
        # Accident type filter
        with st.expander("Accident Type"):
            filters['accident_types'] = st.multiselect(
                "Select Accident Types",
                options=self.accident_types,
                default=None,
                key=f"{prefix}_accident_types",
                help="Filter by type of accident"
            )
        
        # Injury type filter
        with st.expander("Injury Type"):
            filters['injury_types'] = st.multiselect(
                "Select Injury Types",
                options=self.injury_types,
                default=None,
                key=f"{prefix}_injury_types",
                help="Filter by type of injury"
            )
        
        # Claim status filter
        with st.expander("Claim Status"):
            filters['statuses'] = st.multiselect(
                "Select Statuses",
                options=self.claim_statuses,
                default=["Open", "Under Investigation", "Under Review"],
                key=f"{prefix}_statuses",
                help="Filter by claim status"
            )
        
        # Date range filter
        with st.expander("Date Range"):
            col1, col2 = st.columns(2)
            
            with col1:
                filters['start_date'] = st.date_input(
                    "From",
                    value=datetime.now() - timedelta(days=180),
                    key=f"{prefix}_start_date"
                )
            
            with col2:
                filters['end_date'] = st.date_input(
                    "To",
                    value=datetime.now(),
                    key=f"{prefix}_end_date"
                )
        
        # Amount filters
        with st.expander("Claim Amount"):
            filters['min_amount'] = st.number_input(
                "Minimum Total Amount ($)",
                min_value=0,
                value=0,
                step=5000,
                key=f"{prefix}_min_amount",
                help="Filter by minimum total claim amount"
            )
            
            filters['max_amount'] = st.number_input(
                "Maximum Total Amount ($)",
                min_value=0,
                value=0,
                step=5000,
                key=f"{prefix}_max_amount",
                help="Filter by maximum total claim amount (0 = no limit)"
            )
            
            filters['min_property_damage'] = st.number_input(
                "Minimum Property Damage ($)",
                min_value=0,
                value=0,
                step=1000,
                key=f"{prefix}_min_property",
                help="Filter by minimum property damage amount"
            )
            
            filters['min_bodily_injury'] = st.number_input(
                "Minimum Bodily Injury ($)",
                min_value=0,
                value=0,
                step=1000,
                key=f"{prefix}_min_bodily",
                help="Filter by minimum bodily injury amount"
            )
        
        # Fraud indicators
        with st.expander("Fraud Indicators"):
            filters['has_ring_only'] = st.checkbox(
                "🕸️ Ring Members Only",
                value=False,
                key=f"{prefix}_ring_only",
                help="Show only claims linked to fraud rings"
            )
            
            filters['has_attorney'] = st.checkbox(
                "⚖️ With Attorney",
                value=False,
                key=f"{prefix}_has_attorney",
                help="Show only claims with attorney representation"
            )
            
            filters['same_day_report'] = st.checkbox(
                "⏰ Same-Day Reporting",
                value=False,
                key=f"{prefix}_same_day",
                help="Show only claims reported same day as accident"
            )
            
            filters['delayed_report'] = st.checkbox(
                "⏳ Delayed Reporting (30+ days)",
                value=False,
                key=f"{prefix}_delayed",
                help="Show only claims reported 30+ days after accident"
            )
        
        return filters
    
    def render_vehicle_filters(self, prefix: str = "") -> Dict:
        """
        Render vehicle-specific filters
        
        Args:
            prefix: Prefix for session state keys
            
        Returns:
            Dictionary of filter values
        """
        filters = {}
        
        st.markdown("### 🚗 Vehicle Filters")
        
        with st.expander("Vehicle Details", expanded=True):
            filters['vehicle_makes'] = st.multiselect(
                "Vehicle Make",
                options=self.vehicle_makes,
                default=None,
                key=f"{prefix}_vehicle_makes",
                help="Filter by vehicle manufacturer"
            )
            
            filters['min_year'] = st.number_input(
                "Minimum Year",
                min_value=1990,
                max_value=datetime.now().year,
                value=2015,
                key=f"{prefix}_min_year"
            )
            
            filters['max_year'] = st.number_input(
                "Maximum Year",
                min_value=1990,
                max_value=datetime.now().year,
                value=datetime.now().year,
                key=f"{prefix}_max_year"
            )
            
            filters['min_accidents'] = st.number_input(
                "Minimum Accidents",
                min_value=1,
                value=1,
                key=f"{prefix}_min_accidents",
                help="Filter vehicles by minimum number of accidents"
            )
        
        return filters
    
    def render_entity_filters(self, prefix: str = "") -> Dict:
        """
        Render entity-specific filters (body shops, medical providers, etc.)
        
        Args:
            prefix: Prefix for session state keys
            
        Returns:
            Dictionary of filter values
        """
        filters = {}
        
        st.markdown("### 🏢 Entity Filters")
        
        with st.expander("Entity Statistics", expanded=True):
            filters['min_claim_count'] = st.number_input(
                "Minimum Claim Count",
                min_value=1,
                value=5,
                key=f"{prefix}_min_claims",
                help="Minimum number of claims associated with entity"
            )
            
            filters['min_avg_risk'] = st.slider(
                "Minimum Average Risk Score",
                min_value=0,
                max_value=100,
                value=50,
                step=5,
                key=f"{prefix}_min_avg_risk",
                help="Minimum average risk score of entity's claims"
            )
            
            filters['ring_linked_only'] = st.checkbox(
                "🕸️ Linked to Fraud Rings",
                value=False,
                key=f"{prefix}_ring_linked",
                help="Show only entities linked to fraud rings"
            )
        
        return filters
    
    def render_fraud_ring_filters(self, prefix: str = "") -> Dict:
        """
        Render fraud ring filters
        
        Args:
            prefix: Prefix for session state keys
            
        Returns:
            Dictionary of filter values
        """
        filters = {}
        
        st.markdown("### 🕸️ Fraud Ring Filters")
        
        # Ring type
        with st.expander("Ring Type", expanded=True):
            filters['ring_types'] = st.multiselect(
                "Ring Type",
                options=['KNOWN', 'DISCOVERED', 'SUSPICIOUS', 'EMERGING'],
                default=['DISCOVERED', 'SUSPICIOUS'],
                key=f"{prefix}_ring_types"
            )
        
        # Pattern type
        with st.expander("Pattern Type", expanded=True):
            pattern_display = {
                'staged_accident': '🎭 Staged Accidents',
                'body_shop_fraud': '🔧 Body Shop Fraud',
                'medical_mill': '🏥 Medical Mill',
                'attorney_organized': '⚖️ Attorney Organized',
                'phantom_passenger': '👥 Phantom Passengers',
                'tow_truck_kickback': '🚛 Tow Truck Kickback',
                'mixed': '🔀 Mixed Patterns'
            }
            
            selected_patterns = st.multiselect(
                "Pattern Type",
                options=list(pattern_display.keys()),
                format_func=lambda x: pattern_display[x],
                default=None,
                key=f"{prefix}_pattern_types"
            )
            
            filters['pattern_types'] = selected_patterns
        
        # Status
        with st.expander("Status"):
            filters['statuses'] = st.multiselect(
                "Status",
                options=['CONFIRMED', 'UNDER_REVIEW', 'DISMISSED'],
                default=['CONFIRMED', 'UNDER_REVIEW'],
                key=f"{prefix}_ring_statuses"
            )
        
        # Confidence and size
        with st.expander("Ring Characteristics"):
            filters['min_confidence'] = st.slider(
                "Minimum Confidence",
                min_value=0.0,
                max_value=1.0,
                value=0.6,
                step=0.05,
                key=f"{prefix}_min_confidence",
                help="Minimum confidence score"
            )
            
            filters['min_members'] = st.number_input(
                "Minimum Members",
                min_value=2,
                value=3,
                step=1,
                key=f"{prefix}_min_members",
                help="Minimum number of ring members"
            )
            
            filters['min_fraud_amount'] = st.number_input(
                "Minimum Estimated Fraud ($)",
                min_value=0,
                value=0,
                step=10000,
                key=f"{prefix}_min_fraud_amount",
                help="Minimum estimated fraud amount"
            )
        
        return filters
    
    def render_location_filters(self, prefix: str = "") -> Dict:
        """
        Render location-based filters
        
        Args:
            prefix: Prefix for session state keys
            
        Returns:
            Dictionary of filter values
        """
        filters = {}
        
        st.markdown("### 📍 Location Filters")
        
        with st.expander("Location Criteria", expanded=True):
            filters['cities'] = st.multiselect(
                "Cities",
                options=[
                    "Los Angeles", "San Diego", "San Jose", "San Francisco",
                    "Sacramento", "Oakland", "Fresno", "Long Beach"
                ],
                default=None,
                key=f"{prefix}_cities"
            )
            
            filters['hotspot_only'] = st.checkbox(
                "🔥 Hotspots Only (5+ accidents)",
                value=False,
                key=f"{prefix}_hotspot",
                help="Show only locations with 5 or more accidents"
            )
            
            filters['min_accidents'] = st.number_input(
                "Minimum Accidents at Location",
                min_value=1,
                value=1,
                key=f"{prefix}_min_loc_accidents"
            )
        
        return filters
    
    def render_advanced_filters(self, prefix: str = "") -> Dict:
        """
        Render advanced/custom filters
        
        Args:
            prefix: Prefix for session state keys
            
        Returns:
            Dictionary of filter values
        """
        filters = {}
        
        st.markdown("### ⚙️ Advanced Filters")
        
        with st.expander("Advanced Options"):
            filters['exclude_closed'] = st.checkbox(
                "Exclude Closed Claims",
                value=False,
                key=f"{prefix}_exclude_closed"
            )
            
            filters['high_value_only'] = st.checkbox(
                "High Value Only (>$50k)",
                value=False,
                key=f"{prefix}_high_value"
            )
            
            filters['injury_claims_only'] = st.checkbox(
                "Injury Claims Only",
                value=False,
                key=f"{prefix}_injury_only"
            )
            
            filters['multiple_claimants'] = st.checkbox(
                "Multiple Claimants per Vehicle",
                value=False,
                key=f"{prefix}_multi_claimants",
                help="Vehicles with multiple different claimants"
            )
            
            filters['result_limit'] = st.selectbox(
                "Results Limit",
                options=[25, 50, 100, 200, 500],
                index=2,
                key=f"{prefix}_limit"
            )
        
        return filters
    
    def apply_filters_to_query(self, base_query: str, filters: Dict) -> tuple:
        """
        Apply filters to a Cypher query
        
        Args:
            base_query: Base Cypher query string
            filters: Dictionary of filter values
            
        Returns:
            Tuple of (modified_query, parameters_dict)
        """
        where_clauses = []
        params = {}
        
        # Risk score filters
        if filters.get('min_risk'):
            where_clauses.append("cl.risk_score >= $min_risk")
            params['min_risk'] = filters['min_risk']
        
        if filters.get('max_risk') and filters['max_risk'] < 100:
            where_clauses.append("cl.risk_score <= $max_risk")
            params['max_risk'] = filters['max_risk']
        
        # Accident type filter
        if filters.get('accident_types'):
            where_clauses.append("cl.accident_type IN $accident_types")
            params['accident_types'] = filters['accident_types']
        
        # Status filter
        if filters.get('statuses'):
            where_clauses.append("cl.status IN $statuses")
            params['statuses'] = filters['statuses']
        
        # Amount filters
        if filters.get('min_amount'):
            where_clauses.append("cl.total_claim_amount >= $min_amount")
            params['min_amount'] = filters['min_amount']
        
        if filters.get('max_amount') and filters['max_amount'] > 0:
            where_clauses.append("cl.total_claim_amount <= $max_amount")
            params['max_amount'] = filters['max_amount']
        
        # Build WHERE clause
        if where_clauses:
            where_clause = " AND ".join(where_clauses)
            
            # Insert WHERE clause into query
            if "WHERE" in base_query:
                modified_query = base_query.replace("WHERE", f"WHERE {where_clause} AND", 1)
            else:
                # Find appropriate place to insert WHERE
                match_index = base_query.find("RETURN")
                if match_index > 0:
                    modified_query = base_query[:match_index] + f"WHERE {where_clause}\n" + base_query[match_index:]
                else:
                    modified_query = base_query
        else:
            modified_query = base_query
        
        return modified_query, params
