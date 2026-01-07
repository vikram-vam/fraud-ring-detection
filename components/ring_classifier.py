"""
Ring Classifier - Display fraud ring classification and confidence
"""
import streamlit as st
from typing import Dict, List
import logging

logger = logging.getLogger(__name__)


class RingClassifier:
    """
    Component to display fraud ring classification
    """
    
    def __init__(self):
        self.pattern_types = {
            'address_farm': {
                'label': 'Address Farm',
                'icon': '🏠',
                'description': 'Multiple claimants at same address',
                'color': '#E74C3C'
            },
            'provider_centric': {
                'label': 'Provider-Centric',
                'icon': '🏥',
                'description': 'Claimants sharing suspicious provider',
                'color': '#3498DB'
            },
            'attorney_centric': {
                'label': 'Attorney-Centric',
                'icon': '⚖️',
                'description': 'Claimants sharing same attorney',
                'color': '#9B59B6'
            },
            'mixed': {
                'label': 'Mixed Pattern',
                'icon': '🔀',
                'description': 'Multiple shared connection types',
                'color': '#F39C12'
            }
        }
        
        self.ring_types = {
            'KNOWN': {
                'label': 'Known Fraud Ring',
                'badge_color': '#C0392B',
                'icon': '🔴'
            },
            'DISCOVERED': {
                'label': 'Discovered Ring',
                'badge_color': '#E67E22',
                'icon': '🟠'
            },
            'SUSPICIOUS': {
                'label': 'Suspicious Pattern',
                'badge_color': '#F39C12',
                'icon': '🟡'
            },
            'EMERGING': {
                'label': 'Emerging Pattern',
                'badge_color': '#3498DB',
                'icon': '🔵'
            }
        }
        
        self.status_types = {
            'CONFIRMED': {'label': 'Confirmed', 'color': '#E74C3C'},
            'UNDER_REVIEW': {'label': 'Under Review', 'color': '#F39C12'},
            'DISMISSED': {'label': 'Dismissed', 'color': '#95A5A6'}
        }
    
    def render_ring_card(self, ring_data: Dict):
        """
        Render a comprehensive fraud ring card
        
        Args:
            ring_data: Fraud ring data dictionary
        """
        if not ring_data:
            st.warning("No ring data available")
            return
        
        ring_type = ring_data.get('ring_type', 'DISCOVERED')
        pattern_type = ring_data.get('pattern_type', 'mixed')
        status = ring_data.get('status', 'UNDER_REVIEW')
        confidence = ring_data.get('confidence_score', 0)
        
        # Header with ring type and pattern
        self._render_ring_header(ring_type, pattern_type)
        
        # Status and confidence
        col1, col2 = st.columns(2)
        
        with col1:
            self._render_status_badge(status)
        
        with col2:
            self._render_confidence_meter(confidence)
        
        # Ring details
        with st.expander("📋 Ring Details", expanded=True):
            self._render_ring_details(ring_data)
    
    def _render_ring_header(self, ring_type: str, pattern_type: str):
        """Render ring header with type and pattern"""
        ring_info = self.ring_types.get(ring_type, self.ring_types['DISCOVERED'])
        pattern_info = self.pattern_types.get(pattern_type, self.pattern_types['mixed'])
        
        st.markdown(
            f"""
            <div style="
                background: linear-gradient(135deg, {ring_info['badge_color']} 0%, {pattern_info['color']} 100%);
                color: white;
                padding: 20px;
                border-radius: 10px;
                margin-bottom: 20px;
            ">
                <h2 style="margin: 0; color: white;">
                    {ring_info['icon']} {ring_info['label']}
                </h2>
                <p style="margin: 5px 0 0 0; font-size: 16px;">
                    {pattern_info['icon']} {pattern_info['label']}: {pattern_info['description']}
                </p>
            </div>
            """,
            unsafe_allow_html=True
        )
    
    def _render_status_badge(self, status: str):
        """Render status badge"""
        status_info = self.status_types.get(status, self.status_types['UNDER_REVIEW'])
        
        st.markdown(
            f"""
            <div style="
                background-color: {status_info['color']};
                color: white;
                padding: 10px;
                border-radius: 5px;
                text-align: center;
                font-weight: bold;
            ">
                Status: {status_info['label']}
            </div>
            """,
            unsafe_allow_html=True
        )
    
    def _render_confidence_meter(self, confidence: float):
        """Render confidence meter"""
        confidence_pct = confidence * 100 if confidence <= 1 else confidence
        
        color = '#E74C3C' if confidence_pct >= 70 else '#F39C12' if confidence_pct >= 50 else '#3498DB'
        
        st.markdown(
            f"""
            <div style="padding: 10px;">
                <div style="font-weight: bold; margin-bottom: 5px;">
                    Confidence: {confidence_pct:.1f}%
                </div>
                <div style="
                    background-color: #ecf0f1;
                    border-radius: 10px;
                    height: 20px;
                    overflow: hidden;
                ">
                    <div style="
                        background-color: {color};
                        width: {confidence_pct}%;
                        height: 100%;
                        transition: width 0.3s ease;
                    "></div>
                </div>
            </div>
            """,
            unsafe_allow_html=True
        )
    
    def _render_ring_details(self, ring_data: Dict):
        """Render detailed ring information"""
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.metric(
                "Members",
                ring_data.get('member_count', 0)
            )
        
        with col2:
            fraud_amount = ring_data.get('estimated_fraud_amount', 0)
            st.metric(
                "Estimated Fraud",
                f"${fraud_amount:,.0f}"
            )
        
        with col3:
            discovered_date = ring_data.get('discovered_date', 'Unknown')
            if discovered_date and discovered_date != 'Unknown':
                if isinstance(discovered_date, str):
                    discovered_date = discovered_date[:10]
            st.metric(
                "Discovered",
                discovered_date
            )
        
        # Additional details
        if ring_data.get('discovered_by'):
            st.write(f"**Discovered By:** {ring_data['discovered_by']}")
        
        if ring_data.get('confirmed_by'):
            st.write(f"**Confirmed By:** {ring_data['confirmed_by']}")
        
        if ring_data.get('dismissal_reason'):
            st.warning(f"**Dismissal Reason:** {ring_data['dismissal_reason']}")
    
    def render_pattern_explanation(self, pattern_type: str):
        """
        Render explanation of pattern type
        
        Args:
            pattern_type: Pattern type code
        """
        pattern_info = self.pattern_types.get(pattern_type)
        
        if not pattern_info:
            return
        
        st.info(
            f"""
            **{pattern_info['icon']} {pattern_info['label']}**
            
            {pattern_info['description']}
            
            This pattern indicates potential organized fraud where multiple claimants 
            are connected through shared entities or relationships.
            """
        )
    
    def render_ring_list(self, rings: List[Dict], max_display: int = 10):
        """
        Render a list of fraud rings
        
        Args:
            rings: List of ring dictionaries
            max_display: Maximum number to display
        """
        if not rings:
            st.info("No fraud rings found")
            return
        
        st.write(f"**Showing {min(len(rings), max_display)} of {len(rings)} rings**")
        
        for idx, ring in enumerate(rings[:max_display]):
            with st.expander(
                f"{ring.get('ring_id', 'Unknown')} - "
                f"{ring.get('member_count', 0)} members - "
                f"Confidence: {ring.get('confidence_score', 0):.1f}%"
            ):
                self.render_ring_card(ring)
