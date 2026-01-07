"""
Risk Explainer - Explain risk scores with visualizations
"""
import streamlit as st
import plotly.graph_objects as go
from typing import Dict, List
import logging

logger = logging.getLogger(__name__)


class RiskExplainer:
    """
    Component to explain and visualize risk scores
    """
    
    def __init__(self):
        self.risk_colors = {
            'HIGH': '#E74C3C',
            'MEDIUM': '#F39C12',
            'LOW': '#27AE60'
        }
    
    def render_risk_score_card(self, risk_data: Dict):
        """
        Render a risk score card with gauge and explanation
        
        Args:
            risk_data: Dictionary with risk score and components
        """
        if not risk_data or 'risk_score' not in risk_data:
            st.warning("No risk data available")
            return
        
        risk_score = risk_data.get('risk_score', 0)
        risk_level = risk_data.get('risk_level', 'UNKNOWN')
        
        col1, col2 = st.columns([1, 2])
        
        with col1:
            # Risk gauge
            self._render_risk_gauge(risk_score, risk_level)
        
        with col2:
            # Risk level badge
            self._render_risk_badge(risk_level, risk_score)
            
            # Risk factors
            if 'factors' in risk_data and risk_data['factors']:
                st.markdown("**Risk Factors:**")
                for factor in risk_data['factors']:
                    st.markdown(f"- {factor}")
            
            # Risk components breakdown
            if 'components' in risk_data:
                with st.expander("📊 Risk Score Breakdown"):
                    self._render_risk_components(risk_data['components'])
    
    def _render_risk_gauge(self, risk_score: float, risk_level: str):
        """Render a gauge chart for risk score"""
        fig = go.Figure(go.Indicator(
            mode="gauge+number",
            value=risk_score,
            domain={'x': [0, 1], 'y': [0, 1]},
            title={'text': "Risk Score"},
            gauge={
                'axis': {'range': [None, 100]},
                'bar': {'color': self.risk_colors.get(risk_level, '#95A5A6')},
                'steps': [
                    {'range': [0, 40], 'color': "rgba(39, 174, 96, 0.2)"},
                    {'range': [40, 70], 'color': "rgba(243, 156, 18, 0.2)"},
                    {'range': [70, 100], 'color': "rgba(231, 76, 60, 0.2)"}
                ],
                'threshold': {
                    'line': {'color': "red", 'width': 4},
                    'thickness': 0.75,
                    'value': 70
                }
            }
        ))
        
        fig.update_layout(
            height=250,
            margin=dict(l=20, r=20, t=40, b=20)
        )
        
        st.plotly_chart(fig, use_container_width=True)
    
    def _render_risk_badge(self, risk_level: str, risk_score: float):
        """Render risk level badge"""
        color = self.risk_colors.get(risk_level, '#95A5A6')
        
        st.markdown(
            f"""
            <div style="
                background-color: {color};
                color: white;
                padding: 10px 20px;
                border-radius: 5px;
                text-align: center;
                font-size: 20px;
                font-weight: bold;
                margin-bottom: 15px;
            ">
                {risk_level} RISK ({risk_score:.1f}/100)
            </div>
            """,
            unsafe_allow_html=True
        )
    
    def _render_risk_components(self, components: Dict):
        """Render risk score component breakdown"""
        # Create horizontal bar chart
        component_names = []
        component_values = []
        
        component_labels = {
            'amount_risk': 'Claim Amount',
            'frequency_risk': 'Claim Frequency',
            'connection_risk': 'Shared Connections',
            'temporal_risk': 'Temporal Pattern',
            'entity_risk': 'Entity Reputation'
        }
        
        for key, value in components.items():
            label = component_labels.get(key, key.replace('_', ' ').title())
            component_names.append(label)
            component_values.append(value)
        
        fig = go.Figure(go.Bar(
            x=component_values,
            y=component_names,
            orientation='h',
            marker=dict(
                color=component_values,
                colorscale='RdYlGn_r',
                showscale=False
            ),
            text=[f"{v:.1f}" for v in component_values],
            textposition='auto'
        ))
        
        fig.update_layout(
            title="Risk Component Scores",
            xaxis_title="Score",
            xaxis=dict(range=[0, 100]),
            height=300,
            margin=dict(l=150, r=20, t=40, b=40)
        )
        
        st.plotly_chart(fig, use_container_width=True)
    
    def render_risk_comparison(self, entity_scores: List[Dict]):
        """
        Render comparison of risk scores across entities
        
        Args:
            entity_scores: List of dicts with 'name' and 'risk_score'
        """
        if not entity_scores:
            st.warning("No entities to compare")
            return
        
        names = [e['name'] for e in entity_scores]
        scores = [e['risk_score'] for e in entity_scores]
        levels = [e.get('risk_level', 'UNKNOWN') for e in entity_scores]
        
        colors = [self.risk_colors.get(level, '#95A5A6') for level in levels]
        
        fig = go.Figure(go.Bar(
            x=names,
            y=scores,
            marker=dict(color=colors),
            text=[f"{s:.1f}" for s in scores],
            textposition='auto'
        ))
        
        fig.update_layout(
            title="Risk Score Comparison",
            xaxis_title="Entity",
            yaxis_title="Risk Score",
            yaxis=dict(range=[0, 100]),
            height=400
        )
        
        st.plotly_chart(fig, use_container_width=True)
    
    def render_risk_trend(self, trend_data: List[Dict]):
        """
        Render risk score trend over time
        
        Args:
            trend_data: List of dicts with 'date' and 'risk_score'
        """
        if not trend_data:
            st.warning("No trend data available")
            return
        
        dates = [d['date'] for d in trend_data]
        scores = [d['risk_score'] for d in trend_data]
        
        fig = go.Figure()
        
        fig.add_trace(go.Scatter(
            x=dates,
            y=scores,
            mode='lines+markers',
            name='Risk Score',
            line=dict(color='#E74C3C', width=2),
            marker=dict(size=8)
        ))
        
        # Add threshold lines
        fig.add_hline(y=70, line_dash="dash", line_color="red", 
                      annotation_text="High Risk Threshold")
        fig.add_hline(y=40, line_dash="dash", line_color="orange", 
                      annotation_text="Medium Risk Threshold")
        
        fig.update_layout(
            title="Risk Score Trend",
            xaxis_title="Date",
            yaxis_title="Risk Score",
            yaxis=dict(range=[0, 100]),
            height=400
        )
        
        st.plotly_chart(fig, use_container_width=True)
    
    def render_risk_distribution(self, risk_scores: List[float]):
        """
        Render distribution of risk scores
        
        Args:
            risk_scores: List of risk scores
        """
        if not risk_scores:
            st.warning("No risk scores to display")
            return
        
        fig = go.Figure(go.Histogram(
            x=risk_scores,
            nbinsx=20,
            marker=dict(
                color='#3498DB',
                line=dict(color='white', width=1)
            )
        ))
        
        fig.update_layout(
            title="Risk Score Distribution",
            xaxis_title="Risk Score",
            yaxis_title="Count",
            height=300
        )
        
        st.plotly_chart(fig, use_container_width=True)
