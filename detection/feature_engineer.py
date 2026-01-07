"""
Feature Engineer - Extract and engineer features for auto insurance fraud detection
Prepares data for ML models and advanced analytics
"""
import logging
from typing import Dict, List, Optional
from datetime import datetime, timedelta
import pandas as pd

from data.neo4j_driver import get_neo4j_driver
from utils.logger import setup_logger

logger = setup_logger(__name__)


class FeatureEngineer:
    """
    Feature engineering for auto insurance fraud detection
    Extracts graph-based and domain-specific features
    """
    
    def __init__(self):
        self.driver = get_neo4j_driver()
    
    def extract_claim_features(self, claim_id: str) -> Dict:
        """
        Extract comprehensive features for a single claim
        
        Args:
            claim_id: Claim identifier
            
        Returns:
            Dictionary of features
        """
        logger.debug(f"Extracting features for claim {claim_id}")
        
        try:
            features = {}
            
            # 1. Basic claim features
            basic_features = self._extract_basic_claim_features(claim_id)
            features.update(basic_features)
            
            # 2. Temporal features
            temporal_features = self._extract_temporal_features(claim_id)
            features.update(temporal_features)
            
            # 3. Financial features
            financial_features = self._extract_financial_features(claim_id)
            features.update(financial_features)
            
            # 4. Network features
            network_features = self._extract_network_features(claim_id)
            features.update(network_features)
            
            # 5. Entity features
            entity_features = self._extract_entity_features(claim_id)
            features.update(entity_features)
            
            # 6. Historical features
            historical_features = self._extract_historical_features(claim_id)
            features.update(historical_features)
            
            # 7. Location features
            location_features = self._extract_location_features(claim_id)
            features.update(location_features)
            
            return features
            
        except Exception as e:
            logger.error(f"Error extracting features for claim {claim_id}: {e}", exc_info=True)
            return {}
    
    def extract_claimant_features(self, claimant_id: str) -> Dict:
        """
        Extract features for a claimant
        
        Args:
            claimant_id: Claimant identifier
            
        Returns:
            Dictionary of features
        """
        logger.debug(f"Extracting features for claimant {claimant_id}")
        
        try:
            query = """
            MATCH (c:Claimant {claimant_id: $claimant_id})
            OPTIONAL MATCH (c)-[:FILED]->(cl:Claim)
            
            OPTIONAL MATCH (c)-[:MEMBER_OF]->(r:FraudRing)
            
            // Get entity usage
            OPTIONAL MATCH (cl)-[:REPAIRED_AT]->(b:BodyShop)
            OPTIONAL MATCH (cl)-[:TREATED_BY]->(m:MedicalProvider)
            OPTIONAL MATCH (cl)-[:REPRESENTED_BY]->(a:Attorney)
            OPTIONAL MATCH (cl)-[:INVOLVES_VEHICLE]->(v:Vehicle)
            
            WITH c,
                 count(DISTINCT cl) as claim_count,
                 sum(cl.total_claim_amount) as total_claimed,
                 avg(cl.total_claim_amount) as avg_claim_amount,
                 avg(cl.risk_score) as avg_risk_score,
                 collect(DISTINCT r.ring_id) as ring_ids,
                 count(DISTINCT b) as unique_body_shops,
                 count(DISTINCT m) as unique_medical_providers,
                 count(DISTINCT a) as unique_attorneys,
                 count(DISTINCT v) as unique_vehicles,
                 max(cl.accident_date) as last_accident_date,
                 min(cl.accident_date) as first_accident_date
            
            RETURN 
                claim_count,
                total_claimed,
                avg_claim_amount,
                avg_risk_score,
                size([r IN ring_ids WHERE r IS NOT NULL]) as fraud_ring_count,
                unique_body_shops,
                unique_medical_providers,
                unique_attorneys,
                unique_vehicles,
                first_accident_date,
                last_accident_date
            """
            
            results = self.driver.execute_query(query, {'claimant_id': claimant_id})
            
            if not results:
                return {}
            
            data = results[0]
            
            # Calculate derived features
            features = {
                'claim_count': data.get('claim_count', 0),
                'total_claimed': data.get('total_claimed', 0),
                'avg_claim_amount': data.get('avg_claim_amount', 0),
                'avg_risk_score': data.get('avg_risk_score', 0),
                'fraud_ring_member': 1 if data.get('fraud_ring_count', 0) > 0 else 0,
                'fraud_ring_count': data.get('fraud_ring_count', 0),
                'unique_body_shops': data.get('unique_body_shops', 0),
                'unique_medical_providers': data.get('unique_medical_providers', 0),
                'unique_attorneys': data.get('unique_attorneys', 0),
                'unique_vehicles': data.get('unique_vehicles', 0)
            }
            
            # Calculate entity reuse ratios
            if features['claim_count'] > 0:
                features['body_shop_reuse_ratio'] = 1 - (features['unique_body_shops'] / features['claim_count'])
                features['medical_provider_reuse_ratio'] = 1 - (features['unique_medical_providers'] / features['claim_count'])
                features['attorney_reuse_ratio'] = 1 - (features['unique_attorneys'] / features['claim_count'])
            else:
                features['body_shop_reuse_ratio'] = 0
                features['medical_provider_reuse_ratio'] = 0
                features['attorney_reuse_ratio'] = 0
            
            # Calculate claim frequency
            first_date = data.get('first_accident_date')
            last_date = data.get('last_accident_date')
            
            if first_date and last_date and features['claim_count'] > 1:
                try:
                    if isinstance(first_date, str):
                        first_date = datetime.strptime(first_date, '%Y-%m-%d')
                    if isinstance(last_date, str):
                        last_date = datetime.strptime(last_date, '%Y-%m-%d')
                    
                    days_active = (last_date - first_date).days
                    features['days_active'] = days_active
                    features['claims_per_year'] = (features['claim_count'] / max(days_active, 1)) * 365
                except:
                    features['days_active'] = 0
                    features['claims_per_year'] = 0
            else:
                features['days_active'] = 0
                features['claims_per_year'] = 0
            
            return features
            
        except Exception as e:
            logger.error(f"Error extracting claimant features: {e}", exc_info=True)
            return {}
    
    def extract_bulk_features(self, limit: int = 1000) -> pd.DataFrame:
        """
        Extract features for multiple claims in bulk
        
        Args:
            limit: Maximum number of claims to process
            
        Returns:
            DataFrame with features
        """
        logger.info(f"Extracting features for up to {limit} claims")
        
        try:
            query = """
            MATCH (c:Claimant)-[:FILED]->(cl:Claim)
            
            // Get all related entities
            OPTIONAL MATCH (cl)-[:INVOLVES_VEHICLE]->(v:Vehicle)
            OPTIONAL MATCH (cl)-[:REPAIRED_AT]->(b:BodyShop)
            OPTIONAL MATCH (cl)-[:TREATED_BY]->(m:MedicalProvider)
            OPTIONAL MATCH (cl)-[:REPRESENTED_BY]->(a:Attorney)
            OPTIONAL MATCH (cl)-[:TOWED_BY]->(t:TowCompany)
            OPTIONAL MATCH (cl)-[:OCCURRED_AT]->(l:AccidentLocation)
            OPTIONAL MATCH (w:Witness)-[:WITNESSED]->(cl)
            OPTIONAL MATCH (c)-[:MEMBER_OF]->(r:FraudRing)
            
            // Get claimant history
            OPTIONAL MATCH (c)-[:FILED]->(other_cl:Claim)
            WHERE other_cl.claim_id <> cl.claim_id
            
            WITH cl, c, v, b, m, a, t, l, w, r,
                 count(DISTINCT other_cl) as claimant_other_claims,
                 sum(other_cl.total_claim_amount) as claimant_total_other_claims
            
            // Get vehicle history
            OPTIONAL MATCH (v)<-[:INVOLVES_VEHICLE]-(other_v_cl:Claim)
            WHERE other_v_cl.claim_id <> cl.claim_id
            
            WITH cl, c, v, b, m, a, t, l, w, r,
                 claimant_other_claims, claimant_total_other_claims,
                 count(DISTINCT other_v_cl) as vehicle_other_accidents
            
            RETURN 
                cl.claim_id as claim_id,
                cl.total_claim_amount as total_amount,
                cl.property_damage_amount as property_damage,
                cl.bodily_injury_amount as bodily_injury,
                cl.accident_date as accident_date,
                cl.report_date as report_date,
                cl.risk_score as risk_score,
                cl.accident_type as accident_type,
                cl.injury_type as injury_type,
                
                claimant_other_claims,
                claimant_total_other_claims,
                vehicle_other_accidents,
                
                CASE WHEN v IS NOT NULL THEN 1 ELSE 0 END as has_vehicle,
                CASE WHEN b IS NOT NULL THEN 1 ELSE 0 END as has_body_shop,
                CASE WHEN m IS NOT NULL THEN 1 ELSE 0 END as has_medical_provider,
                CASE WHEN a IS NOT NULL THEN 1 ELSE 0 END as has_attorney,
                CASE WHEN t IS NOT NULL THEN 1 ELSE 0 END as has_tow_company,
                CASE WHEN w IS NOT NULL THEN 1 ELSE 0 END as has_witness,
                CASE WHEN r IS NOT NULL THEN 1 ELSE 0 END as fraud_ring_member
            
            ORDER BY cl.accident_date DESC
            LIMIT $limit
            """
            
            results = self.driver.execute_query(query, {'limit': limit})
            
            if not results:
                logger.warning("No claims found for feature extraction")
                return pd.DataFrame()
            
            # Convert to DataFrame
            df = pd.DataFrame(results)
            
            # Calculate derived features
            df = self._calculate_derived_features(df)
            
            logger.info(f"Extracted features for {len(df)} claims")
            return df
            
        except Exception as e:
            logger.error(f"Error extracting bulk features: {e}", exc_info=True)
            return pd.DataFrame()
    
    # Helper methods for feature extraction
    
    def _extract_basic_claim_features(self, claim_id: str) -> Dict:
        """Extract basic claim information"""
        query = """
        MATCH (c:Claimant)-[:FILED]->(cl:Claim {claim_id: $claim_id})
        
        RETURN 
            cl.total_claim_amount as total_amount,
            cl.property_damage_amount as property_damage,
            cl.bodily_injury_amount as bodily_injury,
            cl.accident_type as accident_type,
            cl.injury_type as injury_type,
            cl.status as status,
            cl.risk_score as risk_score
        """
        
        results = self.driver.execute_query(query, {'claim_id': claim_id})
        
        if not results:
            return {}
        
        data = results[0]
        
        return {
            'total_amount': data.get('total_amount', 0),
            'property_damage': data.get('property_damage', 0),
            'bodily_injury': data.get('bodily_injury', 0),
            'accident_type': data.get('accident_type', ''),
            'injury_type': data.get('injury_type', ''),
            'status': data.get('status', ''),
            'risk_score': data.get('risk_score', 0)
        }
    
    def _extract_temporal_features(self, claim_id: str) -> Dict:
        """Extract time-related features"""
        query = """
        MATCH (cl:Claim {claim_id: $claim_id})
        
        RETURN 
            cl.accident_date as accident_date,
            cl.report_date as report_date
        """
        
        results = self.driver.execute_query(query, {'claim_id': claim_id})
        
        if not results:
            return {}
        
        data = results[0]
        features = {}
        
        try:
            accident_date = data.get('accident_date')
            report_date = data.get('report_date')
            
            if accident_date and report_date:
                if isinstance(accident_date, str):
                    accident_date = datetime.strptime(accident_date, '%Y-%m-%d')
                if isinstance(report_date, str):
                    report_date = datetime.strptime(report_date, '%Y-%m-%d')
                
                days_to_report = (report_date - accident_date).days
                features['days_to_report'] = days_to_report
                features['same_day_report'] = 1 if days_to_report == 0 else 0
                features['delayed_report'] = 1 if days_to_report > 30 else 0
                
                # Day of week (0=Monday, 6=Sunday)
                features['accident_day_of_week'] = accident_date.weekday()
                features['accident_is_weekend'] = 1 if accident_date.weekday() >= 5 else 0
                
                # Month
                features['accident_month'] = accident_date.month
        except Exception as e:
            logger.error(f"Error extracting temporal features: {e}")
        
        return features
    
    def _extract_financial_features(self, claim_id: str) -> Dict:
        """Extract financial-related features"""
        query = """
        MATCH (cl:Claim {claim_id: $claim_id})
        
        RETURN 
            cl.total_claim_amount as total_amount,
            cl.property_damage_amount as property_damage,
            cl.bodily_injury_amount as bodily_injury
        """
        
        results = self.driver.execute_query(query, {'claim_id': claim_id})
        
        if not results:
            return {}
        
        data = results[0]
        
        total = data.get('total_amount', 0)
        property_dmg = data.get('property_damage', 0)
        bodily_inj = data.get('bodily_injury', 0)
        
        features = {
            'injury_to_property_ratio': bodily_inj / max(property_dmg, 1),
            'has_bodily_injury': 1 if bodily_inj > 0 else 0,
            'high_value_claim': 1 if total > 50000 else 0,
            'very_high_value_claim': 1 if total > 100000 else 0
        }
        
        return features
    
    def _extract_network_features(self, claim_id: str) -> Dict:
        """Extract graph/network features"""
        query = """
        MATCH (c:Claimant)-[:FILED]->(cl:Claim {claim_id: $claim_id})
        
        // Claimant's connections
        OPTIONAL MATCH (c)-[:FILED]->(other_cl:Claim)
        WHERE other_cl.claim_id <> cl.claim_id
        
        // Fraud ring membership
        OPTIONAL MATCH (c)-[:MEMBER_OF]->(r:FraudRing)
        
        WITH c, cl, count(DISTINCT other_cl) as claimant_other_claims,
             collect(DISTINCT r.ring_id) as ring_ids
        
        // Shared connections with other claimants
        OPTIONAL MATCH (cl)-[:REPAIRED_AT]->(b:BodyShop)<-[:REPAIRED_AT]-(other_cl2:Claim)<-[:FILED]-(other_c:Claimant)
        WHERE other_c.claimant_id <> c.claimant_id
        
        WITH c, cl, claimant_other_claims, ring_ids,
             count(DISTINCT other_c) as shared_body_shop_claimants
        
        RETURN 
            claimant_other_claims,
            size([r IN ring_ids WHERE r IS NOT NULL]) as fraud_ring_count,
            shared_body_shop_claimants
        """
        
        results = self.driver.execute_query(query, {'claim_id': claim_id})
        
        if not results:
            return {}
        
        data = results[0]
        
        return {
            'claimant_other_claims': data.get('claimant_other_claims', 0),
            'fraud_ring_member': 1 if data.get('fraud_ring_count', 0) > 0 else 0,
            'shared_body_shop_claimants': data.get('shared_body_shop_claimants', 0)
        }
    
    def _extract_entity_features(self, claim_id: str) -> Dict:
        """Extract entity-related features"""
        query = """
        MATCH (cl:Claim {claim_id: $claim_id})
        
        OPTIONAL MATCH (cl)-[:REPAIRED_AT]->(b:BodyShop)
        OPTIONAL MATCH (cl)-[:TREATED_BY]->(m:MedicalProvider)
        OPTIONAL MATCH (cl)-[:REPRESENTED_BY]->(a:Attorney)
        OPTIONAL MATCH (cl)-[:TOWED_BY]->(t:TowCompany)
        OPTIONAL MATCH (w:Witness)-[:WITNESSED]->(cl)
        
        // Get entity statistics
        OPTIONAL MATCH (b)<-[:REPAIRED_AT]-(b_claims:Claim)
        OPTIONAL MATCH (m)<-[:TREATED_BY]-(m_claims:Claim)
        OPTIONAL MATCH (a)<-[:REPRESENTED_BY]-(a_claims:Claim)
        
        RETURN 
            CASE WHEN b IS NOT NULL THEN 1 ELSE 0 END as has_body_shop,
            CASE WHEN m IS NOT NULL THEN 1 ELSE 0 END as has_medical_provider,
            CASE WHEN a IS NOT NULL THEN 1 ELSE 0 END as has_attorney,
            CASE WHEN t IS NOT NULL THEN 1 ELSE 0 END as has_tow_company,
            CASE WHEN w IS NOT NULL THEN 1 ELSE 0 END as has_witness,
            count(DISTINCT b_claims) as body_shop_claim_count,
            count(DISTINCT m_claims) as medical_provider_claim_count,
            count(DISTINCT a_claims) as attorney_claim_count
        """
        
        results = self.driver.execute_query(query, {'claim_id': claim_id})
        
        if not results:
            return {}
        
        data = results[0]
        
        return {
            'has_body_shop': data.get('has_body_shop', 0),
            'has_medical_provider': data.get('has_medical_provider', 0),
            'has_attorney': data.get('has_attorney', 0),
            'has_tow_company': data.get('has_tow_company', 0),
            'has_witness': data.get('has_witness', 0),
            'body_shop_volume': data.get('body_shop_claim_count', 0),
            'medical_provider_volume': data.get('medical_provider_claim_count', 0),
            'attorney_volume': data.get('attorney_claim_count', 0)
        }
    
    def _extract_historical_features(self, claim_id: str) -> Dict:
        """Extract historical/claimant features"""
        query = """
        MATCH (c:Claimant)-[:FILED]->(cl:Claim {claim_id: $claim_id})
        MATCH (c)-[:FILED]->(all_claims:Claim)
        
        RETURN 
            count(DISTINCT all_claims) as total_claimant_claims,
            sum(all_claims.total_claim_amount) as total_claimant_amount,
            avg(all_claims.risk_score) as avg_claimant_risk
        """
        
        results = self.driver.execute_query(query, {'claim_id': claim_id})
        
        if not results:
            return {}
        
        data = results[0]
        
        return {
            'claimant_claim_count': data.get('total_claimant_claims', 0),
            'claimant_total_claimed': data.get('total_claimant_amount', 0),
            'claimant_avg_risk': data.get('avg_claimant_risk', 0),
            'frequent_claimant': 1 if data.get('total_claimant_claims', 0) >= 3 else 0
        }
    
    def _extract_location_features(self, claim_id: str) -> Dict:
        """Extract location-related features"""
        query = """
        MATCH (cl:Claim {claim_id: $claim_id})
        OPTIONAL MATCH (cl)-[:OCCURRED_AT]->(l:AccidentLocation)
        
        // Count accidents at this location
        OPTIONAL MATCH (l)<-[:OCCURRED_AT]-(location_claims:Claim)
        
        RETURN 
            CASE WHEN l IS NOT NULL THEN 1 ELSE 0 END as has_location,
            count(DISTINCT location_claims) as location_accident_count
        """
        
        results = self.driver.execute_query(query, {'claim_id': claim_id})
        
        if not results:
            return {}
        
        data = results[0]
        accident_count = data.get('location_accident_count', 0)
        
        return {
            'has_location': data.get('has_location', 0),
            'location_accident_count': accident_count,
            'accident_hotspot': 1 if accident_count >= 5 else 0
        }
    
    def _calculate_derived_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calculate derived features from DataFrame"""
        
        # Calculate days to report
        if 'accident_date' in df.columns and 'report_date' in df.columns:
            df['accident_date'] = pd.to_datetime(df['accident_date'])
            df['report_date'] = pd.to_datetime(df['report_date'])
            df['days_to_report'] = (df['report_date'] - df['accident_date']).dt.days
            df['same_day_report'] = (df['days_to_report'] == 0).astype(int)
            df['delayed_report'] = (df['days_to_report'] > 30).astype(int)
        
        # Calculate injury to property ratio
        if 'bodily_injury' in df.columns and 'property_damage' in df.columns:
            df['injury_to_property_ratio'] = df['bodily_injury'] / df['property_damage'].replace(0, 1)
            df['has_bodily_injury'] = (df['bodily_injury'] > 0).astype(int)
        
        # Calculate value categories
        if 'total_amount' in df.columns:
            df['high_value_claim'] = (df['total_amount'] > 50000).astype(int)
            df['very_high_value_claim'] = (df['total_amount'] > 100000).astype(int)
        
        # Frequent claimant indicator
        if 'claimant_other_claims' in df.columns:
            df['frequent_claimant'] = (df['claimant_other_claims'] >= 3).astype(int)
        
        # Multiple accident vehicle
        if 'vehicle_other_accidents' in df.columns:
            df['multiple_accident_vehicle'] = (df['vehicle_other_accidents'] >= 2).astype(int)
        
        return df
