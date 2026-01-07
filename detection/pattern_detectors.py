"""
Pattern Detectors - Detect specific auto insurance fraud patterns
Identifies staged accidents, body shop fraud, medical mills, and more
"""
import logging
from typing import Dict, List, Optional, Set
from datetime import datetime, timedelta
from collections import defaultdict

from data.neo4j_driver import get_neo4j_driver
from utils.logger import setup_logger

logger = setup_logger(__name__)


class PatternDetector:
    """
    Detects specific fraud patterns in auto insurance claims
    """
    
    def __init__(self):
        self.driver = get_neo4j_driver()
        
        # Pattern detection thresholds
        self.thresholds = {
            'staged_accident': {
                'min_claim_amount': 25000,
                'max_days_to_report': 0,  # Same-day reporting
                'min_participants': 2,
                'min_witness_appearances': 2
            },
            'body_shop_fraud': {
                'min_body_shop_claims': 15,
                'min_avg_risk': 60,
                'min_same_claimants': 3
            },
            'medical_mill': {
                'min_provider_claims': 20,
                'min_avg_injury_amount': 15000,
                'min_same_claimants': 4
            },
            'attorney_organized': {
                'min_attorney_claims': 25,
                'min_avg_risk': 65,
                'min_shared_entities': 3
            },
            'phantom_passenger': {
                'min_injury_without_evidence': 10000,
                'max_property_damage': 5000
            },
            'tow_truck_kickback': {
                'min_tow_company_claims': 15,
                'min_body_shop_overlap': 0.7
            }
        }
    
    def detect_all_patterns(self) -> Dict:
        """
        Detect all fraud patterns across the database
        
        Returns:
            Dictionary with all detected patterns
        """
        logger.info("Starting comprehensive pattern detection")
        
        results = {
            'staged_accidents': self.detect_staged_accidents(),
            'body_shop_fraud': self.detect_body_shop_fraud(),
            'medical_mills': self.detect_medical_mills(),
            'attorney_organized': self.detect_attorney_organized_fraud(),
            'phantom_passengers': self.detect_phantom_passengers(),
            'tow_truck_kickbacks': self.detect_tow_truck_kickbacks(),
            'accident_hotspots': self.detect_accident_hotspots(),
            'professional_witnesses': self.detect_professional_witnesses(),
            'vehicle_recycling': self.detect_vehicle_recycling()
        }
        
        # Summary
        total_patterns = sum(len(patterns) for patterns in results.values() if isinstance(patterns, list))
        logger.info(f"Pattern detection complete. Found {total_patterns} total patterns")
        
        return results
    
    def detect_staged_accidents(self) -> List[Dict]:
        """
        Detect staged accident patterns
        
        Indicators:
        - Multiple vehicles/claimants involved
        - Same-day reporting
        - Shared witnesses
        - High claim amounts
        - Known accident locations
        """
        logger.info("Detecting staged accident patterns")
        
        query = """
        // Find claims with suspicious characteristics
        MATCH (cl:Claim)
        WHERE cl.total_claim_amount >= $min_amount
          AND duration.between(date(cl.accident_date), date(cl.report_date)).days <= $max_days
        
        // Get claimants
        MATCH (c:Claimant)-[:FILED]->(cl)
        
        // Get witnesses
        OPTIONAL MATCH (w:Witness)-[:WITNESSED]->(cl)
        
        // Get location
        OPTIONAL MATCH (cl)-[:OCCURRED_AT]->(l:AccidentLocation)
        
        // Count witness appearances
        WITH cl, c, l,
             collect(DISTINCT w) as witnesses,
             [w IN collect(DISTINCT w) | size((w)-[:WITNESSED]->(:Claim))] as witness_counts
        
        WHERE size(witnesses) > 0 AND any(count IN witness_counts WHERE count >= $min_witness_appearances)
        
        // Get other claims at same location
        OPTIONAL MATCH (l)<-[:OCCURRED_AT]-(other_cl:Claim)
        WHERE other_cl.claim_id <> cl.claim_id
        
        WITH cl, c, l, witnesses,
             count(DISTINCT other_cl) as location_claim_count
        
        WHERE location_claim_count >= 1
        
        RETURN 
            cl.claim_id as claim_id,
            cl.claim_number as claim_number,
            c.name as claimant_name,
            cl.accident_date as accident_date,
            cl.report_date as report_date,
            cl.total_claim_amount as amount,
            l.intersection as location,
            [w IN witnesses | w.name] as witness_names,
            location_claim_count
        ORDER BY cl.total_claim_amount DESC
        LIMIT 50
        """
        
        try:
            results = self.driver.execute_query(query, {
                'min_amount': self.thresholds['staged_accident']['min_claim_amount'],
                'max_days': self.thresholds['staged_accident']['max_days_to_report'],
                'min_witness_appearances': self.thresholds['staged_accident']['min_witness_appearances']
            })
            
            patterns = []
            for result in results:
                pattern = {
                    'pattern_type': 'staged_accident',
                    'claim_id': result['claim_id'],
                    'claim_number': result['claim_number'],
                    'claimant_name': result['claimant_name'],
                    'accident_date': result['accident_date'],
                    'amount': result['amount'],
                    'location': result['location'],
                    'witness_count': len(result['witness_names']),
                    'location_claim_count': result['location_claim_count'],
                    'confidence': self._calculate_staged_accident_confidence(result),
                    'indicators': self._get_staged_accident_indicators(result)
                }
                patterns.append(pattern)
            
            logger.info(f"Found {len(patterns)} staged accident patterns")
            return patterns
            
        except Exception as e:
            logger.error(f"Error detecting staged accidents: {e}", exc_info=True)
            return []
    
    def detect_body_shop_fraud(self) -> List[Dict]:
        """
        Detect body shop fraud patterns
        
        Indicators:
        - High volume of claims
        - Same claimants repeatedly
        - High average risk scores
        - Inflated repair costs
        """
        logger.info("Detecting body shop fraud patterns")
        
        query = """
        MATCH (b:BodyShop)<-[:REPAIRED_AT]-(cl:Claim)
        MATCH (c:Claimant)-[:FILED]->(cl)
        
        WITH b,
             count(DISTINCT cl) as claim_count,
             count(DISTINCT c) as unique_claimants,
             avg(cl.risk_score) as avg_risk,
             sum(cl.property_damage_amount) as total_repairs,
             avg(cl.property_damage_amount) as avg_repair_cost,
             collect(DISTINCT c.claimant_id) as claimant_ids
        
        WHERE claim_count >= $min_claims
          AND avg_risk >= $min_avg_risk
        
        // Check for repeat claimants
        WITH b, claim_count, unique_claimants, avg_risk, total_repairs, avg_repair_cost, claimant_ids
        
        // Count how many claimants have multiple claims
        UNWIND claimant_ids as claimant_id
        MATCH (c:Claimant {claimant_id: claimant_id})-[:FILED]->(cl2:Claim)-[:REPAIRED_AT]->(b)
        
        WITH b, claim_count, unique_claimants, avg_risk, total_repairs, avg_repair_cost,
             claimant_id, count(cl2) as claimant_claim_count
        
        WHERE claimant_claim_count >= 2
        
        WITH b, claim_count, unique_claimants, avg_risk, total_repairs, avg_repair_cost,
             count(DISTINCT claimant_id) as repeat_claimants
        
        WHERE repeat_claimants >= $min_same_claimants
        
        RETURN 
            b.body_shop_id as body_shop_id,
            b.name as name,
            b.city as city,
            claim_count,
            unique_claimants,
            avg_risk,
            total_repairs,
            avg_repair_cost,
            repeat_claimants
        ORDER BY avg_risk DESC, repeat_claimants DESC
        LIMIT 30
        """
        
        try:
            results = self.driver.execute_query(query, {
                'min_claims': self.thresholds['body_shop_fraud']['min_body_shop_claims'],
                'min_avg_risk': self.thresholds['body_shop_fraud']['min_avg_risk'],
                'min_same_claimants': self.thresholds['body_shop_fraud']['min_same_claimants']
            })
            
            patterns = []
            for result in results:
                pattern = {
                    'pattern_type': 'body_shop_fraud',
                    'body_shop_id': result['body_shop_id'],
                    'name': result['name'],
                    'city': result['city'],
                    'claim_count': result['claim_count'],
                    'unique_claimants': result['unique_claimants'],
                    'avg_risk': round(result['avg_risk'], 2),
                    'total_repairs': result['total_repairs'],
                    'avg_repair_cost': round(result['avg_repair_cost'], 2),
                    'repeat_claimants': result['repeat_claimants'],
                    'confidence': self._calculate_body_shop_fraud_confidence(result),
                    'indicators': self._get_body_shop_fraud_indicators(result)
                }
                patterns.append(pattern)
            
            logger.info(f"Found {len(patterns)} body shop fraud patterns")
            return patterns
            
        except Exception as e:
            logger.error(f"Error detecting body shop fraud: {e}", exc_info=True)
            return []
    
    def detect_medical_mills(self) -> List[Dict]:
        """
        Detect medical mill patterns
        
        Indicators:
        - High volume of injury claims
        - Same claimants repeatedly
        - High average injury amounts
        - Soft tissue injuries predominant
        """
        logger.info("Detecting medical mill patterns")
        
        query = """
        MATCH (m:MedicalProvider)<-[:TREATED_BY]-(cl:Claim)
        WHERE cl.bodily_injury_amount > 0
        
        MATCH (c:Claimant)-[:FILED]->(cl)
        
        WITH m,
             count(DISTINCT cl) as claim_count,
             count(DISTINCT c) as unique_patients,
             avg(cl.bodily_injury_amount) as avg_injury_amount,
             avg(cl.risk_score) as avg_risk,
             sum(cl.bodily_injury_amount) as total_injury_claims,
             collect(DISTINCT c.claimant_id) as patient_ids,
             collect(cl.injury_type) as injury_types
        
        WHERE claim_count >= $min_claims
          AND avg_injury_amount >= $min_avg_injury
        
        // Check for repeat patients
        UNWIND patient_ids as patient_id
        MATCH (c:Claimant {claimant_id: patient_id})-[:FILED]->(cl2:Claim)-[:TREATED_BY]->(m)
        
        WITH m, claim_count, unique_patients, avg_injury_amount, avg_risk, 
             total_injury_claims, injury_types, patient_id, count(cl2) as patient_visit_count
        
        WHERE patient_visit_count >= 2
        
        WITH m, claim_count, unique_patients, avg_injury_amount, avg_risk,
             total_injury_claims, injury_types,
             count(DISTINCT patient_id) as repeat_patients
        
        WHERE repeat_patients >= $min_same_claimants
        
        // Calculate soft tissue injury percentage
        WITH m, claim_count, unique_patients, avg_injury_amount, avg_risk,
             total_injury_claims, repeat_patients,
             size([i IN injury_types WHERE i IN ['Whiplash', 'Back Pain', 'Neck Pain', 'Soft Tissue Injury']]) as soft_tissue_count,
             size(injury_types) as total_injury_count
        
        RETURN 
            m.provider_id as provider_id,
            m.name as name,
            m.provider_type as provider_type,
            m.city as city,
            claim_count,
            unique_patients,
            avg_injury_amount,
            avg_risk,
            total_injury_claims,
            repeat_patients,
            toFloat(soft_tissue_count) / total_injury_count as soft_tissue_ratio
        ORDER BY avg_risk DESC, repeat_patients DESC
        LIMIT 30
        """
        
        try:
            results = self.driver.execute_query(query, {
                'min_claims': self.thresholds['medical_mill']['min_provider_claims'],
                'min_avg_injury': self.thresholds['medical_mill']['min_avg_injury_amount'],
                'min_same_claimants': self.thresholds['medical_mill']['min_same_claimants']
            })
            
            patterns = []
            for result in results:
                pattern = {
                    'pattern_type': 'medical_mill',
                    'provider_id': result['provider_id'],
                    'name': result['name'],
                    'provider_type': result['provider_type'],
                    'city': result['city'],
                    'claim_count': result['claim_count'],
                    'unique_patients': result['unique_patients'],
                    'avg_injury_amount': round(result['avg_injury_amount'], 2),
                    'avg_risk': round(result['avg_risk'], 2),
                    'total_injury_claims': result['total_injury_claims'],
                    'repeat_patients': result['repeat_patients'],
                    'soft_tissue_ratio': round(result['soft_tissue_ratio'], 2),
                    'confidence': self._calculate_medical_mill_confidence(result),
                    'indicators': self._get_medical_mill_indicators(result)
                }
                patterns.append(pattern)
            
            logger.info(f"Found {len(patterns)} medical mill patterns")
            return patterns
            
        except Exception as e:
            logger.error(f"Error detecting medical mills: {e}", exc_info=True)
            return []
    
    def detect_attorney_organized_fraud(self) -> List[Dict]:
        """
        Detect attorney-organized fraud patterns
        
        Indicators:
        - High volume of cases
        - Same body shops/medical providers
        - High average risk scores
        - Coordinated referral patterns
        """
        logger.info("Detecting attorney-organized fraud patterns")
        
        query = """
        MATCH (a:Attorney)<-[:REPRESENTED_BY]-(cl:Claim)
        MATCH (c:Claimant)-[:FILED]->(cl)
        
        // Get related entities
        OPTIONAL MATCH (cl)-[:REPAIRED_AT]->(b:BodyShop)
        OPTIONAL MATCH (cl)-[:TREATED_BY]->(m:MedicalProvider)
        
        WITH a,
             count(DISTINCT cl) as case_count,
             count(DISTINCT c) as unique_clients,
             avg(cl.risk_score) as avg_risk,
             sum(cl.total_claim_amount) as total_represented,
             collect(DISTINCT b.body_shop_id) as body_shop_ids,
             collect(DISTINCT m.provider_id) as medical_provider_ids
        
        WHERE case_count >= $min_cases
          AND avg_risk >= $min_avg_risk
        
        // Check for shared entities
        WITH a, case_count, unique_clients, avg_risk, total_represented,
             size(body_shop_ids) as unique_body_shops,
             size(medical_provider_ids) as unique_medical_providers
        
        WHERE (case_count > 10 AND unique_body_shops <= 3) 
           OR (case_count > 10 AND unique_medical_providers <= 3)
        
        RETURN 
            a.attorney_id as attorney_id,
            a.name as name,
            a.firm as firm,
            a.city as city,
            case_count,
            unique_clients,
            avg_risk,
            total_represented,
            unique_body_shops,
            unique_medical_providers
        ORDER BY avg_risk DESC, case_count DESC
        LIMIT 30
        """
        
        try:
            results = self.driver.execute_query(query, {
                'min_cases': self.thresholds['attorney_organized']['min_attorney_claims'],
                'min_avg_risk': self.thresholds['attorney_organized']['min_avg_risk']
            })
            
            patterns = []
            for result in results:
                pattern = {
                    'pattern_type': 'attorney_organized',
                    'attorney_id': result['attorney_id'],
                    'name': result['name'],
                    'firm': result['firm'],
                    'city': result['city'],
                    'case_count': result['case_count'],
                    'unique_clients': result['unique_clients'],
                    'avg_risk': round(result['avg_risk'], 2),
                    'total_represented': result['total_represented'],
                    'unique_body_shops': result['unique_body_shops'],
                    'unique_medical_providers': result['unique_medical_providers'],
                    'confidence': self._calculate_attorney_fraud_confidence(result),
                    'indicators': self._get_attorney_fraud_indicators(result)
                }
                patterns.append(pattern)
            
            logger.info(f"Found {len(patterns)} attorney-organized fraud patterns")
            return patterns
            
        except Exception as e:
            logger.error(f"Error detecting attorney-organized fraud: {e}", exc_info=True)
            return []
    
    def detect_phantom_passengers(self) -> List[Dict]:
        """
        Detect phantom passenger patterns
        
        Indicators:
        - High bodily injury with minimal property damage
        - No injury type documented
        - Multiple claimants from single vehicle
        """
        logger.info("Detecting phantom passenger patterns")
        
        query = """
        MATCH (cl:Claim)
        WHERE cl.bodily_injury_amount >= $min_injury
          AND cl.property_damage_amount <= $max_property_damage
          AND cl.bodily_injury_amount > cl.property_damage_amount * 2
        
        MATCH (c:Claimant)-[:FILED]->(cl)
        MATCH (cl)-[:INVOLVES_VEHICLE]->(v:Vehicle)
        
        // Check for other claimants using same vehicle
        OPTIONAL MATCH (v)<-[:INVOLVES_VEHICLE]-(other_cl:Claim)<-[:FILED]-(other_c:Claimant)
        WHERE other_cl.claim_id <> cl.claim_id
        
        WITH cl, c, v,
             count(DISTINCT other_c) as other_claimants_same_vehicle
        
        RETURN 
            cl.claim_id as claim_id,
            cl.claim_number as claim_number,
            c.name as claimant_name,
            cl.accident_date as accident_date,
            cl.bodily_injury_amount as bodily_injury,
            cl.property_damage_amount as property_damage,
            cl.injury_type as injury_type,
            v.make + ' ' + v.model as vehicle,
            other_claimants_same_vehicle
        ORDER BY cl.bodily_injury_amount DESC
        LIMIT 50
        """
        
        try:
            results = self.driver.execute_query(query, {
                'min_injury': self.thresholds['phantom_passenger']['min_injury_without_evidence'],
                'max_property_damage': self.thresholds['phantom_passenger']['max_property_damage']
            })
            
            patterns = []
            for result in results:
                pattern = {
                    'pattern_type': 'phantom_passenger',
                    'claim_id': result['claim_id'],
                    'claim_number': result['claim_number'],
                    'claimant_name': result['claimant_name'],
                    'accident_date': result['accident_date'],
                    'bodily_injury': result['bodily_injury'],
                    'property_damage': result['property_damage'],
                    'injury_type': result['injury_type'],
                    'vehicle': result['vehicle'],
                    'other_claimants_same_vehicle': result['other_claimants_same_vehicle'],
                    'confidence': self._calculate_phantom_passenger_confidence(result),
                    'indicators': self._get_phantom_passenger_indicators(result)
                }
                patterns.append(pattern)
            
            logger.info(f"Found {len(patterns)} phantom passenger patterns")
            return patterns
            
        except Exception as e:
            logger.error(f"Error detecting phantom passengers: {e}", exc_info=True)
            return []
    
    def detect_tow_truck_kickbacks(self) -> List[Dict]:
        """
        Detect tow truck kickback schemes
        
        Indicators:
        - Tow company always refers to same body shop(s)
        - High volume of tows
        - High overlap with specific body shops
        """
        logger.info("Detecting tow truck kickback patterns")
        
        query = """
        MATCH (t:TowCompany)<-[:TOWED_BY]-(cl:Claim)-[:REPAIRED_AT]->(b:BodyShop)
        
        WITH t, b,
             count(cl) as shared_claims
        
        WITH t,
             collect({body_shop: b.name, body_shop_id: b.body_shop_id, shared_claims: shared_claims}) as body_shop_referrals,
             sum(shared_claims) as total_tows
        
        WHERE total_tows >= $min_tows
        
        // Calculate concentration (how often they refer to top body shop)
        WITH t, body_shop_referrals, total_tows,
             head([r IN body_shop_referrals | r.shared_claims]) as top_referral_count
        
        WITH t, body_shop_referrals, total_tows, top_referral_count,
             toFloat(top_referral_count) / total_tows as concentration_ratio
        
        WHERE concentration_ratio >= $min_overlap
        
        RETURN 
            t.tow_company_id as tow_company_id,
            t.name as name,
            t.city as city,
            total_tows,
            body_shop_referrals,
            concentration_ratio
        ORDER BY concentration_ratio DESC, total_tows DESC
        LIMIT 30
        """
        
        try:
            results = self.driver.execute_query(query, {
                'min_tows': self.thresholds['tow_truck_kickback']['min_tow_company_claims'],
                'min_overlap': self.thresholds['tow_truck_kickback']['min_body_shop_overlap']
            })
            
            patterns = []
            for result in results:
                pattern = {
                    'pattern_type': 'tow_truck_kickback',
                    'tow_company_id': result['tow_company_id'],
                    'name': result['name'],
                    'city': result['city'],
                    'total_tows': result['total_tows'],
                    'top_body_shop': result['body_shop_referrals'][0] if result['body_shop_referrals'] else None,
                    'concentration_ratio': round(result['concentration_ratio'], 2),
                    'confidence': self._calculate_tow_kickback_confidence(result),
                    'indicators': self._get_tow_kickback_indicators(result)
                }
                patterns.append(pattern)
            
            logger.info(f"Found {len(patterns)} tow truck kickback patterns")
            return patterns
            
        except Exception as e:
            logger.error(f"Error detecting tow truck kickbacks: {e}", exc_info=True)
            return []
    
    def detect_accident_hotspots(self) -> List[Dict]:
        """
        Detect accident location hotspots
        
        Indicators:
        - High number of accidents at same location
        - Same witnesses appearing
        - High average claim amounts
        """
        logger.info("Detecting accident hotspots")
        
        query = """
        MATCH (l:AccidentLocation)<-[:OCCURRED_AT]-(cl:Claim)
        MATCH (c:Claimant)-[:FILED]->(cl)
        
        OPTIONAL MATCH (w:Witness)-[:WITNESSED]->(cl)
        
        WITH l,
             count(DISTINCT cl) as accident_count,
             count(DISTINCT c) as unique_claimants,
             avg(cl.total_claim_amount) as avg_amount,
             avg(cl.risk_score) as avg_risk,
             collect(DISTINCT w.witness_id) as witness_ids
        
        WHERE accident_count >= 5
        
        RETURN 
            l.location_id as location_id,
            l.intersection as intersection,
            l.city as city,
            accident_count,
            unique_claimants,
            avg_amount,
            avg_risk,
            size(witness_ids) as witness_count
        ORDER BY accident_count DESC, avg_risk DESC
        LIMIT 30
        """
        
        try:
            results = self.driver.execute_query(query, {})
            
            patterns = []
            for result in results:
                pattern = {
                    'pattern_type': 'accident_hotspot',
                    'location_id': result['location_id'],
                    'intersection': result['intersection'],
                    'city': result['city'],
                    'accident_count': result['accident_count'],
                    'unique_claimants': result['unique_claimants'],
                    'avg_amount': round(result['avg_amount'], 2),
                    'avg_risk': round(result['avg_risk'], 2),
                    'witness_count': result['witness_count'],
                    'confidence': self._calculate_hotspot_confidence(result),
                    'indicators': self._get_hotspot_indicators(result)
                }
                patterns.append(pattern)
            
            logger.info(f"Found {len(patterns)} accident hotspots")
            return patterns
            
        except Exception as e:
            logger.error(f"Error detecting accident hotspots: {e}", exc_info=True)
            return []
    
    def detect_professional_witnesses(self) -> List[Dict]:
        """
        Detect professional witness patterns
        
        Indicators:
        - Witness appears in multiple accidents
        - High average risk scores
        - Connected to fraud rings
        """
        logger.info("Detecting professional witnesses")
        
        query = """
        MATCH (w:Witness)-[:WITNESSED]->(cl:Claim)
        MATCH (c:Claimant)-[:FILED]->(cl)
        
        OPTIONAL MATCH (c)-[:MEMBER_OF]->(r:FraudRing)
        
        WITH w,
             count(DISTINCT cl) as witnessed_count,
             count(DISTINCT c) as unique_claimants,
             avg(cl.risk_score) as avg_risk,
             collect(DISTINCT r.ring_id) as ring_ids
        
        WHERE witnessed_count >= 3
        
        RETURN 
            w.witness_id as witness_id,
            w.name as name,
            w.phone as phone,
            witnessed_count,
            unique_claimants,
            avg_risk,
            size([r IN ring_ids WHERE r IS NOT NULL]) as ring_connections
        ORDER BY witnessed_count DESC, avg_risk DESC
        LIMIT 30
        """
        
        try:
            results = self.driver.execute_query(query, {})
            
            patterns = []
            for result in results:
                pattern = {
                    'pattern_type': 'professional_witness',
                    'witness_id': result['witness_id'],
                    'name': result['name'],
                    'phone': result['phone'],
                    'witnessed_count': result['witnessed_count'],
                    'unique_claimants': result['unique_claimants'],
                    'avg_risk': round(result['avg_risk'], 2),
                    'ring_connections': result['ring_connections'],
                    'confidence': self._calculate_professional_witness_confidence(result),
                    'indicators': self._get_professional_witness_indicators(result)
                }
                patterns.append(pattern)
            
            logger.info(f"Found {len(patterns)} professional witnesses")
            return patterns
            
        except Exception as e:
            logger.error(f"Error detecting professional witnesses: {e}", exc_info=True)
            return []
    
    def detect_vehicle_recycling(self) -> List[Dict]:
        """
        Detect vehicle recycling fraud (same vehicle, multiple accidents, multiple claimants)
        
        Indicators:
        - Vehicle involved in multiple accidents
        - Different claimants for each accident
        - High total damage amounts
        """
        logger.info("Detecting vehicle recycling patterns")
        
        query = """
        MATCH (v:Vehicle)<-[:INVOLVES_VEHICLE]-(cl:Claim)
        MATCH (c:Claimant)-[:FILED]->(cl)
        
        WITH v,
             count(DISTINCT cl) as accident_count,
             count(DISTINCT c) as unique_claimants,
             sum(cl.total_claim_amount) as total_damage,
             avg(cl.risk_score) as avg_risk,
             collect(c.name) as claimant_names
        
        WHERE accident_count >= 3
          AND unique_claimants >= 2
        
        RETURN 
            v.vehicle_id as vehicle_id,
            v.make + ' ' + v.model + ' ' + v.year as vehicle_info,
            v.vin as vin,
            v.license_plate as license_plate,
            accident_count,
            unique_claimants,
            total_damage,
            avg_risk,
            claimant_names
        ORDER BY accident_count DESC, unique_claimants DESC
        LIMIT 30
        """
        
        try:
            results = self.driver.execute_query(query, {})
            
            patterns = []
            for result in results:
                pattern = {
                    'pattern_type': 'vehicle_recycling',
                    'vehicle_id': result['vehicle_id'],
                    'vehicle_info': result['vehicle_info'],
                    'vin': result['vin'],
                    'license_plate': result['license_plate'],
                    'accident_count': result['accident_count'],
                    'unique_claimants': result['unique_claimants'],
                    'total_damage': result['total_damage'],
                    'avg_risk': round(result['avg_risk'], 2),
                    'claimant_names': result['claimant_names'],
                    'confidence': self._calculate_vehicle_recycling_confidence(result),
                    'indicators': self._get_vehicle_recycling_indicators(result)
                }
                patterns.append(pattern)
            
            logger.info(f"Found {len(patterns)} vehicle recycling patterns")
            return patterns
            
        except Exception as e:
            logger.error(f"Error detecting vehicle recycling: {e}", exc_info=True)
            return []
    
    # Confidence calculation methods
    
    def _calculate_staged_accident_confidence(self, data: Dict) -> float:
        """Calculate confidence score for staged accident"""
        confidence = 0.5
        
        if data.get('amount', 0) > 50000:
            confidence += 0.2
        if data.get('witness_count', 0) >= 2:
            confidence += 0.15
        if data.get('location_claim_count', 0) >= 3:
            confidence += 0.15
        
        return min(confidence, 1.0)
    
    def _calculate_body_shop_fraud_confidence(self, data: Dict) -> float:
        """Calculate confidence score for body shop fraud"""
        confidence = 0.5
        
        if data.get('avg_risk', 0) >= 70:
            confidence += 0.25
        if data.get('repeat_claimants', 0) >= 5:
            confidence += 0.15
        if data.get('claim_count', 0) >= 30:
            confidence += 0.10
        
        return min(confidence, 1.0)
    
    def _calculate_medical_mill_confidence(self, data: Dict) -> float:
        """Calculate confidence score for medical mill"""
        confidence = 0.5
        
        if data.get('avg_risk', 0) >= 70:
            confidence += 0.2
        if data.get('repeat_patients', 0) >= 6:
            confidence += 0.15
        if data.get('soft_tissue_ratio', 0) >= 0.7:
            confidence += 0.15
        
        return min(confidence, 1.0)
    
    def _calculate_attorney_fraud_confidence(self, data: Dict) -> float:
        """Calculate confidence score for attorney fraud"""
        confidence = 0.5
        
        if data.get('avg_risk', 0) >= 70:
            confidence += 0.25
        if data.get('unique_body_shops', 0) <= 2 and data.get('case_count', 0) > 20:
            confidence += 0.15
        if data.get('unique_medical_providers', 0) <= 2 and data.get('case_count', 0) > 20:
            confidence += 0.10
        
        return min(confidence, 1.0)
    
    def _calculate_phantom_passenger_confidence(self, data: Dict) -> float:
        """Calculate confidence score for phantom passenger"""
        confidence = 0.5
        
        ratio = data.get('bodily_injury', 0) / max(data.get('property_damage', 1), 1)
        if ratio > 10:
            confidence += 0.3
        elif ratio > 5:
            confidence += 0.2
        
        if data.get('other_claimants_same_vehicle', 0) >= 2:
            confidence += 0.2
        
        return min(confidence, 1.0)
    
    def _calculate_tow_kickback_confidence(self, data: Dict) -> float:
        """Calculate confidence score for tow kickback"""
        confidence = 0.5
        
        if data.get('concentration_ratio', 0) >= 0.9:
            confidence += 0.3
        elif data.get('concentration_ratio', 0) >= 0.8:
            confidence += 0.2
        
        if data.get('total_tows', 0) >= 30:
            confidence += 0.2
        
        return min(confidence, 1.0)
    
    def _calculate_hotspot_confidence(self, data: Dict) -> float:
        """Calculate confidence score for hotspot"""
        confidence = 0.5
        
        if data.get('accident_count', 0) >= 10:
            confidence += 0.3
        elif data.get('accident_count', 0) >= 7:
            confidence += 0.2
        
        if data.get('avg_risk', 0) >= 60:
            confidence += 0.2
        
        return min(confidence, 1.0)
    
    def _calculate_professional_witness_confidence(self, data: Dict) -> float:
        """Calculate confidence score for professional witness"""
        confidence = 0.5
        
        if data.get('witnessed_count', 0) >= 5:
            confidence += 0.3
        elif data.get('witnessed_count', 0) >= 4:
            confidence += 0.2
        
        if data.get('ring_connections', 0) >= 1:
            confidence += 0.2
        
        return min(confidence, 1.0)
    
    def _calculate_vehicle_recycling_confidence(self, data: Dict) -> float:
        """Calculate confidence score for vehicle recycling"""
        confidence = 0.5
        
        if data.get('unique_claimants', 0) >= 3:
            confidence += 0.25
        if data.get('accident_count', 0) >= 4:
            confidence += 0.15
        if data.get('avg_risk', 0) >= 60:
            confidence += 0.10
        
        return min(confidence, 1.0)
    
    # Indicator generation methods
    
    def _get_staged_accident_indicators(self, data: Dict) -> List[str]:
        """Generate indicators for staged accident"""
        indicators = []
        
        if data.get('amount', 0) > 50000:
            indicators.append("High claim amount")
        indicators.append("Same-day reporting")
        if data.get('witness_count', 0) >= 2:
            indicators.append("Multiple witnesses (potential staged)")
        if data.get('location_claim_count', 0) >= 3:
            indicators.append("Accident hotspot location")
        
        return indicators
    
    def _get_body_shop_fraud_indicators(self, data: Dict) -> List[str]:
        """Generate indicators for body shop fraud"""
        indicators = []
        
        if data.get('claim_count', 0) >= 30:
            indicators.append("High volume of claims")
        if data.get('avg_risk', 0) >= 70:
            indicators.append("High average risk score")
        if data.get('repeat_claimants', 0) >= 5:
            indicators.append("Multiple repeat claimants")
        
        return indicators
    
    def _get_medical_mill_indicators(self, data: Dict) -> List[str]:
        """Generate indicators for medical mill"""
        indicators = []
        
        if data.get('claim_count', 0) >= 30:
            indicators.append("High volume of treatments")
        if data.get('avg_risk', 0) >= 70:
            indicators.append("High average risk score")
        if data.get('repeat_patients', 0) >= 6:
            indicators.append("Multiple repeat patients")
        if data.get('soft_tissue_ratio', 0) >= 0.7:
            indicators.append("High soft tissue injury ratio")
        
        return indicators
    
    def _get_attorney_fraud_indicators(self, data: Dict) -> List[str]:
        """Generate indicators for attorney fraud"""
        indicators = []
        
        if data.get('case_count', 0) >= 40:
            indicators.append("High case volume")
        if data.get('avg_risk', 0) >= 70:
            indicators.append("High average risk score")
        if data.get('unique_body_shops', 0) <= 2:
            indicators.append("Limited body shop referrals")
        if data.get('unique_medical_providers', 0) <= 2:
            indicators.append("Limited medical provider referrals")
        
        return indicators
    
    def _get_phantom_passenger_indicators(self, data: Dict) -> List[str]:
        """Generate indicators for phantom passenger"""
        indicators = []
        
        indicators.append("High injury claim with minimal property damage")
        if data.get('other_claimants_same_vehicle', 0) >= 2:
            indicators.append("Multiple claimants using same vehicle")
        if not data.get('injury_type') or data.get('injury_type') == 'No Injury':
            indicators.append("No documented injury type")
        
        return indicators
    
    def _get_tow_kickback_indicators(self, data: Dict) -> List[str]:
        """Generate indicators for tow kickback"""
        indicators = []
        
        if data.get('concentration_ratio', 0) >= 0.9:
            indicators.append("Very high referral concentration to single body shop")
        elif data.get('concentration_ratio', 0) >= 0.8:
            indicators.append("High referral concentration")
        
        if data.get('total_tows', 0) >= 30:
            indicators.append("High volume of tows")
        
        return indicators
    
    def _get_hotspot_indicators(self, data: Dict) -> List[str]:
        """Generate indicators for hotspot"""
        indicators = []
        
        if data.get('accident_count', 0) >= 10:
            indicators.append("Major accident hotspot")
        elif data.get('accident_count', 0) >= 7:
            indicators.append("Accident hotspot")
        
        if data.get('avg_risk', 0) >= 60:
            indicators.append("High average risk score")
        if data.get('witness_count', 0) >= 3:
            indicators.append("Multiple witnesses across accidents")
        
        return indicators
    
    def _get_professional_witness_indicators(self, data: Dict) -> List[str]:
        """Generate indicators for professional witness"""
        indicators = []
        
        if data.get('witnessed_count', 0) >= 5:
            indicators.append("Witnessed 5+ accidents (professional witness)")
        elif data.get('witnessed_count', 0) >= 3:
            indicators.append("Multiple accident witnesses")
        
        if data.get('ring_connections', 0) >= 1:
            indicators.append("Connected to fraud ring(s)")
        if data.get('avg_risk', 0) >= 60:
            indicators.append("High average claim risk")
        
        return indicators
    
    def _get_vehicle_recycling_indicators(self, data: Dict) -> List[str]:
        """Generate indicators for vehicle recycling"""
        indicators = []
        
        if data.get('accident_count', 0) >= 4:
            indicators.append("Multiple accidents (4+)")
        if data.get('unique_claimants', 0) >= 3:
            indicators.append("3+ different claimants")
        if data.get('total_damage', 0) > 100000:
            indicators.append("High total damage amount")
        
        return indicators
