"""
Alert Engine - Generate and manage fraud alerts
Monitors patterns and triggers alerts for suspicious activity
"""
import logging
from typing import Dict, List, Optional
from datetime import datetime, timedelta
from enum import Enum

from data.neo4j_driver import get_neo4j_driver
from utils.logger import setup_logger

logger = setup_logger(__name__)


class AlertSeverity(Enum):
    """Alert severity levels"""
    CRITICAL = "CRITICAL"
    HIGH = "HIGH"
    MEDIUM = "MEDIUM"
    LOW = "LOW"
    INFO = "INFO"


class AlertType(Enum):
    """Alert type enumeration"""
    STAGED_ACCIDENT = "Staged Accident"
    BODY_SHOP_FRAUD = "Body Shop Fraud"
    MEDICAL_MILL = "Medical Mill"
    ATTORNEY_ORGANIZED = "Attorney Organized Fraud"
    PHANTOM_PASSENGER = "Phantom Passenger"
    TOW_KICKBACK = "Tow Truck Kickback"
    ACCIDENT_HOTSPOT = "Accident Hotspot"
    PROFESSIONAL_WITNESS = "Professional Witness"
    VEHICLE_RECYCLING = "Vehicle Recycling"
    FRAUD_RING_DETECTED = "Fraud Ring Detected"
    HIGH_RISK_CLAIM = "High Risk Claim"
    REPEAT_CLAIMANT = "Repeat Claimant"
    SUSPICIOUS_PATTERN = "Suspicious Pattern"


class AlertEngine:
    """
    Alert engine for fraud detection system
    Generates, manages, and tracks fraud alerts
    """
    
    def __init__(self):
        self.driver = get_neo4j_driver()
        
        # Alert thresholds
        self.thresholds = {
            'critical_risk_score': 85,
            'high_risk_score': 70,
            'medium_risk_score': 50,
            'high_claim_amount': 75000,
            'very_high_claim_amount': 150000,
            'repeat_claimant_threshold': 3,
            'professional_witness_threshold': 3,
            'accident_hotspot_threshold': 5
        }
    
    # ==================== ALERT CREATION ====================
    
    def create_alert(
        self,
        alert_type: str,
        severity: str,
        title: str,
        description: str,
        entity_id: str,
        entity_type: str,
        related_ids: Optional[Dict] = None,
        metadata: Optional[Dict] = None
    ) -> Optional[str]:
        """
        Create a new fraud alert
        
        Args:
            alert_type: Type of alert (from AlertType enum)
            severity: Severity level (from AlertSeverity enum)
            title: Alert title
            description: Detailed description
            entity_id: Primary entity ID (claim, claimant, etc.)
            entity_type: Type of entity (Claim, Claimant, BodyShop, etc.)
            related_ids: Dictionary of related entity IDs
            metadata: Additional metadata
            
        Returns:
            Alert ID if successful, None otherwise
        """
        try:
            import uuid
            alert_id = f"ALERT_{uuid.uuid4().hex[:12].upper()}"
            
            query = """
            CREATE (a:Alert {
                alert_id: $alert_id,
                alert_type: $alert_type,
                severity: $severity,
                title: $title,
                description: $description,
                entity_id: $entity_id,
                entity_type: $entity_type,
                status: 'OPEN',
                created_at: datetime($created_at),
                updated_at: datetime($updated_at),
                resolved: false,
                assigned_to: null,
                resolution_notes: null
            })
            RETURN a.alert_id as alert_id
            """
            
            now = datetime.now().isoformat()
            
            result = self.driver.execute_write(query, {
                'alert_id': alert_id,
                'alert_type': alert_type,
                'severity': severity,
                'title': title,
                'description': description,
                'entity_id': entity_id,
                'entity_type': entity_type,
                'created_at': now,
                'updated_at': now
            })
            
            if result:
                # Link alert to entity
                self._link_alert_to_entity(alert_id, entity_id, entity_type)
                
                # Link related entities if provided
                if related_ids:
                    self._link_related_entities(alert_id, related_ids)
                
                logger.info(f"Created alert: {alert_id} - {title}")
                return alert_id
            
            return None
            
        except Exception as e:
            logger.error(f"Error creating alert: {e}", exc_info=True)
            return None
    
    def _link_alert_to_entity(self, alert_id: str, entity_id: str, entity_type: str) -> bool:
        """Link alert to primary entity"""
        try:
            query = f"""
            MATCH (a:Alert {{alert_id: $alert_id}})
            MATCH (e:{entity_type} {{{{entity_type.lower()}}_id: $entity_id}})
            MERGE (a)-[:ALERTS]->(e)
            RETURN a.alert_id as alert_id
            """
            
            # Adjust query based on entity type
            if entity_type == "Claim":
                id_field = "claim_id"
            elif entity_type == "Claimant":
                id_field = "claimant_id"
            elif entity_type == "BodyShop":
                id_field = "body_shop_id"
            elif entity_type == "MedicalProvider":
                id_field = "provider_id"
            elif entity_type == "Vehicle":
                id_field = "vehicle_id"
            elif entity_type == "Attorney":
                id_field = "attorney_id"
            elif entity_type == "TowCompany":
                id_field = "tow_company_id"
            elif entity_type == "AccidentLocation":
                id_field = "location_id"
            elif entity_type == "Witness":
                id_field = "witness_id"
            elif entity_type == "FraudRing":
                id_field = "ring_id"
            else:
                id_field = f"{entity_type.lower()}_id"
            
            query = f"""
            MATCH (a:Alert {{alert_id: $alert_id}})
            MATCH (e:{entity_type} {{{id_field}: $entity_id}})
            MERGE (a)-[:ALERTS]->(e)
            RETURN a.alert_id as alert_id
            """
            
            result = self.driver.execute_write(query, {
                'alert_id': alert_id,
                'entity_id': entity_id
            })
            
            return result is not None
            
        except Exception as e:
            logger.error(f"Error linking alert to entity: {e}", exc_info=True)
            return False
    
    def _link_related_entities(self, alert_id: str, related_ids: Dict) -> None:
        """Link alert to related entities"""
        for entity_type, entity_id in related_ids.items():
            try:
                self._link_alert_to_entity(alert_id, entity_id, entity_type)
            except Exception as e:
                logger.error(f"Error linking related entity {entity_type}: {e}")
    
    # ==================== ALERT MONITORING ====================
    
    def monitor_high_risk_claims(self) -> List[str]:
        """Monitor and create alerts for high risk claims"""
        logger.info("Monitoring high risk claims")
        
        try:
            query = """
            MATCH (cl:Claim)
            WHERE cl.risk_score >= $high_risk_score
              AND NOT EXISTS((cl)<-[:ALERTS]-(:Alert {alert_type: 'High Risk Claim'}))
            
            MATCH (c:Claimant)-[:FILED]->(cl)
            
            RETURN 
                cl.claim_id as claim_id,
                cl.claim_number as claim_number,
                c.name as claimant_name,
                cl.risk_score as risk_score,
                cl.total_claim_amount as amount,
                cl.accident_type as accident_type
            ORDER BY cl.risk_score DESC
            LIMIT 50
            """
            
            results = self.driver.execute_query(query, {
                'high_risk_score': self.thresholds['high_risk_score']
            })
            
            alert_ids = []
            
            for result in results:
                severity = self._determine_severity_by_risk(result['risk_score'])
                
                alert_id = self.create_alert(
                    alert_type=AlertType.HIGH_RISK_CLAIM.value,
                    severity=severity,
                    title=f"High Risk Claim Detected: {result['claim_number']}",
                    description=f"Claim filed by {result['claimant_name']} has risk score of {result['risk_score']}. "
                               f"Accident type: {result['accident_type']}, Amount: ${result['amount']:,.2f}",
                    entity_id=result['claim_id'],
                    entity_type="Claim"
                )
                
                if alert_id:
                    alert_ids.append(alert_id)
            
            logger.info(f"Created {len(alert_ids)} high risk claim alerts")
            return alert_ids
            
        except Exception as e:
            logger.error(f"Error monitoring high risk claims: {e}", exc_info=True)
            return []
    
    def monitor_repeat_claimants(self) -> List[str]:
        """Monitor and create alerts for repeat claimants"""
        logger.info("Monitoring repeat claimants")
        
        try:
            query = """
            MATCH (c:Claimant)-[:FILED]->(cl:Claim)
            
            WITH c, count(cl) as claim_count
            WHERE claim_count >= $threshold
              AND NOT EXISTS((c)<-[:ALERTS]-(:Alert {alert_type: 'Repeat Claimant'}))
            
            MATCH (c)-[:FILED]->(claims:Claim)
            
            WITH c, claim_count,
                 sum(claims.total_claim_amount) as total_claimed,
                 avg(claims.risk_score) as avg_risk
            
            RETURN 
                c.claimant_id as claimant_id,
                c.name as name,
                claim_count,
                total_claimed,
                avg_risk
            ORDER BY claim_count DESC, avg_risk DESC
            LIMIT 50
            """
            
            results = self.driver.execute_query(query, {
                'threshold': self.thresholds['repeat_claimant_threshold']
            })
            
            alert_ids = []
            
            for result in results:
                severity = self._determine_severity_by_risk(result['avg_risk'])
                
                alert_id = self.create_alert(
                    alert_type=AlertType.REPEAT_CLAIMANT.value,
                    severity=severity,
                    title=f"Repeat Claimant Detected: {result['name']}",
                    description=f"{result['name']} has filed {result['claim_count']} claims "
                               f"totaling ${result['total_claimed']:,.2f}. "
                               f"Average risk score: {result['avg_risk']:.1f}",
                    entity_id=result['claimant_id'],
                    entity_type="Claimant"
                )
                
                if alert_id:
                    alert_ids.append(alert_id)
            
            logger.info(f"Created {len(alert_ids)} repeat claimant alerts")
            return alert_ids
            
        except Exception as e:
            logger.error(f"Error monitoring repeat claimants: {e}", exc_info=True)
            return []
    
    def monitor_professional_witnesses(self) -> List[str]:
        """Monitor and create alerts for professional witnesses"""
        logger.info("Monitoring professional witnesses")
        
        try:
            query = """
            MATCH (w:Witness)-[:WITNESSED]->(cl:Claim)
            
            WITH w, count(cl) as witnessed_count
            WHERE witnessed_count >= $threshold
              AND NOT EXISTS((w)<-[:ALERTS]-(:Alert {alert_type: 'Professional Witness'}))
            
            MATCH (w)-[:WITNESSED]->(claims:Claim)
            MATCH (c:Claimant)-[:FILED]->(claims)
            
            WITH w, witnessed_count,
                 count(DISTINCT c) as unique_claimants,
                 avg(claims.risk_score) as avg_risk
            
            RETURN 
                w.witness_id as witness_id,
                w.name as name,
                witnessed_count,
                unique_claimants,
                avg_risk
            ORDER BY witnessed_count DESC
            LIMIT 30
            """
            
            results = self.driver.execute_query(query, {
                'threshold': self.thresholds['professional_witness_threshold']
            })
            
            alert_ids = []
            
            for result in results:
                severity = AlertSeverity.HIGH.value if result['witnessed_count'] >= 5 else AlertSeverity.MEDIUM.value
                
                alert_id = self.create_alert(
                    alert_type=AlertType.PROFESSIONAL_WITNESS.value,
                    severity=severity,
                    title=f"Professional Witness Detected: {result['name']}",
                    description=f"{result['name']} has witnessed {result['witnessed_count']} accidents "
                               f"involving {result['unique_claimants']} different claimants. "
                               f"Average claim risk: {result['avg_risk']:.1f}",
                    entity_id=result['witness_id'],
                    entity_type="Witness"
                )
                
                if alert_id:
                    alert_ids.append(alert_id)
            
            logger.info(f"Created {len(alert_ids)} professional witness alerts")
            return alert_ids
            
        except Exception as e:
            logger.error(f"Error monitoring professional witnesses: {e}", exc_info=True)
            return []
    
    def monitor_accident_hotspots(self) -> List[str]:
        """Monitor and create alerts for accident hotspots"""
        logger.info("Monitoring accident hotspots")
        
        try:
            query = """
            MATCH (l:AccidentLocation)<-[:OCCURRED_AT]-(cl:Claim)
            
            WITH l, count(cl) as accident_count
            WHERE accident_count >= $threshold
              AND NOT EXISTS((l)<-[:ALERTS]-(:Alert {alert_type: 'Accident Hotspot'}))
            
            MATCH (l)<-[:OCCURRED_AT]-(claims:Claim)
            
            WITH l, accident_count,
                 avg(claims.risk_score) as avg_risk,
                 sum(claims.total_claim_amount) as total_amount
            
            RETURN 
                l.location_id as location_id,
                l.intersection as intersection,
                l.city as city,
                accident_count,
                avg_risk,
                total_amount
            ORDER BY accident_count DESC, avg_risk DESC
            LIMIT 30
            """
            
            results = self.driver.execute_query(query, {
                'threshold': self.thresholds['accident_hotspot_threshold']
            })
            
            alert_ids = []
            
            for result in results:
                severity = AlertSeverity.HIGH.value if result['accident_count'] >= 10 else AlertSeverity.MEDIUM.value
                
                alert_id = self.create_alert(
                    alert_type=AlertType.ACCIDENT_HOTSPOT.value,
                    severity=severity,
                    title=f"Accident Hotspot: {result['intersection']}",
                    description=f"{result['accident_count']} accidents at {result['intersection']}, {result['city']}. "
                               f"Total claims: ${result['total_amount']:,.2f}. "
                               f"Average risk: {result['avg_risk']:.1f}",
                    entity_id=result['location_id'],
                    entity_type="AccidentLocation"
                )
                
                if alert_id:
                    alert_ids.append(alert_id)
            
            logger.info(f"Created {len(alert_ids)} accident hotspot alerts")
            return alert_ids
            
        except Exception as e:
            logger.error(f"Error monitoring accident hotspots: {e}", exc_info=True)
            return []
    
    def monitor_fraud_patterns(self, patterns: Dict) -> List[str]:
        """
        Create alerts for detected fraud patterns
        
        Args:
            patterns: Dictionary of detected patterns from PatternDetector
            
        Returns:
            List of created alert IDs
        """
        logger.info("Creating alerts for detected fraud patterns")
        
        alert_ids = []
        
        try:
            # Staged accidents
            for pattern in patterns.get('staged_accidents', []):
                if pattern.get('confidence', 0) >= 0.7:
                    alert_id = self.create_alert(
                        alert_type=AlertType.STAGED_ACCIDENT.value,
                        severity=AlertSeverity.CRITICAL.value,
                        title=f"Staged Accident: {pattern.get('claim_number')}",
                        description=f"Potential staged accident detected. Confidence: {pattern.get('confidence'):.2%}. "
                                   f"Indicators: {', '.join(pattern.get('indicators', []))}",
                        entity_id=pattern['claim_id'],
                        entity_type="Claim"
                    )
                    if alert_id:
                        alert_ids.append(alert_id)
            
            # Body shop fraud
            for pattern in patterns.get('body_shop_fraud', []):
                if pattern.get('confidence', 0) >= 0.7:
                    alert_id = self.create_alert(
                        alert_type=AlertType.BODY_SHOP_FRAUD.value,
                        severity=AlertSeverity.HIGH.value,
                        title=f"Body Shop Fraud: {pattern.get('name')}",
                        description=f"Suspicious body shop activity. {pattern.get('claim_count')} claims, "
                                   f"{pattern.get('repeat_claimants')} repeat customers. "
                                   f"Avg risk: {pattern.get('avg_risk'):.1f}",
                        entity_id=pattern['body_shop_id'],
                        entity_type="BodyShop"
                    )
                    if alert_id:
                        alert_ids.append(alert_id)
            
            # Medical mills
            for pattern in patterns.get('medical_mills', []):
                if pattern.get('confidence', 0) >= 0.7:
                    alert_id = self.create_alert(
                        alert_type=AlertType.MEDICAL_MILL.value,
                        severity=AlertSeverity.HIGH.value,
                        title=f"Medical Mill: {pattern.get('name')}",
                        description=f"Suspicious medical provider. {pattern.get('claim_count')} treatments, "
                                   f"{pattern.get('repeat_patients')} repeat patients. "
                                   f"Soft tissue ratio: {pattern.get('soft_tissue_ratio'):.1%}",
                        entity_id=pattern['provider_id'],
                        entity_type="MedicalProvider"
                    )
                    if alert_id:
                        alert_ids.append(alert_id)
            
            # Tow truck kickbacks
            for pattern in patterns.get('tow_truck_kickbacks', []):
                if pattern.get('confidence', 0) >= 0.7:
                    alert_id = self.create_alert(
                        alert_type=AlertType.TOW_KICKBACK.value,
                        severity=AlertSeverity.HIGH.value,
                        title=f"Tow Kickback Scheme: {pattern.get('name')}",
                        description=f"Suspicious tow company referral pattern. "
                                   f"Concentration ratio: {pattern.get('concentration_ratio'):.1%}. "
                                   f"Total tows: {pattern.get('total_tows')}",
                        entity_id=pattern['tow_company_id'],
                        entity_type="TowCompany"
                    )
                    if alert_id:
                        alert_ids.append(alert_id)
            
            logger.info(f"Created {len(alert_ids)} fraud pattern alerts")
            return alert_ids
            
        except Exception as e:
            logger.error(f"Error creating fraud pattern alerts: {e}", exc_info=True)
            return alert_ids
    
    def monitor_fraud_rings(self, rings: List[Dict]) -> List[str]:
        """
        Create alerts for detected fraud rings
        
        Args:
            rings: List of detected fraud rings
            
        Returns:
            List of created alert IDs
        """
        logger.info("Creating alerts for detected fraud rings")
        
        alert_ids = []
        
        try:
            for ring in rings:
                if ring.get('confidence_score', 0) >= 0.7:
                    alert_id = self.create_alert(
                        alert_type=AlertType.FRAUD_RING_DETECTED.value,
                        severity=AlertSeverity.CRITICAL.value,
                        title=f"Fraud Ring Detected: {ring.get('pattern_type')}",
                        description=f"Fraud ring with {ring.get('member_count')} members detected. "
                                   f"Pattern: {ring.get('pattern_type')}. "
                                   f"Estimated fraud: ${ring.get('estimated_fraud_amount', 0):,.2f}. "
                                   f"Confidence: {ring.get('confidence_score'):.2%}",
                        entity_id=ring['ring_id'],
                        entity_type="FraudRing"
                    )
                    if alert_id:
                        alert_ids.append(alert_id)
            
            logger.info(f"Created {len(alert_ids)} fraud ring alerts")
            return alert_ids
            
        except Exception as e:
            logger.error(f"Error creating fraud ring alerts: {e}", exc_info=True)
            return alert_ids
    
    def run_all_monitors(self) -> Dict[str, List[str]]:
        """
        Run all monitoring functions
        
        Returns:
            Dictionary with alert IDs by category
        """
        logger.info("Running all alert monitors")
        
        results = {
            'high_risk_claims': self.monitor_high_risk_claims(),
            'repeat_claimants': self.monitor_repeat_claimants(),
            'professional_witnesses': self.monitor_professional_witnesses(),
            'accident_hotspots': self.monitor_accident_hotspots()
        }
        
        total_alerts = sum(len(alerts) for alerts in results.values())
        logger.info(f"Monitoring complete. Generated {total_alerts} total alerts")
        
        return results
    
    # ==================== ALERT RETRIEVAL ====================
    
    def get_open_alerts(self, limit: int = 100) -> List[Dict]:
        """Get all open alerts"""
        try:
            query = """
            MATCH (a:Alert)
            WHERE a.status = 'OPEN'
            
            RETURN 
                a.alert_id as alert_id,
                a.alert_type as alert_type,
                a.severity as severity,
                a.title as title,
                a.description as description,
                a.entity_id as entity_id,
                a.entity_type as entity_type,
                a.created_at as created_at,
                a.assigned_to as assigned_to
            ORDER BY 
                CASE a.severity
                    WHEN 'CRITICAL' THEN 1
                    WHEN 'HIGH' THEN 2
                    WHEN 'MEDIUM' THEN 3
                    WHEN 'LOW' THEN 4
                    ELSE 5
                END,
                a.created_at DESC
            LIMIT $limit
            """
            
            results = self.driver.execute_query(query, {'limit': limit})
            return results
            
        except Exception as e:
            logger.error(f"Error getting open alerts: {e}", exc_info=True)
            return []
    
    def get_alerts_by_severity(self, severity: str, limit: int = 100) -> List[Dict]:
        """Get alerts by severity level"""
        try:
            query = """
            MATCH (a:Alert)
            WHERE a.severity = $severity AND a.status = 'OPEN'
            
            RETURN 
                a.alert_id as alert_id,
                a.alert_type as alert_type,
                a.severity as severity,
                a.title as title,
                a.description as description,
                a.entity_id as entity_id,
                a.entity_type as entity_type,
                a.created_at as created_at
            ORDER BY a.created_at DESC
            LIMIT $limit
            """
            
            results = self.driver.execute_query(query, {
                'severity': severity,
                'limit': limit
            })
            return results
            
        except Exception as e:
            logger.error(f"Error getting alerts by severity: {e}", exc_info=True)
            return []
    
    def get_alert_by_id(self, alert_id: str) -> Optional[Dict]:
        """Get alert by ID"""
        try:
            query = """
            MATCH (a:Alert {alert_id: $alert_id})
            
            RETURN 
                a.alert_id as alert_id,
                a.alert_type as alert_type,
                a.severity as severity,
                a.title as title,
                a.description as description,
                a.entity_id as entity_id,
                a.entity_type as entity_type,
                a.status as status,
                a.created_at as created_at,
                a.updated_at as updated_at,
                a.assigned_to as assigned_to,
                a.resolution_notes as resolution_notes
            """
            
            results = self.driver.execute_query(query, {'alert_id': alert_id})
            
            if results:
                return results[0]
            
            return None
            
        except Exception as e:
            logger.error(f"Error getting alert by ID: {e}", exc_info=True)
            return None
    
    # ==================== ALERT MANAGEMENT ====================
    
    def assign_alert(self, alert_id: str, assignee: str) -> bool:
        """Assign alert to investigator"""
        try:
            query = """
            MATCH (a:Alert {alert_id: $alert_id})
            SET a.assigned_to = $assignee,
                a.status = 'ASSIGNED',
                a.updated_at = datetime($updated_at)
            RETURN a.alert_id as alert_id
            """
            
            result = self.driver.execute_write(query, {
                'alert_id': alert_id,
                'assignee': assignee,
                'updated_at': datetime.now().isoformat()
            })
            
            if result:
                logger.info(f"Assigned alert {alert_id} to {assignee}")
                return True
            
            return False
            
        except Exception as e:
            logger.error(f"Error assigning alert: {e}", exc_info=True)
            return False
    
    def update_alert_status(self, alert_id: str, status: str) -> bool:
        """Update alert status"""
        try:
            query = """
            MATCH (a:Alert {alert_id: $alert_id})
            SET a.status = $status,
                a.updated_at = datetime($updated_at)
            RETURN a.alert_id as alert_id
            """
            
            result = self.driver.execute_write(query, {
                'alert_id': alert_id,
                'status': status,
                'updated_at': datetime.now().isoformat()
            })
            
            if result:
                logger.info(f"Updated alert {alert_id} status to {status}")
                return True
            
            return False
            
        except Exception as e:
            logger.error(f"Error updating alert status: {e}", exc_info=True)
            return False
    
    def resolve_alert(self, alert_id: str, resolution_notes: str) -> bool:
        """Resolve an alert"""
        try:
            query = """
            MATCH (a:Alert {alert_id: $alert_id})
            SET a.status = 'RESOLVED',
                a.resolved = true,
                a.resolution_notes = $resolution_notes,
                a.updated_at = datetime($updated_at)
            RETURN a.alert_id as alert_id
            """
            
            result = self.driver.execute_write(query, {
                'alert_id': alert_id,
                'resolution_notes': resolution_notes,
                'updated_at': datetime.now().isoformat()
            })
            
            if result:
                logger.info(f"Resolved alert {alert_id}")
                return True
            
            return False
            
        except Exception as e:
            logger.error(f"Error resolving alert: {e}", exc_info=True)
            return False
    
    def dismiss_alert(self, alert_id: str, reason: str) -> bool:
        """Dismiss a false positive alert"""
        try:
            query = """
            MATCH (a:Alert {alert_id: $alert_id})
            SET a.status = 'DISMISSED',
                a.resolved = true,
                a.resolution_notes = $reason,
                a.updated_at = datetime($updated_at)
            RETURN a.alert_id as alert_id
            """
            
            result = self.driver.execute_write(query, {
                'alert_id': alert_id,
                'reason': reason,
                'updated_at': datetime.now().isoformat()
            })
            
            if result:
                logger.info(f"Dismissed alert {alert_id}")
                return True
            
            return False
            
        except Exception as e:
            logger.error(f"Error dismissing alert: {e}", exc_info=True)
            return False
    
    # ==================== HELPER METHODS ====================
    
    def _determine_severity_by_risk(self, risk_score: float) -> str:
        """Determine alert severity based on risk score"""
        if risk_score >= self.thresholds['critical_risk_score']:
            return AlertSeverity.CRITICAL.value
        elif risk_score >= self.thresholds['high_risk_score']:
            return AlertSeverity.HIGH.value
        elif risk_score >= self.thresholds['medium_risk_score']:
            return AlertSeverity.MEDIUM.value
        else:
            return AlertSeverity.LOW.value
    
    def get_alert_statistics(self) -> Dict:
        """Get alert statistics"""
        try:
            query = """
            MATCH (a:Alert)
            
            WITH a
            
            RETURN 
                count(a) as total_alerts,
                count(CASE WHEN a.status = 'OPEN' THEN 1 END) as open_alerts,
                count(CASE WHEN a.status = 'ASSIGNED' THEN 1 END) as assigned_alerts,
                count(CASE WHEN a.status = 'RESOLVED' THEN 1 END) as resolved_alerts,
                count(CASE WHEN a.severity = 'CRITICAL' THEN 1 END) as critical_alerts,
                count(CASE WHEN a.severity = 'HIGH' THEN 1 END) as high_alerts,
                count(CASE WHEN a.severity = 'MEDIUM' THEN 1 END) as medium_alerts,
                count(CASE WHEN a.severity = 'LOW' THEN 1 END) as low_alerts
            """
            
            results = self.driver.execute_query(query, {})
            
            if results:
                return results[0]
            
            return {}
            
        except Exception as e:
            logger.error(f"Error getting alert statistics: {e}", exc_info=True)
            return {}
