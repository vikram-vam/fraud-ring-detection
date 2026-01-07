"""
Case Manager - Manage fraud investigation cases
Handles case creation, tracking, and workflow management
"""
import logging
from typing import Dict, List, Optional
from datetime import datetime, timedelta
from enum import Enum
import uuid

from data.neo4j_driver import get_neo4j_driver
from utils.logger import setup_logger

logger = setup_logger(__name__)


class CaseStatus(Enum):
    """Case status enumeration"""
    NEW = "NEW"
    UNDER_INVESTIGATION = "UNDER_INVESTIGATION"
    EVIDENCE_GATHERING = "EVIDENCE_GATHERING"
    REVIEW = "REVIEW"
    PENDING_LEGAL = "PENDING_LEGAL"
    CLOSED_CONFIRMED_FRAUD = "CLOSED_CONFIRMED_FRAUD"
    CLOSED_NO_FRAUD = "CLOSED_NO_FRAUD"
    CLOSED_INSUFFICIENT_EVIDENCE = "CLOSED_INSUFFICIENT_EVIDENCE"


class CasePriority(Enum):
    """Case priority levels"""
    CRITICAL = "CRITICAL"
    HIGH = "HIGH"
    MEDIUM = "MEDIUM"
    LOW = "LOW"


class CaseType(Enum):
    """Case type enumeration"""
    STAGED_ACCIDENT = "Staged Accident"
    BODY_SHOP_FRAUD = "Body Shop Fraud"
    MEDICAL_MILL = "Medical Mill"
    ATTORNEY_ORGANIZED = "Attorney Organized Fraud"
    PHANTOM_PASSENGER = "Phantom Passenger"
    TOW_KICKBACK = "Tow Truck Kickback"
    FRAUD_RING = "Fraud Ring"
    VEHICLE_RECYCLING = "Vehicle Recycling"
    IDENTITY_FRAUD = "Identity Fraud"
    CLAIM_INFLATION = "Claim Inflation"
    MULTIPLE_VIOLATIONS = "Multiple Violations"


class CaseManager:
    """
    Case manager for fraud investigations
    Manages investigation lifecycle from creation to closure
    """
    
    def __init__(self):
        self.driver = get_neo4j_driver()
    
    # ==================== CASE CREATION ====================
    
    def create_case(
        self,
        case_type: str,
        priority: str,
        title: str,
        description: str,
        assigned_to: str,
        related_entities: Dict[str, List[str]],
        evidence: Optional[List[Dict]] = None,
        estimated_fraud_amount: float = 0.0
    ) -> Optional[str]:
        """
        Create a new investigation case
        
        Args:
            case_type: Type of case (from CaseType enum)
            priority: Priority level (from CasePriority enum)
            title: Case title
            description: Detailed description
            assigned_to: Investigator assigned
            related_entities: Dictionary of related entity IDs {entity_type: [ids]}
            evidence: List of evidence items
            estimated_fraud_amount: Estimated fraud amount
            
        Returns:
            Case ID if successful, None otherwise
        """
        try:
            case_id = f"CASE_{uuid.uuid4().hex[:12].upper()}"
            
            query = """
            CREATE (c:Case {
                case_id: $case_id,
                case_number: $case_number,
                case_type: $case_type,
                priority: $priority,
                status: $status,
                title: $title,
                description: $description,
                assigned_to: $assigned_to,
                estimated_fraud_amount: $estimated_fraud_amount,
                created_at: datetime($created_at),
                updated_at: datetime($updated_at),
                opened_by: $assigned_to,
                closed_at: null,
                closure_reason: null,
                final_determination: null
            })
            RETURN c.case_id as case_id
            """
            
            now = datetime.now().isoformat()
            case_number = f"INV-{datetime.now().strftime('%Y%m%d')}-{uuid.uuid4().hex[:6].upper()}"
            
            result = self.driver.execute_write(query, {
                'case_id': case_id,
                'case_number': case_number,
                'case_type': case_type,
                'priority': priority,
                'status': CaseStatus.NEW.value,
                'title': title,
                'description': description,
                'assigned_to': assigned_to,
                'estimated_fraud_amount': estimated_fraud_amount,
                'created_at': now,
                'updated_at': now
            })
            
            if result:
                # Link related entities
                self._link_entities_to_case(case_id, related_entities)
                
                # Add initial evidence if provided
                if evidence:
                    for item in evidence:
                        self.add_evidence(case_id, item)
                
                # Add initial activity log
                self.add_case_activity(
                    case_id,
                    activity_type="CASE_CREATED",
                    description=f"Case created and assigned to {assigned_to}",
                    performed_by=assigned_to
                )
                
                logger.info(f"Created case: {case_id} - {title}")
                return case_id
            
            return None
            
        except Exception as e:
            logger.error(f"Error creating case: {e}", exc_info=True)
            return None
    
    def create_case_from_alert(self, alert_id: str, assigned_to: str) -> Optional[str]:
        """
        Create investigation case from an alert
        
        Args:
            alert_id: Alert ID to convert to case
            assigned_to: Investigator to assign
            
        Returns:
            Case ID if successful, None otherwise
        """
        try:
            # Get alert details
            alert_query = """
            MATCH (a:Alert {alert_id: $alert_id})
            OPTIONAL MATCH (a)-[:ALERTS]->(entity)
            
            RETURN 
                a.alert_type as alert_type,
                a.severity as severity,
                a.title as title,
                a.description as description,
                a.entity_id as entity_id,
                a.entity_type as entity_type,
                labels(entity) as entity_labels
            """
            
            results = self.driver.execute_query(alert_query, {'alert_id': alert_id})
            
            if not results:
                logger.error(f"Alert {alert_id} not found")
                return None
            
            alert = results[0]
            
            # Determine priority from severity
            priority_map = {
                'CRITICAL': CasePriority.CRITICAL.value,
                'HIGH': CasePriority.HIGH.value,
                'MEDIUM': CasePriority.MEDIUM.value,
                'LOW': CasePriority.LOW.value
            }
            priority = priority_map.get(alert.get('severity'), CasePriority.MEDIUM.value)
            
            # Create case
            related_entities = {
                alert['entity_type']: [alert['entity_id']]
            }
            
            case_id = self.create_case(
                case_type=alert['alert_type'],
                priority=priority,
                title=f"Investigation: {alert['title']}",
                description=f"Case created from alert.\n\n{alert['description']}",
                assigned_to=assigned_to,
                related_entities=related_entities
            )
            
            if case_id:
                # Link case to alert
                link_query = """
                MATCH (c:Case {case_id: $case_id})
                MATCH (a:Alert {alert_id: $alert_id})
                MERGE (c)-[:ORIGINATED_FROM]->(a)
                """
                
                self.driver.execute_write(link_query, {
                    'case_id': case_id,
                    'alert_id': alert_id
                })
                
                logger.info(f"Created case {case_id} from alert {alert_id}")
                return case_id
            
            return None
            
        except Exception as e:
            logger.error(f"Error creating case from alert: {e}", exc_info=True)
            return None
    
    def _link_entities_to_case(self, case_id: str, related_entities: Dict[str, List[str]]) -> None:
        """Link related entities to case"""
        for entity_type, entity_ids in related_entities.items():
            for entity_id in entity_ids:
                try:
                    # Determine ID field based on entity type
                    id_field_map = {
                        'Claim': 'claim_id',
                        'Claimant': 'claimant_id',
                        'BodyShop': 'body_shop_id',
                        'MedicalProvider': 'provider_id',
                        'Vehicle': 'vehicle_id',
                        'Attorney': 'attorney_id',
                        'TowCompany': 'tow_company_id',
                        'AccidentLocation': 'location_id',
                        'Witness': 'witness_id',
                        'FraudRing': 'ring_id'
                    }
                    
                    id_field = id_field_map.get(entity_type, f"{entity_type.lower()}_id")
                    
                    query = f"""
                    MATCH (c:Case {{case_id: $case_id}})
                    MATCH (e:{entity_type} {{{id_field}: $entity_id}})
                    MERGE (c)-[:INVESTIGATES]->(e)
                    """
                    
                    self.driver.execute_write(query, {
                        'case_id': case_id,
                        'entity_id': entity_id
                    })
                    
                except Exception as e:
                    logger.error(f"Error linking {entity_type} to case: {e}")
    
    # ==================== CASE RETRIEVAL ====================
    
    def get_case_by_id(self, case_id: str) -> Optional[Dict]:
        """Get case by ID with all details"""
        try:
            query = """
            MATCH (c:Case {case_id: $case_id})
            
            // Get related entities
            OPTIONAL MATCH (c)-[:INVESTIGATES]->(entity)
            
            // Get evidence
            OPTIONAL MATCH (c)-[:HAS_EVIDENCE]->(ev:Evidence)
            
            // Get activities
            OPTIONAL MATCH (c)-[:HAS_ACTIVITY]->(act:CaseActivity)
            
            WITH c,
                 collect(DISTINCT {
                     entity_type: head(labels(entity)),
                     entity_id: coalesce(
                         entity.claim_id, entity.claimant_id, entity.body_shop_id,
                         entity.provider_id, entity.vehicle_id, entity.attorney_id,
                         entity.tow_company_id, entity.location_id, entity.witness_id,
                         entity.ring_id
                     )
                 }) as entities,
                 collect(DISTINCT ev {.*}) as evidence_items,
                 collect(DISTINCT act {.*}) as activities
            
            RETURN 
                c.case_id as case_id,
                c.case_number as case_number,
                c.case_type as case_type,
                c.priority as priority,
                c.status as status,
                c.title as title,
                c.description as description,
                c.assigned_to as assigned_to,
                c.estimated_fraud_amount as estimated_fraud_amount,
                c.created_at as created_at,
                c.updated_at as updated_at,
                c.opened_by as opened_by,
                c.closed_at as closed_at,
                c.closure_reason as closure_reason,
                c.final_determination as final_determination,
                [e IN entities WHERE e.entity_id IS NOT NULL] as related_entities,
                [ev IN evidence_items WHERE ev.evidence_id IS NOT NULL] as evidence,
                [a IN activities WHERE a.activity_id IS NOT NULL | a ORDER BY a.timestamp DESC] as activities
            """
            
            results = self.driver.execute_query(query, {'case_id': case_id})
            
            if results:
                return results[0]
            
            return None
            
        except Exception as e:
            logger.error(f"Error getting case: {e}", exc_info=True)
            return None
    
    def get_open_cases(self, assigned_to: Optional[str] = None, limit: int = 100) -> List[Dict]:
        """Get open investigation cases"""
        try:
            where_clause = "WHERE c.status IN ['NEW', 'UNDER_INVESTIGATION', 'EVIDENCE_GATHERING', 'REVIEW']"
            params = {'limit': limit}
            
            if assigned_to:
                where_clause += " AND c.assigned_to = $assigned_to"
                params['assigned_to'] = assigned_to
            
            query = f"""
            MATCH (c:Case)
            {where_clause}
            
            RETURN 
                c.case_id as case_id,
                c.case_number as case_number,
                c.case_type as case_type,
                c.priority as priority,
                c.status as status,
                c.title as title,
                c.assigned_to as assigned_to,
                c.estimated_fraud_amount as estimated_fraud_amount,
                c.created_at as created_at,
                c.updated_at as updated_at
            ORDER BY 
                CASE c.priority
                    WHEN 'CRITICAL' THEN 1
                    WHEN 'HIGH' THEN 2
                    WHEN 'MEDIUM' THEN 3
                    WHEN 'LOW' THEN 4
                    ELSE 5
                END,
                c.created_at DESC
            LIMIT $limit
            """
            
            results = self.driver.execute_query(query, params)
            return results
            
        except Exception as e:
            logger.error(f"Error getting open cases: {e}", exc_info=True)
            return []
    
    def get_cases_by_status(self, status: str, limit: int = 100) -> List[Dict]:
        """Get cases by status"""
        try:
            query = """
            MATCH (c:Case {status: $status})
            
            RETURN 
                c.case_id as case_id,
                c.case_number as case_number,
                c.case_type as case_type,
                c.priority as priority,
                c.status as status,
                c.title as title,
                c.assigned_to as assigned_to,
                c.estimated_fraud_amount as estimated_fraud_amount,
                c.created_at as created_at,
                c.updated_at as updated_at
            ORDER BY c.updated_at DESC
            LIMIT $limit
            """
            
            results = self.driver.execute_query(query, {
                'status': status,
                'limit': limit
            })
            return results
            
        except Exception as e:
            logger.error(f"Error getting cases by status: {e}", exc_info=True)
            return []
    
    def search_cases(
        self,
        case_type: Optional[str] = None,
        priority: Optional[str] = None,
        assigned_to: Optional[str] = None,
        status: Optional[str] = None,
        limit: int = 100
    ) -> List[Dict]:
        """Search cases with filters"""
        try:
            where_clauses = []
            params = {'limit': limit}
            
            if case_type:
                where_clauses.append("c.case_type = $case_type")
                params['case_type'] = case_type
            
            if priority:
                where_clauses.append("c.priority = $priority")
                params['priority'] = priority
            
            if assigned_to:
                where_clauses.append("c.assigned_to = $assigned_to")
                params['assigned_to'] = assigned_to
            
            if status:
                where_clauses.append("c.status = $status")
                params['status'] = status
            
            where_clause = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""
            
            query = f"""
            MATCH (c:Case)
            {where_clause}
            
            RETURN 
                c.case_id as case_id,
                c.case_number as case_number,
                c.case_type as case_type,
                c.priority as priority,
                c.status as status,
                c.title as title,
                c.assigned_to as assigned_to,
                c.estimated_fraud_amount as estimated_fraud_amount,
                c.created_at as created_at,
                c.updated_at as updated_at
            ORDER BY c.updated_at DESC
            LIMIT $limit
            """
            
            results = self.driver.execute_query(query, params)
            return results
            
        except Exception as e:
            logger.error(f"Error searching cases: {e}", exc_info=True)
            return []
    
    # ==================== CASE MANAGEMENT ====================
    
    def update_case_status(self, case_id: str, status: str, notes: Optional[str] = None) -> bool:
        """Update case status"""
        try:
            query = """
            MATCH (c:Case {case_id: $case_id})
            SET c.status = $status,
                c.updated_at = datetime($updated_at)
            RETURN c.case_id as case_id
            """
            
            result = self.driver.execute_write(query, {
                'case_id': case_id,
                'status': status,
                'updated_at': datetime.now().isoformat()
            })
            
            if result:
                # Log activity
                activity_desc = f"Case status updated to {status}"
                if notes:
                    activity_desc += f". Notes: {notes}"
                
                self.add_case_activity(
                    case_id,
                    activity_type="STATUS_CHANGE",
                    description=activity_desc,
                    performed_by="System"
                )
                
                logger.info(f"Updated case {case_id} status to {status}")
                return True
            
            return False
            
        except Exception as e:
            logger.error(f"Error updating case status: {e}", exc_info=True)
            return False
    
    def assign_case(self, case_id: str, assigned_to: str, notes: Optional[str] = None) -> bool:
        """Reassign case to different investigator"""
        try:
            query = """
            MATCH (c:Case {case_id: $case_id})
            SET c.assigned_to = $assigned_to,
                c.updated_at = datetime($updated_at)
            RETURN c.case_id as case_id
            """
            
            result = self.driver.execute_write(query, {
                'case_id': case_id,
                'assigned_to': assigned_to,
                'updated_at': datetime.now().isoformat()
            })
            
            if result:
                # Log activity
                activity_desc = f"Case reassigned to {assigned_to}"
                if notes:
                    activity_desc += f". Reason: {notes}"
                
                self.add_case_activity(
                    case_id,
                    activity_type="REASSIGNMENT",
                    description=activity_desc,
                    performed_by="System"
                )
                
                logger.info(f"Reassigned case {case_id} to {assigned_to}")
                return True
            
            return False
            
        except Exception as e:
            logger.error(f"Error assigning case: {e}", exc_info=True)
            return False
    
    def update_case_priority(self, case_id: str, priority: str) -> bool:
        """Update case priority"""
        try:
            query = """
            MATCH (c:Case {case_id: $case_id})
            SET c.priority = $priority,
                c.updated_at = datetime($updated_at)
            RETURN c.case_id as case_id
            """
            
            result = self.driver.execute_write(query, {
                'case_id': case_id,
                'priority': priority,
                'updated_at': datetime.now().isoformat()
            })
            
            if result:
                self.add_case_activity(
                    case_id,
                    activity_type="PRIORITY_CHANGE",
                    description=f"Priority changed to {priority}",
                    performed_by="System"
                )
                
                logger.info(f"Updated case {case_id} priority to {priority}")
                return True
            
            return False
            
        except Exception as e:
            logger.error(f"Error updating case priority: {e}", exc_info=True)
            return False
    
    def close_case(
        self,
        case_id: str,
        final_determination: str,
        closure_reason: str,
        closed_by: str
    ) -> bool:
        """
        Close an investigation case
        
        Args:
            case_id: Case ID
            final_determination: FRAUD_CONFIRMED, NO_FRAUD, INSUFFICIENT_EVIDENCE
            closure_reason: Detailed closure reason
            closed_by: Investigator closing the case
            
        Returns:
            True if successful
        """
        try:
            # Map determination to status
            status_map = {
                'FRAUD_CONFIRMED': CaseStatus.CLOSED_CONFIRMED_FRAUD.value,
                'NO_FRAUD': CaseStatus.CLOSED_NO_FRAUD.value,
                'INSUFFICIENT_EVIDENCE': CaseStatus.CLOSED_INSUFFICIENT_EVIDENCE.value
            }
            
            status = status_map.get(final_determination, CaseStatus.CLOSED_NO_FRAUD.value)
            
            query = """
            MATCH (c:Case {case_id: $case_id})
            SET c.status = $status,
                c.final_determination = $final_determination,
                c.closure_reason = $closure_reason,
                c.closed_at = datetime($closed_at),
                c.updated_at = datetime($updated_at)
            RETURN c.case_id as case_id
            """
            
            now = datetime.now().isoformat()
            
            result = self.driver.execute_write(query, {
                'case_id': case_id,
                'status': status,
                'final_determination': final_determination,
                'closure_reason': closure_reason,
                'closed_at': now,
                'updated_at': now
            })
            
            if result:
                self.add_case_activity(
                    case_id,
                    activity_type="CASE_CLOSED",
                    description=f"Case closed: {final_determination}. {closure_reason}",
                    performed_by=closed_by
                )
                
                logger.info(f"Closed case {case_id}: {final_determination}")
                return True
            
            return False
            
        except Exception as e:
            logger.error(f"Error closing case: {e}", exc_info=True)
            return False
    
    # ==================== EVIDENCE MANAGEMENT ====================
    
    def add_evidence(
        self,
        case_id: str,
        evidence_data: Dict,
        added_by: Optional[str] = None
    ) -> Optional[str]:
        """
        Add evidence to case
        
        Args:
            case_id: Case ID
            evidence_data: Evidence information {type, description, source, etc}
            added_by: Person adding evidence
            
        Returns:
            Evidence ID if successful
        """
        try:
            evidence_id = f"EVID_{uuid.uuid4().hex[:12].upper()}"
            
            query = """
            MATCH (c:Case {case_id: $case_id})
            CREATE (ev:Evidence {
                evidence_id: $evidence_id,
                evidence_type: $evidence_type,
                description: $description,
                source: $source,
                collected_date: datetime($collected_date),
                added_by: $added_by,
                file_path: $file_path,
                notes: $notes
            })
            MERGE (c)-[:HAS_EVIDENCE]->(ev)
            RETURN ev.evidence_id as evidence_id
            """
            
            result = self.driver.execute_write(query, {
                'case_id': case_id,
                'evidence_id': evidence_id,
                'evidence_type': evidence_data.get('type', 'DOCUMENT'),
                'description': evidence_data.get('description', ''),
                'source': evidence_data.get('source', ''),
                'collected_date': datetime.now().isoformat(),
                'added_by': added_by or 'System',
                'file_path': evidence_data.get('file_path', ''),
                'notes': evidence_data.get('notes', '')
            })
            
            if result:
                self.add_case_activity(
                    case_id,
                    activity_type="EVIDENCE_ADDED",
                    description=f"Evidence added: {evidence_data.get('type')} - {evidence_data.get('description')}",
                    performed_by=added_by or 'System'
                )
                
                logger.info(f"Added evidence {evidence_id} to case {case_id}")
                return evidence_id
            
            return None
            
        except Exception as e:
            logger.error(f"Error adding evidence: {e}", exc_info=True)
            return None
    
    def get_case_evidence(self, case_id: str) -> List[Dict]:
        """Get all evidence for a case"""
        try:
            query = """
            MATCH (c:Case {case_id: $case_id})-[:HAS_EVIDENCE]->(ev:Evidence)
            
            RETURN 
                ev.evidence_id as evidence_id,
                ev.evidence_type as evidence_type,
                ev.description as description,
                ev.source as source,
                ev.collected_date as collected_date,
                ev.added_by as added_by,
                ev.file_path as file_path,
                ev.notes as notes
            ORDER BY ev.collected_date DESC
            """
            
            results = self.driver.execute_query(query, {'case_id': case_id})
            return results
            
        except Exception as e:
            logger.error(f"Error getting case evidence: {e}", exc_info=True)
            return []
    
    # ==================== ACTIVITY LOG ====================
    
    def add_case_activity(
        self,
        case_id: str,
        activity_type: str,
        description: str,
        performed_by: str
    ) -> Optional[str]:
        """Add activity log entry to case"""
        try:
            activity_id = f"ACT_{uuid.uuid4().hex[:12].upper()}"
            
            query = """
            MATCH (c:Case {case_id: $case_id})
            CREATE (act:CaseActivity {
                activity_id: $activity_id,
                activity_type: $activity_type,
                description: $description,
                performed_by: $performed_by,
                timestamp: datetime($timestamp)
            })
            MERGE (c)-[:HAS_ACTIVITY]->(act)
            RETURN act.activity_id as activity_id
            """
            
            result = self.driver.execute_write(query, {
                'case_id': case_id,
                'activity_id': activity_id,
                'activity_type': activity_type,
                'description': description,
                'performed_by': performed_by,
                'timestamp': datetime.now().isoformat()
            })
            
            if result:
                return activity_id
            
            return None
            
        except Exception as e:
            logger.error(f"Error adding case activity: {e}", exc_info=True)
            return None
    
    def get_case_activities(self, case_id: str, limit: int = 50) -> List[Dict]:
        """Get activity log for case"""
        try:
            query = """
            MATCH (c:Case {case_id: $case_id})-[:HAS_ACTIVITY]->(act:CaseActivity)
            
            RETURN 
                act.activity_id as activity_id,
                act.activity_type as activity_type,
                act.description as description,
                act.performed_by as performed_by,
                act.timestamp as timestamp
            ORDER BY act.timestamp DESC
            LIMIT $limit
            """
            
            results = self.driver.execute_query(query, {
                'case_id': case_id,
                'limit': limit
            })
            return results
            
        except Exception as e:
            logger.error(f"Error getting case activities: {e}", exc_info=True)
            return []
    
    # ==================== STATISTICS ====================
    
    def get_case_statistics(self, assigned_to: Optional[str] = None) -> Dict:
        """Get case statistics"""
        try:
            where_clause = ""
            params = {}
            
            if assigned_to:
                where_clause = "WHERE c.assigned_to = $assigned_to"
                params['assigned_to'] = assigned_to
            
            query = f"""
            MATCH (c:Case)
            {where_clause}
            
            RETURN 
                count(c) as total_cases,
                count(CASE WHEN c.status = 'NEW' THEN 1 END) as new_cases,
                count(CASE WHEN c.status IN ['UNDER_INVESTIGATION', 'EVIDENCE_GATHERING', 'REVIEW'] THEN 1 END) as active_cases,
                count(CASE WHEN c.status LIKE 'CLOSED%' THEN 1 END) as closed_cases,
                count(CASE WHEN c.final_determination = 'FRAUD_CONFIRMED' THEN 1 END) as fraud_confirmed,
                count(CASE WHEN c.priority = 'CRITICAL' THEN 1 END) as critical_cases,
                count(CASE WHEN c.priority = 'HIGH' THEN 1 END) as high_priority_cases,
                sum(CASE WHEN c.final_determination = 'FRAUD_CONFIRMED' THEN c.estimated_fraud_amount ELSE 0 END) as total_fraud_prevented
            """
            
            results = self.driver.execute_query(query, params)
            
            if results:
                return results[0]
            
            return {}
            
        except Exception as e:
            logger.error(f"Error getting case statistics: {e}", exc_info=True)
            return {}
