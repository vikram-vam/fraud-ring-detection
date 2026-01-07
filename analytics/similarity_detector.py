"""
Similarity Detector - Detect similarities between entities
Uses various similarity metrics to identify potential fraud connections
"""
from typing import Dict, List, Tuple
import logging
from difflib import SequenceMatcher
import re

from data.neo4j_driver import get_neo4j_driver

logger = logging.getLogger(__name__)


class SimilarityDetector:
    """
    Detect similarities between entities for fraud detection
    """
    
    def __init__(self):
        self.driver = get_neo4j_driver()
        
        # Similarity thresholds
        self.name_similarity_threshold = 0.85
        self.address_similarity_threshold = 0.90
        self.phone_similarity_threshold = 0.80
    
    def find_similar_claimants(
        self,
        claimant_id: str,
        threshold: float = 0.85
    ) -> List[Dict]:
        """
        Find claimants with similar names, addresses, or phone numbers
        
        Args:
            claimant_id: Reference claimant ID
            threshold: Similarity threshold (0-1)
            
        Returns:
            List of similar claimants with similarity scores
        """
        try:
            # Get reference claimant data
            ref_query = """
            MATCH (c:Claimant {claimant_id: $claimant_id})
            OPTIONAL MATCH (c)-[:LIVES_AT]->(a:Address)
            OPTIONAL MATCH (c)-[:HAS_PHONE]->(p:Phone)
            RETURN 
                c.name as name,
                c.email as email,
                collect(DISTINCT a.street) as addresses,
                collect(DISTINCT p.phone_number) as phones
            """
            
            ref_results = self.driver.execute_query(ref_query, {'claimant_id': claimant_id})
            
            if not ref_results:
                return []
            
            ref_data = ref_results[0]
            ref_name = ref_data.get('name', '')
            ref_email = ref_data.get('email', '')
            ref_addresses = ref_data.get('addresses', [])
            ref_phones = ref_data.get('phones', [])
            
            # Get all other claimants
            all_query = """
            MATCH (c:Claimant)
            WHERE c.claimant_id <> $claimant_id
            OPTIONAL MATCH (c)-[:LIVES_AT]->(a:Address)
            OPTIONAL MATCH (c)-[:HAS_PHONE]->(p:Phone)
            RETURN 
                c.claimant_id as claimant_id,
                c.name as name,
                c.email as email,
                collect(DISTINCT a.street) as addresses,
                collect(DISTINCT p.phone_number) as phones
            """
            
            all_results = self.driver.execute_query(all_query, {'claimant_id': claimant_id})
            
            similar_claimants = []
            
            for claimant in all_results:
                # Calculate similarity scores
                name_sim = self._calculate_string_similarity(ref_name, claimant.get('name', ''))
                email_sim = self._calculate_string_similarity(ref_email, claimant.get('email', ''))
                
                # Address similarity
                addr_sim = 0.0
                for ref_addr in ref_addresses:
                    for comp_addr in claimant.get('addresses', []):
                        addr_sim = max(addr_sim, self._calculate_string_similarity(ref_addr, comp_addr))
                
                # Phone similarity
                phone_sim = 0.0
                for ref_phone in ref_phones:
                    for comp_phone in claimant.get('phones', []):
                        phone_sim = max(phone_sim, self._calculate_phone_similarity(ref_phone, comp_phone))
                
                # Overall similarity (weighted average)
                overall_sim = (
                    name_sim * 0.4 +
                    email_sim * 0.2 +
                    addr_sim * 0.2 +
                    phone_sim * 0.2
                )
                
                if overall_sim >= threshold:
                    similar_claimants.append({
                        'claimant_id': claimant.get('claimant_id'),
                        'name': claimant.get('name'),
                        'similarity_score': round(overall_sim, 3),
                        'details': {
                            'name_similarity': round(name_sim, 3),
                            'email_similarity': round(email_sim, 3),
                            'address_similarity': round(addr_sim, 3),
                            'phone_similarity': round(phone_sim, 3)
                        }
                    })
            
            # Sort by similarity score
            similar_claimants.sort(key=lambda x: x['similarity_score'], reverse=True)
            
            return similar_claimants
            
        except Exception as e:
            logger.error(f"Error finding similar claimants: {e}", exc_info=True)
            return []
    
    def detect_duplicate_claims(
        self,
        claimant_id: str,
        days_window: int = 30
    ) -> List[Dict]:
        """
        Detect potentially duplicate claims
        
        Args:
            claimant_id: Claimant ID
            days_window: Days window to check for duplicates
            
        Returns:
            List of potential duplicate claim groups
        """
        try:
            query = """
            MATCH (c:Claimant {claimant_id: $claimant_id})-[:FILED]->(cl:Claim)
            WITH cl
            ORDER BY cl.claim_date
            WITH collect(cl) as claims
            
            UNWIND range(0, size(claims)-2) as i
            WITH claims[i] as claim1, claims[i+1] as claim2
            
            WHERE duration.between(claim1.claim_date, claim2.claim_date).days <= $days_window
            
            RETURN 
                claim1.claim_id as claim1_id,
                claim1.claim_number as claim1_number,
                claim1.claim_amount as amount1,
                claim1.claim_date as date1,
                claim2.claim_id as claim2_id,
                claim2.claim_number as claim2_number,
                claim2.claim_amount as amount2,
                claim2.claim_date as date2,
                duration.between(claim1.claim_date, claim2.claim_date).days as days_apart
            """
            
            results = self.driver.execute_query(query, {
                'claimant_id': claimant_id,
                'days_window': days_window
            })
            
            duplicates = []
            
            for result in results:
                amount_diff = abs(result.get('amount1', 0) - result.get('amount2', 0))
                amount_similarity = 1.0 - (amount_diff / max(result.get('amount1', 1), result.get('amount2', 1)))
                
                if amount_similarity >= 0.9:  # Claims are 90% similar in amount
                    duplicates.append({
                        'claim1': {
                            'claim_id': result.get('claim1_id'),
                            'claim_number': result.get('claim1_number'),
                            'amount': result.get('amount1'),
                            'date': result.get('date1')
                        },
                        'claim2': {
                            'claim_id': result.get('claim2_id'),
                            'claim_number': result.get('claim2_number'),
                            'amount': result.get('amount2'),
                            'date': result.get('date2')
                        },
                        'days_apart': result.get('days_apart'),
                        'amount_similarity': round(amount_similarity, 3)
                    })
            
            return duplicates
            
        except Exception as e:
            logger.error(f"Error detecting duplicate claims: {e}", exc_info=True)
            return []
    
    def find_name_variations(self, name: str) -> List[str]:
        """
        Find common name variations and misspellings
        
        Args:
            name: Name to find variations for
            
        Returns:
            List of potential name variations
        """
        variations = set()
        
        # Add original
        variations.add(name)
        
        # Remove middle initials
        parts = name.split()
        if len(parts) > 2:
            variations.add(f"{parts[0]} {parts[-1]}")
        
        # Add initials
        if len(parts) >= 2:
            variations.add(f"{parts[0][0]}. {parts[-1]}")
        
        # Swap first and last name
        if len(parts) >= 2:
            variations.add(f"{parts[-1]}, {parts[0]}")
        
        return list(variations)
    
    def _calculate_string_similarity(self, str1: str, str2: str) -> float:
        """Calculate similarity between two strings using SequenceMatcher"""
        if not str1 or not str2:
            return 0.0
        
        # Normalize strings
        str1 = str1.lower().strip()
        str2 = str2.lower().strip()
        
        # Use SequenceMatcher for similarity
        return SequenceMatcher(None, str1, str2).ratio()
    
    def _calculate_phone_similarity(self, phone1: str, phone2: str) -> float:
        """Calculate similarity between phone numbers"""
        if not phone1 or not phone2:
            return 0.0
        
        # Extract digits only
        digits1 = re.sub(r'\D', '', phone1)
        digits2 = re.sub(r'\D', '', phone2)
        
        # Compare last 10 digits (US phone number)
        if len(digits1) >= 10 and len(digits2) >= 10:
            digits1 = digits1[-10:]
            digits2 = digits2[-10:]
            
            if digits1 == digits2:
                return 1.0
            
            # Check if last 7 digits match (local number)
            if digits1[-7:] == digits2[-7:]:
                return 0.9
        
        # Use general string similarity
        return self._calculate_string_similarity(digits1, digits2)
    
    def calculate_claim_similarity(self, claim1_id: str, claim2_id: str) -> Dict:
        """
        Calculate similarity between two claims
        
        Args:
            claim1_id: First claim ID
            claim2_id: Second claim ID
            
        Returns:
            Dictionary with similarity analysis
        """
        try:
            query = """
            MATCH (cl1:Claim {claim_id: $claim1_id})
            MATCH (cl2:Claim {claim_id: $claim2_id})
            RETURN 
                cl1.claim_type as type1,
                cl1.claim_amount as amount1,
                cl1.claim_date as date1,
                cl2.claim_type as type2,
                cl2.claim_amount as amount2,
                cl2.claim_date as date2
            """
            
            results = self.driver.execute_query(query, {
                'claim1_id': claim1_id,
                'claim2_id': claim2_id
            })
            
            if not results:
                return {'similarity': 0.0}
            
            data = results[0]
            
            # Type similarity
            type_sim = 1.0 if data.get('type1') == data.get('type2') else 0.0
            
            # Amount similarity
            amount1 = data.get('amount1', 0)
            amount2 = data.get('amount2', 0)
            amount_diff = abs(amount1 - amount2)
            amount_sim = 1.0 - (amount_diff / max(amount1, amount2)) if max(amount1, amount2) > 0 else 0.0
            
            # Overall similarity
            overall_sim = (type_sim * 0.4 + amount_sim * 0.6)
            
            return {
                'similarity': round(overall_sim, 3),
                'type_match': type_sim == 1.0,
                'amount_similarity': round(amount_sim, 3),
                'amount_difference': amount_diff
            }
            
        except Exception as e:
            logger.error(f"Error calculating claim similarity: {e}", exc_info=True)
            return {'similarity': 0.0, 'error': str(e)}
