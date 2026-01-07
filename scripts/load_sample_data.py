"""
Load Sample Data Script - AUTO INSURANCE FRAUD DETECTION
Generates comprehensive synthetic auto insurance fraud dataset
Includes staged accidents, body shop rings, medical mills, and organized fraud
"""
import sys
import os
import random
from datetime import datetime, timedelta
from typing import List, Dict, Tuple, Set
import uuid

# Add parent directory to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from data.neo4j_driver import get_neo4j_driver
from utils.logger import setup_logger

logger = setup_logger(__name__)

# Auto Insurance Specific Data
FIRST_NAMES = [
    "John", "Jane", "Michael", "Sarah", "David", "Emily", "Robert", "Lisa",
    "William", "Jennifer", "James", "Mary", "Christopher", "Patricia", "Daniel",
    "Nancy", "Matthew", "Linda", "Anthony", "Barbara", "Mark", "Susan", "Donald",
    "Jessica", "Steven", "Margaret", "Paul", "Karen", "Andrew", "Betty", "Joshua",
    "Helen", "Kenneth", "Sandra", "Kevin", "Donna", "Brian", "Carol", "George"
]

LAST_NAMES = [
    "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis",
    "Rodriguez", "Martinez", "Hernandez", "Lopez", "Wilson", "Anderson", "Thomas",
    "Taylor", "Moore", "Jackson", "Martin", "Lee", "Perez", "Thompson", "White"
]

# Vehicle Data
CAR_MAKES = ["Toyota", "Honda", "Ford", "Chevrolet", "Nissan", "BMW", "Mercedes", 
             "Audi", "Lexus", "Hyundai", "Kia", "Mazda", "Subaru", "Volkswagen"]

CAR_MODELS = {
    "Toyota": ["Camry", "Corolla", "RAV4", "Highlander", "Tacoma"],
    "Honda": ["Accord", "Civic", "CR-V", "Pilot", "Odyssey"],
    "Ford": ["F-150", "Explorer", "Escape", "Mustang", "Focus"],
    "Chevrolet": ["Silverado", "Equinox", "Malibu", "Tahoe", "Camaro"],
    "Nissan": ["Altima", "Sentra", "Rogue", "Pathfinder", "Maxima"],
    "BMW": ["3 Series", "5 Series", "X3", "X5", "7 Series"],
    "Mercedes": ["C-Class", "E-Class", "GLE", "GLC", "S-Class"],
    "Audi": ["A4", "A6", "Q5", "Q7", "A3"],
    "Lexus": ["RX", "ES", "NX", "GX", "IS"],
    "Hyundai": ["Elantra", "Sonata", "Tucson", "Santa Fe", "Kona"],
    "Kia": ["Optima", "Sorento", "Sportage", "Soul", "Forte"],
    "Mazda": ["Mazda3", "Mazda6", "CX-5", "CX-9", "MX-5"],
    "Subaru": ["Outback", "Forester", "Crosstrek", "Impreza", "Legacy"],
    "Volkswagen": ["Jetta", "Passat", "Tiguan", "Atlas", "Golf"]
}

CAR_YEARS = list(range(2015, 2026))
CAR_COLORS = ["Black", "White", "Silver", "Gray", "Blue", "Red", "Green", "Brown", "Gold"]

# Accident/Claim Types
ACCIDENT_TYPES = [
    "Rear-End Collision",
    "Side-Impact Collision", 
    "Head-On Collision",
    "Hit and Run",
    "Single Vehicle Accident",
    "Parking Lot Collision",
    "Intersection Collision",
    "Multi-Vehicle Pileup"
]

INJURY_TYPES = [
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

# Body Shops
BODY_SHOP_NAMES = [
    "Quick Fix Auto Body", "Premium Collision Center", "Elite Auto Repair",
    "Express Body Shop", "Perfect Paint & Body", "AutoCraft Collision",
    "Precision Auto Body", "Speedy Repairs Inc", "Master Collision Center",
    "Supreme Auto Body", "Golden Gate Auto Repair", "Rapid Response Collision",
    "Pro Touch Auto Body", "Certified Collision Experts", "Ultimate Auto Repair"
]

# Medical Providers (Chiropractors, Physical Therapy, etc.)
MEDICAL_PROVIDER_TYPES = [
    "Chiropractic Clinic",
    "Physical Therapy Center", 
    "Pain Management Clinic",
    "Orthopedic Clinic",
    "Rehabilitation Center",
    "Medical Diagnostic Center",
    "Sports Medicine Clinic"
]

MEDICAL_PROVIDER_NAMES = [
    "Advanced Care", "Health Plus", "Wellness Center", "Complete Recovery",
    "Premier Health", "Rapid Recovery", "Optimal Health", "Total Wellness",
    "First Choice Medical", "Elite Health Services", "Supreme Care Center"
]

# Law Firms specializing in auto accidents
LAW_FIRM_NAMES = [
    "Accident Injury Law Group", "Auto Collision Attorneys", "Crash Claims Legal",
    "Highway Injury Lawyers", "Car Wreck Legal Services", "Impact Law Firm",
    "Collision Justice Attorneys", "Injury Recovery Law", "Auto Claims Legal Group"
]

# Tow Companies
TOW_COMPANY_NAMES = [
    "Quick Tow Services", "24/7 Towing", "Emergency Tow Pros", "Rapid Response Towing",
    "City Tow Company", "Express Towing Services", "Metro Tow & Recovery",
    "All Hours Towing", "Premier Tow Services", "Roadside Towing Solutions"
]

# Accident Locations (common fraud intersection hot spots)
ACCIDENT_INTERSECTIONS = [
    "Main St & Oak Ave", "Broadway & 5th Street", "Highway 101 & Elm Road",
    "Park Ave & Washington Blvd", "Market St & 3rd Avenue", "Lincoln Way & Sunset Dr",
    "River Road & Pine Street", "Central Ave & Maple Drive", "Harbor Blvd & Ocean View",
    "Valley Road & Hill Street", "Lake Shore Dr & Forest Ave", "Industrial Blvd & Commerce St"
]

CITIES = [
    "Los Angeles", "San Diego", "San Jose", "San Francisco", "Sacramento",
    "Oakland", "Fresno", "Long Beach", "Riverside", "Bakersfield"
]

STATES = ["CA"]  # Focus on California for auto insurance


class AutoInsuranceFraudDataGenerator:
    """Comprehensive auto insurance fraud data generator"""
    
    def __init__(self, driver):
        self.driver = driver
        self.claimants = []
        self.vehicles = []
        self.policies = []
        self.body_shops = []
        self.medical_providers = []
        self.attorneys = []
        self.tow_companies = []
        self.witnesses = []
        self.accident_locations = []
        self.claims = []
        self.fraud_rings = []
        
    def generate_all_data(
        self,
        num_claimants: int = 150,
        num_fraud_rings: int = 15
    ):
        """Generate complete auto insurance dataset"""
        
        print("=" * 80)
        print("AUTO INSURANCE FRAUD DETECTION - COMPREHENSIVE DATASET GENERATOR")
        print("=" * 80)
        print()
        
        # Phase 1: Base Entities
        print("PHASE 1: Generating Base Entities")
        print("-" * 80)
        self._generate_base_entities(num_claimants)
        
        # Phase 2: Fraud Rings
        print("\nPHASE 2: Creating Auto Insurance Fraud Ring Patterns")
        print("-" * 80)
        self._create_auto_fraud_rings(num_fraud_rings)
        
        # Phase 3: Claims
        print("\nPHASE 3: Generating Auto Insurance Claims")
        print("-" * 80)
        self._generate_auto_claims()
        
        # Phase 4: Load to Neo4j
        print("\nPHASE 4: Loading Data into Neo4j")
        print("-" * 80)
        self._load_to_neo4j()
        
        print("\n" + "=" * 80)
        print("✓ AUTO INSURANCE DATASET GENERATION COMPLETE!")
        print("=" * 80)
    
    def _generate_base_entities(self, num_claimants: int):
        """Generate base entities for auto insurance"""
        
        # Claimants
        print(f"Generating {num_claimants} claimants...")
        for i in range(num_claimants):
            self.claimants.append({
                'claimant_id': f"CLMT_{i:04d}",
                'name': f"{random.choice(FIRST_NAMES)} {random.choice(LAST_NAMES)}",
                'email': f"claimant{i}@example.com",
                'phone': f"555-{random.randint(100, 999)}-{random.randint(1000, 9999)}",
                'date_of_birth': (datetime.now() - timedelta(days=random.randint(7300, 25550))).strftime('%Y-%m-%d'),
                'drivers_license': f"D{random.randint(1000000, 9999999)}"
            })
        print(f"   ✓ Generated {len(self.claimants)} claimants")
        
        # Vehicles (more vehicles than claimants for complexity)
        num_vehicles = int(num_claimants * 1.3)
        print(f"Generating {num_vehicles} vehicles...")
        for i in range(num_vehicles):
            make = random.choice(CAR_MAKES)
            model = random.choice(CAR_MODELS[make])
            year = random.choice(CAR_YEARS)
            
            self.vehicles.append({
                'vehicle_id': f"VEH_{i:04d}",
                'vin': f"1HGBH{random.randint(10, 99)}8H7N{random.randint(100000, 999999)}",
                'make': make,
                'model': model,
                'year': year,
                'color': random.choice(CAR_COLORS),
                'license_plate': f"{random.randint(1, 9)}{random.choice('ABCDEFGHJKLMNPRSTUVWXYZ')}{random.choice('ABCDEFGHJKLMNPRSTUVWXYZ')}{random.choice('ABCDEFGHJKLMNPRSTUVWXYZ')}{random.randint(100, 999)}"
            })
        print(f"   ✓ Generated {len(self.vehicles)} vehicles")
        
        # Body Shops
        num_body_shops = 20
        print(f"Generating {num_body_shops} body shops...")
        for i in range(num_body_shops):
            self.body_shops.append({
                'body_shop_id': f"SHOP_{i:03d}",
                'name': random.choice(BODY_SHOP_NAMES),
                'license_number': f"BS-{random.randint(100000, 999999)}",
                'street': f"{random.randint(100, 9999)} Industrial Blvd",
                'city': random.choice(CITIES),
                'state': 'CA',
                'zip_code': f"{random.randint(90000, 96999)}",
                'phone': f"555-{random.randint(100, 999)}-{random.randint(1000, 9999)}"
            })
        print(f"   ✓ Generated {len(self.body_shops)} body shops")
        
        # Medical Providers
        num_medical = 25
        print(f"Generating {num_medical} medical providers...")
        for i in range(num_medical):
            self.medical_providers.append({
                'provider_id': f"MED_{i:03d}",
                'name': f"{random.choice(MEDICAL_PROVIDER_NAMES)} {random.choice(MEDICAL_PROVIDER_TYPES)}",
                'provider_type': random.choice(MEDICAL_PROVIDER_TYPES),
                'license_number': f"MED-{random.randint(100000, 999999)}",
                'street': f"{random.randint(100, 9999)} Medical Plaza",
                'city': random.choice(CITIES),
                'state': 'CA',
                'zip_code': f"{random.randint(90000, 96999)}",
                'phone': f"555-{random.randint(100, 999)}-{random.randint(1000, 9999)}"
            })
        print(f"   ✓ Generated {len(self.medical_providers)} medical providers")
        
        # Attorneys
        num_attorneys = 15
        print(f"Generating {num_attorneys} attorneys...")
        for i in range(num_attorneys):
            self.attorneys.append({
                'attorney_id': f"ATT_{i:03d}",
                'name': f"{random.choice(FIRST_NAMES)} {random.choice(LAST_NAMES)}",
                'firm': random.choice(LAW_FIRM_NAMES),
                'bar_number': f"BAR-{random.randint(100000, 999999)}",
                'street': f"{random.randint(100, 9999)} Legal Plaza",
                'city': random.choice(CITIES),
                'state': 'CA',
                'zip_code': f"{random.randint(90000, 96999)}",
                'phone': f"555-{random.randint(100, 999)}-{random.randint(1000, 9999)}",
                'email': f"attorney{i}@lawfirm.com"
            })
        print(f"   ✓ Generated {len(self.attorneys)} attorneys")
        
        # Tow Companies
        num_tow = 12
        print(f"Generating {num_tow} tow companies...")
        for i in range(num_tow):
            self.tow_companies.append({
                'tow_company_id': f"TOW_{i:03d}",
                'name': random.choice(TOW_COMPANY_NAMES),
                'license_number': f"TOW-{random.randint(10000, 99999)}",
                'city': random.choice(CITIES),
                'state': 'CA',
                'phone': f"555-{random.randint(100, 999)}-{random.randint(1000, 9999)}"
            })
        print(f"   ✓ Generated {len(self.tow_companies)} tow companies")
        
        # Accident Locations
        print(f"Generating accident locations...")
        for i, intersection in enumerate(ACCIDENT_INTERSECTIONS):
            self.accident_locations.append({
                'location_id': f"LOC_{i:03d}",
                'intersection': intersection,
                'city': random.choice(CITIES),
                'state': 'CA',
                'latitude': round(random.uniform(33.0, 38.0), 6),
                'longitude': round(random.uniform(-122.0, -117.0), 6)
            })
        print(f"   ✓ Generated {len(self.accident_locations)} accident locations")
        
        # Witnesses (can appear in multiple accidents - FRAUD INDICATOR)
        num_witnesses = 30
        print(f"Generating {num_witnesses} witnesses...")
        for i in range(num_witnesses):
            self.witnesses.append({
                'witness_id': f"WIT_{i:03d}",
                'name': f"{random.choice(FIRST_NAMES)} {random.choice(LAST_NAMES)}",
                'phone': f"555-{random.randint(100, 999)}-{random.randint(1000, 9999)}"
            })
        print(f"   ✓ Generated {len(self.witnesses)} witnesses")
    
    def _create_auto_fraud_rings(self, num_rings: int):
        """Create auto insurance specific fraud rings"""
        
        # Pattern 1: Staged Accident Rings (4 rings) - HIGHEST CONFIDENCE
        print("\n1. Creating Staged Accident Rings...")
        print("   (Same vehicles, same witnesses, same locations)")
        for i in range(4):
            ring = self._create_staged_accident_ring(i)
            self.fraud_rings.append(ring)
            print(f"   ✓ Ring {i+1}: {ring['member_count']} members staging accidents")
        
        # Pattern 2: Body Shop Fraud Rings (3 rings)
        print("\n2. Creating Body Shop Fraud Rings...")
        print("   (Inflated repairs, kickbacks, multiple claimants)")
        for i in range(3):
            ring = self._create_body_shop_ring(i)
            self.fraud_rings.append(ring)
            print(f"   ✓ Ring {i+1}: {ring['member_count']} members using {ring['entity_name']}")
        
        # Pattern 3: Medical Mill Rings (3 rings)
        print("\n3. Creating Medical Mill Fraud Rings...")
        print("   (Unnecessary treatments, exaggerated injuries)")
        for i in range(3):
            ring = self._create_medical_mill_ring(i)
            self.fraud_rings.append(ring)
            print(f"   ✓ Ring {i+1}: {ring['member_count']} patients at {ring['entity_name']}")
        
        # Pattern 4: Attorney-Organized Rings (2 rings)
        print("\n4. Creating Attorney-Organized Fraud Rings...")
        print("   (Attorney recruits claimants, directs to providers/shops)")
        for i in range(2):
            ring = self._create_attorney_organized_ring(i)
            self.fraud_rings.append(ring)
            print(f"   ✓ Ring {i+1}: {ring['member_count']} clients of {ring['entity_name']}")
        
        # Pattern 5: Phantom Passenger Rings (2 rings)
        print("\n5. Creating Phantom Passenger Rings...")
        print("   (Fake passengers in multiple accidents)")
        for i in range(2):
            ring = self._create_phantom_passenger_ring(i)
            self.fraud_rings.append(ring)
            print(f"   ✓ Ring {i+1}: {ring['member_count']} 'phantom passengers'")
        
        # Pattern 6: Tow Truck Kickback Ring (1 ring)
        print("\n6. Creating Tow Truck Kickback Ring...")
        print("   (Tow company steers to specific body shop)")
        ring = self._create_tow_truck_ring(0)
        self.fraud_rings.append(ring)
        print(f"   ✓ Ring: {ring['member_count']} members with kickback scheme")
        
        print(f"\n   ✓ Total Fraud Rings Created: {len(self.fraud_rings)}")
    
    def _create_staged_accident_ring(self, ring_index: int) -> Dict:
        """Staged accident ring - HIGHEST CONFIDENCE (0.90-0.98)"""
        num_members = random.randint(4, 7)
        
        available = [c for c in self.claimants if not any(c in r['members'] for r in self.fraud_rings)]
        members = random.sample(available, min(num_members, len(available)))
        
        # Select shared elements (KEY FRAUD INDICATORS)
        shared_vehicles = random.sample(self.vehicles, random.randint(2, 4))
        shared_location = random.choice(self.accident_locations)
        shared_witnesses = random.sample(self.witnesses, random.randint(2, 4))
        shared_body_shop = random.choice(self.body_shops)
        
        return {
            'ring_id': f"RING_STAGED_{ring_index:02d}",
            'ring_type': 'DISCOVERED',
            'pattern_type': 'staged_accident',
            'status': 'UNDER_REVIEW',
            'confidence_score': random.uniform(0.90, 0.98),  # VERY HIGH
            'members': members,
            'member_count': len(members),
            'shared_vehicles': shared_vehicles,
            'shared_location': shared_location,
            'shared_witnesses': shared_witnesses,
            'shared_body_shop': shared_body_shop,
            'entity_name': f"Staged Accidents at {shared_location['intersection']}"
        }
    
    def _create_body_shop_ring(self, ring_index: int) -> Dict:
        """Body shop fraud ring"""
        num_members = random.randint(6, 10)
        
        available = [c for c in self.claimants if not any(c in r['members'] for r in self.fraud_rings)]
        members = random.sample(available, min(num_members, len(available)))
        
        body_shop = random.choice(self.body_shops)
        shared_attorney = random.choice(self.attorneys)  # Attorney kickback
        
        return {
            'ring_id': f"RING_BODYSHOP_{ring_index:02d}",
            'ring_type': 'DISCOVERED',
            'pattern_type': 'body_shop_fraud',
            'status': 'UNDER_REVIEW',
            'confidence_score': random.uniform(0.78, 0.91),
            'members': members,
            'member_count': len(members),
            'body_shop': body_shop,
            'attorney': shared_attorney,
            'entity_name': body_shop['name']
        }
    
    def _create_medical_mill_ring(self, ring_index: int) -> Dict:
        """Medical mill fraud ring"""
        num_members = random.randint(7, 12)
        
        available = [c for c in self.claimants if not any(c in r['members'] for r in self.fraud_rings)]
        members = random.sample(available, min(num_members, len(available)))
        
        medical_provider = random.choice(self.medical_providers)
        shared_attorney = random.choice(self.attorneys)
        
        return {
            'ring_id': f"RING_MEDMILL_{ring_index:02d}",
            'ring_type': 'DISCOVERED',
            'pattern_type': 'medical_mill',
            'status': 'UNDER_REVIEW',
            'confidence_score': random.uniform(0.75, 0.89),
            'members': members,
            'member_count': len(members),
            'medical_provider': medical_provider,
            'attorney': shared_attorney,
            'entity_name': medical_provider['name']
        }
    
    def _create_attorney_organized_ring(self, ring_index: int) -> Dict:
        """Attorney-organized fraud ring"""
        num_members = random.randint(6, 10)
        
        available = [c for c in self.claimants if not any(c in r['members'] for r in self.fraud_rings)]
        members = random.sample(available, min(num_members, len(available)))
        
        attorney = random.choice(self.attorneys)
        captive_body_shop = random.choice(self.body_shops)
        captive_medical = random.choice(self.medical_providers)
        
        return {
            'ring_id': f"RING_ATTORNEY_{ring_index:02d}",
            'ring_type': 'DISCOVERED',
            'pattern_type': 'attorney_organized',
            'status': 'UNDER_REVIEW',
            'confidence_score': random.uniform(0.82, 0.94),
            'members': members,
            'member_count': len(members),
            'attorney': attorney,
            'body_shop': captive_body_shop,
            'medical_provider': captive_medical,
            'entity_name': attorney['firm']
        }
    
    def _create_phantom_passenger_ring(self, ring_index: int) -> Dict:
        """Phantom passenger fraud ring"""
        num_members = random.randint(4, 6)
        
        available = [c for c in self.claimants if not any(c in r['members'] for r in self.fraud_rings)]
        members = random.sample(available, min(num_members, len(available)))
        
        # These "passengers" appear in multiple accidents
        return {
            'ring_id': f"RING_PHANTOM_{ring_index:02d}",
            'ring_type': 'SUSPICIOUS',
            'pattern_type': 'phantom_passenger',
            'status': 'UNDER_REVIEW',
            'confidence_score': random.uniform(0.70, 0.85),
            'members': members,
            'member_count': len(members),
            'entity_name': f"Phantom Passenger Group {ring_index + 1}"
        }
    
    def _create_tow_truck_ring(self, ring_index: int) -> Dict:
        """Tow truck kickback ring"""
        num_members = random.randint(5, 8)
        
        available = [c for c in self.claimants if not any(c in r['members'] for r in self.fraud_rings)]
        members = random.sample(available, min(num_members, len(available)))
        
        tow_company = random.choice(self.tow_companies)
        body_shop = random.choice(self.body_shops)
        
        return {
            'ring_id': f"RING_TOW_{ring_index:02d}",
            'ring_type': 'DISCOVERED',
            'pattern_type': 'tow_truck_kickback',
            'status': 'UNDER_REVIEW',
            'confidence_score': random.uniform(0.72, 0.87),
            'members': members,
            'member_count': len(members),
            'tow_company': tow_company,
            'body_shop': body_shop,
            'entity_name': f"{tow_company['name']} → {body_shop['name']}"
        }
    
    def _generate_auto_claims(self):
        """Generate auto insurance claims"""
        
        claim_counter = 0
        
        # HIGH RISK claims for fraud ring members
        print("Generating HIGH-RISK claims for fraud ring members...")
        for ring in self.fraud_rings:
            for member in ring['members']:
                num_claims = random.randint(2, 5)  # Multiple claims = suspicious
                
                for _ in range(num_claims):
                    claim = self._generate_auto_claim(
                        member, 
                        claim_counter, 
                        risk_profile='HIGH',
                        ring=ring
                    )
                    self.claims.append(claim)
                    claim_counter += 1
        
        print(f"   ✓ Generated {len(self.claims)} high-risk claims")
        
        # NORMAL claims for non-ring members
        print("Generating NORMAL claims for other claimants...")
        ring_member_ids = set()
        for ring in self.fraud_rings:
            for member in ring['members']:
                ring_member_ids.add(member['claimant_id'])
        
        normal_claimants = [c for c in self.claimants if c['claimant_id'] not in ring_member_ids]
        
        for claimant in normal_claimants:
            if random.random() > 0.3:  # 70% have claims
                num_claims = random.randint(1, 2)
                risk_profile = random.choice(['LOW', 'LOW', 'MEDIUM', 'HIGH'])
                
                for _ in range(num_claims):
                    claim = self._generate_auto_claim(
                        claimant,
                        claim_counter,
                        risk_profile=risk_profile,
                        ring=None
                    )
                    self.claims.append(claim)
                    claim_counter += 1
        
        print(f"   ✓ Generated {len(self.claims)} total claims")
    
    def _generate_auto_claim(
        self, 
        claimant: Dict, 
        claim_id: int, 
        risk_profile: str = 'MEDIUM',
        ring: Dict = None
    ) -> Dict:
        """Generate individual auto insurance claim"""
        
        # Dates
        accident_date = datetime.now() - timedelta(days=random.randint(1, 365))
        
        if risk_profile == 'HIGH':
            days_to_report = random.choice([0, 1, 35, 45, 60])  # Too quick or too delayed
        else:
            days_to_report = random.randint(1, 14)
        
        report_date = accident_date + timedelta(days=days_to_report)
        
        # Claim amount
        if risk_profile == 'HIGH':
            property_damage = random.uniform(8000, 35000)
            bodily_injury = random.uniform(15000, 85000)
        elif risk_profile == 'MEDIUM':
            property_damage = random.uniform(3000, 12000)
            bodily_injury = random.uniform(5000, 25000)
        else:
            property_damage = random.uniform(500, 6000)
            bodily_injury = random.uniform(0, 10000) if random.random() > 0.5 else 0
        
        total_claim_amount = property_damage + bodily_injury
        
        # Risk score
        if risk_profile == 'HIGH':
            risk_score = random.uniform(70, 95)
        elif risk_profile == 'MEDIUM':
            risk_score = random.uniform(40, 69)
        else:
            risk_score = random.uniform(10, 39)
        
        # Vehicle
        vehicle = random.choice(self.vehicles)
        
        # Accident type
        accident_type = random.choice(ACCIDENT_TYPES)
        
        # Injury
        injury_type = random.choice(INJURY_TYPES) if bodily_injury > 0 else "No Injury"
        
        claim = {
            'claim_id': f"CLM_{claim_id:05d}",
            'claim_number': f"AUTO-{random.randint(100000, 999999)}",
            'claimant_id': claimant['claimant_id'],
            'vehicle_id': vehicle['vehicle_id'],
            'accident_date': accident_date.strftime('%Y-%m-%d'),
            'report_date': report_date.strftime('%Y-%m-%d'),
            'accident_type': accident_type,
            'injury_type': injury_type,
            'property_damage_amount': round(property_damage, 2),
            'bodily_injury_amount': round(bodily_injury, 2),
            'total_claim_amount': round(total_claim_amount, 2),
            'status': random.choice(['Open', 'Under Investigation', 'Closed', 'Pending Payment']),
            'description': f"{accident_type} resulting in {injury_type}",
            'risk_score': round(risk_score, 2),
            'ring_id': ring['ring_id'] if ring else None
        }
        
        # Add ring-specific attributes
        if ring:
            if ring['pattern_type'] == 'staged_accident':
                claim['location_id'] = ring['shared_location']['location_id']
                claim['witness_ids'] = [w['witness_id'] for w in ring['shared_witnesses']]
                claim['body_shop_id'] = ring['shared_body_shop']['body_shop_id']
            
            elif ring['pattern_type'] == 'body_shop_fraud':
                claim['body_shop_id'] = ring['body_shop']['body_shop_id']
                claim['attorney_id'] = ring['attorney']['attorney_id']
            
            elif ring['pattern_type'] == 'medical_mill':
                claim['medical_provider_id'] = ring['medical_provider']['provider_id']
                claim['attorney_id'] = ring['attorney']['attorney_id']
            
            elif ring['pattern_type'] == 'attorney_organized':
                claim['attorney_id'] = ring['attorney']['attorney_id']
                claim['body_shop_id'] = ring['body_shop']['body_shop_id']
                claim['medical_provider_id'] = ring['medical_provider']['provider_id']
            
            elif ring['pattern_type'] == 'tow_truck_kickback':
                claim['tow_company_id'] = ring['tow_company']['tow_company_id']
                claim['body_shop_id'] = ring['body_shop']['body_shop_id']
        else:
            # Random assignments for normal claims
            if random.random() > 0.3:
                claim['body_shop_id'] = random.choice(self.body_shops)['body_shop_id']
            
            if bodily_injury > 0 and random.random() > 0.4:
                claim['medical_provider_id'] = random.choice(self.medical_providers)['provider_id']
            
            if total_claim_amount > 15000 and random.random() > 0.5:
                claim['attorney_id'] = random.choice(self.attorneys)['attorney_id']
            
            if accident_type in ['Single Vehicle Accident', 'Hit and Run'] and random.random() > 0.6:
                claim['tow_company_id'] = random.choice(self.tow_companies)['tow_company_id']
            
            claim['location_id'] = random.choice(self.accident_locations)['location_id']
        
        return claim
    
    def _load_to_neo4j(self):
        """Load all data into Neo4j"""
        
        # Load claimants
        print(f"Loading {len(self.claimants)} claimants...")
        for claimant in self.claimants:
            query = """
            CREATE (c:Claimant {
                claimant_id: $claimant_id,
                name: $name,
                email: $email,
                phone: $phone,
                date_of_birth: date($date_of_birth),
                drivers_license: $drivers_license,
                created_at: datetime()
            })
            """
            self.driver.execute_write(query, claimant)
        print(f"   ✓ Loaded {len(self.claimants)} claimants")
        
        # Load vehicles
        print(f"Loading {len(self.vehicles)} vehicles...")
        for vehicle in self.vehicles:
            query = """
            CREATE (v:Vehicle {
                vehicle_id: $vehicle_id,
                vin: $vin,
                make: $make,
                model: $model,
                year: $year,
                color: $color,
                license_plate: $license_plate,
                created_at: datetime()
            })
            """
            self.driver.execute_write(query, vehicle)
        print(f"   ✓ Loaded {len(self.vehicles)} vehicles")
        
        # Load body shops
        print(f"Loading {len(self.body_shops)} body shops...")
        for shop in self.body_shops:
            query = """
            CREATE (b:BodyShop {
                body_shop_id: $body_shop_id,
                name: $name,
                license_number: $license_number,
                street: $street,
                city: $city,
                state: $state,
                zip_code: $zip_code,
                phone: $phone,
                created_at: datetime()
            })
            """
            self.driver.execute_write(query, shop)
        print(f"   ✓ Loaded {len(self.body_shops)} body shops")
        
        # Load medical providers
        print(f"Loading {len(self.medical_providers)} medical providers...")
        for provider in self.medical_providers:
            query = """
            CREATE (m:MedicalProvider {
                provider_id: $provider_id,
                name: $name,
                provider_type: $provider_type,
                license_number: $license_number,
                street: $street,
                city: $city,
                state: $state,
                zip_code: $zip_code,
                phone: $phone,
                created_at: datetime()
            })
            """
            self.driver.execute_write(query, provider)
        print(f"   ✓ Loaded {len(self.medical_providers)} medical providers")
        
        # Load attorneys
        print(f"Loading {len(self.attorneys)} attorneys...")
        for attorney in self.attorneys:
            query = """
            CREATE (a:Attorney {
                attorney_id: $attorney_id,
                name: $name,
                firm: $firm,
                bar_number: $bar_number,
                street: $street,
                city: $city,
                state: $state,
                zip_code: $zip_code,
                phone: $phone,
                email: $email,
                created_at: datetime()
            })
            """
            self.driver.execute_write(query, attorney)
        print(f"   ✓ Loaded {len(self.attorneys)} attorneys")
        
        # Load tow companies
        print(f"Loading {len(self.tow_companies)} tow companies...")
        for tow in self.tow_companies:
            query = """
            CREATE (t:TowCompany {
                tow_company_id: $tow_company_id,
                name: $name,
                license_number: $license_number,
                city: $city,
                state: $state,
                phone: $phone,
                created_at: datetime()
            })
            """
            self.driver.execute_write(query, tow)
        print(f"   ✓ Loaded {len(self.tow_companies)} tow companies")
        
        # Load accident locations
        print(f"Loading {len(self.accident_locations)} accident locations...")
        for location in self.accident_locations:
            query = """
            CREATE (l:AccidentLocation {
                location_id: $location_id,
                intersection: $intersection,
                city: $city,
                state: $state,
                latitude: $latitude,
                longitude: $longitude,
                created_at: datetime()
            })
            """
            self.driver.execute_write(query, location)
        print(f"   ✓ Loaded {len(self.accident_locations)} accident locations")
        
        # Load witnesses
        print(f"Loading {len(self.witnesses)} witnesses...")
        for witness in self.witnesses:
            query = """
            CREATE (w:Witness {
                witness_id: $witness_id,
                name: $name,
                phone: $phone,
                created_at: datetime()
            })
            """
            self.driver.execute_write(query, witness)
        print(f"   ✓ Loaded {len(self.witnesses)} witnesses")
        
        # Load claims with relationships
        print(f"Loading {len(self.claims)} claims...")
        for claim in self.claims:
            # Create claim
            query = """
            MATCH (c:Claimant {claimant_id: $claimant_id})
            MATCH (v:Vehicle {vehicle_id: $vehicle_id})
            CREATE (cl:Claim {
                claim_id: $claim_id,
                claim_number: $claim_number,
                accident_date: date($accident_date),
                report_date: date($report_date),
                accident_type: $accident_type,
                injury_type: $injury_type,
                property_damage_amount: $property_damage_amount,
                bodily_injury_amount: $bodily_injury_amount,
                total_claim_amount: $total_claim_amount,
                status: $status,
                description: $description,
                risk_score: $risk_score,
                created_at: datetime()
            })
            CREATE (c)-[:FILED]->(cl)
            CREATE (cl)-[:INVOLVES_VEHICLE]->(v)
            """
            
            self.driver.execute_write(query, {
                'claim_id': claim['claim_id'],
                'claim_number': claim['claim_number'],
                'claimant_id': claim['claimant_id'],
                'vehicle_id': claim['vehicle_id'],
                'accident_date': claim['accident_date'],
                'report_date': claim['report_date'],
                'accident_type': claim['accident_type'],
                'injury_type': claim['injury_type'],
                'property_damage_amount': claim['property_damage_amount'],
                'bodily_injury_amount': claim['bodily_injury_amount'],
                'total_claim_amount': claim['total_claim_amount'],
                'status': claim['status'],
                'description': claim['description'],
                'risk_score': claim['risk_score']
            })
            
            # Link to location
            if claim.get('location_id'):
                query = """
                MATCH (cl:Claim {claim_id: $claim_id})
                MATCH (l:AccidentLocation {location_id: $location_id})
                MERGE (cl)-[:OCCURRED_AT]->(l)
                """
                self.driver.execute_write(query, {
                    'claim_id': claim['claim_id'],
                    'location_id': claim['location_id']
                })
            
            # Link to body shop
            if claim.get('body_shop_id'):
                query = """
                MATCH (cl:Claim {claim_id: $claim_id})
                MATCH (b:BodyShop {body_shop_id: $body_shop_id})
                MERGE (cl)-[:REPAIRED_AT]->(b)
                """
                self.driver.execute_write(query, {
                    'claim_id': claim['claim_id'],
                    'body_shop_id': claim['body_shop_id']
                })
            
            # Link to medical provider
            if claim.get('medical_provider_id'):
                query = """
                MATCH (cl:Claim {claim_id: $claim_id})
                MATCH (m:MedicalProvider {provider_id: $provider_id})
                MERGE (cl)-[:TREATED_BY]->(m)
                """
                self.driver.execute_write(query, {
                    'claim_id': claim['claim_id'],
                    'provider_id': claim['medical_provider_id']
                })
            
            # Link to attorney
            if claim.get('attorney_id'):
                query = """
                MATCH (cl:Claim {claim_id: $claim_id})
                MATCH (a:Attorney {attorney_id: $attorney_id})
                MERGE (cl)-[:REPRESENTED_BY]->(a)
                """
                self.driver.execute_write(query, {
                    'claim_id': claim['claim_id'],
                    'attorney_id': claim['attorney_id']
                })
            
            # Link to tow company
            if claim.get('tow_company_id'):
                query = """
                MATCH (cl:Claim {claim_id: $claim_id})
                MATCH (t:TowCompany {tow_company_id: $tow_company_id})
                MERGE (cl)-[:TOWED_BY]->(t)
                """
                self.driver.execute_write(query, {
                    'claim_id': claim['claim_id'],
                    'tow_company_id': claim['tow_company_id']
                })
            
            # Link to witnesses
            if claim.get('witness_ids'):
                for witness_id in claim['witness_ids']:
                    query = """
                    MATCH (cl:Claim {claim_id: $claim_id})
                    MATCH (w:Witness {witness_id: $witness_id})
                    MERGE (w)-[:WITNESSED]->(cl)
                    """
                    self.driver.execute_write(query, {
                        'claim_id': claim['claim_id'],
                        'witness_id': witness_id
                    })
        
        print(f"   ✓ Loaded {len(self.claims)} claims with relationships")
        
        # Create fraud rings
        print(f"Creating {len(self.fraud_rings)} fraud rings...")
        for ring in self.fraud_rings:
            # Create ring node
            query = """
            CREATE (r:FraudRing {
                ring_id: $ring_id,
                ring_type: $ring_type,
                pattern_type: $pattern_type,
                status: $status,
                confidence_score: $confidence_score,
                member_count: $member_count,
                estimated_fraud_amount: 0,
                discovered_date: datetime(),
                discovered_by: 'AUTO_DETECTION_SYSTEM'
            })
            """
            self.driver.execute_write(query, {
                'ring_id': ring['ring_id'],
                'ring_type': ring['ring_type'],
                'pattern_type': ring['pattern_type'],
                'status': ring['status'],
                'confidence_score': ring['confidence_score'],
                'member_count': ring['member_count']
            })
            
            # Link members to ring
            for member in ring['members']:
                query = """
                MATCH (c:Claimant {claimant_id: $claimant_id})
                MATCH (r:FraudRing {ring_id: $ring_id})
                MERGE (c)-[:MEMBER_OF]->(r)
                """
                self.driver.execute_write(query, {
                    'claimant_id': member['claimant_id'],
                    'ring_id': ring['ring_id']
                })
        
        print(f"   ✓ Created {len(self.fraud_rings)} fraud rings")


def load_sample_data(num_claimants: int = 150, num_fraud_rings: int = 15):
    """
    Load comprehensive auto insurance sample data
    
    Args:
        num_claimants: Number of claimants (default: 150)
        num_fraud_rings: Number of fraud rings (default: 15)
    """
    
    try:
        print("Connecting to Neo4j...")
        driver = get_neo4j_driver()
        
        if not driver.test_connection():
            print("   ✗ Could not connect to database")
            return False
        
        print("   ✓ Connected successfully\n")
        
        # Generate and load data
        generator = AutoInsuranceFraudDataGenerator(driver)
        generator.generate_all_data(num_claimants, num_fraud_rings)
        
        # Verify statistics
        print("\nFINAL DATABASE STATISTICS:")
        print("-" * 80)
        stats = driver.get_statistics()
        print(f"Claimants:           {stats.get('claimants', 0)}")
        print(f"Claims:              {stats.get('claims', 0)}")
        print(f"Vehicles:            {driver.get_node_count('Vehicle')}")
        print(f"Body Shops:          {driver.get_node_count('BodyShop')}")
        print(f"Medical Providers:   {driver.get_node_count('MedicalProvider')}")
        print(f"Attorneys:           {stats.get('attorneys', 0)}")
        print(f"Tow Companies:       {driver.get_node_count('TowCompany')}")
        print(f"Accident Locations:  {driver.get_node_count('AccidentLocation')}")
        print(f"Witnesses:           {driver.get_node_count('Witness')}")
        print(f"Fraud Rings:         {stats.get('fraud_rings', 0)}")
        print(f"Total Relationships: {stats.get('total_relationships', 0)}")
        
        return True
        
    except Exception as e:
        print(f"\n✗ Failed to load sample data: {str(e)}")
        logger.error(f"Failed to load sample data: {e}", exc_info=True)
        return False


def main():
    """Main function"""
    
    import argparse
    
    parser = argparse.ArgumentParser(description="Load auto insurance fraud detection data")
    parser.add_argument(
        '--claimants',
        type=int,
        default=150,
        help='Number of claimants to generate (default: 150)'
    )
    parser.add_argument(
        '--rings',
        type=int,
        default=15,
        help='Number of fraud rings to create (default: 15)'
    )
    
    args = parser.parse_args()
    
    success = load_sample_data(
        num_claimants=args.claimants,
        num_fraud_rings=args.rings
    )
    
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
