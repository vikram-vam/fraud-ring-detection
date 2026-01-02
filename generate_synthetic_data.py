"""
AUTO INSURANCE FRAUD SYNTHETIC DATA GENERATOR
Generates realistic auto insurance data with embedded fraud rings
"""

import random
import pandas as pd
import numpy as np
from faker import Faker
from datetime import datetime, timedelta
import json

# Initialize Faker
fake = Faker()
Faker.seed(42)
random.seed(42)
np.random.seed(42)

# Configuration
CONFIG = {
    'num_legitimate_claimants': 800,
    'num_fraud_rings': 8,
    'claimants_per_ring': [12, 8, 15, 10, 9, 13, 7, 11],
    'num_repair_shops': 50,
    'num_medical_providers': 40,
    'num_lawyers': 30,
    'fraud_repair_shops': 5,  # shops involved in fraud
    'fraud_medical_providers': 4,
    'fraud_lawyers': 3,
    'claim_probability': 0.15,  # 15% of policyholders file claims
    'fraud_claim_probability': 0.8  # 80% of fraud ring members file claims
}

# Claim types and their typical amounts
CLAIM_TYPES = {
    'Collision': (2000, 15000),
    'Comprehensive': (1000, 8000),
    'Liability': (5000, 25000),
    'Personal Injury Protection': (3000, 50000),
    'Uninsured Motorist': (4000, 30000)
}

# Vehicle makes and models
VEHICLES = {
    'Toyota': ['Camry', 'Corolla', 'RAV4', 'Highlander'],
    'Honda': ['Accord', 'Civic', 'CR-V', 'Pilot'],
    'Ford': ['F-150', 'Escape', 'Explorer', 'Mustang'],
    'Chevrolet': ['Silverado', 'Equinox', 'Malibu', 'Tahoe'],
    'Nissan': ['Altima', 'Rogue', 'Sentra', 'Pathfinder']
}

def generate_vin():
    """Generate realistic VIN"""
    return ''.join(random.choices('ABCDEFGHJKLMNPRSTUVWXYZ0123456789', k=17))

def generate_claimants(num_claimants, is_fraud_ring=False, ring_id=None):
    """Generate claimant data"""
    claimants = []
    
    # For fraud rings, create shared addresses/phones
    if is_fraud_ring:
        shared_addresses = [fake.address() for _ in range(max(1, num_claimants // 3))]
        shared_phones = [fake.phone_number() for _ in range(max(1, num_claimants // 4))]
    
    for i in range(num_claimants):
        claimant_id = f"CLM_{ring_id}_{i}" if ring_id else f"CLM_LEG_{i}"
        
        # Fraud ring members may share addresses/phones
        if is_fraud_ring and random.random() < 0.4:
            address = random.choice(shared_addresses)
            phone = random.choice(shared_phones) if random.random() < 0.3 else fake.phone_number()
        else:
            address = fake.address()
            phone = fake.phone_number()
        
        claimant = {
            'claimant_id': claimant_id,
            'name': fake.name(),
            'ssn': fake.ssn(),
            'dob': fake.date_of_birth(minimum_age=18, maximum_age=80).isoformat(),
            'address': address,
            'phone': phone,
            'email': fake.email(),
            'license_number': f"DL{fake.random_number(digits=8)}",
            'is_fraud_ring': is_fraud_ring,
            'fraud_ring_id': ring_id
        }
        claimants.append(claimant)
    
    return claimants

def generate_relationships(claimants, ring_id):
    """Generate relationships within fraud ring"""
    relationships = []
    ring_members = [c for c in claimants if c['fraud_ring_id'] == ring_id]
    
    # Create family relationships
    num_relationships = len(ring_members) // 2
    for _ in range(num_relationships):
        if len(ring_members) >= 2:
            member1, member2 = random.sample(ring_members, 2)
            relationships.append({
                'from_claimant': member1['claimant_id'],
                'to_claimant': member2['claimant_id'],
                'relationship_type': random.choice(['family', 'associate', 'friend']),
                'ring_id': ring_id
            })
    
    return relationships

def generate_policies(claimants):
    """Generate insurance policies"""
    policies = []
    for claimant in claimants:
        num_policies = 1 if random.random() < 0.9 else 2
        for p in range(num_policies):
            start_date = datetime.now() - timedelta(days=random.randint(180, 1095))
            policy = {
                'policy_id': f"POL_{claimant['claimant_id']}_{p}",
                'policy_number': f"PN{fake.random_number(digits=10)}",
                'claimant_id': claimant['claimant_id'],
                'start_date': start_date.isoformat(),
                'end_date': (start_date + timedelta(days=365)).isoformat(),
                'premium': round(random.uniform(800, 2500), 2),
                'coverage_type': random.choice(['Full Coverage', 'Liability Only', 'Comprehensive'])
            }
            policies.append(policy)
    
    return policies

def generate_vehicles(policies):
    """Generate vehicles"""
    vehicles = []
    for policy in policies:
        make = random.choice(list(VEHICLES.keys()))
        model = random.choice(VEHICLES[make])
        year = random.randint(2010, 2024)
        
        vehicle = {
            'vehicle_id': f"VEH_{policy['policy_id']}",
            'vin': generate_vin(),
            'policy_id': policy['policy_id'],
            'make': make,
            'model': model,
            'year': year,
            'color': fake.color_name(),
            'license_plate': f"{fake.random_letter()}{fake.random_letter()}{fake.random_number(digits=4)}"
        }
        vehicles.append(vehicle)
    
    return vehicles

def generate_repair_shops(num_shops, num_fraud_shops):
    """Generate repair shops"""
    shops = []
    for i in range(num_shops):
        shop = {
            'shop_id': f"SHOP_{i}",
            'name': f"{fake.last_name()}'s Auto Repair" if i >= num_fraud_shops else f"{fake.company()} Auto Body",
            'address': fake.address(),
            'phone': fake.phone_number(),
            'license_number': f"SL{fake.random_number(digits=8)}",
            'is_fraud_involved': i < num_fraud_shops
        }
        shops.append(shop)
    
    return shops

def generate_medical_providers(num_providers, num_fraud_providers):
    """Generate medical providers"""
    providers = []
    specialties = ['Orthopedics', 'Neurology', 'Physical Therapy', 'Chiropractic', 'Pain Management']
    
    for i in range(num_providers):
        provider = {
            'provider_id': f"MED_{i}",
            'name': f"Dr. {fake.name()}",
            'specialty': random.choice(specialties),
            'address': fake.address(),
            'phone': fake.phone_number(),
            'npi_number': fake.random_number(digits=10),
            'is_fraud_involved': i < num_fraud_providers
        }
        providers.append(provider)
    
    return providers

def generate_lawyers(num_lawyers, num_fraud_lawyers):
    """Generate lawyers"""
    lawyers = []
    for i in range(num_lawyers):
        lawyer = {
            'lawyer_id': f"LAW_{i}",
            'name': fake.name(),
            'bar_number': f"BAR{fake.random_number(digits=7)}",
            'firm_name': f"{fake.last_name()} & Associates",
            'address': fake.address(),
            'phone': fake.phone_number(),
            'is_fraud_involved': i < num_fraud_lawyers
        }
        lawyers.append(lawyer)
    
    return lawyers

def generate_claims(claimants, policies, vehicles, shops, providers, lawyers):
    """Generate claims with fraud patterns"""
    claims = []
    claim_counter = 0
    
    # Get fraud-involved entities
    fraud_shops = [s['shop_id'] for s in shops if s['is_fraud_involved']]
    fraud_providers = [p['provider_id'] for p in providers if p['is_fraud_involved']]
    fraud_lawyers = [l['lawyer_id'] for l in lawyers if l['is_fraud_involved']]
    legitimate_shops = [s['shop_id'] for s in shops if not s['is_fraud_involved']]
    legitimate_providers = [p['provider_id'] for p in providers if not p['is_fraud_involved']]
    legitimate_lawyers = [l['lawyer_id'] for l in lawyers if not l['is_fraud_involved']]
    
    for claimant in claimants:
        # Get claimant's policies
        claimant_policies = [p for p in policies if p['claimant_id'] == claimant['claimant_id']]
        
        # Determine if claimant files claim
        if claimant['is_fraud_ring']:
            files_claim = random.random() < CONFIG['fraud_claim_probability']
        else:
            files_claim = random.random() < CONFIG['claim_probability']
        
        if files_claim and claimant_policies:
            policy = random.choice(claimant_policies)
            vehicle = next((v for v in vehicles if v['policy_id'] == policy['policy_id']), None)
            
            if vehicle:
                claim_type = random.choice(list(CLAIM_TYPES.keys()))
                min_amt, max_amt = CLAIM_TYPES[claim_type]
                
                # Fraud claims tend to be higher
                if claimant['is_fraud_ring']:
                    claim_amount = round(random.uniform(max_amt * 0.7, max_amt * 1.3), 2)
                else:
                    claim_amount = round(random.uniform(min_amt, max_amt), 2)
                
                incident_date = datetime.now() - timedelta(days=random.randint(1, 180))
                claim_date = incident_date + timedelta(days=random.randint(1, 7))
                
                # Fraud rings use specific shops/providers/lawyers based on their MO
                if claimant['is_fraud_ring']:
                    ring_id = claimant['fraud_ring_id']
                    
                    # RING_0 and RING_1: The "Body Shop" Rings (Target SHOP_0)
                    if ring_id in ['RING_0', 'RING_1']:
                        shop_id = 'SHOP_0' # Forces the 'Shared Repair Shop' pattern
                        # Random provider to avoid double-flagging unless necessary
                        provider_id = random.choice(fraud_providers) if random.random() < 0.5 else random.choice(legitimate_providers)
                        lawyer_id = random.choice(legitimate_lawyers) # Less lawyer involvement
                        
                    # RING_2 and RING_3: The "Medical Mills" (Target MED_0 - Dr. Roy Cook)
                    elif ring_id in ['RING_2', 'RING_3']:
                        shop_id = random.choice(legitimate_shops)
                        provider_id = 'MED_0' # Forces the 'Shared Medical Provider' pattern
                        lawyer_id = random.choice(fraud_lawyers) if random.random() < 0.7 else None
                        
                    # Other rings: General organized fraud (mix of bad actors)
                    else:
                        shop_id = random.choice(fraud_shops) if random.random() < 0.9 else random.choice(legitimate_shops)
                        provider_id = random.choice(fraud_providers) if random.random() < 0.8 else random.choice(legitimate_providers)
                        lawyer_id = random.choice(fraud_lawyers) if random.random() < 0.85 else random.choice(legitimate_lawyers)
                else:
                    shop_id = random.choice(legitimate_shops)
                    provider_id = random.choice(legitimate_providers) if random.random() < 0.3 else None
                    lawyer_id = random.choice(legitimate_lawyers) if random.random() < 0.2 else None
                
                claim = {
                    'claim_id': f"CLM_{claim_counter}",
                    'claim_number': f"CN{fake.random_number(digits=10)}",
                    'claimant_id': claimant['claimant_id'],
                    'policy_id': policy['policy_id'],
                    'vehicle_id': vehicle['vehicle_id'],
                    'claim_date': claim_date.isoformat(),
                    'incident_date': incident_date.isoformat(),
                    'claim_amount': claim_amount,
                    'claim_type': claim_type,
                    'status': random.choice(['Open', 'Under Investigation', 'Approved', 'Denied']),
                    'description': f"{claim_type} incident involving {vehicle['make']} {vehicle['model']}",
                    'location': fake.city(),
                    'weather_condition': random.choice(['Clear', 'Rain', 'Snow', 'Fog']),
                    'repair_shop_id': shop_id,
                    'medical_provider_id': provider_id,
                    'lawyer_id': lawyer_id,
                    'is_fraud_ring': claimant['is_fraud_ring'],
                    'fraud_ring_id': claimant['fraud_ring_id']
                }
                claims.append(claim)
                claim_counter += 1
    
    return claims

def generate_witnesses(claims):
    """Generate witnesses for claims"""
    witnesses = []
    witness_counter = 0
    
    # Some witnesses appear in multiple claims (fraud indicator)
    recurring_witnesses = []
    for _ in range(5):
        recurring_witnesses.append({
            'witness_id': f"WIT_{witness_counter}",
            'name': fake.name(),
            'phone': fake.phone_number(),
            'address': fake.address(),
            'is_recurring': True
        })
        witness_counter += 1
    
    claim_witnesses = []
    for claim in claims:
        # Fraud ring claims more likely to have recurring witnesses
        if claim['is_fraud_ring'] and random.random() < 0.4:
            witness = random.choice(recurring_witnesses)
        else:
            if random.random() < 0.3:  # 30% of claims have witnesses
                witness = {
                    'witness_id': f"WIT_{witness_counter}",
                    'name': fake.name(),
                    'phone': fake.phone_number(),
                    'address': fake.address(),
                    'is_recurring': False
                }
                witnesses.append(witness)
                witness_counter += 1
            else:
                continue
        
        claim_witnesses.append({
            'claim_id': claim['claim_id'],
            'witness_id': witness['witness_id'],
            'relationship': random.choice(['Passenger', 'Bystander', 'Other Driver', 'Unknown'])
        })
    
    witnesses.extend(recurring_witnesses)
    return witnesses, claim_witnesses

def main():
    """Main data generation function"""
    print("Generating synthetic auto insurance fraud data...")
    
    # Generate legitimate claimants
    print("Generating legitimate claimants...")
    claimants = generate_claimants(CONFIG['num_legitimate_claimants'], is_fraud_ring=False)
    
    # Generate fraud rings
    print("Generating fraud rings...")
    all_relationships = []
    for ring_idx, num_members in enumerate(CONFIG['claimants_per_ring']):
        ring_id = f"RING_{ring_idx}"
        ring_claimants = generate_claimants(num_members, is_fraud_ring=True, ring_id=ring_id)
        claimants.extend(ring_claimants)
        
        # Generate relationships within ring
        relationships = generate_relationships(claimants, ring_id)
        all_relationships.extend(relationships)
    
    print(f"Total claimants: {len(claimants)}")
    
    # Generate other entities
    print("Generating policies...")
    policies = generate_policies(claimants)
    
    print("Generating vehicles...")
    vehicles = generate_vehicles(policies)
    
    print("Generating repair shops...")
    repair_shops = generate_repair_shops(CONFIG['num_repair_shops'], CONFIG['fraud_repair_shops'])
    
    print("Generating medical providers...")
    medical_providers = generate_medical_providers(CONFIG['num_medical_providers'], CONFIG['fraud_medical_providers'])
    
    print("Generating lawyers...")
    lawyers = generate_lawyers(CONFIG['num_lawyers'], CONFIG['fraud_lawyers'])
    
    print("Generating claims...")
    claims = generate_claims(claimants, policies, vehicles, repair_shops, medical_providers, lawyers)
    
    print("Generating witnesses...")
    witnesses, claim_witnesses = generate_witnesses(claims)
    
    # Convert to DataFrames and save
    print("Saving data to CSV files...")
    pd.DataFrame(claimants).to_csv('data/claimants.csv', index=False)
    pd.DataFrame(policies).to_csv('data/policies.csv', index=False)
    pd.DataFrame(vehicles).to_csv('data/vehicles.csv', index=False)
    pd.DataFrame(repair_shops).to_csv('data/repair_shops.csv', index=False)
    pd.DataFrame(medical_providers).to_csv('data/medical_providers.csv', index=False)
    pd.DataFrame(lawyers).to_csv('data/lawyers.csv', index=False)
    pd.DataFrame(claims).to_csv('data/claims.csv', index=False)
    pd.DataFrame(witnesses).to_csv('data/witnesses.csv', index=False)
    pd.DataFrame(claim_witnesses).to_csv('data/claim_witnesses.csv', index=False)
    pd.DataFrame(all_relationships).to_csv('data/claimant_relationships.csv', index=False)
    
    # Generate summary statistics
    summary = {
        'total_claimants': len(claimants),
        'fraud_ring_claimants': len([c for c in claimants if c['is_fraud_ring']]),
        'total_claims': len(claims),
        'fraud_ring_claims': len([c for c in claims if c['is_fraud_ring']]),
        'total_policies': len(policies),
        'total_vehicles': len(vehicles),
        'num_fraud_rings': CONFIG['num_fraud_rings'],
        'repair_shops': len(repair_shops),
        'medical_providers': len(medical_providers),
        'lawyers': len(lawyers)
    }
    
    with open('data/summary.json', 'w') as f:
        json.dump(summary, f, indent=2)
    
    print("\n" + "="*50)
    print("Data Generation Complete!")
    print("="*50)
    print(f"Total Claimants: {summary['total_claimants']}")
    print(f"Fraud Ring Members: {summary['fraud_ring_claimants']}")
    print(f"Total Claims: {summary['total_claims']}")
    print(f"Fraud Ring Claims: {summary['fraud_ring_claims']}")
    print(f"Number of Fraud Rings: {summary['num_fraud_rings']}")
    print("="*50)

if __name__ == "__main__":
    import os
    os.makedirs('data', exist_ok=True)
    main()