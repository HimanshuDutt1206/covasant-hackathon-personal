"""
Generate sample healthcare data with Indian and global identifiers
"""

import pandas as pd
from faker import Faker
import random
from datetime import datetime, timedelta
import string


def generate_indian_aadhaar():
    """Generate a random 12-digit Aadhaar number"""
    return ''.join(random.choices(string.digits, k=12))


def generate_indian_pan():
    """Generate a random PAN card number"""
    letters = string.ascii_uppercase
    return f"{''.join(random.choices(letters, k=5))}{random.randint(1000, 9999)}{random.choice(letters)}"


def generate_indian_passport():
    """Generate Indian passport number (Format: A/B/C/D/E/F/G/H/J/K/L/M/N/P/R + 7 digits)"""
    prefix = random.choice(
        ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'J', 'K', 'L', 'M', 'N', 'P', 'R'])
    return f"{prefix}{random.randint(1000000, 9999999)}"


def generate_indian_dl():
    """Generate Indian driving license number (Format: SS-RR-YYYYXXNNNN)"""
    states = ['MH', 'DL', 'KA', 'TN', 'AP', 'GJ', 'WB', 'UP', 'MP', 'KL']
    state = random.choice(states)
    rto = str(random.randint(1, 99)).zfill(2)
    year = str(random.randint(2000, 2023))[2:]
    return f"{state}{rto}{year}{''.join(random.choices(string.digits, k=7))}"


def generate_indian_phone():
    """Generate an Indian phone number"""
    return f"+91 {random.choice(['6', '7', '8', '9'])}{random.randint(100000000, 999999999)}"


def generate_fake_data(num_records=100):
    # Set English locale for names and text
    fake = Faker(['en_IN'])
    Faker.seed(0)  # For reproducibility
    data = []

    # Indian-specific lists
    indian_cities = [
        "Mumbai", "Delhi", "Bangalore", "Hyderabad", "Chennai", "Kolkata",
        "Pune", "Ahmedabad", "Jaipur", "Lucknow", "Kochi", "Chandigarh"
    ]

    indian_states = [
        "Maharashtra", "Delhi", "Karnataka", "Telangana", "Tamil Nadu",
        "West Bengal", "Gujarat", "Rajasthan", "Uttar Pradesh", "Kerala",
        "Punjab", "Haryana"
    ]

    diagnoses = [
        "Type 2 Diabetes", "Hypertension", "Coronary Artery Disease",
        "Tuberculosis", "Dengue Fever", "Malaria", "Typhoid Fever",
        "Upper Respiratory Tract Infection", "Acute Gastroenteritis",
        "Iron Deficiency Anemia", "Hypothyroidism", "Bronchial Asthma"
    ]

    departments = [
        "General Medicine", "Cardiology", "Endocrinology", "Pulmonology",
        "Pediatrics", "Orthopedics", "Gynecology", "Neurology",
        "Ayurvedic Medicine", "Dental Care", "Ophthalmology", "ENT"
    ]

    insurance_providers = [
        "LIC Health Insurance", "Star Health Insurance", "HDFC ERGO Health",
        "ICICI Lombard Health", "Bajaj Allianz Health", "Max Bupa Health",
        "National Insurance Health", "New India Assurance Health"
    ]

    blood_types = ["A+", "A-", "B+", "B-", "AB+", "AB-", "O+", "O-"]

    indian_occupations = [
        "Software Engineer", "Medical Doctor", "School Teacher", "Business Analyst",
        "Bank Manager", "Government Officer", "Retail Store Owner",
        "Chartered Accountant", "Corporate Lawyer", "University Professor",
        "Civil Services Officer", "Agricultural Farmer", "Business Owner"
    ]

    medications = [
        "Metformin 500mg", "Amlodipine 5mg", "Telmisartan 40mg", "Aspirin 75mg",
        "Levothyroxine 25mcg", "Pantoprazole 40mg", "Paracetamol 500mg",
        "Azithromycin 500mg", "Montelukast 10mg", "Insulin Glargine"
    ]

    hospital_names = [
        "Apollo Hospitals", "Fortis Healthcare Center", "Max Super Specialty Hospital",
        "Manipal Hospitals", "AIIMS Hospital", "Medanta The Medicity",
        "Narayana Health City", "Lilavati Hospital", "Kokilaben Hospital",
        "Ruby Hall Clinic"
    ]

    languages = [
        "English", "Hindi", "Both English and Hindi",
        "English and Regional Language", "Hindi and Regional Language",
        "English, Hindi and Regional Language"
    ]

    visa_types = [
        "Tourist Visa", "Business Visa", "Student Visa", "Work Visa",
        "Medical Visa", "Conference Visa", "Employment Visa"
    ]

    for i in range(num_records):
        gender = random.choice(['Male', 'Female'])
        dob = fake.date_of_birth(minimum_age=18, maximum_age=90)
        state = random.choice(indian_states)
        city = random.choice(indian_cities)

        # Generate sensitive data
        record = {
            # PII (Personally Identifiable Information) - Both Indian and Global
            'patient_id': f'INPH{i+1:04d}',
            'first_name': fake.first_name(),
            'last_name': fake.last_name(),
            'date_of_birth': dob.strftime('%Y-%m-%d'),
            'age': (datetime.now().date() - dob).days // 365,
            'gender': gender,

            # Indian-specific identifiers
            'aadhaar_number': generate_indian_aadhaar(),
            'pan_number': generate_indian_pan(),

            # Global identifiers (Indian format)
            'passport_number': generate_indian_passport(),
            'passport_expiry': (datetime.now() + timedelta(days=random.randint(1, 3650))).strftime('%Y-%m-%d'),
            'drivers_license': generate_indian_dl(),
            'visa_type': random.choice(visa_types),

            # Contact Information
            'email': fake.email(),
            'phone_number': generate_indian_phone(),
            'emergency_contact': generate_indian_phone(),
            'address': f"{fake.building_number()}, {fake.street_name()}, {city}, {state}, India - {fake.postcode()}",
            'pincode': fake.postcode(),

            # PCI (Payment Card Information) - Both Indian and Global
            'credit_card_number': fake.credit_card_number(),
            'credit_card_provider': fake.credit_card_provider(),
            'bank_account_number': fake.bban(),
            'ifsc_code': f"{''.join(random.choices(string.ascii_uppercase, k=4))}{random.randint(100000, 999999)}",
            'swift_code': fake.swift(),
            'upi_id': f"{fake.user_name()}@{random.choice(['upi', 'paytm', 'gpay', 'phonepe'])}",

            # PHI (Protected Health Information)
            'medical_record_number': f'MRN{i+1:06d}',
            'diagnosis': random.choice(diagnoses),
            'medication_list': ', '.join(random.sample(medications, random.randint(1, 3))),
            'blood_type': random.choice(blood_types),
            'hospital_name': random.choice(hospital_names),
            'insurance_provider': random.choice(insurance_providers),
            'policy_number': f"{''.join(random.choices(string.ascii_uppercase + string.digits, k=10))}",
            'last_visit_date': (datetime.now() - timedelta(days=random.randint(1, 365))).strftime('%Y-%m-%d'),

            # Non-sensitive data
            'department': random.choice(departments),
            'treatment_status': random.choice(['Active', 'Completed', 'On Hold', 'Scheduled for Review']),
            'height_cm': random.randint(150, 190),
            'weight_kg': random.randint(45, 100),
            'bmi': round(random.uniform(18.5, 35.0), 1),
            'blood_pressure': f"{random.randint(100, 160)}/{random.randint(60, 100)} mmHg",
            'visit_count': random.randint(1, 20),
            'appointment_duration_mins': random.choice([15, 30, 45, 60]),
            'preferred_language': random.choice(languages),
            'occupation': random.choice(indian_occupations),
            'registration_date': (datetime.now() - timedelta(days=random.randint(365, 1825))).strftime('%Y-%m-%d'),
            'last_payment_amount': f"₹{round(random.uniform(500, 50000), 2)}",
            'consultation_fee': f"₹{random.choice([500, 750, 1000, 1500, 2000])}",
            'follow_up_required': random.choice(['Yes', 'No']),
            'next_appointment_due': random.choice(['Yes', 'No']),
            'regular_checkup': random.choice(['Monthly', 'Quarterly', 'Half-yearly', 'Yearly'])
        }
        data.append(record)

    df = pd.DataFrame(data)
    return df


if __name__ == "__main__":
    # Generate 10 records for demonstration
    df = generate_fake_data(num_records=10)
    df.to_csv("sample_data.csv", index=False)
    print("Generated new sample_data.csv with 10 records containing both Indian and global identifiers.")
    print("\nData includes:")
    print("Indian IDs: Aadhaar, PAN")
    print("Global IDs (Indian format): Passport, Driver's License, Visa")
    print("PII: Names, contact details")
    print("PHI: Medical records, diagnosis, medications")
    print("PCI: Bank details, credit cards, UPI")
    print("Non-sensitive: Department, appointment details, etc.")
