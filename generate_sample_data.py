import pandas as pd
from faker import Faker
import random
from datetime import datetime, timedelta


def generate_fake_data(num_records=100):
    # Updated list of Faker locales for broader international data
    locales = [
        'en_US', 'en_GB', 'fr_FR', 'de_DE', 'es_ES', 'ja_JP', 'zh_CN', 'hi_IN', 'ar_SA', 'pt_BR', 'ru_RU'
    ]
    fake = Faker(locales)
    data = []

    # Lists for various fields
    diagnoses = [
        "Hypertension", "Diabetes Type 2", "Asthma", "Migraine", "Depression",
        "Common Cold", "Influenza", "Bronchitis", "Allergic Rhinitis", "Anxiety Disorder",
        "Osteoarthritis", "Rheumatoid Arthritis", "Coronary Artery Disease", "Congestive Heart Failure",
        "Chronic Kidney Disease", "Gastroesophageal Reflux Disease", "Irritable Bowel Syndrome",
        "Ulcerative Colitis", "Crohn's Disease", "Pneumonia", "Urinary Tract Infection",
        "Appendicitis", "Gallstones", "Thyroid Disorder", "Obesity"
    ]

    departments = [
        "Cardiology", "Endocrinology", "Pulmonology", "Neurology", "Psychiatry",
        "General Medicine", "Pediatrics", "Orthopedics", "Dermatology", "Oncology",
        "Gastroenterology", "Nephrology", "Urology", "Ophthalmology", "ENT"
    ]

    insurance_providers = [
        "BlueCross", "Aetna", "Cigna", "UnitedHealth", "Kaiser",
        "Humana", "MetLife", "Prudential", "Allianz", "AXA"
    ]

    blood_types = ["A+", "A-", "B+", "B-", "AB+", "AB-", "O+", "O-"]

    occupations = [
        "Engineer", "Doctor", "Teacher", "Nurse", "Accountant",
        "Artist", "Software Developer", "Data Scientist", "Marketing Manager",
        "Sales Representative", "Chef", "Electrician", "Plumber", "Police Officer",
        "Firefighter", "Journalist", "Lawyer", "Architect", "Pharmacist"
    ]

    # List of common medications (example)
    medications = [
        "Aspirin", "Ibuprofen", "Paracetamol", "Amoxicillin", "Lisinopril",
        "Metformin", "Albuterol", "Sumatriptan", "Sertraline", "Fluoxetine"
    ]

    # List of common hospital names (example)
    hospital_names = [
        "General Hospital", "St. Jude's Medical Center", "City Community Hospital",
        "University Hospital", "National Medical Center", "Regional Health Clinic"
    ]

    for i in range(num_records):
        gender = random.choice(['Male', 'Female', 'Other'])
        current_locale = random.choice(locales)
        fake_country = Faker(current_locale)

        record = {
            'patient_id': f'P{i+1:04d}',
            'first_name': fake_country.first_name(),
            'last_name': fake_country.last_name(),
            'date_of_birth': fake_country.date_of_birth(minimum_age=18, maximum_age=90).strftime('%Y-%m-%d'),
            # Generate locale-specific SSN/equivalent if available, otherwise a generic ID
            'ssn': fake_country.ssn() if hasattr(fake_country, 'ssn') else fake_country.unique.random_number(digits=9),
            # Generate locale-specific drivers license if available, otherwise a generic license plate
            'drivers_license_number': fake_country.license_plate() if hasattr(fake_country, 'license_plate') else fake_country.unique.bothify(text='??######'),
            # Generic passport format
            'passport_number': fake_country.bothify(text='??######').upper(),
            'email': fake_country.email(),
            'phone_number': fake_country.phone_number(),
            'address': fake_country.address().replace('\n', ', '),
            # Using country as nationality for simplicity
            'nationality': fake_country.country(),
            'country': fake_country.country(),
            'occupation': random.choice(occupations),
            'credit_card_number': fake_country.credit_card_number(card_type=None),
            'bank_account_number': fake_country.iban(),
            'swift_code': fake_country.swift(),
            'salary': round(random.uniform(30000, 150000), 2),
            'ip_address': fake_country.ipv4(),
            'medical_record_number': f'MR{i+1:03d}',
            'diagnosis': random.choice(diagnoses),
            # Multiple medications
            'medication_list': ', '.join(random.sample(medications, random.randint(1, 3))),
            'hospital_name': random.choice(hospital_names),
            'insurance_provider': random.choice(insurance_providers),
            'last_visit_date': (datetime.now() - timedelta(days=random.randint(1, 365))).strftime('%Y-%m-%d'),
            'department': random.choice(departments),
            'treatment_status': random.choice(['Active', 'Completed', 'On Hold', 'Pending']),
            'blood_type': random.choice(blood_types),
            'height_cm': random.randint(150, 190),
            'weight_kg': random.randint(50, 100),
            'visit_count': random.randint(1, 20),
            'appointment_duration': random.choice([15, 30, 45, 60]),
            'medication_count': random.randint(0, 5),
            'allergic_reactions': random.choice(['None', 'Pollen', 'Dust', 'Peanuts', 'Penicillin']),
            'emergency_contact_relation': random.choice(['Spouse', 'Parent', 'Sibling', 'Child', 'Friend']),
            'preferred_language': fake_country.language_code()  # Generated by locale
        }
        data.append(record)

    df = pd.DataFrame(data)
    return df


if __name__ == "__main__":
    # Generate 10 records for demonstration
    df = generate_fake_data(num_records=10)
    df.to_csv("sample_data.csv", index=False)
    print("Generated new sample_data.csv with 10 records.")
