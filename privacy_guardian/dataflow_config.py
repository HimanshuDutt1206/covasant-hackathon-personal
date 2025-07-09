"""
Configuration settings for Privacy Guardian Dataflow pipeline
"""

import os

# Project Configuration
PROJECT_ID = "covasant-hackathon"
REGION = "us-central1"

# Cloud Storage Configuration
BUCKET_NAME = "sample-data-covasant-hackathon"
INPUT_FILE_PATH = f"gs://{BUCKET_NAME}/sample_data.csv"
TEMP_LOCATION = f"gs://{BUCKET_NAME}/temp/"
STAGING_LOCATION = f"gs://{BUCKET_NAME}/staging/"

# BigQuery Configuration
DATASET_ID = "anonymized_sample_data"
TABLE_ID = "anonymized_sample_data"
BIGQUERY_TABLE = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

# Processing Configuration
BATCH_SIZE = 5
MAX_NUM_WORKERS = 10
MACHINE_TYPE = "n1-standard-2"

# DLP Configuration
DLP_PROJECT_ID = PROJECT_ID

# Dataflow Pipeline Options
DATAFLOW_OPTIONS = {
    'runner': 'DataflowRunner',
    'project': PROJECT_ID,
    'region': REGION,
    'temp_location': TEMP_LOCATION,
    'staging_location': STAGING_LOCATION,
    'setup_file': './setup.py',
    'max_num_workers': MAX_NUM_WORKERS,
    'machine_type': MACHINE_TYPE,
    'use_public_ips': False,
    'save_main_session': True,
}

# BigQuery Schema - All fields as STRING for consistency with DLP output
BIGQUERY_SCHEMA = [
    {'name': 'patient_id', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'first_name', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'last_name', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'date_of_birth', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'age', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'gender', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'aadhaar_number', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'pan_number', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'passport_number', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'passport_expiry', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'drivers_license', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'visa_type', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'email', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'phone_number', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'emergency_contact', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'address', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'pincode', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'credit_card_number', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'credit_card_provider', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'bank_account_number', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'ifsc_code', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'swift_code', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'upi_id', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'medical_record_number', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'diagnosis', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'medication_list', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'blood_type', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'hospital_name', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'insurance_provider', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'policy_number', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'last_visit_date', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'department', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'treatment_status', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'height_cm', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'weight_kg', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'bmi', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'blood_pressure', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'visit_count', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'appointment_duration_mins', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'preferred_language', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'occupation', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'registration_date', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'last_payment_amount', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'consultation_fee', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'follow_up_required', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'next_appointment_due', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'regular_checkup', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'processing_timestamp', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'batch_id', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'anonymization_timestamp', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'anonymization_method', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'sensitive_fields_detected', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'sensitive_field_names', 'type': 'STRING', 'mode': 'NULLABLE'}
]
