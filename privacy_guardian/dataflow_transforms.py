"""
Custom Apache Beam transforms for Privacy Guardian Dataflow pipeline
"""

from privacy_guardian.dataflow_config import BATCH_SIZE, DLP_PROJECT_ID
from privacy_guardian.anonymizer import Anonymizer
import apache_beam as beam
import pandas as pd
import logging
import json
import uuid
from datetime import datetime
from typing import List, Dict, Any, Tuple
from google.cloud import dlp_v2
import csv
import io
import sys
import os

# Add the parent directory to sys.path to make the package importable
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


class ParseCSVFn(beam.DoFn):
    """DoFn to parse CSV lines into dictionaries"""

    def __init__(self, headers=None):
        self.headers = headers

    def process(self, line, headers=None):
        """Parse a CSV line into dictionary with all values as strings"""
        try:
            # Use headers from side input if provided, otherwise use instance headers
            if headers:
                header_list = headers
            elif self.headers:
                header_list = self.headers
            else:
                # Use the actual headers from sample_data.csv
                header_list = [
                    'patient_id', 'first_name', 'last_name', 'date_of_birth', 'age',
                    'gender', 'aadhaar_number', 'pan_number', 'passport_number',
                    'passport_expiry', 'drivers_license', 'visa_type', 'email',
                    'phone_number', 'emergency_contact', 'address', 'pincode',
                    'credit_card_number', 'credit_card_provider', 'bank_account_number',
                    'ifsc_code', 'swift_code', 'upi_id', 'medical_record_number',
                    'diagnosis', 'medication_list', 'blood_type', 'hospital_name',
                    'insurance_provider', 'policy_number', 'last_visit_date',
                    'department', 'treatment_status', 'height_cm', 'weight_kg',
                    'bmi', 'blood_pressure', 'visit_count', 'appointment_duration_mins',
                    'preferred_language', 'occupation', 'registration_date',
                    'last_payment_amount', 'consultation_fee', 'follow_up_required',
                    'next_appointment_due', 'regular_checkup'
                ]

            # Use CSV reader to handle quoted fields properly
            reader = csv.reader([line])
            row = next(reader)

            # Create dictionary from row with all values as strings
            record = {}
            for i, value in enumerate(row):
                if i < len(header_list):
                    record[header_list[i]] = value.strip()

            # Add processing metadata
            record['processing_timestamp'] = datetime.utcnow().isoformat()
            record['batch_id'] = str(uuid.uuid4())
            record['anonymization_timestamp'] = None
            record['anonymization_method'] = None
            record['sensitive_fields_detected'] = '0'
            record['sensitive_field_names'] = None

            yield record

        except Exception as e:
            logging.error(f"Error parsing CSV line: {line}, Error: {e}")
            # Skip malformed lines
            pass


class AnonymizeBatchDoFn(beam.DoFn):
    """DoFn to anonymize batches of records using DLP"""

    def __init__(self, batch_size=BATCH_SIZE):
        self.batch_size = batch_size
        self.anonymizer = None

    def setup(self):
        """Initialize the anonymizer"""
        try:
            self.anonymizer = Anonymizer()
            logging.info("AnonymizeBatchDoFn setup completed")
        except Exception as e:
            logging.error(f"Error setting up anonymizer: {e}")
            raise

    def process(self, batch):
        """Process a batch of records"""
        try:
            if not self.anonymizer:
                raise ValueError("Anonymizer not initialized")

            # Convert batch to DataFrame
            df = pd.DataFrame(batch)

            # Anonymize the DataFrame
            anonymized_df, sensitive_count, sensitive_fields = self.anonymizer.anonymize_dataframe(
                df)

            # Convert back to records
            for _, row in anonymized_df.iterrows():
                record = row.to_dict()

                # Add anonymization metadata
                record['sensitive_fields_detected'] = str(sensitive_count)
                record['sensitive_field_names'] = ','.join(
                    sensitive_fields) if sensitive_fields else None
                record['anonymization_timestamp'] = datetime.utcnow().isoformat()
                record['anonymization_method'] = 'dlp'

                yield record

        except Exception as e:
            logging.error(f"Error in anonymization: {e}")
            # Return original batch with error info
            for record in batch:
                record['anonymization_error'] = str(e)
                record['anonymization_timestamp'] = datetime.utcnow().isoformat()
                record['anonymization_method'] = 'error'
                record['sensitive_fields_detected'] = '0'
                record['sensitive_field_names'] = None
                yield record


class GroupIntoBatches(beam.DoFn):
    """DoFn to group records into batches"""

    def __init__(self, batch_size=BATCH_SIZE):
        self.batch_size = batch_size
        self.buffer = []

    def process(self, element):
        """Add element to buffer and yield when batch is full"""
        self.buffer.append(element)

        if len(self.buffer) >= self.batch_size:
            yield self.buffer
            self.buffer = []

    def finish_bundle(self):
        """Yield remaining elements in buffer"""
        if self.buffer:
            yield self.buffer
            self.buffer = []


class AnonymizeDataTransform(beam.PTransform):
    """Transform to anonymize data in batches"""

    def __init__(self, batch_size=BATCH_SIZE):
        self.batch_size = batch_size

    def expand(self, pcoll):
        """Apply the anonymization transform"""
        return (
            pcoll
            | 'GroupIntoBatches' >> beam.ParDo(GroupIntoBatches(self.batch_size))
            | 'AnonymizeBatches' >> beam.ParDo(AnonymizeBatchDoFn(self.batch_size))
        )
