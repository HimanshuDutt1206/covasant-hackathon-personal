"""
Main Dataflow pipeline for Privacy Guardian Agent
Processes CSV data from GCS, anonymizes it using DLP, and loads to BigQuery
"""

from privacy_guardian.dataflow_config import (
    PROJECT_ID, REGION, TEMP_LOCATION, STAGING_LOCATION,
    MAX_NUM_WORKERS, MACHINE_TYPE, INPUT_FILE_PATH, BIGQUERY_TABLE,
    BIGQUERY_SCHEMA, DATAFLOW_OPTIONS, BATCH_SIZE
)
from privacy_guardian.dataflow_transforms import ParseCSVFn, AnonymizeDataTransform
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions, SetupOptions, WorkerOptions
from apache_beam.io import ReadFromText, WriteToBigQuery, WriteToText
import logging
import argparse
import sys
import os

# Add the parent directory to sys.path to make the package importable
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


class PrivacyGuardianOptions(PipelineOptions):
    """Custom options for Privacy Guardian pipeline."""

    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument(
            '--input_file',
            default=INPUT_FILE_PATH,
            help='Input CSV file path in GCS'
        )
        parser.add_argument(
            '--output_table',
            default=BIGQUERY_TABLE,
            help='Output BigQuery table'
        )
        parser.add_argument(
            '--batch_size',
            type=int,
            default=BATCH_SIZE,
            help='Batch size for processing'
        )


def run_pipeline():
    """Run the Privacy Guardian pipeline."""
    logging.info("Starting Privacy Guardian pipeline...")

    # Parse command line arguments
    parser = argparse.ArgumentParser()
    known_args, pipeline_args = parser.parse_known_args()

    # Set up pipeline options
    pipeline_options = PrivacyGuardianOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True

    # Get input path and output table
    input_path = pipeline_options.get_all_options()['input_file']
    output_table = pipeline_options.get_all_options()['output_table']
    batch_size = pipeline_options.get_all_options()['batch_size']

    logging.info(f"Input file: {input_path}")
    logging.info(f"Output table: {output_table}")
    logging.info(f"Batch size: {batch_size}")
    logging.info(f"Runner: {pipeline_options.get_all_options()['runner']}")

    # Define schema for BigQuery table
    schema = {
        'fields': [
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
            {'name': 'appointment_duration_mins',
                'type': 'STRING', 'mode': 'NULLABLE'},
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
            {'name': 'anonymization_timestamp',
                'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'anonymization_method', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'sensitive_fields_detected',
                'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'sensitive_field_names', 'type': 'STRING', 'mode': 'NULLABLE'}
        ]
    }

    # Create and run the pipeline
    with beam.Pipeline(options=pipeline_options) as p:
        # Read CSV data
        lines = p | 'ReadCSV' >> ReadFromText(input_path, skip_header_lines=1)

        # Parse CSV lines
        records = lines | 'ParseCSV' >> beam.ParDo(ParseCSVFn())

        # Apply anonymization transform
        anonymized = records | 'Anonymize' >> AnonymizeDataTransform(
            batch_size=batch_size)

        # Write to BigQuery
        anonymized | 'WriteToBigQuery' >> WriteToBigQuery(
            output_table,
            schema=schema,
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
        )


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    run_pipeline()
