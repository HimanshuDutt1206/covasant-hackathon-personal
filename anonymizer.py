"""
Privacy Guardian Agent - Anonymizer
Implements privacy-preserving transformations for sensitive data
"""

import hashlib
import re
import pandas as pd
from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime
import ipaddress
from google.cloud import dlp_v2
import os


class Anonymizer:
    """
    Privacy Guardian Agent - Anonymizer

    Applies privacy-preserving transformations to sensitive data using
    Google Cloud Data Loss Prevention (DLP) API.
    """

    # Define the InfoTypes to be detected by Google DLP
    DLP_INFO_TYPES = [
        # --- PII (Personally Identifiable Information) ---
        {"name": "PERSON_NAME"},               # Full names of individuals
        {"name": "EMAIL_ADDRESS"},             # Standard email addresses
        # Phone numbers (various formats)
        {"name": "PHONE_NUMBER"},
        {"name": "STREET_ADDRESS"},            # Physical street addresses
        {"name": "IP_ADDRESS"},                # Internet Protocol addresses
        {"name": "US_SOCIAL_SECURITY_NUMBER"},  # U.S. Social Security Numbers
        {"name": "PASSPORT"},                  # Generic passport numbers
        {"name": "US_DRIVERS_LICENSE_NUMBER"},  # U.S. Driver's License Numbers
        {"name": "DATE_OF_BIRTH"},             # Dates of birth
        {"name": "AGE"},                       # Age (numerical values)
        {"name": "GENDER"},                    # Gender information
        {"name": "LOCATION"},                  # General location mentions
        # Domain names that might appear in PII
        {"name": "DOMAIN_NAME"},
        {"name": "URL"},                       # URLs that might contain PII

        # --- India Specific PII ---
        {"name": "INDIA_AADHAAR_INDIVIDUAL"},
        {"name": "INDIA_PAN_INDIVIDUAL"},
        {"name": "INDIA_GST_INDIVIDUAL"},

        # --- PCI (Payment Card Industry Data) ---
        # Credit and debit card numbers (e.g., Visa, Mastercard)
        {"name": "CREDIT_CARD_NUMBER"},
        # U.S. specific bank account numbers
        # International Bank Account Numbers
        {"name": "IBAN_CODE"},
        # SWIFT/BIC codes for international transfers
        {"name": "SWIFT_CODE"},

        # --- PHI (Protected Health Information) ---
        # Generic dates related to health events (e.g., hospital visit dates)
        {"name": "DATE"},
    ]

    def __init__(self):
        """
        Initialize the Anonymizer with Google DLP client.
        """
        self.dlp_client = dlp_v2.DlpServiceClient()
        self.project_id = os.getenv('GOOGLE_CLOUD_PROJECT') or os.getenv(
            'GCP_PROJECT') or os.getenv('GCLOUD_PROJECT')
        if not self.project_id:
            raise ValueError(
                "Google Cloud Project ID not found in environment variables. Please set GOOGLE_CLOUD_PROJECT, GCP_PROJECT, or GCLOUD_PROJECT.")
        self.transformation_log = []  # Track all transformations

    def anonymize_dataset(self, input_file: str, output_file: str) -> Dict[str, Any]:
        """
        Anonymize an entire dataset using Google DLP API.

        Args:
            input_file: Path to input CSV file
            output_file: Path to save anonymized CSV file

        Returns:
            Dictionary with anonymization statistics
        """
        print(f"🔐 PRIVACY GUARDIAN AGENT - ANONYMIZING DATASET WITH GOOGLE DLP")
        print("=" * 60)

        df = pd.read_csv(input_file)
        original_df = df.copy()

        print(f"📊 Loaded dataset: {len(df)} rows, {len(df.columns)} columns")

        anonymized_df, sensitive_columns_detected, sensitive_column_names = self.anonymize_dataframe(
            df)

        anonymized_df.to_csv(output_file, index=False)

        stats = {
            'input_file': input_file,
            'output_file': output_file,
            'total_fields': len(df.columns),
            'sensitive_fields_detected': sensitive_columns_detected,
            'sensitive_fields_names': sensitive_column_names,
            'total_columns_names': df.columns.tolist(),
            'total_records': len(df),
            'transformation_log': self.transformation_log.copy()
        }

        print(f"\n✅ ANONYMIZATION COMPLETE")
        print(f"💾 Saved to: {output_file}")
        print(
            f"�� Applied {sensitive_columns_detected} sensitive columns detected using Google DLP")

        return stats

    def anonymize_dataframe(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, int, List[str]]:
        """
        Anonymize a pandas DataFrame using Google DLP API.

        Args:
            df: Input DataFrame to anonymize

        Returns:
            Tuple: (Anonymized DataFrame, count of sensitive columns detected, list of sensitive column names)
        """
        anonymized_df = df.copy()
        total_transformations_applied = 0
        sensitive_columns_detected = 0  # New variable to count sensitive columns
        sensitive_column_names = []  # New list to store names of sensitive columns
        BATCH_SIZE = 500  # Define a suitable batch size

        print(
            f"🔐 Processing DataFrame: {len(df)} rows, {len(df.columns)} columns using Google DLP")

        for column in anonymized_df.columns:
            column_had_sensitive_data = False  # Flag for current column
            # Convert column to string type to ensure DLP can process it
            # This is a simplification; for mixed types, more robust handling might be needed
            anonymized_df[column] = anonymized_df[column].astype(str)

            print(f"🔐 Anonymizing column: {column}")
            original_column_values = anonymized_df[column].tolist()
            redacted_column_values = []

            for i in range(0, len(original_column_values), BATCH_SIZE):
                batch = original_column_values[i:i + BATCH_SIZE]
                redacted_batch = self._apply_dlp_redaction(batch)
                redacted_column_values.extend(redacted_batch)

                for original_value, redacted_value in zip(batch, redacted_batch):
                    if original_value != redacted_value:
                        total_transformations_applied += 1
                        if not column_had_sensitive_data:
                            sensitive_columns_detected += 1
                            sensitive_column_names.append(
                                column)  # Add column name to list
                            column_had_sensitive_data = True
                        self.transformation_log.append(
                            f"Field: {column}, Original: '{original_value[:50]}...', Transformed: '{redacted_value[:50]}...'"
                        )
            anonymized_df[column] = redacted_column_values

        return anonymized_df, sensitive_columns_detected, sensitive_column_names

    def _apply_dlp_redaction(self, texts_to_deidentify: List[str]) -> List[str]:
        """
        Applies Google DLP redaction to a list of text strings.
        """
        if not texts_to_deidentify:
            return []

        # Filter out NaN/None values before sending to DLP
        filtered_texts = [t for t in texts_to_deidentify if pd.notna(
            t) and t is not None and t != '']
        if not filtered_texts:
            return texts_to_deidentify  # Return original list if all are empty/NaN

        # Create a mapping from original index to filtered index
        original_to_filtered_map = {idx: f_idx for f_idx, (idx, t) in enumerate(zip(range(len(
            texts_to_deidentify)), texts_to_deidentify)) if pd.notna(t) and t is not None and t != ''}

        # Prepare items for DLP request
        items = [{"value": text} for text in filtered_texts]
        parent = f"projects/{self.project_id}"

        # Initialize with original values
        redacted_results = list(texts_to_deidentify)

        try:
            inspect_config = {
                "info_types": self.DLP_INFO_TYPES,
                # Detect anything with at least possible likelihood
                "min_likelihood": dlp_v2.Likelihood.POSSIBLE,
                "include_quote": True
            }

            deidentify_config = {
                "info_type_transformations": {
                    "transformations": [
                        {
                            "primitive_transformation": {
                                "character_mask_config": {
                                    "masking_character": "*",
                                    "number_to_mask": 0,  # Mask all characters
                                }
                            }
                        }
                    ]
                }
            }
            # The deidentify_content method processes multiple items in a single request
            response = self.dlp_client.deidentify_content(
                request={
                    "parent": parent,
                    "item": {"table": {"headers": [{"name": "value"}], "rows": [{"values": [{"string_value": item["value"]}]} for item in items]}},
                    "inspect_config": inspect_config,
                    "deidentify_config": deidentify_config
                }
            )

            # Extract redacted values from the response
            # The response will contain the transformed values in the same order as the input items
            if response.item and response.item.table and response.item.table.rows:
                for f_idx, row in enumerate(response.item.table.rows):
                    if row.values and row.values[0].string_value is not None:
                        # Find the original index for this filtered item
                        original_idx = next(
                            idx for idx, f_idx_val in original_to_filtered_map.items() if f_idx_val == f_idx)
                        redacted_results[original_idx] = row.values[0].string_value

        except Exception as e:
            print(
                f"⚠️  Error applying DLP redaction to batch: {e}. Returning original values.")
            # If an error occurs, return the original list to avoid data loss
            return texts_to_deidentify

        return redacted_results

    def generate_anonymization_report(self, stats: Dict[str, Any]) -> str:
        """
        Generates a summary report of the anonymization process.
        """
        report = []
        report.append("=" * 60)
        report.append(
            "        PRIVACY GUARDIAN AGENT - ANONYMIZATION REPORT        ")
        report.append("=" * 60)
        report.append(
            f"\nREPORT DATE: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report.append("\n--- Input and Output Files ---")
        report.append(f"Input File: {stats.get('input_file', 'N/A')}")
        report.append(f"Output File: {stats.get('output_file', 'N/A')}")

        report.append("\n--- Anonymization Summary ---")
        report.append(
            f"Total Records Processed: {stats.get('total_records', 'N/A')}")
        report.append(
            f"Total Fields Processed: {stats.get('total_fields', 'N/A')}")
        report.append(
            f"Sensitive Fields Redacted (by DLP): {stats.get('sensitive_fields_detected', 'N/A')}")

        report.append("\n--- Sensitive Fields Detected ---")
        if stats.get('sensitive_fields_names'):
            for column in stats['sensitive_fields_names']:
                report.append(f"- {column}")
        else:
            report.append(
                "No specific sensitive fields detected (all handled by DLP redaction).")

        # List non-sensitive columns
        # Get all columns from stats
        all_columns = stats.get('total_columns_names', [])
        sensitive_columns = stats.get('sensitive_fields_names', [])
        non_sensitive_columns = [
            col for col in all_columns if col not in sensitive_columns]

        report.append(
            "\n--- Non-Sensitive Columns (No DLP Redaction Applied) ---")
        if non_sensitive_columns:
            for column in non_sensitive_columns:
                report.append(f"- {column}")
        else:
            report.append(
                "All columns were identified as sensitive or no columns were present.")

        report.append("\n--- Transformation Log ---")
        if stats.get('transformation_log'):
            for entry in stats['transformation_log']:
                report.append(f"- {entry}")
        else:
            report.append(
                "No specific transformations logged (all handled by DLP redaction).")

        report.append(
            "\n--- Compliance Information (DLP handles detection and redaction) ---")
        report.append(
            "Google DLP API was used for sensitive data detection and redaction.")
        report.append(
            "All identified sensitive data was redacted using character masking ('*').")
        report.append(
            f"DLP InfoTypes used: {', '.join([it['name'] for it in self.DLP_INFO_TYPES])}")

        report.append("\n\nEnd of Report")
        report.append("=" * 60)

        return "\n".join(report)


# Example usage and testing
if __name__ == "__main__":
    # Initialize components
    anonymizer = Anonymizer()

    # Anonymize the sample dataset
    input_file = "sample_data.csv"
    output_file = "anonymized_data.csv"

    print("🚀 Starting dataset anonymization...")
    stats = anonymizer.anonymize_dataset(input_file, output_file)

    # Generate and display report
    report = anonymizer.generate_anonymization_report(stats)
    print("\n" + report)

    # Save report to file
    with open("anonymization_report.txt", "w") as f:
        f.write(report)

    print(f"\n📋 Detailed report saved to: anonymization_report.txt")
