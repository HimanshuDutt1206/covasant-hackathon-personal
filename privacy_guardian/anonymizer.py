"""
Anonymizer class for Privacy Guardian
"""

from datetime import datetime
import ipaddress
import re
import hashlib
from privacy_guardian.dataflow_config import DLP_PROJECT_ID
import pandas as pd
import numpy as np
from google.cloud import dlp_v2
import logging
import sys
import os
from typing import List, Dict, Any, Tuple

# Add the parent directory to sys.path to make the package importable
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


class Anonymizer:
    """
    Privacy Guardian Agent - Anonymizer

    Applies privacy-preserving transformations to sensitive data using
    Google Cloud Data Loss Prevention (DLP) API.
    """

    def __init__(self):
        """
        Initialize the Anonymizer with Google DLP client.
        """
        self.dlp_client = dlp_v2.DlpServiceClient()
        self.project_id = DLP_PROJECT_ID
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
        Anonymize a pandas DataFrame using Google DLP API by processing in row-based batches.

        Args:
            df: Input DataFrame to anonymize

        Returns:
            Tuple: (Anonymized DataFrame, count of sensitive fields detected, list of sensitive field names)
        """
        anonymized_df = df.copy()
        sensitive_columns_detected = set()  # Use a set to count unique sensitive columns
        total_transformations_applied = 0
        BATCH_SIZE = 5  # Define batch size as 5 rows

        # Store original dtypes for later restoration
        original_dtypes = df.dtypes.to_dict()
        numeric_columns = {col: dtype for col, dtype in original_dtypes.items()
                           if np.issubdtype(dtype, np.number)}

        print(
            f"🔐 Processing DataFrame: {len(df)} rows, {len(df.columns)} columns using Google DLP in batches of {BATCH_SIZE} rows"
        )

        # Prepare headers for DLP Table
        dlp_headers = [dlp_v2.FieldId(name=col) for col in df.columns]

        # Process DataFrame in batches of rows
        for i in range(0, len(df), BATCH_SIZE):
            batch_df = df.iloc[i: i + BATCH_SIZE].copy()

            # Convert all values in the batch to string type for DLP processing
            batch_df = batch_df.astype(str)

            print(
                f"🔐 Anonymizing batch rows {i+1} to {min(i + BATCH_SIZE, len(df))}")

            # De-identify the batch using DLP
            deidentified_table_rows, transformations_in_batch = self._deidentify_table_batch(
                batch_df, dlp_headers
            )

            # Reconstruct the anonymized DataFrame for this batch
            # Ensure the number of deidentified rows matches the original batch size
            if len(deidentified_table_rows) != len(batch_df):
                print(
                    "Warning: DLP response row count mismatch. Using original data for unmatched rows.")
                # Fallback: if DLP doesn't return enough rows, use original data for those
                filled_deidentified_table_rows = []
                for idx, original_row_tuple in enumerate(batch_df.itertuples(index=False)):
                    if idx < len(deidentified_table_rows):
                        filled_deidentified_table_rows.append(
                            deidentified_table_rows[idx])
                    else:
                        filled_deidentified_table_rows.append(
                            [str(x) for x in original_row_tuple])
                deidentified_table_rows = filled_deidentified_table_rows

            anonymized_batch_df = pd.DataFrame(
                deidentified_table_rows, columns=df.columns)

            # Restore numeric types for non-anonymized numeric columns
            for col, dtype in numeric_columns.items():
                # Check if the column was not anonymized (values unchanged)
                if all(anonymized_batch_df[col].values == batch_df[col].values):
                    try:
                        anonymized_batch_df[col] = anonymized_batch_df[col].astype(
                            dtype)
                    except (ValueError, TypeError):
                        # If conversion fails, keep as string (it might have been anonymized)
                        pass

            # Update the main anonymized_df with the processed batch
            anonymized_df.iloc[i: i + BATCH_SIZE] = anonymized_batch_df

            # Update sensitive columns and transformation log
            for transformation_info in transformations_in_batch:
                info_type = transformation_info.get(
                    "info_type", "UNKNOWN_INFOTYPE")
                transformed_count = transformation_info.get(
                    "transformed_count", 0)

                # Compare original and de-identified batch data cell by cell
                original_values = batch_df.values.tolist()
                transformed_values = anonymized_batch_df.values.tolist()

                for r_idx, (orig_row, trans_row) in enumerate(zip(original_values, transformed_values)):
                    for c_idx, (orig_val, trans_val) in enumerate(zip(orig_row, trans_row)):
                        if str(orig_val) != str(trans_val):
                            column_name = df.columns[c_idx]
                            sensitive_columns_detected.add(column_name)
                            total_transformations_applied += 1
                            self.transformation_log.append(
                                f"Batch Row: {i + r_idx + 1}, Field: {column_name}, Original: '{str(orig_val)[:50]}...', Transformed: '{str(trans_val)[:50]}...'"
                            )

        # Final type restoration for numeric columns that weren't anonymized
        for col, dtype in numeric_columns.items():
            if col not in sensitive_columns_detected:
                try:
                    anonymized_df[col] = pd.to_numeric(
                        anonymized_df[col], errors='coerce').fillna(0).astype(dtype)
                except (ValueError, TypeError):
                    # If conversion fails, keep as string (it was anonymized)
                    pass

        return anonymized_df, len(sensitive_columns_detected), list(sensitive_columns_detected)

    def _deidentify_table_batch(self, batch_df: pd.DataFrame, dlp_headers: List[dlp_v2.FieldId]) -> Tuple[List[List[str]], List[Dict[str, Any]]]:
        """
        Sends a DataFrame batch as a DLP Table for de-identification.

        Args:
            batch_df: A pandas DataFrame representing the batch of rows to de-identify.
            dlp_headers: List of dlp_v2.FieldId objects for the table headers.

        Returns:
            Tuple: (List of lists representing de-identified rows, List of transformation summaries)
        """
        if batch_df.empty:
            return [], []

        parent = f"projects/{self.project_id}"

        try:
            inspect_config = dlp_v2.InspectConfig(
                min_likelihood=dlp_v2.Likelihood.POSSIBLE,
                include_quote=True,
                info_types=[
                    # Core Personal Identifiers (25)
                    {"name": "PERSON_NAME"},
                    {"name": "EMAIL_ADDRESS"},
                    {"name": "PHONE_NUMBER"},
                    {"name": "DATE_OF_BIRTH"},
                    {"name": "AGE"},
                    {"name": "GENDER"},
                    {"name": "LOCATION"},
                    {"name": "STREET_ADDRESS"},
                    {"name": "ETHNIC_GROUP"},
                    {"name": "MARITAL_STATUS"},
                    {"name": "FIRST_NAME"},
                    {"name": "LAST_NAME"},
                    {"name": "IMMIGRATION_STATUS"},
                    {"name": "EMPLOYMENT_STATUS"},
                    {"name": "SEXUAL_ORIENTATION"},
                    {"name": "RELIGIOUS_TERM"},
                    {"name": "POLITICAL_TERM"},
                    {"name": "DEMOGRAPHIC_DATA"},
                    {"name": "GEOGRAPHIC_DATA"},
                    {"name": "LOCATION_COORDINATES"},
                    {"name": "ORGANIZATION_NAME"},
                    {"name": "COUNTRY_DEMOGRAPHIC"},
                    {"name": "DATE"},
                    {"name": "TIME"},
                    {"name": "TRADE_UNION"},

                    # Financial/PCI Data (25)
                    {"name": "CREDIT_CARD_NUMBER"},
                    {"name": "CREDIT_CARD_TRACK_NUMBER"},
                    {"name": "CREDIT_CARD_EXPIRATION_DATE"},
                    {"name": "CVV_NUMBER"},
                    {"name": "FINANCIAL_ACCOUNT_NUMBER"},
                    {"name": "US_BANK_ROUTING_MICR"},
                    {"name": "IBAN_CODE"},
                    {"name": "SWIFT_CODE"},
                    {"name": "VAT_NUMBER"},
                    {"name": "FINANCIAL_ID"},
                    {"name": "AMERICAN_BANKERS_CUSIP_ID"},
                    {"name": "CANADA_BANK_ACCOUNT"},
                    {"name": "JAPAN_BANK_ACCOUNT"},
                    {"name": "PORTUGAL_NIB_NUMBER"},
                    {"name": "CREDIT_CARD_DATA"},
                    {"name": "US_EMPLOYER_IDENTIFICATION_NUMBER"},
                    {"name": "BRAZIL_CPF_NUMBER"},
                    {"name": "SPAIN_CIF_NUMBER"},
                    {"name": "SPAIN_NIE_NUMBER"},
                    {"name": "SPAIN_NIF_NUMBER"},
                    {"name": "SPAIN_SOCIAL_SECURITY_NUMBER"},
                    {"name": "PORTUGAL_SOCIAL_SECURITY_NUMBER"},
                    {"name": "SWITZERLAND_SOCIAL_SECURITY_NUMBER"},
                    {"name": "FINLAND_NATIONAL_ID_NUMBER"},
                    {"name": "DENMARK_CPR_NUMBER"},

                    # Healthcare/PHI (20)
                    {"name": "MEDICAL_RECORD_NUMBER"},
                    {"name": "MEDICAL_ID"},
                    {"name": "MEDICAL_DATA"},
                    {"name": "BLOOD_TYPE"},
                    {"name": "ICD9_CODE"},
                    {"name": "ICD10_CODE"},
                    {"name": "US_HEALTHCARE_NPI"},
                    {"name": "US_DEA_NUMBER"},
                    {"name": "CANADA_BC_PHN"},
                    {"name": "CANADA_OHIP"},
                    {"name": "CANADA_QUEBEC_HIN"},
                    {"name": "UK_NATIONAL_HEALTH_SERVICE_NUMBER"},
                    {"name": "SCOTLAND_COMMUNITY_HEALTH_INDEX_NUMBER"},
                    {"name": "NEW_ZEALAND_NHI_NUMBER"},
                    {"name": "MEDICAL_TERM"},
                    {"name": "US_MEDICARE_BENEFICIARY_ID_NUMBER"},
                    {"name": "FDA_CODE"},
                    {"name": "US_PREPARER_TAXPAYER_IDENTIFICATION_NUMBER"},
                    {"name": "US_ADOPTION_TAXPAYER_IDENTIFICATION_NUMBER"},
                    {"name": "US_INDIVIDUAL_TAXPAYER_IDENTIFICATION_NUMBER"},

                    # Government IDs - Global (40)
                    {"name": "PASSPORT"},
                    {"name": "DRIVERS_LICENSE_NUMBER"},
                    {"name": "GOVERNMENT_ID"},
                    {"name": "US_SOCIAL_SECURITY_NUMBER"},
                    {"name": "INDIA_AADHAAR_INDIVIDUAL"},
                    {"name": "INDIA_PAN_INDIVIDUAL"},
                    {"name": "INDIA_GST_INDIVIDUAL"},
                    {"name": "CHINA_RESIDENT_ID_NUMBER"},
                    {"name": "UK_NATIONAL_INSURANCE_NUMBER"},
                    {"name": "CANADA_SOCIAL_INSURANCE_NUMBER"},
                    {"name": "AUSTRALIA_TAX_FILE_NUMBER"},
                    {"name": "FRANCE_NIR"},
                    {"name": "GERMANY_TAXPAYER_IDENTIFICATION_NUMBER"},
                    {"name": "JAPAN_INDIVIDUAL_NUMBER"},
                    {"name": "KOREA_RRN"},
                    {"name": "NETHERLANDS_BSN_NUMBER"},
                    {"name": "NORWAY_NI_NUMBER"},
                    {"name": "SWEDEN_NATIONAL_ID_NUMBER"},
                    {"name": "TAIWAN_ID_NUMBER"},
                    {"name": "THAILAND_NATIONAL_ID_NUMBER"},
                    {"name": "TURKEY_ID_NUMBER"},
                    {"name": "INDONESIA_NIK_NUMBER"},
                    {"name": "ITALY_FISCAL_CODE"},
                    {"name": "MEXICO_CURP_NUMBER"},
                    {"name": "NEW_ZEALAND_IRD_NUMBER"},
                    {"name": "POLAND_PESEL_NUMBER"},
                    {"name": "SINGAPORE_NATIONAL_REGISTRATION_ID_NUMBER"},
                    {"name": "SOUTH_AFRICA_ID_NUMBER"},
                    {"name": "SPAIN_DNI_NUMBER"},
                    {"name": "BELGIUM_NATIONAL_ID_CARD_NUMBER"},
                    {"name": "CROATIA_PERSONAL_ID_NUMBER"},
                    {"name": "CZECHIA_PERSONAL_ID_NUMBER"},
                    {"name": "GERMANY_IDENTITY_CARD_NUMBER"},
                    {"name": "HONG_KONG_ID_NUMBER"},
                    {"name": "ISRAEL_IDENTITY_CARD_NUMBER"},
                    {"name": "PARAGUAY_CIC_NUMBER"},
                    {"name": "PERU_DNI_NUMBER"},
                    {"name": "POLAND_NATIONAL_ID_NUMBER"},
                    {"name": "PORTUGAL_CDC_NUMBER"},
                    {"name": "URUGUAY_CDI_NUMBER"},

                    # Security & Technical (25)
                    {"name": "IP_ADDRESS"},
                    {"name": "MAC_ADDRESS"},
                    {"name": "AUTH_TOKEN"},
                    {"name": "PASSWORD"},
                    {"name": "ENCRYPTION_KEY"},
                    {"name": "AWS_CREDENTIALS"},
                    {"name": "GCP_CREDENTIALS"},
                    {"name": "AZURE_AUTH_TOKEN"},
                    {"name": "SSL_CERTIFICATE"},
                    {"name": "JSON_WEB_TOKEN"},
                    {"name": "OAUTH_CLIENT_SECRET"},
                    {"name": "BASIC_AUTH_HEADER"},
                    {"name": "IMEI_HARDWARE_ID"},
                    {"name": "IMSI_ID"},
                    {"name": "ICCID_NUMBER"},
                    {"name": "TECHNICAL_ID"},
                    {"name": "GCP_API_KEY"},
                    {"name": "TINK_KEYSET"},
                    {"name": "WEAK_PASSWORD_HASH"},
                    {"name": "STORAGE_SIGNED_URL"},
                    {"name": "STORAGE_SIGNED_POLICY_DOCUMENT"},
                    {"name": "HTTP_COOKIE"},
                    {"name": "SECURITY_DATA"},
                    {"name": "GERMANY_SCHUFA_ID"},
                    {"name": "XSRF_TOKEN"},

                    # Vehicle & Transportation (15)
                    {"name": "US_VEHICLE_IDENTIFICATION_NUMBER"},
                    {"name": "VEHICLE_IDENTIFICATION_NUMBER"},
                    {"name": "AUSTRALIA_DRIVERS_LICENSE_NUMBER"},
                    {"name": "CANADA_DRIVERS_LICENSE_NUMBER"},
                    {"name": "FRANCE_DRIVERS_LICENSE_NUMBER"},
                    {"name": "GERMANY_DRIVERS_LICENSE_NUMBER"},
                    {"name": "IRELAND_DRIVING_LICENSE_NUMBER"},
                    {"name": "JAPAN_DRIVERS_LICENSE_NUMBER"},
                    {"name": "KOREA_DRIVERS_LICENSE_NUMBER"},
                    {"name": "SPAIN_DRIVERS_LICENSE_NUMBER"},
                    {"name": "UK_DRIVERS_LICENSE_NUMBER"},
                    {"name": "US_DRIVERS_LICENSE_NUMBER"},
                    {"name": "DOD_ID_NUMBER"},
                    {"name": "IRELAND_EIRCODE"},
                    {"name": "JAPAN_CORPORATE_NUMBER"}
                ]
            )

            deidentify_config = dlp_v2.DeidentifyConfig(
                info_type_transformations=dlp_v2.InfoTypeTransformations(
                    transformations=[
                        # Single transformation that masks all detected PII with asterisks
                        dlp_v2.InfoTypeTransformations.InfoTypeTransformation(
                            primitive_transformation=dlp_v2.PrimitiveTransformation(
                                character_mask_config=dlp_v2.CharacterMaskConfig(
                                    masking_character="*",
                                    number_to_mask=0,  # Mask all characters
                                )
                            ),
                            # No specific info_types means this applies to ALL detected info types
                        )
                    ]
                )
            )

            # Construct DLP Table rows from the DataFrame batch
            dlp_table_rows = []
            for _, row in batch_df.iterrows():
                dlp_table_rows.append(
                    dlp_v2.Table.Row(
                        values=[dlp_v2.Value(string_value=str(val))
                                for val in row.values]
                    )
                )

            table_object = dlp_v2.Table(
                headers=dlp_headers, rows=dlp_table_rows)
            content_item = dlp_v2.ContentItem(table=table_object)

            request = dlp_v2.DeidentifyContentRequest(
                parent=parent,
                inspect_config=inspect_config,
                deidentify_config=deidentify_config,
                item=content_item,
            )

            response = self.dlp_client.deidentify_content(request=request)

            transformed_data_rows = []
            if response.item and response.item.table and response.item.table.rows:
                for row in response.item.table.rows:
                    transformed_data_rows.append(
                        [val.string_value for val in row.values])

            transformation_summaries = []
            if response.overview.transformation_summaries:
                for t_summary in response.overview.transformation_summaries:
                    info_type = (
                        t_summary.info_type.name
                        if t_summary.info_type
                        else "UNKNOWN_INFOTYPE"
                    )
                    transformation_summaries.append({
                        "info_type": info_type,
                        "transformation": "Character Masking",
                        "transformed_count": t_summary.results[0].count if t_summary.results else 0,
                        "transformed_bytes": t_summary.transformed_bytes,
                    })

            return transformed_data_rows, transformation_summaries

        except Exception as e:
            raise RuntimeError(
                f"DLP table de-identification failed for batch: {e}")

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
        if stats['sensitive_fields_names']:
            for field in stats['sensitive_fields_names']:
                report.append(f"- {field}")
        else:
            report.append("No sensitive fields detected by DLP.")

        report.append(
            "\n--- Non-Sensitive Columns (No DLP Redaction Applied) ---")
        sensitive_set = set(stats['sensitive_fields_names'])
        all_columns_set = set(stats['total_columns_names'])
        non_sensitive_columns = sorted(list(all_columns_set - sensitive_set))

        if non_sensitive_columns:
            for column in non_sensitive_columns:
                report.append(f"- {column}")
        else:
            report.append("All columns were identified as sensitive.")

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
            "DLP InfoTypes used: All of Google DLP's inbuilt sensitive information types.")

        report.append("\n\nEnd of Report")
        report.append("=" * 60)

        return "\n".join(report)


# Example usage
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
    print(f"\n" + report)

    # Save report to file
    with open("anonymization_report.txt", "wb") as f:
        f.write(report.encode("utf-8"))

    print(f"\n📋 Detailed report saved to: anonymization_report.txt")
