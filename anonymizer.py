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

        parent = f"projects/{self.project_id}"

        try:
            # Define inspect configuration using dlp_v2 objects
            inspect_config = dlp_v2.InspectConfig(
                min_likelihood=dlp_v2.Likelihood.POSSIBLE,
                include_quote=True,
            )

            # Define the de-identify configuration using dlp_v2 objects
            deidentify_config = dlp_v2.DeidentifyConfig(
                info_type_transformations=dlp_v2.InfoTypeTransformations(
                    transformations=[
                        dlp_v2.InfoTypeTransformations.InfoTypeTransformation(
                            primitive_transformation=dlp_v2.PrimitiveTransformation(
                                character_mask_config=dlp_v2.CharacterMaskConfig(
                                    masking_character="*",
                                    number_to_mask=0,
                                )
                            ),
                            # No specific info_types here, means all detected info types will be masked
                        )
                    ]
                )
            )

            # Construct a table item for structured data processing using dlp_v2 objects
            table_headers = [dlp_v2.FieldId(name="value")]
            table_rows = []
            for text in filtered_texts:
                table_rows.append(dlp_v2.Table.Row(
                    values=[dlp_v2.Value(string_value=text)]))

            table_object = dlp_v2.Table(headers=table_headers, rows=table_rows)
            content_item = dlp_v2.ContentItem(table=table_object)

            # Construct the DeidentifyContentRequest object explicitly
            request = dlp_v2.DeidentifyContentRequest(
                parent=parent,
                inspect_config=inspect_config,
                deidentify_config=deidentify_config,
                item=content_item,
            )

            # Call the DLP API
            response = self.dlp_client.deidentify_content(request=request)

            # Process the DLP response
            transformed_texts = []
            if response.item and response.item.table and response.item.table.rows:
                for row in response.item.table.rows:
                    if row.values and row.values[0].string_value is not None:
                        transformed_texts.append(row.values[0].string_value)

            # Log transformations (this part needs careful adjustment as transformation_summaries are for findings, not raw rows)
            if response.overview.transformation_summaries:
                for t_summary in response.overview.transformation_summaries:
                    info_type = (
                        t_summary.info_type.name
                        if t_summary.info_type
                        else "UNKNOWN_INFOTYPE"
                    )
                    self.transformation_log.append({
                        "info_type": info_type,
                        "transformation": "Character Masking",
                        "status": t_summary.results[0].count if t_summary.results else 0,
                        "transformed_bytes": t_summary.transformed_bytes,
                    })

            # If DLP processes successfully but returns no transformations (e.g., no sensitive data),
            # ensure we return the original filtered texts to maintain array length.
            if not transformed_texts and filtered_texts:
                return filtered_texts

            # Ensure the number of transformed texts matches the number of filtered texts
            # If DLP doesn't return a transformed value for every input, we fill with original
            if len(transformed_texts) < len(filtered_texts):
                for i in range(len(transformed_texts), len(filtered_texts)):
                    transformed_texts.append(filtered_texts[i])

            return transformed_texts
        except Exception as e:
            raise RuntimeError(f"DLP redaction failed: {e}")

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
    print(f"\n" + report)

    # Save report to file
    with open("anonymization_report.txt", "wb") as f:
        f.write(report.encode("utf-8"))

    print(f"\n📋 Detailed report saved to: anonymization_report.txt")
