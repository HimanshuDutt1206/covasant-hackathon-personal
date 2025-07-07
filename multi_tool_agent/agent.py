from google.adk.agents import Agent
from anonymizer import Anonymizer
import os
import sys
import pandas as pd
from typing import Dict, Any, Optional
from pathlib import Path

# Add parent directory to path to import our modules
sys.path.append(str(Path(__file__).parent.parent))


def anonymize_csv_data(input_file: str, output_file: Optional[str] = None) -> dict:
    """Anonymizes sensitive data in a CSV file using Privacy Guardian policies.

    Args:
        input_file (str): Path to the input CSV file to anonymize.
        output_file (str, optional): Path for the output file. If not provided, 
                                   defaults to 'anonymized_' + input_file.

    Returns:
        dict: Status and results of the anonymization process.
    """
    try:
        # Set default output file if not provided
        if output_file is None:
            base_name = os.path.splitext(input_file)[0]
            output_file = f"anonymized_{base_name}.csv"

        # Initialize anonymizer
        anonymizer = Anonymizer()

        # Check if input file exists
        if not os.path.exists(input_file):
            return {
                "status": "error",
                "error_message": f"Input file '{input_file}' not found."
            }

        # Load and process the data
        df = pd.read_csv(input_file)
        anonymized_df, sensitive_columns_detected, sensitive_column_names = anonymizer.anonymize_dataframe(
            df)

        # Save anonymized data
        anonymized_df.to_csv(output_file, index=False)

        # Generate statistics
        stats = {
            'input_file': input_file,
            'output_file': output_file,
            'total_records': len(df),
            'total_fields': len(df.columns),
            'total_columns_names': df.columns.tolist(),
            # This is the count of redacted items
            'sensitive_fields_detected': sensitive_columns_detected,
            'sensitive_fields_names': sensitive_column_names,
            'transformation_log': anonymizer.transformation_log.copy()
        }

        return {
            "status": "success",
            "message": f"Successfully anonymized {stats['total_records']} records from '{input_file}' to '{output_file}'",
            "statistics": stats
        }

    except Exception as e:
        return {
            "status": "error",
            "error_message": f"Anonymization failed: {str(e)}"
        }


def generate_anonymization_report(stats: Dict[str, Any], report_file: Optional[str] = None) -> dict:
    """Generates a detailed anonymization report for processed data.

    Args:
        stats (dict): Statistics generated from the anonymization process.
        report_file (str, optional): Path for the report file. Defaults to 'anonymization_report.txt'.

    Returns:
        dict: Status and report generation results.
    """
    try:
        # Set default paths
        if report_file is None:
            report_file = "anonymization_report.txt"

        # Initialize anonymizer (only to use its report generation method)
        anonymizer = Anonymizer()

        # Generate report using the provided stats
        report_content = anonymizer.generate_anonymization_report(stats)

        # Save report
        with open(report_file, 'w', encoding='utf-8') as f:
            f.write(report_content)

        return {
            "status": "success",
            "message": f"Report generated successfully and saved to '{report_file}'",
            "report_preview": report_content[:500] + "..." if len(report_content) > 500 else report_content
        }

    except Exception as e:
        return {
            "status": "error",
            "error_message": f"Report generation failed: {str(e)}"
        }


def process_sample_data() -> dict:
    """Processes the sample data file with Privacy Guardian anonymization.

    Returns:
        dict: Status and results of processing the sample data.
    """
    try:
        input_file = "sample_data.csv"
        output_file = "anonymized_data.csv"
        report_file = "anonymization_report.txt"

        # First anonymize the data
        anonymize_result = anonymize_csv_data(input_file, output_file)
        if anonymize_result["status"] != "success":
            return anonymize_result

        # Then generate the report
        report_result = generate_anonymization_report(
            anonymize_result["statistics"], report_file)
        if report_result["status"] != "success":
            return report_result

        return {
            "status": "success",
            "message": "Sample data processed successfully!",
            "files_created": [output_file, report_file],
            "statistics": anonymize_result["statistics"]
        }

    except Exception as e:
        return {
            "status": "error",
            "error_message": f"Sample data processing failed: {str(e)}"
        }


def get_data_preview(file_path: str, rows: int = 5) -> dict:
    """Gets a preview of data from a CSV file.

    Args:
        file_path (str): Path to the CSV file to preview.
        rows (int): Number of rows to preview (default: 5).

    Returns:
        dict: Status and data preview.
    """
    try:
        if not os.path.exists(file_path):
            return {
                "status": "error",
                "error_message": f"File '{file_path}' not found."
            }

        # Use anonymizer to preview data (anonymized for privacy)
        df = pd.read_csv(file_path)
        # For preview, we don't need to save the file, just anonymize the df in memory
        anonymized_preview_df, _ = Anonymizer().anonymize_dataframe(df.head(rows))

        # Convert DataFrame to a string for display, e.g., to_markdown or to_string
        preview_content = anonymized_preview_df.to_markdown(index=False)

        return {
            "status": "success",
            "message": f"Preview of '{file_path}' (first {rows} rows, anonymized):",
            "preview_data": preview_content
        }

    except Exception as e:
        return {
            "status": "error",
            "error_message": f"Error getting data preview: {str(e)}"
        }


class PrivacyGuardianAgent(Agent):
    def __init__(self):
        super().__init__(
            name="privacy_guardian_agent",
            model="gemini-2.0-flash",  # You can choose a different model if needed
            description=(
                "Privacy Guardian Agent - An advanced data anonymization and privacy protection system "
                "that uses Google Cloud Data Loss Prevention (DLP) API to identify and redact sensitive information."
            ),
            instruction=(
                "You are the Privacy Guardian Agent, an expert in data privacy and anonymization. "
                "You help users protect sensitive information by applying Google Cloud DLP for redaction. "
                "You can process CSV files, generate detailed reports, and provide safe data previews."
            ),
            tools=[
                anonymize_csv_data,
                generate_anonymization_report,
                process_sample_data,
                get_data_preview
            ]
        )


# Instantiate the agent to be discoverable by ADK
root_agent = PrivacyGuardianAgent()
