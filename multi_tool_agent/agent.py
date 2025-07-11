"""
Privacy Guardian Agent - Simplified version that only triggers Dataflow jobs
"""

from privacy_guardian.dataflow_config import (
    PROJECT_ID, REGION, INPUT_FILE_PATH, BIGQUERY_TABLE,
    TEMP_LOCATION, STAGING_LOCATION, MAX_NUM_WORKERS, MACHINE_TYPE
)
from google.adk.agents import Agent
import subprocess
import os
from pathlib import Path
import sys

# Add parent directory to path to import our modules
sys.path.append(str(Path(__file__).parent.parent))


def process_data() -> dict:
    """Run the Dataflow job with pre-configured settings.

    Returns:
        dict: Status and result or error message.
    """
    try:
        # Dataflow job parameters
        job_name = "privacy-guardian-anonymization"

        # Build the command
        cmd = [
            "python", "-m", "privacy_guardian.dataflow_pipeline",
            "--runner=DataflowRunner",
            f"--project={PROJECT_ID}",
            f"--region={REGION}",
            f"--temp_location={TEMP_LOCATION}",
            f"--staging_location={STAGING_LOCATION}",
            f"--job_name={job_name}",
            f"--max_num_workers={MAX_NUM_WORKERS}",
            f"--machine_type={MACHINE_TYPE}",
            f"--input_file={INPUT_FILE_PATH}",
            f"--output_table={BIGQUERY_TABLE}",
            "--batch_size=5",
            "--setup_file=./setup.py"
        ]

        print("Starting Dataflow job...")
        print(
            f"Monitor your job here: https://console.cloud.google.com/dataflow/jobs/{REGION}?project={PROJECT_ID}")

        # Run the command
        result = subprocess.run(cmd, capture_output=True, text=True)

        if result.returncode == 0:
            return {
                "status": "success",
                "report": "Job started successfully. Check the console link above to monitor progress."
            }
        else:
            return {
                "status": "error",
                "error_message": "Failed to start job. Please check your configuration."
            }

    except Exception as e:
        return {
            "status": "error",
            "error_message": str(e)
        }


# Create the root agent instance
root_agent = Agent(
    name="privacy_guardian",
    model="gemini-2.0-flash",
    description="Agent to process data using Privacy Guardian with Google Cloud Dataflow",
    instruction="You are a helpful agent who can process data using Privacy Guardian. When asked to process data, you will trigger a Dataflow job and provide a monitoring link.",
    tools=[process_data]
)
