#!/usr/bin/env python3
"""
Script to run the Privacy Guardian Dataflow job
"""

import subprocess
import sys
import logging
from privacy_guardian.dataflow_config import (
    PROJECT_ID, REGION, INPUT_FILE_PATH, BIGQUERY_TABLE,
    TEMP_LOCATION, STAGING_LOCATION, MAX_NUM_WORKERS, MACHINE_TYPE
)


def run_dataflow_job():
    """Run the Dataflow job with proper configuration"""

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

    logging.info("Starting Dataflow job...")
    logging.info(f"Command: {' '.join(cmd)}")

    try:
        # Run the command
        result = subprocess.run(cmd, capture_output=True, text=True)

        if result.returncode == 0:
            logging.info("Dataflow job submitted successfully!")
            logging.info("Job output:")
            print(result.stdout)
        else:
            logging.error("Dataflow job failed!")
            logging.error("Error output:")
            print(result.stderr)
            return False

    except Exception as e:
        logging.error(f"Error running Dataflow job: {e}")
        return False

    return True


def main():
    """Main function"""
    logging.basicConfig(level=logging.INFO)

    print("=" * 60)
    print("Privacy Guardian - Dataflow Job Runner")
    print("=" * 60)
    print(f"Project: {PROJECT_ID}")
    print(f"Region: {REGION}")
    print(f"Input: {INPUT_FILE_PATH}")
    print(f"Output: {BIGQUERY_TABLE}")
    print("=" * 60)

    # Confirm before running
    response = input("Do you want to run the Dataflow job? (y/n): ")
    if response.lower() != 'y':
        print("Job cancelled.")
        return

    success = run_dataflow_job()

    if success:
        print("\n✅ Dataflow job submitted successfully!")
        print("You can monitor the job in the Google Cloud Console:")
        print(
            f"https://console.cloud.google.com/dataflow/jobs/{REGION}?project={PROJECT_ID}")
    else:
        print("\n❌ Dataflow job submission failed!")
        sys.exit(1)


if __name__ == "__main__":
    main()
