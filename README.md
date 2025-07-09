# Privacy Guardian

A privacy-focused data anonymization solution using Google Cloud DLP (Data Loss Prevention) API and Apache Beam/Dataflow for scalable data processing.

## Features

- Automatic detection and anonymization of sensitive data using Google Cloud DLP
- Support for both local anonymization and cloud-based processing with Dataflow
- Handles various sensitive data types (PII, PHI, financial data, etc.)
- Scalable processing for large datasets using Apache Beam
- Detailed anonymization reports and logging

## Setup Instructions

### Prerequisites

1. Python 3.8+ installed
2. Google Cloud SDK installed
3. A Google Cloud Project with:
   - Billing enabled
   - Required APIs enabled:
     - Cloud Data Loss Prevention API
     - Dataflow API
     - Cloud Storage API
     - BigQuery API

### Initial Setup

1. Clone the repository:

   ```bash
   git clone <repository-url>
   cd project-personal
   ```

2. Install dependencies:

   ```bash
   pip install -r requirements.txt
   ```

3. Authenticate with Google Cloud:

   ```bash
   # Login with your Google account
   gcloud auth login

   # Set your project
   gcloud config set project YOUR_PROJECT_ID

   # Enable application-default credentials (needed for local development)
   gcloud auth application-default login
   ```

### DLP API Setup

1. Enable the DLP API:

   ```bash
   gcloud services enable dlp.googleapis.com
   ```

For local development and testing, the above setup using `gcloud auth login` and `gcloud auth application-default login` is sufficient. The code will automatically use your user credentials.

For production environments or when running on GCP services, you may want to use a service account instead:

1. Create a service account (optional, for production use):

   ```bash
   # Create service account
   gcloud iam service-accounts create dlp-sa --display-name="DLP Service Account"

   # Grant DLP User role
   gcloud projects add-iam-policy-binding YOUR_PROJECT_ID \
       --member="serviceAccount:dlp-sa@YOUR_PROJECT_ID.iam.gserviceaccount.com" \
       --role="roles/dlp.user"
   ```

2. If using service account, create and download key (optional):

   ```bash
   gcloud iam service-accounts keys create dlp-key.json \
       --iam-account=dlp-sa@YOUR_PROJECT_ID.iam.gserviceaccount.com

   # Set environment variable if using service account
   export GOOGLE_APPLICATION_CREDENTIALS=path/to/dlp-key.json  # Linux/Mac
   set GOOGLE_APPLICATION_CREDENTIALS=path/to/dlp-key.json     # Windows
   ```

### Other GCP Services Setup

1. Enable remaining APIs:

   ```bash
   gcloud services enable dataflow.googleapis.com storage.googleapis.com bigquery.googleapis.com
   ```

2. Set up Cloud Storage:

   ```bash
   # Create a bucket for data
   gsutil mb -l us-central1 gs://YOUR_BUCKET_NAME

   # Upload sample data
   gsutil cp sample_data.csv gs://YOUR_BUCKET_NAME/
   ```

3. Create BigQuery dataset:
   ```bash
   bq mk --dataset anonymized_sample_data
   ```

### Configuration

1. Update `privacy_guardian/dataflow_config.py` with your GCP settings:
   - PROJECT_ID
   - BUCKET_NAME
   - DATASET_ID
   - Other configuration as needed

## Usage

The project supports two modes of operation:

### 1. Local Anonymization with DLP

This mode uses the DLP API to anonymize data locally and save it to a new file:

```python
from privacy_guardian.anonymizer import Anonymizer

# Initialize anonymizer
anonymizer = Anonymizer()

# Anonymize data
result = anonymizer.anonymize_dataset(
    input_file="sample_data.csv",
    output_file="anonymized_data.csv"
)

# Check what was detected and anonymized
print(f"Sensitive fields detected: {result['sensitive_fields_detected']}")
print(f"Fields anonymized: {result['sensitive_fields_names']}")
```

The DLP API will automatically:

- Detect 50+ types of sensitive data including:
  - Personal identifiers (names, emails, phone numbers, DOB)
  - Financial data (credit cards, bank accounts)
  - Healthcare data (medical records, diagnoses)
  - Government IDs (passports, licenses)
- Apply appropriate anonymization based on data type
- Track what was changed and why

### 2. Cloud Dataflow Processing (Large-Scale)

This mode processes data using Cloud Dataflow for scalable anonymization:

```bash
python run_dataflow_job.py
```

The Dataflow job will:

1. Read data from Cloud Storage
2. Process it through DLP API
3. Write results to BigQuery

**Note:** The agent integration for automated pipeline management is currently under development. For now, use `run_dataflow_job.py` to manually trigger Dataflow jobs.

## Sample Data Generation

You can generate sample test data using:

```bash
python generate_sample_data.py
```

## Project Structure

- `privacy_guardian/` - Main package with DLP and Dataflow implementation
- `run_dataflow_job.py` - Script to run the Dataflow pipeline
- `generate_sample_data.py` - Generate sample test data
- `setup.py` - Package setup for Dataflow
- `requirements.txt` - Project dependencies

## Security Notes

1. Never commit sensitive data or credentials
2. Use service account keys with minimum required permissions
3. Store credentials securely (use environment variables or secret management)
4. Monitor DLP and Dataflow costs
5. DLP API Best Practices:
   - Use minimum required permissions for service accounts
   - Regularly rotate service account keys
   - Monitor DLP findings in Cloud Audit Logs
   - Set up alerts for unusual DLP activity
   - Use VPC Service Controls if needed

## Troubleshooting

1. If Dataflow job fails:

   - Check Cloud Console for job logs
   - Verify all APIs are enabled
   - Ensure service account has necessary permissions

2. If DLP API fails:
   - Verify project has billing enabled
   - Check API quotas and limits
   - Ensure data format matches expected schema
   - Verify GOOGLE_APPLICATION_CREDENTIALS is set correctly
   - Check service account permissions
   - Look for errors in Cloud Audit Logs

## Contributing

1. Fork the repository
2. Create a feature branch
3. Submit a pull request

## License

[Your License Here]
