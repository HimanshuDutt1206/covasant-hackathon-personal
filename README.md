# Privacy Guardian Agent - Google ADK Integration

A sophisticated privacy-preserving data anonymization system built with Google Agent Development Kit (ADK).

## 🚀 Project Overview

The Privacy Guardian Agent is an advanced system that protects sensitive data through intelligent anonymization while preserving data utility for analysis. It implements policy-based transformations for various data types including PII, PHI, PCI, and financial information.

## 📁 Project Structure

```
project/
├── multi_tool_agent/           # Google ADK Agent
│   ├── __init__.py            # Package initialization
│   ├── agent.py               # Main agent implementation
│   └── .env                   # Environment configuration
├── anonymizer.py              # Core anonymization logic
├── sample_data.csv            # Test dataset
├── anonymized_data.csv        # Anonymized output
└── anonymization_report.txt   # Processing report
```

## 🛠️ Setup Instructions

GOOGLE_GENAI_USE_VERTEXAI=FALSE
GOOGLE_API_KEY=your-api-key
in .env in multi_tool_agent

### 1. Install Google ADK

First, install the Google Agent Development Kit:

```bash
pip install google-adk
```

### 2. Get Google API Key

1. Go to [Google AI Studio](https://aistudio.google.com/)
2. Create a project and get your API key
   GOOGLE_GENAI_USE_VERTEXAI=FALSE
   GOOGLE_API_KEY=your-api-key
   in .env in multi_tool_agent

### 3. Install Dependencies

Make sure you have the required Python packages:

```bash
pip install pandas google-cloud-dlp
```

$env:GOOGLE_CLOUD_PROJECT = "your-project-id"
In the terminal at the start. If terminal is refreshed, run again.

## 🎯 Agent Capabilities

The Privacy Guardian Agent provides 3 main tools:

### 1. `anonymize_csv_data(input_file, output_file=None)`

- Anonymizes sensitive data in CSV files using Google DLP API.
- Applies redaction (character masking) to all identified sensitive data.
- Returns processing statistics and transformation log.

### 2. `generate_anonymization_report(input_file, output_file=None, report_file=None)`

- Creates detailed anonymization reports based on DLP processing.
- Shows transformation statistics and documents DLP usage.

### 3. `process_sample_data()`

- Processes the included sample dataset with Google DLP.
- Demonstrates full anonymization workflow.
- Creates both anonymized data and report.

### 4. `get_data_preview(file_path, rows=5)`

- Safely previews CSV file contents with sensitive data redacted by DLP.
- Shows data structure and column names.
- Limits output for privacy protection.

## 🏃‍♂️ Running the Agent

### Option 1: Terminal Interface (Recommended)

```bash
adk run multi_tool_agent
```

### Option 2: Web UI

```bash
adk web multi_tool_agent
```

### Option 3: API Server

```bash
adk api_server multi_tool_agent
```

## 💬 Example Prompts

Try these prompts when chatting with your agent:

- "Process the sample data and show me the results"
- "Anonymize my data file called 'customer_data.csv'"
- "Generate a report for the processed data"
- "Show me a preview of the anonymized data"
- "Explain how sensitive data is protected"

## 🔒 Privacy Features

### Anonymization Techniques (using Google DLP API)

- **Redaction (Character Masking)**: All identified sensitive data is replaced with asterisks (`*`).

### Data Classifications (detected by Google DLP API)

- **PII**: Person names, email addresses, phone numbers, street addresses, IP addresses, SSNs, passport numbers, driver's license numbers, dates of birth, age, gender, ethnic group, place of birth, national IDs, general locations, domain names, URLs.
- **PCI**: Credit card numbers, bank account numbers (generic, US specific, IBAN, SWIFT), financial account numbers.
- **PHI**: Medical record numbers, US healthcare IDs, healthcare provider IDs, phone numbers, email addresses, dates, diseases, drug codes, ICD-10 codes.

### Preserved Data

- Data not identified as sensitive by Google DLP will be preserved.

## 📊 Sample Data

The project includes a sample healthcare dataset with 10 patient records containing sensitive information that will be anonymized by Google DLP.

## 🔧 Configuration

Privacy policies are now handled by the Google DLP API. The `anonymizer.py` file contains the list of `InfoTypes` used for detection and redaction.

## 🚀 Getting Started

1. **Setup**: Follow the setup instructions above (including setting your Google Cloud Project ID in environment variables)
2. **Test**: Run the agent with `adk run multi_tool_agent`
3. **Demo**: Try "Process the sample data and show me the results"
4. **Explore**: Use the example prompts to explore all features

## 🎉 Next Steps

Once your agent is running, you can:

- Process your own CSV files with sensitive data.
- Integrate with your data pipeline to automate anonymization.
- Scale for enterprise use leveraging Google Cloud's infrastructure.

---

**Privacy Guardian Agent** - Protecting sensitive data while preserving analytical value.
