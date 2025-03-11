# Data Squid: de-tote-bag-data-transformation

## Project Overview
Team members: Carlos Byrne, Liam Biggar, Nicolas Tolksdorf, Shay Doherty, Girish Joshi and Ethan Labouchardiere.

This is a three-week group project completed as part of the Northcoders Data Engineering Bootcamp. The project implements a data pipeline to extract, transform, and load (ETL) data from an operational database (`totesys`) into an AWS-based data lake and data warehouse. The goal is to create a robust, automated, and scalable data platform that supports analytical reporting and business intelligence.

This project implements a data pipeline to extract, transform, and load (ETL) data from an operational database (`totesys`) into an AWS-based data lake and data warehouse. The goal is to create a robust, automated, and scalable data platform that supports analytical reporting and business intelligence.

## Architecture

The architecture consists of the following key components:

* **Source Database (`totesys`):** A simulated operational database containing transactional data.
* **Data Lake (S3):** Two S3 buckets:
    * `ingestion-bucket`: Stores raw data extracted from the `totesys` database.
    * `processed-bucket`: Stores transformed data in Parquet format, ready for loading into the data warehouse.
* **Data Warehouse (AWS):** A relational data warehouse hosted in AWS, designed with three star schemas (Sales, Purchases, Payments). Our project focussed on implementing the Sales star schema as a Minimum Viable Product (MVP).
* **AWS Lambda Functions:** Python-based Lambda functions for:
    * Data extraction from the `totesys` database.
    * Data transformation and remodeling.
    * Data loading into the data warehouse.
* **AWS EventBridge:** Used for scheduling and orchestrating the data pipeline.
* **AWS CloudWatch:** Used for logging, monitoring, and alerting.
* **GitHub Actions:** For continuous integration and continuous deployment (CI/CD).
* **Terraform:** For infrastructure as code (IaC).

![img](./mvp.png)

## Getting Started

### Prerequisites

* AWS account with appropriate permissions.
* Python 3.12
* Terraform
* GitHub account

### Setup

1.  **Clone the Repository:**

    ```bash
    git clone https://github.com/lbiggar/de-tote-bag-data-transformation.git
    cd de-tote-bag-data-transformation
    ```

2.  **Create a Virtual Environment (Recommended):**

    ```bash
    python3 -m venv venv
    source venv/bin/activate
    ```

3.  **Install Dependencies:**

    ```bash
    pip install -r requirements.txt
    ```

4.  **Configure AWS Credentials:**

    * Set up AWS credentials in your environment or configure them using AWS CLI.
    * Store AWS credentials as GitHub Secrets (`AWS_REGION`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`) for CI/CD.

5.  **Terraform Initialization:**

    ```bash
    cd terraform
    terraform init
    ```

6.  **Deploy Infrastructure:**

    ```bash
    terraform apply -auto-approve
    ```

7.  **Configure GitHub Actions:**

    * Set up GitHub Secrets in your repository settings.
    * Push code to the `main` branch to trigger the CI/CD pipeline.

### Project Structure
```
./
├── bin/                       
├── Makefile                   # Automation file for build, test, and deployment tasks.
├── mvp.png                    # Minimum Viable Product diagram.
├── README.md                  # Project documentation with instructions and information.
├── requirements-lambda.txt    # Python dependencies specifically for Lambda functions.
├── requirements.txt           # General Python dependencies for the project.
├── src/                       # Source code directory.
│   ├── extraction_lambda/     
│   │   └── main.py                 # Python code for the extraction Lambda function.
│   ├── load_lambda/           
│   │   └── main.py                 # Python code for the loading Lambda function.
│   └── transform_lambda/      
│       └── main.py            # Python code for the transformation Lambda function.
├── terraform/                 # Infrastructure as Code (IaC) directory containing Terraform configurations for:
│   ├── cloudwatch.tf               # AWS CloudWatch resources (logging, monitoring).
│   ├── events.tf                   # AWS EventBridge resources (scheduling).
│   ├── iam.tf                      # AWS IAM resources (permissions).
│   ├── lambda_layers.tf            # AWS Lambda layers (dependencies).
│   ├── lambda.tf                   # AWS Lambda functions.
│   ├── provider.tf                 # the AWS provider.
│   ├── s3.tf                       # AWS S3 buckets (data lake).
│   ├── sns.tf                      # AWS SNS resources (notifications).
│   └── vars.tf                     # Terraform variable definitions.
├── tests/                     # Test directory for unit and integration tests.
│   ├── extraction_tests/     
│   │   └── test_extraction.py      # Python unit tests for the extraction Lambda.
│   ├── load_tests/            
│   │   └── test_load_utils.py      # Python unit tests for the loading Lambda utilities.
│   └── transform_tests/       
│       └── test_transform_utils.py # Python unit tests for the transformation Lambda utilities.
└── utils/                     # Utility functions directory
    └── lambda_utils.py             # Python utility functions used across Lambda functions.
```
### Running Tests

```bash
make all
CI/CD Pipeline
The CI/CD pipeline is implemented using GitHub Actions. It automates the following processes:

Code Checks: Linting, unit tests, security scans, and code formatting.
Lambda Deployment: Deployment of Lambda functions and related infrastructure using Terraform.
Data Ingestion
The data ingestion process extracts data from the totesys database and uploads it to the ingestion-bucket in S3. It supports both initial and continuous data extraction.

Data Transformation
The data transformation process remodels the data into the data warehouse schema and stores it in Parquet format in the processed-bucket in S3.

Data Loading
The data loading process loads the transformed data from S3 into the data warehouse.

Data Visualization
Data visualization is performed using tools like AWS QuickSight or similar BI tools.

Monitoring and Logging
AWS CloudWatch is used for monitoring the data pipeline and logging events.

Security
AWS IAM roles and policies are used to control access to AWS resources.
GitHub Secrets are used to store sensitive information.
bandit and pip-audit are used for security vulnerability checks.
Future Enhancements
Implement data quality checks.
Enhance monitoring and alerting.
Add more data sources.
Improve data visualization capabilities.
Implement data lineage tracking.
