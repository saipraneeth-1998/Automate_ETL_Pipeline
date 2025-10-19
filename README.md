# Automate_ETL_Pipeline
Managing large-scale ETL pipelines manually is time-consuming, error-prone, and often lacks real-time insights. We wanted to build a system that automates the movement, transformation, and analysis of data, while also allowing business users to interact with it using natural language

# SmartETL – AI-Driven Automation for Data Operations

## Inspiration
Data teams often spend a lot of time on repetitive ETL tasks like cleaning, transforming, and aggregating data. SmartETL was created to automate the entire data pipeline process using AWS services and optionally allow business users to query results via a chatbot interface.

---

## What it Does
SmartETL automates the following workflow:

1. Ingests raw data into the **Bronze** layer (S3 bucket).
2. Cleans and transforms data using **AWS DataBrew**, storing results in the **Silver** layer.
3. Aggregates and prepares analytics-ready data into the **Gold** layer.
4. Updates AWS **Glue Catalog** with crawlers for each layer.
5. Executes queries on Gold data using **Athena**.
6. (Optional) Allows natural language queries via **AWS Lex / Bedrock LLM**.

---

## How We Built It
- **AWS S3** – Data lake storage for Bronze, Silver, and Gold layers.
- **AWS Glue** – Crawlers for schema discovery and cataloging.
- **AWS DataBrew** – ETL jobs for data transformation and cleaning.
- **AWS Athena** – Query Gold data for analytics and KPIs.
- **AWS Lambda** – Orchestrates ETL workflow and triggers jobs.
- **AWS EventBridge** – Automates pipeline on new data uploads.
- **AWS Lex / Bedrock** – Optional chatbot interface for querying data.

---

## Challenges We Ran Into
- **DataBrew Job Timing** – Large datasets caused Lambda timeouts. Solved by increasing Lambda timeout and monitoring via CloudWatch.
- **Permissions** – Fine-grained IAM permissions were required for S3, Glue, DataBrew, Athena, and LLM services.
- **Crawler Management** – Ensuring crawlers ran in the correct sequence to avoid missing tables.

---

## Accomplishments We're Proud Of
- Fully automated ETL pipeline from raw ingestion to analytics-ready Gold layer.
- Metadata logging for all ETL jobs, including job run IDs and statuses.
- Optional AI-driven chatbot integration for non-technical users to query KPIs in natural language.

---

## What We Learned
- Orchestrating multiple AWS services (S3, Glue, DataBrew, Athena, Lambda) requires careful sequencing and error handling.
- IAM policies must be comprehensive but secure.
- Event-driven architecture (EventBridge → Lambda → ETL jobs) reduces manual intervention and improves automation.

---

## What's Next
- Add real-time streaming ingestion using Kinesis or SQS.
- Integrate dashboards for visual analytics.
- Extend chatbot capabilities to provide automated insights and suggestions based on business KPIs.

---

## Setup Instructions

### Prerequisites
- AWS Account
- S3 Bucket for ETL layers
- IAM Role with permissions for S3, Glue, DataBrew, Athena, Lambda, Bedrock (if using LLM)

### Steps to Deploy
1. **Upload Raw Data**  
   Place your raw data files into `s3://<bucket>/Bronze/`.

2. **Configure Glue Crawlers**  
   - Bronze → Silver → Gold
   - Set corresponding databases (`silver_db`, `gold_db`).

3. **Create DataBrew Jobs**  
   - Bronze → Silver (`bronze-silver-etl-job`)  
   - Silver → Gold (`silver-gold`)

4. **Deploy Lambda Function**  
   - Paste the SmartETL code into Lambda.
   - Assign IAM role with required permissions.
   - Increase timeout (15+ minutes) and memory (3 GB recommended).

5. **Test Lambda**  
   - Test event example:
   ```json
   {
     "message": "run etl"
   }
