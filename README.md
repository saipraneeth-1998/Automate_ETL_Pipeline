# SmartETL – AI-Driven Automation for Data Operations

**Live Demo URL:** [http://54.221.181.57:8501](http://54.221.181.57:8501)  

Managing large-scale ETL pipelines manually is time-consuming, error-prone, and often lacks real-time insights. SmartETL automates the movement, transformation, and analysis of data while optionally allowing business users to query results using natural language.

---

## Inspiration
Data teams spend a lot of time on repetitive ETL tasks like cleaning, transforming, and aggregating data. SmartETL was created to:

- Automate the entire data pipeline process using AWS services.
- Allow business users to query results via a chatbot interface (optional).

---

## What It Does
SmartETL automates the following workflow:

1. **Bronze Layer (Raw Data)** – Upload raw data to S3.
2. **Silver Layer (Cleaned/Transformed Data)** – Transform with AWS DataBrew.
3. **Gold Layer (Analytics-Ready Data)** – Aggregate and prepare data for analysis.
4. **AWS Glue Catalog** – Crawlers update table schemas for each layer.
5. **Athena Queries** – Execute queries on Gold layer.
6. **Optional AI Queries** – Use AWS Lex or Bedrock LLM for natural language queries.

---

## Architecture Overview

---

## AWS Services Used
- S3 – Storage for Bronze, Silver, Gold layers  
- Glue – Crawlers for schema discovery  
- DataBrew – ETL jobs for transformations  
- Athena – Query analytics-ready data  
- Lambda – Orchestrates ETL workflow  
- EventBridge – Trigger ETL jobs automatically  
- Lex / Bedrock – Optional natural language interface

---

## Challenges & Solutions
- **Lambda Timeouts:** Large datasets caused timeouts → increased timeout + CloudWatch monitoring  
- **IAM Permissions:** Fine-grained access needed → configured roles for S3, Glue, DataBrew, Athena, LLM  
- **Crawler Sequencing:** Ensured correct execution order to prevent missing tables  

---

## Accomplishments
- Fully automated ETL pipeline from ingestion to Gold layer  
- Metadata logging (job IDs, statuses) for ETL jobs  
- AI-driven chatbot integration for non-technical users  

---

## Next Steps
- Add real-time streaming ingestion (Kinesis/SQS)  
- Integrate dashboards for analytics  
- Extend chatbot with automated insights for KPIs  

---

## Setup Instructions

### Prerequisites
- AWS Account  
- S3 Bucket for ETL layers  
- IAM Role with permissions for: S3, Glue, DataBrew, Athena, Lambda, Bedrock  

---

### Deployment Steps

#### 1. Upload Raw Data
Place files into:  

#### 2. Configure Glue Crawlers
- Bronze → Silver → Gold  
- Set corresponding databases (`silver_db`, `gold_db`)  

#### 3. Create DataBrew Jobs
- Bronze → Silver: `bronze-silver-etl-job`  
- Silver → Gold: `silver-gold-etl-job`  

#### 4. Deploy Lambda Function
- Paste SmartETL Lambda code  
- Assign IAM role  
- Increase timeout (15+ min) & memory (3GB)  

**Test Event Example:**
```json
{
    "action": "etl"
}
{
    "action": "query",
    "user_message": "top sellers"
}
