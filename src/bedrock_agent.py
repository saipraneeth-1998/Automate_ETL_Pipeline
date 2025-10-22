import boto3
import time
import json
from datetime import datetime

# -------------------------------
# AWS Clients
# -------------------------------
s3 = boto3.client("s3")
glue = boto3.client("glue")
databrew = boto3.client("databrew")
athena = boto3.client("athena")
bedrock = boto3.client("bedrock-runtime")  # LLM client

# -------------------------------
# Configuration
# -------------------------------
BRONZE_BUCKET = "datalakenewai"
BRONZE_PREFIX = "Bronze/"

SILVER_DB = "silver_db"
GOLD_DB = "gold_db"

GOLD_BUCKET = "datalakenewai"
GOLD_PREFIX = "Gold/"

META_BUCKET = "datalakenewai"
META_PREFIX = "meta-data/"

ATHENA_OUTPUT = "s3://datalakenewai/meta-data/athena-results/"


# Crawlers
BRONZE_CRAWLER = "bronze_crawler"
SILVER_CRAWLER = "silver-crawler"
GOLD_CRAWLER = "gold_crawler"

# DataBrew Jobs
BRONZE_TO_SILVER_JOB = "bronze-silver-etl-job"
SILVER_TO_GOLD_JOB = "silver-gold"

# -------------------------------
# System Prompt for LLM
# -------------------------------
SYSTEM_PROMPT = """
You are a highly skilled Business Analyst with expertise in data analytics, sales, customer behavior, and e-commerce metrics. 
Your task is to interpret user requests, generate insights from the provided data (Athena tables or other sources), and respond in a clear, professional, and actionable manner.

Guidelines:
1. Determine if the request needs ETL, Athena query, or insight explanation.
2. Provide actionable insights in plain business language.
3. Translate questions into Athena-compatible SQL if needed.
4. Ask clarifying questions if the data source is unclear.
5. Summarize insights in 1-3 sentences.
"""

# -------------------------------
# Helper Functions
# -------------------------------
def bronze_has_data():
    resp = s3.list_objects_v2(Bucket=BRONZE_BUCKET, Prefix=BRONZE_PREFIX)
    count = resp.get("KeyCount", 0)
    print(f"Found {count} files in Bronze layer.")
    return count > 0

def trigger_glue_crawler(crawler_name):
    glue.start_crawler(Name=crawler_name)
    print(f"Crawler {crawler_name} triggered.")
    while True:
        status = glue.get_crawler(Name=crawler_name)["Crawler"]["State"]
        if status == "READY":
            break
        print(f"Waiting for crawler {crawler_name} to finish...")
        time.sleep(5)
    print(f"Crawler {crawler_name} finished.")

def run_databrew_job(job_name):
    resp = databrew.start_job_run(Name=job_name)
    run_id = resp["RunId"]
    print(f"DataBrew Job {job_name} started with RunId: {run_id}")
    return run_id

def wait_for_databrew_job(job_name, run_id):
    while True:
        resp = databrew.describe_job_run(Name=job_name, RunId=run_id)
        status = resp.get("State")
        if status in ("SUCCEEDED", "FAILED", "CANCELLED"):
            print(f"DataBrew Job {job_name} completed with status: {status}")
            return status
        print(f"Waiting for DataBrew job {job_name} to finish...")
        time.sleep(10)

def log_job_meta(job_name, run_id, status, table_name=None):
    meta = {
        "job_name": job_name,
        "job_run_id": run_id,
        "table_name": table_name,
        "status": status,
        "timestamp": datetime.utcnow().isoformat()
    }
    key = f"{META_PREFIX}logs/{job_name}_{run_id}_{table_name or 'NA'}.json"
    s3.put_object(Bucket=META_BUCKET, Key=key, Body=json.dumps(meta))
    print(f"Logged metadata to {META_BUCKET}/{key}")

def list_silver_tables():
    tables_resp = glue.get_tables(DatabaseName=SILVER_DB)
    tables = [t['Name'] for t in tables_resp['TableList']]
    print(f"Silver tables: {tables}")
    return tables

# -------------------------------
# ETL Pipeline
# -------------------------------
def etl_pipeline_databrew():
    if not bronze_has_data():
        return "No Bronze data to process."

    print("Triggering Bronze crawler...")
    trigger_glue_crawler(BRONZE_CRAWLER)

    print("Running Bronze → Silver DataBrew job...")
    run_id_silver = run_databrew_job(BRONZE_TO_SILVER_JOB)
    status_silver = wait_for_databrew_job(BRONZE_TO_SILVER_JOB, run_id_silver)
    log_job_meta(BRONZE_TO_SILVER_JOB, run_id_silver, status_silver)
    if status_silver != "SUCCEEDED":
        return "Silver ETL failed."

    print("Triggering Silver crawler...")
    trigger_glue_crawler(SILVER_CRAWLER)

    silver_tables = list_silver_tables()
    if not silver_tables:
        return "No Silver tables found."

    for table in silver_tables:
        print(f"Running Silver → Gold DataBrew job for {table}")
        run_id_gold = run_databrew_job(SILVER_TO_GOLD_JOB)
        status_gold = wait_for_databrew_job(SILVER_TO_GOLD_JOB, run_id_gold)
        log_job_meta(SILVER_TO_GOLD_JOB, run_id_gold, status_gold, table_name=table)

    print("Triggering Gold crawler...")
    trigger_glue_crawler(GOLD_CRAWLER)

    return "ETL pipeline completed successfully."

# -------------------------------
# Athena Queries
# -------------------------------
def query_athena(sql_query, database=GOLD_DB):
    resp = athena.start_query_execution(
        QueryString=sql_query,
        QueryExecutionContext={"Database": database},
        ResultConfiguration={"OutputLocation": ATHENA_OUTPUT}
    )
    qid = resp["QueryExecutionId"]
    while True:
        status = athena.get_query_execution(QueryExecutionId=qid)["QueryExecution"]["Status"]["State"]
        if status in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        time.sleep(2)

    if status != "SUCCEEDED":
        return {"error": status}

    results = athena.get_query_results(QueryExecutionId=qid)
    header = [col["Label"] for col in results["ResultSet"]["ResultSetMetadata"]["ColumnInfo"]]
    rows = []
    for row in results["ResultSet"]["Rows"][1:]:
        rows.append({header[i]: row["Data"][i].get("VarCharValue", "") for i in range(len(header))})
    return rows

# -------------------------------
# LLM Integration
# -------------------------------
def ask_llm(user_message):
    prompt = f"{SYSTEM_PROMPT}\nUser: {user_message}\nResponse:"
    response = bedrock.invoke_model(
        ModelId="amazon.nova-pro-v1:0",  # example LLM, replace with your model
        Body=json.dumps({
            "inputText": prompt,
            "maxTokensToSample": 500
        }),
        ContentType="application/json"
    )
    output = json.loads(response['Body'].read())['completion']
    return output

# -------------------------------
# User Request Processor
# -------------------------------
def process_user_request(message):
    message_lower = message.lower()
    if "run etl" in message_lower or "start pipeline" in message_lower:
        return etl_pipeline_databrew()
    elif "top customers" in message_lower or "sales" in message_lower:
        # For any data-driven question, query Athena first
        sql = f"SELECT * FROM top_customers LIMIT 5"  # Replace with dynamic SQL if needed
        results = query_athena(sql)
        return results
    elif "show tables" in message_lower:
        return {"silver_tables": list_silver_tables()}
    else:
        # Otherwise, forward to LLM for analysis/insight
        return ask_llm(message)

# -------------------------------
# Lambda Handler Example
# -------------------------------
def lambda_handler(event, context):
    user_message = event.get("message", "Show me top customers")
    response = process_user_request(user_message)
    return {
        "statusCode": 200,
        "body": json.dumps(response)
    }
