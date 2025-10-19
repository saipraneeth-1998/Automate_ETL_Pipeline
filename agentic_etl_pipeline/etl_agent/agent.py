import boto3
import os
import json
import time
from botocore.exceptions import ClientError

# -------------------------
# Environment Variables
# -------------------------
GLUE_JOBS = os.environ.get("GLUE_JOBS", "").split(",")
APPFLOWS = os.environ.get("APPFLOWS", "").split(",")
DMS_TASKS = os.environ.get("DMS_TASKS", "").split(",")
BRONZE_BUCKET = os.environ.get("BRONZE_BUCKET")
SILVER_BUCKET = os.environ.get("SILVER_BUCKET")
GOLD_BUCKET = os.environ.get("GOLD_BUCKET")
HUBSPOT_SECRET_ARN = os.environ.get("HUBSPOT_SECRET_ARN")
BIGQUERY_SECRET_ARN = os.environ.get("BIGQUERY_SECRET_ARN")
RDS_SECRET_ARN = os.environ.get("RDS_SECRET_ARN")
BEDROCK_MODEL = os.environ.get("BEDROCK_MODEL", "amazon.titan-text")

# -------------------------
# Boto3 Clients
# -------------------------
glue = boto3.client("glue")
appflow = boto3.client("appflow")
dms = boto3.client("dms")
secretsmanager = boto3.client("secretsmanager")
bedrock = boto3.client("bedrock")
qbusiness = boto3.client("qbusiness")
cloudwatch = boto3.client("cloudwatch")
sns = boto3.client("sns")  # optional for notifications

# -------------------------
# Helper Functions
# -------------------------

def get_secret(secret_arn):
    """Fetch secret value from Secrets Manager"""
    try:
        response = secretsmanager.get_secret_value(SecretId=secret_arn)
        secret = response.get("SecretString")
        return json.loads(secret)
    except ClientError as e:
        print(f"Error fetching secret {secret_arn}: {e}")
        return None

def trigger_appflow(flow_name):
    """Start an AppFlow flow"""
    try:
        print(f"Starting AppFlow: {flow_name}")
        appflow.start_flow(flowName=flow_name)
    except ClientError as e:
        print(f"Failed to start AppFlow {flow_name}: {e}")
        return False
    return True

def trigger_dms(task_arn):
    """Start DMS replication task"""
    try:
        print(f"Starting DMS task: {task_arn}")
        dms.start_replication_task(
            ReplicationTaskArn=task_arn,
            StartReplicationTaskType='reload-target'
        )
    except ClientError as e:
        print(f"Failed to start DMS task {task_arn}: {e}")
        return False
    return True

def run_glue_job(job_name):
    """Start Glue ETL job and wait until completion"""
    try:
        print(f"Starting Glue job: {job_name}")
        response = glue.start_job_run(JobName=job_name)
        job_run_id = response['JobRunId']
        # Polling for job completion
        while True:
            status = glue.get_job_run(JobName=job_name, RunId=job_run_id)['JobRun']['JobRunState']
            print(f"Glue job {job_name} status: {status}")
            if status in ['SUCCEEDED', 'FAILED', 'STOPPED']:
                break
            time.sleep(30)
        if status != 'SUCCEEDED':
            print(f"Glue job {job_name} failed: {status}")
            return False
    except ClientError as e:
        print(f"Failed to start Glue job {job_name}: {e}")
        return False
    return True

def run_crawler(crawler_name):
    """Run Glue crawler"""
    try:
        print(f"Starting crawler: {crawler_name}")
        glue.start_crawler(Name=crawler_name)
    except ClientError as e:
        print(f"Failed to start crawler {crawler_name}: {e}")

def fallback_q_check():
    """Trigger Amazon Q fallback logic"""
    try:
        print("Running Amazon Q fallback check")
        response = qbusiness.ChatSync(
            QueryText="Check ETL fallback status",
            BotId="default",
            SessionId=str(int(time.time()))
        )
        print("Amazon Q fallback response:", response)
    except ClientError as e:
        print(f"Amazon Q fallback failed: {e}")

def invoke_bedrock(prompt_text):
    """Invoke Bedrock model for insights"""
    try:
        print("Invoking Bedrock model...")
        response = bedrock.invoke_model(
            ModelId=BEDROCK_MODEL,
            Body=json.dumps({"inputText": prompt_text}),
            ContentType="application/json"
        )
        result = json.loads(response['Body'].read().decode())
        print("Bedrock output:", result)
    except ClientError as e:
        print(f"Bedrock invocation failed: {e}")

# -------------------------
# Lambda Handler
# -------------------------
def handler(event, context):
    print("Starting Strands orchestration pipeline...")

    # Fetch secrets
    hubspot_creds = get_secret(HUBSPOT_SECRET_ARN)
    bigquery_creds = get_secret(BIGQUERY_SECRET_ARN)
    rds_creds = get_secret(RDS_SECRET_ARN)

    if not hubspot_creds or not bigquery_creds or not rds_creds:
        print("One or more secrets not available, triggering fallback.")
        fallback_q_check()
        return {"status": "failed", "reason": "Missing credentials"}

    # Step 1: Trigger AppFlow flows
    for flow in APPFLOWS:
        if not trigger_appflow(flow):
            fallback_q_check()

    # Step 2: Trigger DMS tasks
    for task in DMS_TASKS:
        if not trigger_dms(task):
            fallback_q_check()

    # Step 3: Run Glue jobs
    for job in GLUE_JOBS:
        if not run_glue_job(job):
            fallback_q_check()

    # Step 4: Run Glue crawlers for schema update
    for crawler in ["bronze-crawler", "silver-crawler", "gold-crawler"]:
        run_crawler(f"{crawler}-{os.environ.get('Environment', 'dev')}")

    # Step 5: Invoke Bedrock chatbot for insights
    invoke_bedrock("Generate summary of curated gold layer data")

    print("Pipeline completed successfully")
    return {"status": "success"}
