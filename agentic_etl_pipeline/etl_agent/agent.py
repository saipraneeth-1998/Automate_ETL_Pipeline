import json
import boto3
from datetime import datetime
import time
import math

# -------------------------------
# AWS Clients
# -------------------------------
s3 = boto3.client("s3")
glue = boto3.client("glue")
databrew = boto3.client("databrew")
athena = boto3.client("athena", region_name="us-east-1")
bedrock_client = boto3.client("bedrock-runtime", region_name="us-east-1")

# -------------------------------
# ETL Config
# -------------------------------
BRONZE_BUCKET = "datalakenewai"
BRONZE_PREFIX = "Bronze/"
SILVER_BUCKET = "datalakenewai"
SILVER_PREFIX = "Silver/"
GOLD_BUCKET = "datalakenewai"
GOLD_PREFIX = "Gold/"
META_BUCKET = "datalakenewai"
META_PREFIX = "meta-data/"

BRONZE_CRAWLER = "bronze_crawler"
SILVER_CRAWLER = "silver_crawler"
GOLD_CRAWLER = "gold_crawler"

BRONZE_TO_SILVER_JOB = "bronze-silver-etl-job"
SILVER_TO_GOLD_JOB = "silver-gold"

# -------------------------------
# Athena / Bedrock Config
# -------------------------------
ATHENA_OUTPUT = "s3://datalakenewai/meta-data/athena-results/"
GOLD_DB = "gold_db"
TABLE_NAME = "gold"

MODEL_ID = "amazon.nova-pro-v1:0"
SYSTEM_PROMPT = """
You are an analytics assistant. Only respond in JSON:
{
  "action": "chat" | "query",
  "sql": "<Athena SQL query, optional>",
  "reply": "<Human-readable response>"
}
"""
TABLE_SCHEMA = """
Table: gold
Columns: brand, model, color, memory, storage, rating, selling_price, original_price, profit
"""

# -------------------------------
# Few-Shot Examples (In-Memory)
# -------------------------------
examples = [
    {"example_input_question": "What are the top 5 selling phones?", "brand": "Apple", "model": "iPhone 14", "profit": "100"},
    {"example_input_question": "Which laptops have highest profit?", "brand": "Dell", "model": "XPS 13", "profit": "300"},
    {"example_input_question": "Most profitable smartphones?", "brand": "Samsung", "model": "Galaxy S23", "profit": "150"},
    {"example_input_question": "Top rated laptops?", "brand": "HP", "model": "Spectre x360", "profit": "300"}
]

# -------------------------------
# Bedrock Embeddings
# -------------------------------
def get_embedding(text):
    response = bedrock_client.invoke_model(
        modelId="amazon.titan-embed-g1-text-02",
        contentType="application/json",
        body=json.dumps({"inputText": text})  # Correct Bedrock schema
    )
    result = json.loads(response["body"].read())
    return result["embedding"]

# Precompute embeddings
example_vectors = [get_embedding(ex["example_input_question"]) for ex in examples]

# -------------------------------
# Helper Functions
# -------------------------------
def bronze_has_data():
    resp = s3.list_objects_v2(Bucket=BRONZE_BUCKET, Prefix=BRONZE_PREFIX)
    return resp.get("KeyCount", 0) > 0

def trigger_glue_crawler(crawler_name):
    try:
        glue.start_crawler(Name=crawler_name)
        print(f"Triggered Glue Crawler: {crawler_name}")
    except glue.exceptions.CrawlerRunningException:
        print(f"Crawler '{crawler_name}' already running.")

def run_databrew_job(job_name):
    resp = databrew.start_job_run(Name=job_name)
    run_id = resp["RunId"]
    print(f"Started DataBrew Job: {job_name}, RunId: {run_id}")
    return run_id

def log_job_meta(job_name, run_id, status, table_name=None):
    meta = {
        "job_name": job_name,
        "job_run_id": run_id,
        "table_name": table_name or "NA",
        "status": status,
        "timestamp": datetime.utcnow().isoformat()
    }
    key = f"{META_PREFIX}logs/{job_name}_{run_id}_{table_name or 'NA'}.json"
    s3.put_object(Bucket=META_BUCKET, Key=key, Body=json.dumps(meta))

def cosine_sim(v1, v2):
    dot = sum(a*b for a,b in zip(v1,v2))
    norm1 = math.sqrt(sum(a*a for a in v1))
    norm2 = math.sqrt(sum(b*b for b in v2))
    return dot/(norm1*norm2) if norm1 and norm2 else 0

def get_top_few_shot(user_question, top_k=2):
    user_vec = get_embedding(user_question)
    sims = [cosine_sim(user_vec, vec) for vec in example_vectors]
    top_indices = sorted(range(len(sims)), key=lambda i: sims[i], reverse=True)[:top_k]
    return [examples[i] for i in top_indices]

# -------------------------------
# Athena Query
# -------------------------------
def query_athena(sql_query):
    resp = athena.start_query_execution(
        QueryString=sql_query,
        QueryExecutionContext={"Database": GOLD_DB},
        ResultConfiguration={"OutputLocation": ATHENA_OUTPUT}
    )
    qid = resp["QueryExecutionId"]
    while True:
        status = athena.get_query_execution(QueryExecutionId=qid)["QueryExecution"]["Status"]["State"]
        if status in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        time.sleep(1)
    if status != "SUCCEEDED":
        return []
    result = athena.get_query_results(QueryExecutionId=qid)
    headers = [col["Label"] for col in result["ResultSet"]["ResultSetMetadata"]["ColumnInfo"]]
    rows = []
    for row in result["ResultSet"]["Rows"][1:]:
        values = [cell.get("VarCharValue","") for cell in row["Data"]]
        rows.append(dict(zip(headers, values)))
    return rows

def deduplicate_rows(rows):
    seen = set()
    deduped = []
    for r in rows:
        key = (r.get("brand"), r.get("model"), r.get("profit"))
        if key not in seen:
            deduped.append(r)
            seen.add(key)
    return deduped

# -------------------------------
# LLM SQL Generation
# -------------------------------
def ask_llm_generate_sql(user_question):
    prompt = f"""
{SYSTEM_PROMPT}

Table schema:
{TABLE_SCHEMA}

Few-shot examples:
{json.dumps(get_top_few_shot(user_question))}

User question: {user_question}

Output format:
{{
    "action": "query",
    "sql": "<SQL query based on question>",
    "reply": "<Optional human-readable text>"
}}
"""
    try:
        resp = bedrock_client.converse(
            modelId=MODEL_ID,
            messages=[{"role":"user","content":[{"text":prompt}]}],
            inferenceConfig={"maxTokens":512,"temperature":0}
        )
        text = resp["output"]["message"]["content"][0]["text"]
        try:
            return json.loads(text)
        except:
            return {"action":"chat","sql":"","reply":text}
    except Exception as e:
        return {"action":"chat","sql":"","reply":f"Error calling LLM: {e}"}

# -------------------------------
# ETL Pipeline
# -------------------------------
def etl_pipeline_async():
    if not bronze_has_data():
        return {"reply":"No Bronze data to process."}
    trigger_glue_crawler(BRONZE_CRAWLER)
    run_id_silver = run_databrew_job(BRONZE_TO_SILVER_JOB)
    log_job_meta(BRONZE_TO_SILVER_JOB, run_id_silver, "STARTED", "silver_table")
    trigger_glue_crawler(SILVER_CRAWLER)
    run_id_gold = run_databrew_job(SILVER_TO_GOLD_JOB)
    log_job_meta(SILVER_TO_GOLD_JOB, run_id_gold, "STARTED", "gold_table")
    trigger_glue_crawler(GOLD_CRAWLER)
    log_job_meta(GOLD_CRAWLER, "NA", "STARTED")
    return {"reply":"ETL triggered asynchronously.","run_ids":{"silver":run_id_silver,"gold":run_id_gold}}


