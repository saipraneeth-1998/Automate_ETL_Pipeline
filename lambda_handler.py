# glue_bronze_to_silver.py
import sys
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.types import *
import boto3
import json
import re
from datetime import datetime

args = getResolvedOptions(sys.argv, ['JOB_NAME','ENV','BRONZE_BUCKET','SILVER_BUCKET','JOIN_MAP_SSM_KEY'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

s3 = boto3.client('s3')
ssm = boto3.client('ssm')

env = args['ENV']
bronze_bucket = args['BRONZE_BUCKET']
silver_bucket = args['SILVER_BUCKET']

# Optionally fetch join-map JSON from SSM parameter store to define relations:
join_map = {}
if args.get('JOIN_MAP_SSM_KEY'):
    try:
        resp = ssm.get_parameter(Name=args['JOIN_MAP_SSM_KEY'])
        join_map = json.loads(resp['Parameter']['Value'])
    except Exception as e:
        print("No join map found or parse error:", e)

# simple helper to list objects in a prefix
def list_s3_keys(bucket, prefix):
    keys=[]
    kwargs = {'Bucket': bucket, 'Prefix': prefix}
    while True:
        resp = s3.list_objects_v2(**kwargs)
        for obj in resp.get('Contents',[]):
            keys.append(obj['Key'])
        token = resp.get('NextContinuationToken')
        if not token:
            break
        kwargs['ContinuationToken'] = token
    return keys

sources = {
    "hubspot": "hubspot/",
    "bigquery": "bigquery/",
    "rds": "rds/"
}

ingest_date = datetime.utcnow().strftime('%Y-%m-%d')

for src_name, prefix in sources.items():
    s3_path = f"s3://{bronze_bucket}/{prefix}"
    print("Processing source:", src_name, s3_path)
    # Read Bronze (attempt to infer format automatically)
    try:
        df = spark.read.option("recursiveFileLookup","true").parquet(s3_path)
    except Exception as e:
        print("Parquet read failed, trying JSON/CSV:", e)
        try:
            df = spark.read.json(s3_path)
        except:
            try:
                df = spark.read.option("header",True).csv(s3_path)
            except Exception as e2:
                print("Unable to read source:", e2)
                continue

    # basic cleanup: drop columns with all nulls, trim strings
    # drop null columns
    non_null_cols = [c for c in df.columns if df.filter(F.col(c).isNotNull()).limit(1).count() > 0]
    df = df.select(*non_null_cols)

    # trim strings
    for c, t in df.dtypes:
        if t.startswith('string'):
            df = df.withColumn(c, F.when(F.col(c).isNotNull(), F.trim(F.col(c))).otherwise(None))

    # dedupe using defined primary key or all columns
    pk_cols = []
    # heuristics: look for id, _id, or "<source>_id"
    for c in df.columns:
        if re.search(r'(^id$|_id$|%s_id$)' % src_name, c, re.I):
            pk_cols.append(c)
    if not pk_cols:
        # fallback to all columns
        pk_cols = df.columns

    df = df.dropDuplicates(pk_cols)

    # remove rows where all columns are null
    non_null_expr = F.expr(" OR ".join([f"{c} IS NOT NULL" for c in df.columns]))
    df = df.filter(non_null_expr)

    # write to silver; partition by ingest_date
    silver_path = f"s3://{silver_bucket}/{src_name}/"
    df = df.withColumn("ingest_date", F.lit(ingest_date))

    # coalesce to reasonable number of files: use dynamic partitioning
    df.repartition(8).write.mode("overwrite").partitionBy("ingest_date").parquet(silver_path)

    # optionally attempt joins if join_map provided
    if src_name in join_map:
        # join_map example: {"hubspot": {"join_with": "rds", "left_key": "crm_id", "right_key": "customer_id"}}
        instr = join_map[src_name]
        other = instr.get('join_with')
        left_key = instr.get('left_key')
        right_key = instr.get('right_key')
        if other:
            other_path = f"s3://{silver_bucket}/{other}/"
            try:
                df_other = spark.read.option("recursiveFileLookup","true").parquet(other_path)
                # perform left join and write back to silver/curated join area
                joined = df.join(df_other, df[left_key]==df_other[right_key], "left")
                out_path = f"s3://{silver_bucket}/joined/{src_name}_{other}/"
                joined.withColumn("ingest_date", F.lit(ingest_date)).repartition(8).write.mode("overwrite").parquet(out_path)
            except Exception as e:
                print("Join attempt failed:", e)

job.commit()
