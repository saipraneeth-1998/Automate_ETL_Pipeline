from bedrock_agent import etl_pipeline_async,query_athena,ask_llm_generate_sql,deduplicate_rows
import json


def lambda_handler(event, context):
    """
    event = {
        "action": "etl" | "query",
        "user_message": "string"
    }
    """
    action = event.get("action")
    user_message = event.get("user_message", "")

    if action == "etl":
        return {
            "statusCode": 200,
            "body": json.dumps(etl_pipeline_async())
        }

    elif action == "query" and user_message:
        llm_output = ask_llm_generate_sql(user_message)
        sql_query = llm_output.get("sql")
        reply = llm_output.get("reply", "")

        data = []
        if llm_output.get("action") == "query" and sql_query:
            data = deduplicate_rows(query_athena(sql_query))

        return {
            "statusCode": 200,
            "body": json.dumps({"reply": reply, "data": data})
        }

    else:
        return {
            "statusCode": 400,
            "body": json.dumps({"reply": "Invalid action or missing user_message"})
        }