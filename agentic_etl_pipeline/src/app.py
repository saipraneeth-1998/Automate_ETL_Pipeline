from flask import Flask, request, jsonify, render_template
from bedrock_agent import etl_pipeline_async, query_athena, ask_llm_generate_sql, deduplicate_rows

app = Flask(__name__)

@app.route("/")
def index():
    return render_template("index.html")

@app.route("/api/etl", methods=["POST"])
def run_etl():
    try:
        result = etl_pipeline_async()
        return jsonify({"status": "success", "result": result})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route("/api/query", methods=["POST"])
def run_query():
    data = request.get_json()
    user_message = data.get("user_message", "")

    if not user_message:
        return jsonify({"status": "error", "message": "Missing 'user_message'"}), 400

    try:
        llm_output = ask_llm_generate_sql(user_message)
        sql_query = llm_output.get("sql")
        reply = llm_output.get("reply", "")
        result_data = []

        if llm_output.get("action") == "query" and sql_query:
            result_data = deduplicate_rows(query_athena(sql_query))

        return jsonify({"status": "success", "reply": reply, "data": result_data})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
