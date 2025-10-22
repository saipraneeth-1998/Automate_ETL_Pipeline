import streamlit as st
import requests
import json
import pandas as pd

# -------------------------------
# API Gateway URL
# -------------------------------
API_URL = "https://ydeezayp69.execute-api.us-east-1.amazonaws.com/dev"  # replace with your endpoint

st.title("AI Analytics Assistant")

# -------------------------------
# Sidebar for choosing action
# -------------------------------
action = st.sidebar.selectbox("Select Action", ["query", "etl"])

# -------------------------------
# ETL Pipeline Trigger
# -------------------------------
if action == "etl":
    st.write("### Trigger ETL Pipeline")
    if st.button("Run ETL"):
        payload = {"action": "etl"}
        with st.spinner("Running ETL..."):
            try:
                response = requests.post(API_URL, headers={"Content-Type": "application/json"}, data=json.dumps(payload))
                if response.status_code == 200:
                    # Parse nested body
                    result = json.loads(response.json().get("body", "{}"))
                    st.success("ETL Triggered Successfully!")
                    st.json(result)
                else:
                    st.error(f"Error: {response.status_code} - {response.text}")
            except Exception as e:
                st.error(f"Request failed: {e}")

# -------------------------------
# Query LLM + Athena
# -------------------------------
elif action == "query":
    st.write("### Ask a Question")
    user_message = st.text_area("Your Question", "")

    if st.button("Submit Query") and user_message.strip():
        payload = {"action": "query", "user_message": user_message}
        with st.spinner("Querying LLM and Athena..."):
            try:
                response = requests.post(API_URL, headers={"Content-Type": "application/json"}, data=json.dumps(payload))
                if response.status_code == 200:
                    # Parse the Lambda body JSON
                    body = json.loads(response.json().get("body", "{}"))

                    # Display LLM reply
                    st.write("**LLM Reply:**")
                    st.write(body.get("reply", ""))

                    # Display data in table
                    data = body.get("data", [])
                    if data:
                        df = pd.DataFrame(data)
                        st.write("**Athena / ETL Data:**")
                        st.dataframe(df)  # nice table view
                    else:
                        st.info("No data returned.")
                else:
                    st.error(f"Error: {response.status_code} - {response.text}")
            except Exception as e:
                st.error(f"Request failed: {e}")
