# Use official Python 3.12 image as base
FROM python:3.12-slim

# Set working directory
WORKDIR /app

# Copy dependency file (you must have requirements.txt in this directory)
COPY requirements.txt .

# Install dependencies
RUN pip install --upgrade pip
RUN pip install -r requirements.txt

# Copy the entire project
COPY . .

# Expose Streamlit port
EXPOSE 8501

# Set default AWS region (update if needed)
ENV AWS_DEFAULT_REGION=us-east-1

# Run Streamlit app
CMD ["streamlit", "run", "frontend.py", "--server.address=0.0.0.0", "--server.port=8501"]
