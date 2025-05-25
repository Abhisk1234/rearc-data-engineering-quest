# 🧠 Rearc Data Engineering Quest Submission - Abhishek Nandgadkar

This project demonstrates an end-to-end data engineering pipeline using **Python, AWS S3, Lambda, SQS, and CloudFormation** (with LocalStack for local development). It fetches open data from public sources, syncs it to S3, triggers processing pipelines, and generates insights automatically.

---

## ✅ Step 1: Republish BLS Open Dataset to Amazon S3

- **S3 Bucket**: `bls-data`
- **Script**: [`step1_ingest_sync/ingest.py`](./step1_ingest_sync/ingest.py)

### Features:
- Scrapes BLS data file links from [BLS PR Series](https://download.bls.gov/pub/time.series/pr/)
- Syncs files to S3 (uploads new/updated, deletes missing)
- Handles HTTP 403 with proper headers (User-Agent for compliance)
- No hardcoded filenames
- Skips duplicates intelligently using Last-Modified header

---

## 🌐 Step 2: Fetch and Store Data from Public API

- **API Source**: [https://datausa.io/api/data?drilldowns=Nation&measures=Population](https://datausa.io/api/data?drilldowns=Nation&measures=Population)
- **Script**: [`step2_api_fetch/api_fetch.py`](./step2_api_fetch/api_fetch.py)
- **Output**: JSON file (`api/datausa_population.json`) stored in the same S3 bucket

### Features:
- Uses `requests` to fetch data
- Saves result to S3 under `api/datausa_population.json`

---

## 📊 Step 3: Analytics on Combined Data

- **Notebook**: [`step3_analytics/analytics.ipynb`](./step3_analytics/analytics.ipynb)

### Summary of Analysis:
- Loads BLS `.data` files and API JSON
- Cleans and transforms the datasets
- Performs basic population trend analysis and year-wise comparisons
- Uses Pandas for insights and logs results

---

## ⚙️ Step 4: Infrastructure as Code - Automated Data Pipeline

- **CloudFormation Template**: [`step4_pipeline/template.yaml`](./step4_pipeline/template.yaml)
- **Lambda Code**:
  - [`lambda/ingest.py`](./step4_pipeline/lambda/ingest.py): Combines Step 1 & Step 2 (runs daily)
  - [`lambda/analytics.py`](./step4_pipeline/lambda/analytics.py): Triggered via SQS on new `data.json`


