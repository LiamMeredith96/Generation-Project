# CoffeeOverflowError – Café ETL and Analytics Pipeline

This project is a team-built ETL pipeline for a fictional café chain that has expanded to hundreds of branches across the UK. Each branch generates daily CSV files of all transactions. The business wants to understand:

- Which products sell best
- Which branches perform strongly or poorly
- How trends change over time

Their existing tools only produce per-branch reports and make it difficult to see company-wide patterns. This repo contains the Python ETL code and database schema that solve that problem.

---

# Members

Liam (Scrum Master)
Kirian
Kevin
Jewen

## Business problem

The café’s existing setup:

- Each branch generates a daily CSV containing all transactions.
- At 8pm, a CSV is uploaded to local back-office software.
- Reports are produced at branch level (daily / weekly / monthly).

Pain points:

- Reports are limited to **single branches**.
- Collating data for **all branches** is time consuming.
- It is hard to identify **company-wide trends and opportunities**, so they are likely missing revenue.

---

## Solution overview

We designed and implemented a **scalable ETL (Extract, Transform, Load) pipeline** that:

- Collects CSV transaction data from all branches.
- Normalises it into a relational warehouse schema.
- Makes it easy to query sales by:
  - Branch
  - Product
  - Date
  - Payment method

Core technologies:

- **Python** – ETL logic (`app.py`, `database_util_sql.py`)
- **MySQL** – data warehouse schema (branch, product, payment_method, transaction, order_details)
- **AWS** (original deployment):
  - Hosted the database and ETL code in the cloud
  - Used as the backend for BI dashboards
- **Grafana** – dashboards and visualisation (sales trends, top products, branch comparison)

> Note: the AWS infrastructure used during the project (Redshift, S3, CloudWatch, EC2, etc.) is no longer active, but the Python ETL code in this repo is the same code that was used when the system was running in the cloud.

---


### Local / development view

1. **Extract**
   - CSV files placed in a `Data/` directory.
   - `app.py` scans this directory and picks up any `.csv` files that are not yet marked as processed.

2. **Transform**
   - Cleans and parses transaction rows.
   - Splits product strings such as `"Regular Latte - 2.50, Large Latte - 3.10"` into structured records.
   - Normalises:
     - Branches → `branch` table
     - Products → `product` table
     - Payment methods → `payment_method` table
   - Formats dates consistently.
   - Aggregates item counts into `order_details`.

3. **Load**
   - Writes into a MySQL database using the tables:
     - `branch`
     - `payment_method`
     - `product`
     - `transaction`
     - `order_details`
   - After successful processing, each CSV is renamed with a `_done.csv` suffix to avoid re-ingestion.

### Original AWS usage

The same ETL logic was used as part of an AWS-based setup:

- The warehouse ran on an AWS database service (Redshift).
- ETL code (equivalent to `app.py` and `database_util_sql.py`) ran against it.
- Grafana was connected to Redshift as a data source to build dashboards (sales by branch, product trends, etc.).

Even though the AWS environment is no longer live, the **pipeline design** and **code** are cloud-ready: the ETL connects to whatever MySQL-compatible endpoint is configured via environment variables.


