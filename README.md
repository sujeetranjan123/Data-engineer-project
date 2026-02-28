# Medallion Architecture Project

This repository demonstrates a simple medallion architecture (bronze, silver, gold) using PySpark on data that is sourced from a MySQL database and stored in S3. Credentials are obtained via AWS Secrets Manager and IAM role-based access is assumed; no passwords are hardcoded.

## Structure

```
Personal_project/
├── medallion/
│   ├── __init__.py
│   ├── config.py          # dataclasses for configuration
│   ├── secrets.py         # utility to read AWS Secrets Manager
│   └── etl.py             # core ETL transformations
├── main.py                # entry point for all layers
├── bronze_layer.py        # script to ingest raw data into bronze
├── silver_layer.py        # script to transform bronze ➜ silver
├── gold_layer.py          # script to aggregate silver ➜ gold
├── requirements.txt       # Python dependencies
└── README.md              # this file
```

## Usage

1. Install dependencies:
   ```sh
   pip install -r requirements.txt
   ```

2. Ensure the environment where the script runs has an attached IAM role with permissions for S3 and Secrets Manager.

3. Store MySQL credentials in Secrets Manager under a name such as `prod/mysql/credentials` with JSON content:
   ```json
   {
     "username": "dbuser",
     "password": "secretpass"
   }
   ```

4. Adjust S3 paths and MySQL configuration in `main.py` as needed.

5. Run (on local machine) via:
   ```sh
   python main.py
   ```

### EMR deployment

Standalone layer scripts are also provided for execution on an EMR cluster:

* `bronze_layer.py` – reads from MySQL and writes raw data to the bronze path.
* `silver_layer.py` – reads the bronze dataset, performs cleaning, and writes to silver.
* `gold_layer.py` – reads silver, performs business aggregations, and writes to gold.

Each script is self‑contained and can be submitted as a Spark step; they reuse the shared `medallion` package. No Jupyter notebooks are required in the final project – any earlier notebooks have been removed.

## Layers

- **Bronze:** Raw data from MySQL is written directly to an S3 `bronze/` prefix as Parquet.
- **Silver:** Basic cleaning (drop nulls) is performed and output to `silver/`.
- **Gold:** Business-level aggregation or modeling can be added; output to `gold/`.

Feel free to extend the transformations for your use case.
