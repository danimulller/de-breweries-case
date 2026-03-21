# Brewery Data Pipeline

A data engineering project that fetches brewery data from an open-source API, transforms it, and stores it in a data lake using the medallion architecture.

## 🏗️ Architecture

This solution implements a three-layer medallion architecture:
- **Bronze Layer**: Raw data storage (JSON, partitioned by `extraction_date`)
- **Silver Layer**: Curated data in Parquet format, partitioned by `country` and `state`
- **Gold Layer**: Analytical aggregated layer — brewery count by type and location

## 🛠️ Technologies Used

- **Orchestration**: Apache Airflow 2.9.3
- **Storage**: MinIO (S3-compatible object storage)
- **Data Processing**: Pandas, NumPy, PyArrow
- **Containerization**: Docker & Docker Compose

## 🚀 Quick Start

### Prerequisites

- Docker or Docker Desktop ([download here](https://www.docker.com/products/docker-desktop/))
- At least 16 GB RAM recommended

### Setup

1. **Clone the repository**
   ```bash
   git clone https://github.com/danimulller/de-breweries-case.git
   cd de-breweries-case
   ```

2. **Create your `.env` file**
   ```bash
   cp .env.example .env
   ```
   The default values in `.env.example` work out of the box for local development.

3. **Start the services**
   ```bash
   docker compose up --build -d
   ```
   This will download all necessary images and dependencies. Setup typically takes about 5 minutes.

4. **Verify services are running**
   - **MinIO**: http://localhost:9001 (user: `minio`, password: `minio123`)
   - **Airflow**: http://localhost:8080 (user: `admin`, password: `admin`)
     > Note: Airflow may take longer to start than MinIO

5. **Shutting Down the Environment**
   To stop all services and clean up resources after use:

    ```bash
    docker compose down
    ```

    This will stop and remove all containers, networks, and volumes created by the services.

## 📊 Running the Pipeline

1. Access Airflow at http://localhost:8080
2. Navigate to the DAGs section
3. Find and trigger the `full_pipeline` DAG

### Pipeline Tasks

#### 1. fetch_and_save_to_bronze
- Connects to Open Brewery DB API
- Fetches all available breweries from `https://api.openbrewerydb.org/v1/breweries`
- Saves JSON data to bronze layer on MinIO, partitioned by `extraction_date`

#### 2. transform_bronze_to_silver
- Reads the latest JSON file from bronze layer
- Normalizes state and country columns (strips whitespace, removes accents, expands Australian state abbreviations)
- Saves data as Parquet files to silver layer, partitioned by `country` and `state`

#### 3. aggregate_and_write_to_gold
- Reads all Parquet files from silver layer
- Combines them into a single Pandas DataFrame
- Creates aggregated view grouped by `brewery_type`, `country`, and `state`
- Saves results to gold layer as Parquet file for the analytics team

## 🔔 Monitoring & Alerting

### Pipeline Failure Alerts
Airflow's built-in `on_failure_callback` can be used to send notifications via email whenever a task fails. All DAGs already have `retries: 3` with a 5-minute `retry_delay` configured, which handles transient failures (API timeouts, network blips) automatically.

### Data Quality Checks
Key checks to add between pipeline stages:

- **Bronze → Silver**: Assert row count from API matches rows written to bronze. Assert the JSON is valid and contains the expected keys.
- **Silver → Gold**: Assert no duplicate `id` values exist after transformation. Assert `country` and `state` are never both null for the same row.
- **Gold**: Assert `brewery_count` is always greater than zero. Assert the total count in gold matches the total in silver.

These checks can be implemented as separate Airflow tasks using `PythonOperator` with explicit assertions, or with a library like [Great Expectations](https://greatexpectations.io/) for more comprehensive data profiling.

## 🤔 Trade-Offs and Decisions

- **Data Processing**: This solution uses Pandas, NumPy, and PyArrow for simplicity and lower resource usage. For production environments with datasets larger than memory, PySpark would be the right choice. A `SparkClient` module (`src/utils/spark_client.py`) has already been implemented and can be swapped in with minimal changes to the writer modules.

- **Orchestration**: Airflow was chosen for its scheduling capabilities, retry logic, XCom-based task communication, and intuitive UI for tracking pipeline runs.

- **Storage**: MinIO provides S3-compatible object storage that can be swapped for AWS S3 or GCS with only environment variable changes.

- **Partitioning strategy**: The silver layer uses `country/state` partitioning to optimize query performance for location-based analytical workloads, which is the primary use case described in the gold layer requirements.

## 📁 Project Structure

```
├── dags/                    # Airflow DAGs
│   ├── full_pipeline.py     # End-to-end pipeline (scheduled hourly)
│   ├── bronze_ingestion.py  # Bronze-only DAG
│   ├── silver_ingestion.py  # Silver-only DAG
│   └── gold_ingestion.py    # Gold-only DAG
├── src/
│   ├── ingestion/
│   │   ├── brewery_api.py   # Open Brewery DB API client
│   │   ├── bronze_writer.py # Raw JSON → MinIO bronze
│   │   ├── silver_writer.py # JSON → partitioned Parquet (silver)
│   │   └── gold_writer.py   # Parquet → aggregated Parquet (gold)
│   └── utils/
│       ├── minio_client.py  # MinIO connection helper
│       └── spark_client.py  # PySpark session builder (future use)
├── .env.example             # Environment variable template
├── docker-compose.yml       # Docker services configuration
├── Dockerfile               # Container definition
└── requirements.txt         # Python dependencies
```