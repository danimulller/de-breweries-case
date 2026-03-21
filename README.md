# Brewery Data Pipeline

A data engineering project that fetches brewery data from an open-source API, transforms it, and stores it in a data lake using the medallion architecture.

## 🏗️ Architecture

This solution implements a three-layer medallion architecture:
- **Bronze Layer**: Raw data storage
- **Silver Layer**: Curated data partitioned by location
- **Gold Layer**: Analytical aggregated layer

## 🛠️ Technologies Used

- **Orchestration**: Apache Airflow
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

2. **Start the services**
   ```bash
   docker compose up --build -d
   ```
   This will download all necessary images and dependencies. Setup typically takes about 5 minutes.

3. **Verify services are running**
   - **MinIO**: http://localhost:9001 (user: `minio`, password: `minio123`)
   - **Airflow**: http://localhost:8080 (user: `admin`, password: `admin`)
     > Note: Airflow may take longer to start than MinIO

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
- Normalizes state and country columns using Pandas and NumPy
- Saves data as Parquet files to silver layer, partitioned by country and state using PyArrow

#### 3. aggregate_and_write_to_gold
- Reads all Parquet files from silver layer
- Combines them into a single Pandas DataFrame
- Creates aggregated view grouped by `brewery_type`, `country`, and `state`
- Saves results to gold layer as Parquet file for analytics team

## 🤔 Trade-Offs and Decisions

- **Data Processing**: While this solution uses Pandas, NumPy, and PyArrow for simplicity, PySpark would be more suitable for production environments with large datasets. A Spark Client module (`src/utils/spark_client.py`) has been implemented for future scalability.

- **Orchestration**: Airflow was chosen for its lightweight design, efficiency, and intuitive interface for tracking pipeline runs.

- **Storage**: MinIO provides S3-compatible object storage that integrates well with this architecture.

## 📁 Project Structure

```
├── dags/                    # Airflow DAGs
├── src/
│   ├── ingestion/          # Data ingestion modules
│   └── utils/              # Utility modules (including Spark client)
├── tests/                  # Test files
├── docker-compose.yml      # Docker services configuration
├── Dockerfile             # Container definition
└── requirements.txt       # Python dependencies
```