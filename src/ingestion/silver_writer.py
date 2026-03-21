import os
from io import BytesIO

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, when, lit, explode

from src.utils.minio_client import MinioClient
from src.utils.spark_client import SparkClient

MINIO_BUCKET_BRONZE = os.getenv("MINIO_BUCKET_BRONZE")
MINIO_BUCKET_SILVER = os.getenv("MINIO_BUCKET_SILVER")
OPENBREWERYDB_API_PREFIX = os.getenv("OPENBREWERYDB_API_PREFIX")

def get_latest_bronze_object() -> str:
    """Lists all objects under the bronze prefix and returns the most recent one by last_modified."""

    client = MinioClient().get_minio_client()

    objects = list(client.list_objects(MINIO_BUCKET_BRONZE, prefix=OPENBREWERYDB_API_PREFIX, recursive=True))

    if not objects:
        raise FileNotFoundError(f"No objects found in bronze bucket under prefix '{OPENBREWERYDB_API_PREFIX}'.")

    # Finding the most recent object by last_modified timestamp
    latest = max(objects, key=lambda obj: obj.last_modified)

    return latest.object_name


def _read_bronze_json(spark: SparkSession, object_name: str) -> DataFrame:
    """ Downloads the bronze JSON file from MinIO and loads it as a Spark DataFrame. """

    client = MinioClient().get_minio_client()

    response = client.get_object(MINIO_BUCKET_BRONZE, object_name)
    raw_bytes = response.read()

    tmp_path = "/tmp/bronze_latest.json"
    with open(tmp_path, "wb") as f:
        f.write(raw_bytes)

    # Configure spark reading options
    raw_df = spark.read.json(tmp_path)

    # Returning "data" field in JSON
    df_data = raw_df.select(explode("data").alias("data")).select("data.*")

    return df_data

def _normalize_partitions(df: DataFrame) -> DataFrame:
    """ Fills null or empty country/state values with 'unknown' (e.g. country=unknown/state=unknown). """

    df = df.withColumn(
            "country",
            when((col("country").isNull()) | (col("country") == ""), lit("unknown"))
            .otherwise(col("country"))
        ).withColumn(
            "state",
            when((col("state").isNull()) | (col("state") == ""), lit("unknown"))
            .otherwise(col("state"))
        )
    
    return df


def save_to_silver(object_name: str) -> str:
    """
        Reads the given bronze object, transforms it, and writes partitioned Parquet
        files to the silver bucket.

        Returns:
            The S3A output path where the silver data was written.
    """

    spark = SparkClient().get_spark_session("silver_transformer")

    minio = MinioClient()
    client = minio.get_minio_client()

    if not client.bucket_exists(MINIO_BUCKET_SILVER):
        client.make_bucket(MINIO_BUCKET_SILVER)

    df = _read_bronze_json(spark, object_name)

    print(df.printSchema())

    df = _normalize_partitions(df)

    output_path = f"s3a://{MINIO_BUCKET_SILVER}/{OPENBREWERYDB_API_PREFIX}"

    print(f"Starting writing to {output_path}! Length of dataframe: {df.count()}")

    # Partitioning by country/state:
    df.write.mode("overwrite").partitionBy("country", "state").parquet(output_path)

    print(f"Finished writing!")

    spark.stop()

    return output_path
