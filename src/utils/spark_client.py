from pyspark.sql import SparkSession, DataFrame
import os

MINIO_ROOT_USER = os.getenv("MINIO_ROOT_USER")
MINIO_ROOT_PASSWORD = os.getenv("MINIO_ROOT_PASSWORD")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")

class SparkClient():

    def get_spark_session(self, appName: str) -> SparkSession:
        """Creates a local SparkSession configured to read/write from MinIO via S3A."""

        return (
            SparkSession.builder
                .appName(appName)
                .master("local[*]")
                .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
                .config("spark.hadoop.fs.s3a.access.key", MINIO_ROOT_USER)
                .config("spark.hadoop.fs.s3a.secret.key", MINIO_ROOT_PASSWORD)
                .config("spark.hadoop.fs.s3a.path.style.access", "true")
                .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
                .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4")
                .getOrCreate()
        )