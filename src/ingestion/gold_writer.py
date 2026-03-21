import os
from io import BytesIO

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count

from src.utils.minio_client import MinioClient
from src.utils.spark_client import SparkClient

MINIO_BUCKET_SILVER = os.getenv("MINIO_BUCKET_SILVER")
MINIO_BUCKET_GOLD = os.getenv("MINIO_BUCKET_GOLD")
OPENBREWERYDB_API_PREFIX = os.getenv("OPENBREWERYDB_API_PREFIX")


def _read_silver_parquet(spark, silver_prefix: str) -> DataFrame:
    """Reads all partitioned parquet files from the silver layer via S3A."""

    path = f"s3a://{MINIO_BUCKET_SILVER}/{silver_prefix}"

    print(f"Reading silver parquet from: {path}")
    
    return spark.read.parquet(path)


def _aggregate(df: DataFrame) -> DataFrame:
    """
    Aggregates brewery count by brewery_type, country, and state.

    Output schema:
        brewery_type | country | state | brewery_count
    """

    return (
        df.groupBy("brewery_type", "country", "state")
        .agg(count("*").alias("brewery_count"))
        .orderBy("country", "state", "brewery_type")
    )


def save_to_gold(silver_prefix: str) -> str:
    """
    Reads the silver layer, computes the aggregated view, and writes a single
    Parquet file to the gold bucket.

    Returns:
        The MinIO object name where the gold data was written.
    """

    spark = SparkClient().get_spark_session("gold_aggregator")

    try:
        client = MinioClient().get_minio_client()

        if not client.bucket_exists(MINIO_BUCKET_GOLD):
            client.make_bucket(MINIO_BUCKET_GOLD)

        df_silver = _read_silver_parquet(spark, silver_prefix)
        df_gold = _aggregate(df_silver)

        print(f"Gold rows: {df_gold.count()}")
        df_gold.show(10, truncate=False)

        # Collect to pandas and upload via MinIO client — avoids S3A write issues on Windows/Docker Desktop.
        import pyarrow as pa
        import pyarrow.parquet as pq

        gold_pandas = df_gold.toPandas()
        table = pa.Table.from_pandas(gold_pandas, preserve_index=False)

        buffer = BytesIO()
        pq.write_table(table, buffer)
        buffer.seek(0)
        parquet_bytes = buffer.getvalue()

        object_name = f"{OPENBREWERYDB_API_PREFIX}/breweries_by_type_and_location.parquet"

        client.put_object(
            bucket_name=MINIO_BUCKET_GOLD,
            object_name=object_name,
            data=BytesIO(parquet_bytes),
            length=len(parquet_bytes),
            content_type="application/octet-stream",
        )

        print(f"Gold write complete: s3://{MINIO_BUCKET_GOLD}/{object_name}")

        return object_name
    finally:
        spark.stop()
