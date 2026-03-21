import os
from io import BytesIO

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from src.utils.minio_client import MinioClient

MINIO_BUCKET_SILVER = os.getenv("MINIO_BUCKET_SILVER")
MINIO_BUCKET_GOLD = os.getenv("MINIO_BUCKET_GOLD")
OPENBREWERYDB_API_PREFIX = os.getenv("OPENBREWERYDB_API_PREFIX")

def read_silver_parquets() -> pd.DataFrame:
    """
    Lists all parquet files under the silver prefix and concatenates them
    into a single DataFrame. Raises FileNotFoundError if no files are found.
    """
    client = MinioClient().get_minio_client()

    objects = list(
        client.list_objects(MINIO_BUCKET_SILVER, prefix=OPENBREWERYDB_API_PREFIX, recursive=True)
    )

    parquet_files = [obj for obj in objects if obj.object_name.endswith(".parquet")]

    if not parquet_files:
        raise FileNotFoundError(
            f"No parquet files found in silver bucket under prefix '{OPENBREWERYDB_API_PREFIX}'."
        )

    print(f"Found {len(parquet_files)} parquet partitions in silver.")

    frames = []
    for file in parquet_files:
        response = client.get_object(MINIO_BUCKET_SILVER, file.object_name)
        df = pd.read_parquet(BytesIO(response.read()))
        frames.append(df)

    return pd.concat(frames, ignore_index=True)


def _aggregate(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregates brewery count by brewery_type, country, and state.

    Output schema:
        brewery_type | country | state | brewery_count
    """

    return (
        df.groupby(["brewery_type", "country", "state"], dropna=False)
        .size()
        .reset_index(name="brewery_count")
        .sort_values(["country", "state", "brewery_type"])
        .reset_index(drop=True)
    )


def save_to_gold(df_silver: pd.DataFrame) -> str:
    """
    Receives the silver DataFrame, computes the aggregated view, and writes
    a single parquet file to the gold bucket.

    Returns:
        The MinIO object name where the gold data was written.
    """
    client = MinioClient().get_minio_client()

    if not client.bucket_exists(MINIO_BUCKET_GOLD):
        client.make_bucket(MINIO_BUCKET_GOLD)

    print(f"Total silver rows loaded: {len(df_silver)}")

    df_gold = _aggregate(df_silver)
    
    print(f"Gold rows after aggregation: {len(df_gold)}")
    print(df_gold.head(10).to_string(index=False))

    table = pa.Table.from_pandas(df_gold, preserve_index=False)

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