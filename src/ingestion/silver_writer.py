import os
import json
import unicodedata
import re
from io import BytesIO

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from src.utils.minio_client import MinioClient

MINIO_BUCKET_BRONZE = os.getenv("MINIO_BUCKET_BRONZE")
MINIO_BUCKET_SILVER = os.getenv("MINIO_BUCKET_SILVER")
OPENBREWERYDB_API_PREFIX = os.getenv("OPENBREWERYDB_API_PREFIX")


def get_latest_bronze_object() -> str:
    """Returns the most recent bronze object path, sorted by last_modified."""

    client = MinioClient().get_minio_client()
    objects = list(
        client.list_objects(MINIO_BUCKET_BRONZE, prefix=OPENBREWERYDB_API_PREFIX, recursive=True)
    )

    if not objects:
        raise FileNotFoundError(
            f"No objects found in bronze bucket under prefix '{OPENBREWERYDB_API_PREFIX}'."
        )

    # Selecting the object with the latest last_modified timestamp
    latest = max(objects, key=lambda obj: obj.last_modified)

    # Returning the object name (path) of the latest bronze file
    return latest.object_name


def _read_bronze_json(client, object_name: str) -> pd.DataFrame:
    """Downloads the bronze JSON from MinIO and returns the data as a DataFrame."""

    response = client.get_object(MINIO_BUCKET_BRONZE, object_name)

    payload = json.loads(response.read())

    df = pd.DataFrame(payload["data"])

    # Includes updated_at column based on the ingestion timestamp from metadata
    df["updated_at"] = pd.to_datetime(payload["metadata"]["ingestion_timestamp"], errors="coerce")

    string_columns = df.select_dtypes(include=["object"]).columns

    for col in string_columns:
        df[col] = df[col].str.strip().replace("", None)

    return df


def _normalize_partitions(df: pd.DataFrame) -> pd.DataFrame:
    """ Fills null or empty country/state values with 'unknown'. """

    def normalize_column(col):

        # - Remove leading/trailing whitespace;
        # - Convert to lowercase;
        # - Replace spaces with "-"";
        # - Replace empty strings with 'unknown';

        return col.str.strip().str.lower().str.replace(" ", "-").replace("", None).fillna("unknown")

    df["country"] = normalize_column(df["country"])
    df["state"] = normalize_column(df["state"])

    return df

def _remove_special_characters(text: str) -> str:
    """
        Normalize unicode to NFD and strip combining characters (accents), so 'Kärnten' becomes 'Karnten'.
    """

    # - Remove leading/trailing whitespace;
    # - Convert to lowercase;
    # - Replace spaces with "-"";

    text = text.strip().lower().replace(" ", "-")

    # Decompose unicode and drop combining (accent) characters
    normalized = unicodedata.normalize("NFD", text)
    ascii_value = "".join(c for c in normalized if unicodedata.category(c) != "Mn")

    # Lowercase, replace anything that isn't alphanumeric or hyphen with underscore
    slug = re.sub(r"[^\w\-]", "_", ascii_value.lower())

    # Collapse multiple underscores
    slug = re.sub(r"_+", "_", slug).strip("_")

    # - Replace empty strings with 'unknown';
    return slug or "unknown"


def save_to_silver(object_name: str) -> str:
    """
        Reads the given bronze object, normalizes the data, and writes partitioned
        Parquet files to the silver bucket using pyarrow.

        Partitioning: country / state

        Returns:
            The MinIO prefix where the silver data was written.
    """

    client = MinioClient().get_minio_client()

    if not client.bucket_exists(MINIO_BUCKET_SILVER):
        client.make_bucket(MINIO_BUCKET_SILVER)

    df = _read_bronze_json(client, object_name)
    # df = _normalize_partitions(df)

    print(f"Rows to write: {len(df)}")
    print(f"Schema: {df.dtypes.to_dict()}")

    # Write each partition directly to MinIO as individual parquet files.
    for (country, state), partition_df in df.groupby(["country", "state"]):
        partition_table = pa.Table.from_pandas(partition_df, preserve_index=False)

        buffer = BytesIO()
        pq.write_table(partition_table, buffer)
        buffer.seek(0)
        parquet_bytes = buffer.getvalue()

        object_name_out = (
            f"{OPENBREWERYDB_API_PREFIX}"
            f"/country={_remove_special_characters(country)}"
            f"/state={_remove_special_characters(state)}"
            f"/data.parquet"
        )

        client.put_object(
            bucket_name=MINIO_BUCKET_SILVER,
            object_name=object_name_out,
            data=BytesIO(parquet_bytes),
            length=len(parquet_bytes),
            content_type="application/octet-stream",
        )

    output_prefix = f"{OPENBREWERYDB_API_PREFIX}"
    print(f"Silver write complete. Prefix: s3://{MINIO_BUCKET_SILVER}/{output_prefix}")

    return output_prefix
