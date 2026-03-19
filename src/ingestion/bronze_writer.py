import json
import os
from datetime import datetime, timezone
from io import BytesIO

from src.utils.minio_client import MinioClient

MINIO_BUCKET_BRONZE = os.getenv("MINIO_BUCKET_BRONZE")
BRONZE_PREFIX = "openbrewerydb"

def save_to_bronze(payload: dict) -> str:

    minio = MinioClient()
    client = minio.get_minio_client()

    if not client.bucket_exists(MINIO_BUCKET_BRONZE):
        client.make_bucket(MINIO_BUCKET_BRONZE)

    now = datetime.now(timezone.utc)
    partition_date = now.strftime("%Y-%m-%d")
    file_name = f"breweries_{now.strftime('%Y%m%d_%H%M%S')}.json"

    object_name = f"{BRONZE_PREFIX}/extraction_date={partition_date}/{file_name}"

    data_bytes = json.dumps(payload, ensure_ascii=False).encode("utf-8")

    client.put_object(
        bucket_name=MINIO_BUCKET_BRONZE,
        object_name=object_name,
        data=BytesIO(data_bytes),
        length=len(data_bytes),
        content_type="application/json",
    )

    return object_name