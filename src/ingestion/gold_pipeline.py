from src.ingestion.silver_writer import get_latest_bronze_object
from src.ingestion.gold_writer import save_to_gold
import os

OPENBREWERYDB_API_PREFIX = os.getenv("OPENBREWERYDB_API_PREFIX")


def run_gold_pipeline() -> str:
    """
    Reads the silver layer and produces the aggregated gold view.

    The silver prefix is stable (not tied to a specific file), so we always
    read the full partitioned dataset and overwrite the gold output.

    Returns:
        The MinIO object name of the gold parquet file.
    """

    print(f"Starting gold aggregation from silver prefix: {OPENBREWERYDB_API_PREFIX}")

    object_name = save_to_gold(OPENBREWERYDB_API_PREFIX)

    print("Gold aggregation completed successfully!")
    print(f"Gold object: {object_name}")

    return object_name
