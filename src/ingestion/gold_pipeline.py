import os

from src.ingestion.gold_writer import save_to_gold

OPENBREWERYDB_API_PREFIX = os.getenv("OPENBREWERYDB_API_PREFIX")


def run_gold_pipeline() -> str:
    """
    Reads all silver parquet partitions and produces the aggregated gold view.

    Returns:
        The MinIO object name of the gold parquet file.
    """

    print(f"Starting gold aggregation from silver prefix: {OPENBREWERYDB_API_PREFIX}")

    object_name = save_to_gold()

    print("Gold aggregation completed successfully!")
    print(f"Gold object: {object_name}")

    return object_name