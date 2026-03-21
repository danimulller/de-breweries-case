from src.ingestion.silver_writer import get_latest_bronze_object, save_to_silver


def run_silver_pipeline() -> str:
    """
        Finds the most recent bronze file and transforms it into partitioned
        Parquet files in the silver layer.

        Returns:
            The output path where the silver data was written.
    """
    latest_object = get_latest_bronze_object()

    print(f"Latest bronze object: {latest_object}")

    output_path = save_to_silver(latest_object)

    print("Silver transformation completed successfully!")
    print(f"Silver output path: {output_path}")

    return output_path
