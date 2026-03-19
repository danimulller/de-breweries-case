from src.ingestion.brewery_api import BreweryAPI
from src.ingestion.bronze_writer import save_to_bronze

def run_bronze_pipeline() -> str:

    api = BreweryAPI()
    response = api.fetch_breweries()

    object_name = save_to_bronze(response)

    print("Bronze ingestion completed successfully!")
    print(f"Records fetched: {response['metadata']['record_count']}")
    print(f"Bronze object path: {object_name}")

    return object_name