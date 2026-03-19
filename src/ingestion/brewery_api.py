import requests
from datetime import datetime, timezone

BASE_URL = "https://api.openbrewerydb.org/v1/breweries"

class BreweryAPI():

    def __send_request(self, url, timeout=10) -> dict:
        try:
            response = requests.get(url, timeout=timeout)
            response.raise_for_status()

            data = response.json()

            return {
                "data": data,
                "metadata": {
                    "source": "openbrewerydb",
                    "endpoint": BASE_URL,
                    "ingestion_timestamp": datetime.now(timezone.utc).isoformat(),
                    "status_code": response.status_code,
                    "record_count": len(data),
                },
            }
        except requests.exceptions.Timeout as exc:
            raise Exception("Request timed out while calling Open Brewery API.") from exc
        except requests.exceptions.HTTPError as exc:
            raise Exception(f"HTTP error while calling Open Brewery API: {exc}") from exc
        except requests.exceptions.RequestException as exc:
            raise Exception(f"Request error while calling Open Brewery API: {exc}") from exc

    def fetch_breweries(self) -> dict:
        try:
            response = self.__send_request(BASE_URL)

            return response
        except ValueError as exc:
            raise Exception("Failed to parse API response as JSON.") from exc
        
    def fetch_single_brewery(self, brewery_id: str) -> dict:
        try:
            url = f"{BASE_URL}/{brewery_id}"
            response = self.__send_request(url)

            return response
        except ValueError as exc:
            raise Exception("Failed to parse API response as JSON.") from exc