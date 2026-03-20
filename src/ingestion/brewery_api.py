import requests
from datetime import datetime
import pytz

BASE_URL = "https://api.openbrewerydb.org/v1/breweries"
PER_PAGE = 200  # max allowed by the API

class BreweryAPI:

    def __send_request(self, url: str, params: dict = None, timeout: int = 10) -> list:
        try:
            response = requests.get(url, params=params, timeout=timeout)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.Timeout as exc:
            raise Exception("Request timed out while calling Open Brewery API.") from exc
        except requests.exceptions.HTTPError as exc:
            raise Exception(f"HTTP error while calling Open Brewery API: {exc}") from exc
        except requests.exceptions.RequestException as exc:
            raise Exception(f"Request error while calling Open Brewery API: {exc}") from exc

    def fetch_breweries(self) -> dict:
        """
            Fetches all breweries from the API using pagination.
            Eg: GET https://api.openbrewerydb.org/v1/breweries?page=15&per_page=200
        """

        all_breweries = []
        page = 1

        while True:
            params = {"page": page, "per_page": PER_PAGE}
            data = self.__send_request(BASE_URL, params=params)

            if not data:
                break

            all_breweries.extend(data)
            page += 1

        return {
            "data": all_breweries,
            "metadata": {
                "source": "openbrewerydb",
                "endpoint": BASE_URL,
                "ingestion_timestamp": datetime.now(pytz.timezone('America/Sao_Paulo')).isoformat(),
                "record_count": len(all_breweries),
                "pages_fetched": page - 1,
            },
        }

    def fetch_single_brewery(self, brewery_id: str) -> dict:
        """
            Fetches a single brewery from the API by its ID.
            Eg: GET https://api.openbrewerydb.org/v1/breweries/b54b16e1-ac3b-4bff-a11f-f7ae9ddc27e0
        """

        url = f"{BASE_URL}/{brewery_id}"
        data = self.__send_request(url)

        return {
            "data": data,
            "metadata": {
                "source": "openbrewerydb",
                "endpoint": url,
                "ingestion_timestamp": datetime.now(pytz.timezone('America/Sao_Paulo')).isoformat(),
                "record_count": 1,
            },
        }
