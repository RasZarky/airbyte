# Copyright (c) 2025 Airbyte, Inc., all rights reserved.

import time
from typing import Any, Dict

import requests


class BrightDataClient:
    """
    Client for handling Bright Data SERP API authentication and requests
    """

    def __init__(self, config: Dict[str, Any]):
        self.api_key = config["api_key"]
        self.zone = config.get("zone", "serp_api1")
        self.country = config.get("country", "us")
        self.format = config.get("format", "json")
        self.data_format = config.get("data_format")

        self.url_base = "https://api.brightdata.com/request"
        self.headers = {"Authorization": f"Bearer {self.api_key}", "Content-Type": "application/json"}

        self._session = requests.Session()
        self._session.headers.update(self.headers)

    def request_params(self, search_query: str) -> Dict[str, Any]:
        encoded_query = search_query.replace(" ", "+")

        params = {
            "zone": self.zone,
            "url": f"https://www.google.com/search?q={encoded_query}",
            "format": self.format,
            "method": "GET",
            "country": self.country,
        }

        if self.data_format:
            params["data_format"] = self.data_format

        return params

    def make_request(self, search_query: str) -> requests.Response:
        params = self.request_params(search_query)

        max_retries = 3
        for attempt in range(max_retries):
            try:
                response = self._session.post(
                    self.url_base,
                    json=params,
                    timeout=60,
                )

                response.raise_for_status()
                return response

            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 429:
                    if attempt < max_retries - 1:
                        wait_time = 2**attempt
                        time.sleep(wait_time)
                        continue
                raise e
            except Exception as e:
                if attempt < max_retries - 1:
                    wait_time = 2**attempt
                    time.sleep(wait_time)
                    continue
                raise
