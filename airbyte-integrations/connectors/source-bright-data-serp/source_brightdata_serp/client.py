# Copyright (c) 2025 Airbyte, Inc., all rights reserved.

import time
import urllib.parse
from typing import Any, Dict, Optional

import requests

from airbyte_cdk.sources.streams.http import HttpStream
from airbyte_cdk.sources.streams.http.requests_native_auth import TokenAuthenticator


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

        # Initialize session with retry strategy
        self._session = requests.Session()
        self._session.headers.update(self.headers)

    def request_params(self, search_query: str) -> Dict[str, Any]:
        """
        Build request parameters for SERP API with proper URL encoding
        """
        # Use + encoding for spaces (Google accepts this)
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
        """
        Make API request for a specific search query with retry logic
        """
        params = self.request_params(search_query)

        # Add retry logic for rate limits
        max_retries = 3
        for attempt in range(max_retries):
            try:
                print(f"Making request for query: '{search_query}'")
                print(f"Request URL: {params['url']}")
                print(f"Full request params: {params}")

                response = self._session.post(
                    self.url_base,
                    json=params,
                    timeout=60,  # SERP scraping can take time
                )

                print(f"Response status code: {response.status_code}")
                print(f"Response headers: {dict(response.headers)}")
                print(f"First 500 chars of response: {response.text[:500]}")

                response.raise_for_status()
                return response

            except requests.exceptions.HTTPError as e:
                print(f"HTTP Error {e.response.status_code} for query '{search_query}': {e.response.text}")

                if e.response.status_code == 429:  # Rate limit
                    if attempt < max_retries - 1:
                        wait_time = 2**attempt  # Exponential backoff
                        print(f"Rate limit hit, waiting {wait_time} seconds...")
                        time.sleep(wait_time)
                        continue
                raise e
            except Exception as e:
                print(f"Request failed for query '{search_query}': {str(e)}")
                if attempt < max_retries - 1:
                    wait_time = 2**attempt
                    print(f"Retrying in {wait_time} seconds...")
                    time.sleep(wait_time)
                    continue
                raise
