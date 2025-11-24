# Copyright (c) 2025 Airbyte, Inc., all rights reserved.

import logging
from typing import Any, List, Mapping, Tuple

import requests

from airbyte_cdk.sources.declarative.yaml_declarative_source import YamlDeclarativeSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http.requests_native_auth import TokenAuthenticator


class SourceBrightDataSerp(YamlDeclarativeSource):
    def __init__(self):
        super().__init__(**{"path_to_yaml": "manifest.yaml"})

    @staticmethod
    def get_authenticator(config: Mapping[str, Any]):
        token = config.get("api_key")
        return TokenAuthenticator(token=token)

    def check_connection(self, logger: logging.Logger, config: Mapping[str, Any]) -> Tuple[bool, Any]:
        try:
            authenticator = self.get_authenticator(config)
            search_queries = config.get("search_queries", [])

            if not search_queries:
                return False, "No search queries provided in configuration"

            test_query = search_queries[0]
            encoded_query = test_query.replace(" ", "+")
            zone = config.get("zone", "serp_api1")
            country = config.get("country", "us")
            format_type = config.get("format", "json")

            params = {
                "zone": zone,
                "url": f"https://www.google.com/search?q={encoded_query}",
                "format": format_type,
                "method": "GET",
                "country": country,
            }

            headers = authenticator.get_auth_header()
            headers["Content-Type"] = "application/json"

            response = requests.post("https://api.brightdata.com/request", json=params, headers=headers, timeout=30)

            if response.status_code == 200:
                return True, None
            elif response.status_code == 401:
                return False, "Authentication failed. Please check your API key."
            elif response.status_code == 402:
                return False, "Payment required. Please check your Bright Data account balance."
            else:
                return False, f"API request failed with status {response.status_code}: {response.text}"

        except Exception as e:
            return False, f"An error occurred: {str(e)}"

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        return super().streams(config=config)
