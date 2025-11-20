# Copyright (c) 2025 Airbyte, Inc., all rights reserved.

import sys
import urllib.parse
from typing import Any, List, Mapping, Tuple

from airbyte_cdk.models import SyncMode
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream

from .client import BrightDataClient
from .streams import SerpResultsStream


class SourceBrightDataSerp(AbstractSource):
    def check_connection(self, logger, config: Mapping[str, Any]) -> Tuple[bool, any]:
        """
        Validate connection by making a simple API call using the first user query
        """
        try:
            client = BrightDataClient(config)

            # Get the first search query from user config for testing
            search_queries = config.get("search_queries", [])
            if not search_queries:
                return False, "No search queries provided in configuration"

            # Use the first user query for connection test
            test_query = search_queries[0]

            # Use the client's make_request method which handles URL encoding
            response = client.make_request(test_query)

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
        """
        Initialize streams
        """
        client = BrightDataClient(config)
        return [SerpResultsStream(client=client, config=config)]


def run():
    """Main entry point for the connector"""
    source = SourceBrightDataSerp()
    source.run(sys.argv[1:])
