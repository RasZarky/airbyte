# Copyright (c) 2025 Airbyte, Inc., all rights reserved.

from typing import Any, Dict, Iterable, List, Mapping, MutableMapping, Optional

import requests

from airbyte_cdk.models import SyncMode
from airbyte_cdk.sources.streams.http import HttpStream

from .client import BrightDataClient


class SerpResultsStream(HttpStream):
    """
    Stream for SERP results from Bright Data API
    """

    primary_key = "search_query"
    url_base = "https://api.brightdata.com/"

    def __init__(self, client: BrightDataClient, config: Dict[str, Any], **kwargs):
        super().__init__(**kwargs)
        self.client = client
        self.config = config
        self.search_queries = config.get("search_queries", [])

    @property
    def name(self) -> str:
        return "serp_results"

    def path(self, **kwargs) -> str:
        return "request"

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        return None

    def request_params(
        self,
        stream_state: Mapping[str, Any],
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None,
    ) -> MutableMapping[str, Any]:
        return {}

    def parse_response(
        self,
        response: requests.Response,
        stream_state: Mapping[str, Any],
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None,
    ) -> Iterable[Mapping]:
        data = response.json()
        search_query = stream_slice.get("search_query", "")

        record = {
            "search_query": search_query,
            "status_code": data.get("status_code"),
            "url": data.get("url"),
            "serp_data": {
                "html_content": data.get("body", ""),
                "content_length": len(data.get("body", "")),
                "headers": data.get("headers", {}),
            },
            "zone": self.client.zone,
            "country": self.client.country,
            "response_headers": data.get("headers", {}),
        }

        yield record

    def stream_slices(self, **kwargs) -> Iterable[Optional[Mapping[str, Any]]]:
        for query in self.search_queries:
            yield {"search_query": query}

    def read_records(
        self,
        sync_mode: SyncMode,
        cursor_field: List[str] = None,
        stream_slice: Mapping[str, Any] = None,
        stream_state: Mapping[str, Any] = None,
    ) -> Iterable[Mapping[str, Any]]:
        search_query = stream_slice["search_query"] if stream_slice else "unknown"

        try:
            response = self.client.make_request(search_query)
            yield from self.parse_response(response=response, stream_state=stream_state, stream_slice=stream_slice)

        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 429:
                self.logger.warning("Rate limit exceeded, consider adding delays")
            self.logger.error(f"HTTP Error processing search query '{search_query}': {str(e)}")
            raise e
        except Exception as e:
            self.logger.error(f"Error processing search query '{search_query}': {str(e)}")
            raise
