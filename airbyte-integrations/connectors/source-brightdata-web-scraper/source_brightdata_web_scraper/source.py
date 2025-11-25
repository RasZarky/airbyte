# Copyright (c) 2025 Airbyte, Inc., all rights reserved.

import logging
import time
from typing import Any, Dict, Iterable, List, Mapping, MutableMapping, Optional, Tuple

import requests

from airbyte_cdk.models import ConnectorSpecification, SyncMode
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http import HttpStream
from airbyte_cdk.sources.streams.http.auth import TokenAuthenticator


class WebScraperResultsStream(HttpStream):
    """
    Stream for Web Scraper results from Bright Data API
    """

    primary_key = "id"
    url_base = "https://api.brightdata.com/"

    # FIX: http_method should be a class attribute or property that returns a string
    http_method = "POST"

    def __init__(self, config: Dict[str, Any], **kwargs):
        super().__init__(**kwargs)
        self.config = config
        self.api_key = config["api_key"]
        self.dataset_id = config["dataset_id"]
        self.urls = config.get("urls", [])
        self._authenticator = TokenAuthenticator(token=self.api_key)

    @property
    def name(self) -> str:
        return "web_scraper_results"

    @property
    def authenticator(self):
        return self._authenticator

    def path(self, **kwargs) -> str:
        return "datasets/v3/trigger"

    # REMOVE the http_method() method - use the class attribute instead
    # def http_method(self) -> str:
    #     return "POST"

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        return None

    def request_params(
        self,
        stream_state: Mapping[str, Any],
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None,
    ) -> MutableMapping[str, Any]:
        params = {
            "dataset_id": self.dataset_id,
            "format": self.config.get("format", "json"),
        }

        # Add optional parameters if they exist
        optional_params = [
            "custom_output_fields",
            "type",
            "discover_by",
            "include_errors",
            "limit_per_input",
            "limit_multiple_results",
            "notify",
            "endpoint",
            "auth_header",
            "uncompressed_webhook",
        ]

        for param in optional_params:
            if param in self.config and self.config[param] is not None:
                params[param] = self.config[param]

        return params

    def request_body_json(
        self,
        stream_state: Mapping[str, Any],
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None,
    ) -> Optional[Mapping]:
        if stream_slice and "url" in stream_slice:
            return [{"url": stream_slice["url"]}]
        return []

    def parse_response(
        self,
        response: requests.Response,
        stream_state: Mapping[str, Any],
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None,
    ) -> Iterable[Mapping]:
        data = response.json()
        snapshot_id = data.get("snapshot_id")

        if snapshot_id:
            self.logger.info(f"Successfully triggered snapshot: {snapshot_id}")

            # Get results from snapshot
            results = self._get_snapshot_results(snapshot_id)

            for result in results:
                yield {
                    "snapshot_id": snapshot_id,
                    "data": result,
                    "url": result.get("url", result.get("input_url", stream_slice.get("url", ""))),
                    "id": result.get("id", result.get("linkedin_id", "")),
                    "timestamp": result.get("timestamp", ""),
                    "status": "completed",
                }
        else:
            self.logger.error(f"No snapshot_id in response: {data}")
            # Return error info
            yield {"error": "No snapshot_id received", "raw_response": data, "status": "failed"}

    def _get_snapshot_results(self, snapshot_id: str, max_wait_time: int = 300) -> List[Dict[str, Any]]:
        """Poll for snapshot results with exponential backoff"""
        wait_time = 5
        total_wait_time = 0

        self.logger.info(f"Waiting for snapshot {snapshot_id} to be ready...")

        while total_wait_time < max_wait_time:
            try:
                response = requests.get(
                    f"https://api.brightdata.com/datasets/v3/snapshot/{snapshot_id}",
                    params={"format": "json"},
                    headers={"Authorization": f"Bearer {self.api_key}"},
                    timeout=30,
                )

                if response.status_code == 200:
                    data = response.json()
                    if data and len(data) > 0:
                        self.logger.info(f"Snapshot {snapshot_id} ready with {len(data)} records")
                        return data
                    else:
                        self.logger.info(f"Snapshot {snapshot_id} not ready yet, waiting {wait_time}s...")
                        time.sleep(wait_time)
                        total_wait_time += wait_time
                        wait_time = min(wait_time * 1.5, 30)
                else:
                    response.raise_for_status()

            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 404:
                    self.logger.info(f"Snapshot {snapshot_id} not ready yet, waiting {wait_time}s...")
                    time.sleep(wait_time)
                    total_wait_time += wait_time
                    wait_time = min(wait_time * 1.5, 30)
                else:
                    self.logger.error(f"HTTP error checking snapshot: {str(e)}")
                    raise e
            except Exception as e:
                self.logger.error(f"Error checking snapshot: {str(e)}")
                raise e

        raise TimeoutError(f"Snapshot {snapshot_id} not ready after {max_wait_time} seconds")

    def stream_slices(self, **kwargs) -> Iterable[Optional[Mapping[str, Any]]]:
        # Process URLs one by one to avoid API limits
        for url in self.urls:
            self.logger.info(f"Processing URL: {url}")
            yield {"url": url}


class SourceBrightDataWebScraper(AbstractSource):
    def spec(self, logger: logging.Logger) -> ConnectorSpecification:
        """
        Returns the spec for this connector.
        """
        return ConnectorSpecification(
            documentationUrl="https://docs.airbyte.com/integrations/sources/brightdata-web-scraper",
            connectionSpecification={
                "$schema": "http://json-schema.org/draft-07/schema#",
                "title": "Bright Data Web Scraper Spec",
                "type": "object",
                "required": ["api_key", "dataset_id", "urls"],
                "additionalProperties": False,
                "properties": {
                    "api_key": {
                        "type": "string",
                        "title": "API Key",
                        "description": "Your Bright Data API Key",
                        "airbyte_secret": True,
                        "order": 0,
                    },
                    "dataset_id": {
                        "type": "string",
                        "title": "Dataset ID",
                        "description": "Dataset ID for which data collection is triggered",
                        "order": 1,
                        "example": "gd_l1vikfnt1wgvvqz95w",
                    },
                    "urls": {
                        "type": "array",
                        "title": "URLs",
                        "description": "List of URLs to scrape",
                        "items": {"type": "string"},
                        "order": 2,
                        "example": ["https://www.linkedin.com/in/example1", "https://www.linkedin.com/in/example2"],
                    },
                    "custom_output_fields": {
                        "type": "string",
                        "title": "Custom Output Fields",
                        "description": "Filter response data to include only specified fields (pipe-separated)",
                        "order": 3,
                        "example": "url|about.updated_on",
                    },
                    "type": {
                        "type": "string",
                        "title": "Collection Type",
                        "description": 'Set to "discover_new" to trigger collection with discovery phase',
                        "enum": ["discover_new"],
                        "order": 4,
                    },
                    "discover_by": {
                        "type": "string",
                        "title": "Discover By",
                        "description": "Method for discovering new data",
                        "enum": ["keyword", "best_sellers_url", "category_url", "location"],
                        "order": 5,
                    },
                    "include_errors": {
                        "type": "boolean",
                        "title": "Include Errors",
                        "description": "Include errors report with results",
                        "default": False,
                        "order": 6,
                    },
                    "limit_per_input": {
                        "type": "integer",
                        "title": "Limit Per Input",
                        "description": "Limit number of results per input",
                        "minimum": 1,
                        "order": 7,
                    },
                    "limit_multiple_results": {
                        "type": "integer",
                        "title": "Limit Multiple Results",
                        "description": "Limit total number of results",
                        "minimum": 1,
                        "order": 8,
                    },
                    "notify": {
                        "type": "boolean",
                        "title": "Notify",
                        "description": "Enable notifications upon completion",
                        "default": False,
                        "order": 9,
                    },
                    "endpoint": {
                        "type": "string",
                        "title": "Webhook Endpoint",
                        "description": "Webhook URL for notifications",
                        "order": 10,
                    },
                    "format": {
                        "type": "string",
                        "title": "Format",
                        "description": "Data delivery format",
                        "enum": ["json", "ndjson", "jsonl", "csv"],
                        "default": "json",
                        "order": 11,
                    },
                    "auth_header": {
                        "type": "string",
                        "title": "Webhook Auth Header",
                        "description": "Authorization header for webhook delivery",
                        "order": 12,
                    },
                    "uncompressed_webhook": {
                        "type": "boolean",
                        "title": "Uncompressed Webhook",
                        "description": "Send data uncompressed via webhook",
                        "default": False,
                        "order": 13,
                    },
                },
            },
        )

    def check_connection(self, logger: logging.Logger, config: Mapping[str, Any]) -> Tuple[bool, Any]:
        try:
            dataset_id = config.get("dataset_id")
            urls = config.get("urls", [])
            api_key = config.get("api_key")

            if not dataset_id:
                return False, "No dataset_id provided in configuration"

            if not urls:
                return False, "No URLs provided in configuration"

            if not api_key:
                return False, "No API key provided"

            # Test the trigger endpoint
            headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}

            test_data = [{"url": urls[0]}]
            params = {"dataset_id": dataset_id, "format": "json"}

            logger.info(f"Testing API connection with dataset: {dataset_id}")

            response = requests.post(
                "https://api.brightdata.com/datasets/v3/trigger", params=params, json=test_data, headers=headers, timeout=30
            )

            if response.status_code == 200:
                data = response.json()
                snapshot_id = data.get("snapshot_id")
                if snapshot_id:
                    logger.info(f"✅ Connection successful! Snapshot ID: {snapshot_id}")
                    return True, None
                else:
                    return False, f"API returned 200 but no snapshot_id: {data}"
            else:
                return False, f"API request failed with status {response.status_code}: {response.text}"

        except Exception as e:
            return False, f"An error occurred: {str(e)}"

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        return [WebScraperResultsStream(config)]
