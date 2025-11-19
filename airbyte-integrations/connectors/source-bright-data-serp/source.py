#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

import copy
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple, Union

import requests

from airbyte_cdk.models import SyncMode
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.availability_strategy import AvailabilityStrategy
from airbyte_cdk.sources.streams.http import HttpStream
from airbyte_cdk.sources.streams.http.requests_native_auth import TokenAuthenticator
from airbyte_cdk.utils.traced_exception import AirbyteTracedException, FailureType


class BrightDataSERPStream(HttpStream):
    """
    Bright Data SERP API stream implementation.
    Documentation: https://docs.brightdata.com/api-reference/serp-api
    """

    url_base = "https://api.brightdata.com"
    primary_key = "position"

    def __init__(self, config: Mapping[str, Any], **kwargs):
        super().__init__(**kwargs)
        self.config = config
        self.zone = config.get("zone", "serp_api1")
        self.search_url = config["search_url"]
        self.country = config.get("country", "us")
        self.data_format = config.get("data_format")

    @property
    def availability_strategy(self) -> Optional["AvailabilityStrategy"]:
        return None

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        """
        Bright Data SERP API doesn't support pagination in a single request.
        For multiple pages, users should configure multiple streams with different search URLs.
        """
        return None

    def request_body_data(
        self,
        stream_state: Mapping[str, Any],
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None,
    ) -> Optional[Union[Mapping[str, Any], str]]:
        """
        Construct the request body for Bright Data SERP API.
        """
        body = {
            "zone": self.zone,
            "url": self.search_url,
            "format": "json",
            "method": "GET",
            "country": self.country,
        }

        if self.data_format:
            body["data_format"] = self.data_format

        return body

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        """
        Parse the Bright Data SERP API response and extract organic results.
        """
        try:
            response_json = response.json()

            # Handle API errors
            if response.status_code != 200:
                error_message = response_json.get("message", "Unknown API error")
                internal_message = f"API Error {response.status_code}: {error_message}"
                external_message = f"Bright Data API error: {error_message}"

                if response.status_code in [401, 403]:
                    external_message = "Invalid API key or insufficient permissions. Please check your Bright Data credentials."
                    raise AirbyteTracedException(
                        message=external_message, internal_message=internal_message, failure_type=FailureType.config_error
                    )
                elif response.status_code == 429:
                    external_message = "API rate limit exceeded. Please try again later."
                    raise AirbyteTracedException(
                        message=external_message, internal_message=internal_message, failure_type=FailureType.config_error
                    )
                else:
                    raise AirbyteTracedException(
                        message=external_message, internal_message=internal_message, failure_type=FailureType.system_error
                    )

            # Extract organic results from the response
            results = []

            # Organic results
            organic_results = response_json.get("organic", [])
            for result in organic_results:
                result["result_type"] = "organic"
                results.append(result)

            # Paid results (ads)
            paid_results = response_json.get("ads", [])
            for result in paid_results:
                result["result_type"] = "paid"
                results.append(result)

            # Local results
            local_results = response_json.get("local", [])
            for result in local_results:
                result["result_type"] = "local"
                results.append(result)

            # Shopping results
            shopping_results = response_json.get("shopping", [])
            for result in shopping_results:
                result["result_type"] = "shopping"
                results.append(result)

            # Add metadata to each result
            for result in results:
                result["search_url"] = self.search_url
                result["zone"] = self.zone
                result["country"] = self.country
                result["data_format"] = self.data_format

            yield from results

        except AirbyteTracedException:
            raise
        except Exception as e:
            internal_message = f"Failed to parse response: {str(e)}"
            external_message = "Failed to process API response. Please check the connector logs."
            raise AirbyteTracedException(message=external_message, internal_message=internal_message, failure_type=FailureType.system_error)

    def path(
        self,
        *,
        stream_state: Optional[Mapping[str, Any]] = None,
        stream_slice: Optional[Mapping[str, Any]] = None,
        next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> str:
        return "request"

    def request_headers(
        self,
        stream_state: Mapping[str, Any],
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None,
    ) -> Mapping[str, Any]:
        """
        Bright Data API uses Bearer token authentication in headers.
        """
        return {"Content-Type": "application/json"}

    def http_method(self) -> str:
        return "POST"


class SourceBrightDataSerp(AbstractSource):
    """
    Source implementation for Bright Data SERP API.
    This connector extracts search engine results from major search engines.
    """

    def check_connection(self, logger, config) -> Tuple[bool, any]:
        """
        Validate the input configuration by testing the API connection.
        """
        required_fields = ["api_key", "search_url", "zone"]
        for field in required_fields:
            if not config.get(field):
                return False, f"{field} is required"

        try:
            auth = TokenAuthenticator(token=config["api_key"])
            stream = BrightDataSERPStream(config=config, authenticator=auth)

            test_body = stream.request_body_data(stream_state={})
            headers = stream.request_headers(stream_state={})
            headers["Authorization"] = f"Bearer {config['api_key']}"

            response = requests.post(f"{stream.url_base}/{stream.path()}", json=test_body, headers=headers, timeout=30)

            if response.status_code == 200:
                return True, None
            else:
                error_message = response.json().get("message", "Unknown error")
                return False, f"API connection failed: {error_message}"

        except AirbyteTracedException as e:
            return False, str(e)
        except Exception as e:
            return False, f"Unable to connect to Bright Data API: {str(e)}"

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        auth = TokenAuthenticator(token=config["api_key"])
        return [BrightDataSERPStream(config=config, authenticator=auth)]
