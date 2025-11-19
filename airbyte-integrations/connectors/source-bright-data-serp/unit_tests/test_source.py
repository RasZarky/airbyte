# Copyright (c) 2023 Airbyte, Inc., all rights reserved.

import pytest
from source_bright_data_serp import SourceBrightDataSerp
from source_bright_data_serp.source import BrightDataSERPStream


class TestSourceBrightDataSerp:
    def test_check_connection_missing_api_key(self):
        source = SourceBrightDataSerp()
        result, message = source.check_connection({"search_url": "https://www.google.com/search?q=test", "zone": "serp_api1"}, None)
        assert result is False
        assert "api_key is required" in message

    def test_check_connection_missing_search_url(self):
        source = SourceBrightDataSerp()
        result, message = source.check_connection({"api_key": "test_key", "zone": "serp_api1"}, None)
        assert result is False
        assert "search_url is required" in message

    def test_check_connection_missing_zone(self):
        source = SourceBrightDataSerp()
        result, message = source.check_connection({"api_key": "test_key", "search_url": "https://www.google.com/search?q=test"}, None)
        assert result is False
        assert "zone is required" in message

    def test_streams(self):
        source = SourceBrightDataSerp()
        config = {"api_key": "test_key", "search_url": "https://www.google.com/search?q=test", "zone": "serp_api1", "country": "us"}
        streams = source.streams(config)
        assert len(streams) == 1
        assert streams[0].name == "serp_results"
        assert streams[0].primary_key == "position"


class TestBrightDataSERPStream:
    def test_stream_initialization(self):
        config = {"api_key": "test_key", "search_url": "https://www.google.com/search?q=test", "zone": "serp_api1"}
        stream = BrightDataSERPStream(config=config)
        assert stream.primary_key == "position"
        assert stream.url_base == "https://api.brightdata.com"

    def test_path(self):
        config = {"api_key": "test_key", "search_url": "https://www.google.com/search?q=test", "zone": "serp_api1"}
        stream = BrightDataSERPStream(config=config)
        assert stream.path() == "request"

    def test_http_method(self):
        config = {"api_key": "test_key", "search_url": "https://www.google.com/search?q=test", "zone": "serp_api1"}
        stream = BrightDataSERPStream(config=config)
        assert stream.http_method() == "POST"

    def test_request_body_data(self):
        config = {
            "api_key": "test_key",
            "search_url": "https://www.google.com/search?q=pizza",
            "zone": "serp_api1",
            "country": "us",
            "data_format": "markdown",
        }
        stream = BrightDataSERPStream(config=config)
        body = stream.request_body_data(stream_state={})

        assert body["zone"] == "serp_api1"
        assert body["url"] == "https://www.google.com/search?q=pizza"
        assert body["format"] == "json"
        assert body["method"] == "GET"
        assert body["country"] == "us"
        assert body["data_format"] == "markdown"
