# Copyright (c) 2025 Airbyte, Inc., all rights reserved.

import pytest
from unittest.mock import Mock, patch
from source_brightdata_serp.source import SourceBrightDataSerp


class TestSourceBrightDataSerp:
    @pytest.fixture
    def config(self):
        return {
            "api_key": "test_api_key",
            "zone": "serp_api1",
            "search_queries": ["test query"],
            "country": "us",
            "format": "json"
        }
    
    def test_check_connection_success(self, config):
        source = SourceBrightDataSerp()
        with patch('source_brightdata_serp.source.BrightDataClient') as mock_client:
            mock_instance = mock_client.return_value
            mock_instance._session.post.return_value.status_code = 200
            mock_instance._session.post.return_value.json.return_value = {}
            
            result, message = source.check_connection(None, config)
            assert result is True
            assert message is None
    
    def test_check_connection_auth_failure(self, config):
        source = SourceBrightDataSerp()
        with patch('source_brightdata_serp.source.BrightDataClient') as mock_client:
            mock_instance = mock_client.return_value
            mock_instance._session.post.return_value.status_code = 401
            
            result, message = source.check_connection(None, config)
            assert result is False
            assert "Authentication failed" in message
    
    def test_streams(self, config):
        source = SourceBrightDataSerp()
        streams = source.streams(config)
        assert len(streams) == 1
        assert streams[0].name == "serp_results"