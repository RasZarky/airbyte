# Bright Data Web Scraper Source Connector

A lightweight Airbyte source connector that extracts web data using Bright Data's Web Scraper API with asynchronous requests and snapshot-based results.

## Features
- Full refresh sync mode
- Asynchronous data collection with polling
- Configurable dataset parameters
- Support for JSON, NDJSON, JSONL, CSV formats
- Retries and exponential backoff for rate limits
- Batch processing of URLs

## Prerequisites
- Bright Data account with Web Scraper API access
- Bright Data API key
- Dataset ID (e.g., `gd_l1vikfnt1wgvvqz95w`)
- Python 3.9+

## Quick Start

Install dependencies:
```bash
poetry install --with dev

py -3.11 -m poetry install --with dev


poetry run source-brightdata-web-scraper check --config secrets/config.json

py -3.11 -m poetry run source-brightdata-web-scraper check --config secrets/config.json


poetry run source-brightdata-web-scraper discover --config secrets/config.json

py -3.11 -m poetry run source-brightdata-web-scraper discover --config secrets/config.json



poetry run source-brightdata-web-scraper read --config secrets/config.json --catalog integration_tests/configured_catalog.json

py -3.11 -m poetry run source-brightdata-web-scraper read --config secrets/config.json --catalog integration_tests/configured_catalog.json

4. Read with State (for incremental sync)
bash
py -3.11 -m poetry run source-brightdata-web-scraper read --config secrets/config.json --catalog integration_tests/configured_catalog.json --state integration_tests/state.json