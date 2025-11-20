# Bright Data SERP Source Connector

A lightweight Airbyte source connector that extracts search engine results using Bright Data's SERP API. Use it to collect structured SERP data for analytics and pipelines.

## Features
- Full refresh sync mode
- Structured JSON extraction (organic, paid, related searches, metadata)
- Configurable country/proxy zone
- Support for JSON, raw HTML, markdown, screenshots
- Retries and exponential backoff for rate limits and server errors

## Prerequisites
- Bright Data account with SERP API access
- Bright Data API key
- Zone identifier (e.g., `serp_api1`)
- Python 3.8+

## Quick start

Install dependencies (poetry recommended):

```bash
poetry install
# or with pip
pip install airbyte-cdk requests
```

Test connection / run connector:

```bash
# Test connection
python main.py check --config config.json --catalog catalog.json

# Read data
python main.py read --config config.json --catalog catalog.json

# Debug mode
python debug_read.py

#OR

# Test connection
poetry run source-brightdata-serp check --config config.json

# Discover streams and schema
poetry run source-brightdata-serp discover --config config.json

# Read data
poetry run source-brightdata-serp read --config config.json --catalog catalog.json

```

## Configuration

Example `config.json`:

```json
{
  "api_key": "your_brightdata_api_key",
  "zone": "serp_api1",
  "search_queries": [
    "data integration",
    "airbyte tutorial",
    "ETL best practices"
  ],
  "country": "us",
  "format": "json"
}
```

Configuration parameters:
- `api_key` (string, required) — Bright Data API key
- `zone` (string, required) — Bright Data zone (default: `serp_api1`)
- `search_queries` (array of strings, required) — list of queries to fetch
- `country` (string, optional) — two-letter country code (default: `us`)
- `format` (string, optional) — `json` or `raw`

## API endpoint

Endpoint: `https://api.brightdata.com/serp/req`  
Method: POST  
Auth: Bearer token (API key)  
Content-Type: `application/json`

Example request payload:

```json
{
  "zone": "serp_api1",
  "url": "https://www.google.com/search?q=example+search",
  "format": "json",
  "country": "us"
}
```

## Supported streams

- `serp_results` — each record contains:
  - `query` — original search query
  - `timestamp` — collection time (ISO 8601)
  - `organic_results` — array of organic results
  - `paid_results` — array of paid/ad results
  - `related_searches` — array of related terms
  - `search_metadata` — metadata about the request

## Error handling & rate limits
- HTTP 429: automatic retry with backoff
- HTTP 401: authentication error — validate API key
- HTTP 5xx: retries with exponential backoff
Tune concurrency and request rate according to your Bright Data plan.

## File structure (project)
brightdata-serp-connector/
- source_brightdata_serp/
  - __init__.py
  - source.py
  - schemas/
  - ...
- pyproject.toml
- README.md
- config.json

## Troubleshooting
- Authentication failed: verify API key and account status
- Rate limit exceeded: reduce concurrency or contact Bright Data support
- Invalid zone: confirm zone identifier in Bright Data control panel


