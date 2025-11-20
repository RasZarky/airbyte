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




# More Details
Here's a detailed explanation of what each file does in your Bright Data SERP connector project:

## **Core Connector Files**

### `source.py`
- **Main orchestrator** - The central file that Airbyte interacts with
- Contains `SourceBrightDataSerp` class that inherits from Airbyte's `AbstractSource`
- Implements the three main Airbyte methods:
  - `check_connection()`: Tests if the API credentials work
  - `streams()`: Returns the list of available data streams
  - `run()`: Entry point when connector is executed
- Coordinates between Airbyte framework and your custom logic

### `streams.py`
- **Data stream definition** - Defines how data flows from the API
- Contains `SerpResultsStream` class that inherits from Airbyte's `HttpStream`
- Key methods:
  - `stream_slices()`: Creates "slices" for each search query (pizza, restaurants, etc.)
  - `parse_response()`: Transforms raw API response into structured records
  - `read_records()`: Main method that fetches and processes data for each query
- Handles the actual data extraction and transformation

### `client.py`
- **API communication layer** - Handles all interactions with Bright Data's API
- Contains `BrightDataClient` class that:
  - Builds proper API request parameters
  - Handles authentication with Bearer tokens
  - Implements retry logic with exponential backoff
  - Manages rate limiting (HTTP 429 responses)
  - Formats search queries for Google URLs
- Isolates all API-specific logic from the rest of the connector

## **Configuration Files**

### `config.json`
- **User configuration** - Example settings for testing
- Contains actual API key and search queries
- Used with `--config` flag when running the connector
- **⚠️ Security Note**: This contains a real API key - consider removing or using environment variables

### `connector_spec.json`
- **Configuration schema** - Defines what settings users can configure in Airbyte UI
- Specifies required vs optional fields, data types, and validation rules
- Used by Airbyte to generate the configuration form
- Includes helpful descriptions and default values

### `catalog.json`
- **Data schema configuration** - Tells Airbyte which streams to sync and how
- Defines sync modes (full_refresh) and destination behavior (overwrite)
- Used with `--catalog` flag for data reading operations

## **Schema Definition**

### `serp_results.json`
- **Data structure definition** - JSON Schema that describes the output data
- Defines all fields, their types, and descriptions
- Used by Airbyte for type validation and destination compatibility
- Documents what data users can expect from the connector

## **Execution & Testing Files**

### `main.py`
- **Standalone runner** - Allows testing the connector without full Airbyte setup
- Implements command-line interface for `check` and `read` commands
- Useful for development and debugging outside of Airbyte
- Parses command-line arguments and config files

### `debug_read.py`
- **Development/debugging tool** - Enhanced version of main.py with detailed logging
- Provides verbose output about each step of the data extraction process
- Shows exactly what's happening with each search query and API call
- Essential for troubleshooting during development

## **Project Configuration**

### `pyproject.toml`
- **Dependency and build configuration** - Python project metadata
- Defines:
  - Package name, version, and description
  - Dependencies (airbyte-cdk, requests, etc.)
  - Python version compatibility
  - Script entry points for Poetry
- Used by Poetry for dependency management and packaging

### `__init__.py`
- **Python package marker** - Makes the directory a proper Python package
- Exports the main `SourceBrightDataSerp` class for import
- Required for Python to recognize the directory as importable code

## **Documentation**

### `README.md`
- **User documentation** - Instructions for installing and using the connector
- Contains configuration examples, commands, and troubleshooting tips
- What end-users read to understand how to work with your connector

## **How They Work Together**

1. **User runs** `python main.py check --config config.json`
2. **`main.py`** calls **`source.py`** `check_connection()` method
3. **`source.py`** creates a **`client.py`** instance and tests the API key
4. **`client.py`** makes an actual API call to Bright Data to validate credentials
5. If successful, user runs `read` command
6. **`source.py`** creates **`streams.py`** instance
7. **`streams.py`** uses **`client.py`** to fetch data for each search query
8. Raw API responses are transformed using the schema from **`serp_results.json`**
9. Data flows back through the chain to the user

This architecture follows Airbyte's best practices with clear separation of concerns:
- **source.py** = Airbyte framework integration
- **streams.py** = Data flow logic  
- **client.py** = API communication
- **config files** = User settings and schema definitions