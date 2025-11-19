# Bright Data SERP Source Connector

This connector extracts search engine results from major search engines including Google, Bing, Yandex, DuckDuckGo, and more using the [Bright Data SERP API](https://docs.brightdata.com/api-reference/serp-api).

## Features

- Extract organic search results, paid ads, local listings, and shopping results
- Support for multiple search engines (Google, Bing, Yandex, DuckDuckGo, etc.)
- Configurable country targeting
- Structured JSON output with rich metadata
- Support for markdown conversion and screenshot capture

## Setup Guide

### Prerequisites

1. **Bright Data Account**: You need a Bright Data account with SERP API access
2. **API Key**: Obtain your API key from [Bright Data settings](https://brightdata.com/cp/setting/users)
3. **Zone Configuration**: Set up your SERP zone in the [Bright Data control panel](https://brightdata.com/cp/zones)

### Configuration

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `api_key` | string | Yes | Your Bright Data API Key |
| `zone` | string | Yes | Zone identifier (default: "serp_api1") |
| `search_url` | string | Yes | Complete search URL to scrape |
| `country` | string | No | Two-letter country code (default: "us") |
| `data_format` | string | No | Additional format: "markdown" or "screenshot" |

### Example Configuration

```json
{
  "api_key": "your_bright_data_api_key",
  "zone": "serp_api1",
  "search_url": "https://www.google.com/search?q=artificial+intelligence",
  "country": "us",
  "data_format": "json"
}