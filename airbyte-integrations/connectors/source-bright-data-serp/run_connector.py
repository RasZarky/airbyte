#!/usr/bin/env python3
# Copyright (c) 2025 Airbyte, Inc., all rights reserved.

"""
Simple runner script for the Bright Data SERP connector.
Run this directly without using the airbyte-cdk CLI.
"""

import json
import logging
import sys
from pathlib import Path


# Add the src directory to Python path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from source_bright_data_serp.source import SourceBrightDataSerp


# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger("bright_data_serp")


def load_config(config_path="config.json"):
    """Load configuration from JSON file."""
    with open(config_path, "r") as f:
        return json.load(f)


def check_connection():
    """Test the connection to Bright Data API."""
    print("🔍 Checking connection...")
    source = SourceBrightDataSerp()
    config = load_config()

    success, message = source.check_connection(logger, config)
    if success:
        print("✅ Connection successful!")
        return True
    else:
        print(f"❌ Connection failed: {message}")
        return False


def discover_schema():
    """Discover the schema of the SERP results."""
    print("📋 Discovering schema...")
    source = SourceBrightDataSerp()
    config = load_config()

    catalog = source.discover(logger, config)
    print("\nDiscovered Catalog:")
    print(json.dumps(catalog.to_json(), indent=2))

    # Save schema to file
    with open("discovered_schema.json", "w") as f:
        json.dump(catalog.to_json(), f, indent=2)
    print("\n💾 Schema saved to discovered_schema.json")


def read_data():
    """Read data from Bright Data SERP API."""
    print("📖 Reading data...")
    source = SourceBrightDataSerp()
    config = load_config()

    # Create a simple catalog
    catalog = {
        "streams": [
            {
                "stream": {
                    "name": "serp_results",
                    "json_schema": {},
                    "supported_sync_modes": ["full_refresh"],
                    "source_defined_primary_key": [["position"]],
                },
                "sync_mode": "full_refresh",
                "destination_sync_mode": "overwrite",
            }
        ]
    }

    record_count = 0
    print("\n📊 Records:")
    print("-" * 80)

    for message in source.read(logger, config, catalog, None):
        if message.record:
            record_count += 1
            record_data = message.record.data
            print(f"Record {record_count}:")
            print(f"  Position: {record_data.get('position')}")
            print(f"  Title: {record_data.get('title')}")
            print(f"  URL: {record_data.get('url')}")
            print(f"  Type: {record_data.get('result_type')}")
            print("-" * 80)

    print(f"\n✅ Read {record_count} records successfully!")


def main():
    """Main function to run the connector."""
    if len(sys.argv) != 2:
        print("Usage: python run_connector.py <command>")
        print("Commands: check, discover, read")
        sys.exit(1)

    command = sys.argv[1]

    try:
        if command == "check":
            check_connection()
        elif command == "discover":
            discover_schema()
        elif command == "read":
            read_data()
        else:
            print(f"Unknown command: {command}")
            print("Available commands: check, discover, read")
            sys.exit(1)
    except Exception as e:
        logger.error(f"Error: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
