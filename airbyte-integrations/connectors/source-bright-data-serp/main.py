#!/usr/bin/env python
# Copyright (c) 2025 Airbyte, Inc., all rights reserved.


import json
import logging
import sys
from pathlib import Path

from source_bright_data_serp import SourceBrightDataSerp


# Set up logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def main():
    """
    Main entry point for local execution of the Bright Data SERP connector.
    """
    source = SourceBrightDataSerp()

    # Parse command line arguments
    if len(sys.argv) < 2:
        print("Usage: python main.py <command> [options]")
        print("Commands: check, discover, read")
        sys.exit(1)

    command = sys.argv[1]

    try:
        if command == "check":
            # Check connection
            config_path = "secrets/config.json" if len(sys.argv) < 3 else sys.argv[2]
            with open(config_path, "r") as f:
                config = json.load(f)

            result, message = source.check_connection(logger, config)
            if result:
                print("Connection check: SUCCESS")
                sys.exit(0)
            else:
                print(f"Connection check: FAILED - {message}")
                sys.exit(1)

        elif command == "discover":
            # Discover schema
            config_path = "secrets/config.json" if len(sys.argv) < 3 else sys.argv[2]
            with open(config_path, "r") as f:
                config = json.load(f)

            catalog = source.discover(logger, config)
            print(json.dumps(catalog.to_json(), indent=2))

        elif command == "read":
            # Read data
            if len(sys.argv) < 4:
                print("Usage: python main.py read <config_path> <catalog_path>")
                sys.exit(1)

            config_path = sys.argv[2]
            catalog_path = sys.argv[3]

            with open(config_path, "r") as f:
                config = json.load(f)
            with open(catalog_path, "r") as f:
                catalog = json.load(f)

            # Read and print records
            for message in source.read(logger, config, catalog):
                print(json.dumps(message.to_json(), indent=2))

        else:
            print(f"Unknown command: {command}")
            print("Available commands: check, discover, read")
            sys.exit(1)

    except Exception as e:
        logger.error(f"Error executing command '{command}': {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
