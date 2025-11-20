# Copyright (c) 2025 Airbyte, Inc., all rights reserved.

import json
import logging
import sys

from source_brightdata_serp.source import SourceBrightDataSerp

from airbyte_cdk.models import SyncMode


def main():
    if len(sys.argv) < 2:
        print("Usage: python main.py <command> [--config config.json] [--catalog catalog.json]")
        print("Commands: check, read")
        return

    command = sys.argv[1]
    source = SourceBrightDataSerp()

    if command == "check":
        if "--config" in sys.argv:
            config_index = sys.argv.index("--config") + 1
            if config_index < len(sys.argv):
                with open(sys.argv[config_index], "r") as f:
                    config = json.load(f)

                success, message = source.check_connection(logging.getLogger(), config)
                if success:
                    print("Connection check: SUCCESS")
                else:
                    print(f"Connection check: FAILED - {message}")
            else:
                print("Missing config file path after --config")
        else:
            print("Missing --config parameter")

    elif command == "read":
        config = None
        catalog = None

        # Parse config file
        if "--config" in sys.argv:
            config_index = sys.argv.index("--config") + 1
            if config_index < len(sys.argv):
                with open(sys.argv[config_index], "r") as f:
                    config = json.load(f)
            else:
                print("Missing config file path after --config")
                return

        # Parse catalog file
        if "--catalog" in sys.argv:
            catalog_index = sys.argv.index("--catalog") + 1
            if catalog_index < len(sys.argv):
                with open(sys.argv[catalog_index], "r") as f:
                    catalog = json.load(f)
            else:
                print("Missing catalog file path after --catalog")
                return

        if config and catalog:
            print("Reading data from Bright Data SERP API...")

            # Get the streams
            streams = source.streams(config)

            for stream in streams:
                print(f"\n=== Reading from stream: {stream.name} ===")
                record_count = 0

                try:
                    # Get stream slices first
                    slices = list(stream.stream_slices(sync_mode=SyncMode.full_refresh))
                    print(f"Number of slices: {len(slices)}")

                    for slice_data in slices:
                        print(f"Processing slice: {slice_data}")

                        # Read records for each slice
                        for record in stream.read_records(sync_mode=SyncMode.full_refresh, stream_slice=slice_data, stream_state={}):
                            record_count += 1
                            print(f"\n--- Record {record_count} ---")
                            print(f"Search Query: {record.get('search_query')}")
                            print(f"Status Code: {record.get('status_code')}")
                            print(f"URL: {record.get('url')}")
                            print(f"Zone: {record.get('zone')}")
                            print(f"Country: {record.get('country')}")

                            # Show a preview of the SERP data
                            serp_data = record.get("serp_data", {})
                            if serp_data:
                                html_content = serp_data.get("html_content", "")
                                print(f"HTML Content Length: {len(html_content)} characters")
                                if html_content:
                                    print("First 200 chars of HTML:")
                                    print(html_content[:200] + "...")

                    print(f"\n✅ Successfully read {record_count} records from {stream.name}")

                except Exception as e:
                    print(f"❌ Error reading from stream {stream.name}: {str(e)}")
                    import traceback

                    traceback.print_exc()
        else:
            print("Missing --config or --catalog parameter for read command")

    else:
        print(f"Unknown command: {command}")
        print("Available commands: check, read")


if __name__ == "__main__":
    main()
