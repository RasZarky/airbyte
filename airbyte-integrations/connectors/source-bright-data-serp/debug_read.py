# Copyright (c) 2025 Airbyte, Inc., all rights reserved.

import json
import logging

from source_brightdata_serp.source import SourceBrightDataSerp


def debug_read():
    # Load config and catalog
    with open("config.json", "r") as f:
        config = json.load(f)

    with open("catalog.json", "r") as f:
        catalog = json.load(f)

    source = SourceBrightDataSerp()

    print("=== DEBUG: Starting to read data ===")
    print(f"Search queries: {config.get('search_queries')}")

    # Get streams
    streams = source.streams(config)

    for stream in streams:
        print(f"\n=== Processing stream: {stream.name} ===")
        print(f"Number of search queries: {len(stream.search_queries)}")

        try:
            # Get stream slices first
            slices = list(stream.stream_slices())
            print(f"Number of stream slices: {len(slices)}")

            for i, slice_data in enumerate(slices):
                print(f"\n--- Processing slice {i+1}: {slice_data} ---")

                try:
                    # Read records for this slice
                    records = list(stream.read_records(sync_mode="full_refresh", stream_slice=slice_data, stream_state={}))

                    print(f"Records found in this slice: {len(records)}")

                    for record in records:
                        print(f"✅ Successfully processed: {record.get('search_query')}")
                        print(f"   Status: {record.get('status_code')}")

                except Exception as slice_error:
                    print(f"❌ Error in slice {slice_data}: {str(slice_error)}")
                    continue

        except Exception as e:
            print(f"❌ Stream-level error: {str(e)}")
            import traceback

            traceback.print_exc()


if __name__ == "__main__":
    debug_read()
