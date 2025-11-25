# # Copyright (c) 2025 Airbyte, Inc., all rights reserved.

# import logging
# from typing import Any, List, Mapping, Tuple

# import requests
# from airbyte_cdk.sources.declarative.yaml_declarative_source import YamlDeclarativeSource


# class SourceBrightDataWebScraper(YamlDeclarativeSource):
#     def __init__(self):
#         super().__init__(**{"path_to_yaml": "manifest.yaml"})

#     def check_connection(self, logger: logging.Logger, config: Mapping[str, Any]) -> Tuple[bool, Any]:
#         try:
#             dataset_id = config.get("dataset_id")
#             urls = config.get("urls", [])
#             api_key = config.get("api_key")

#             if not dataset_id:
#                 return False, "No dataset_id provided in configuration"

#             if not urls:
#                 return False, "No URLs provided in configuration"

#             if not api_key:
#                 return False, "No API key provided"

#             # Test the trigger endpoint
#             headers = {
#                 "Authorization": f"Bearer {api_key}",
#                 "Content-Type": "application/json"
#             }

#             test_data = [{"url": urls[0]}]
#             params = {"dataset_id": dataset_id, "format": "json"}

#             logger.info(f"Testing API connection with dataset: {dataset_id}")

#             response = requests.post(
#                 "https://api.brightdata.com/datasets/v3/trigger",
#                 params=params,
#                 json=test_data,
#                 headers=headers,
#                 timeout=30
#             )

#             if response.status_code == 200:
#                 data = response.json()
#                 snapshot_id = data.get("snapshot_id")
#                 if snapshot_id:
#                     logger.info(f"✅ Connection successful! Snapshot ID: {snapshot_id}")
#                     return True, None
#                 else:
#                     return False, f"API returned 200 but no snapshot_id: {data}"
#             else:
#                 return False, f"API request failed with status {response.status_code}: {response.text}"

#         except Exception as e:
#             return False, f"An error occurred: {str(e)}"
