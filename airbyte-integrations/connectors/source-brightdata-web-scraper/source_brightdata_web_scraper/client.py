# # Copyright (c) 2025 Airbyte, Inc., all rights reserved.

# import time
# from typing import Any, Dict, List

# import requests


# class BrightDataWebScraperClient:
#     """
#     Client for handling Bright Data Web Scraper API authentication and requests
#     """

#     def __init__(self, config: Dict[str, Any]):
#         self.api_key = config["api_key"]
#         self.dataset_id = config["dataset_id"]
#         self.custom_output_fields = config.get("custom_output_fields")
#         self.collection_type = config.get("type")
#         self.discover_by = config.get("discover_by")
#         self.include_errors = config.get("include_errors", False)
#         self.limit_per_input = config.get("limit_per_input")
#         self.limit_multiple_results = config.get("limit_multiple_results")
#         self.notify = config.get("notify")
#         self.endpoint = config.get("endpoint")
#         self.format = config.get("format", "json")
#         self.auth_header = config.get("auth_header")
#         self.uncompressed_webhook = config.get("uncompressed_webhook")

#         self.trigger_url = "https://api.brightdata.com/datasets/v3/trigger"
#         self.snapshot_url = "https://api.brightdata.com/datasets/v3/snapshot/"

#         self.headers = {
#             "Authorization": f"Bearer {self.api_key}",
#             "Content-Type": "application/json"
#         }

#         self._session = requests.Session()
#         self._session.headers.update(self.headers)

#     def trigger_collection_params(self) -> Dict[str, Any]:
#         params = {
#             "dataset_id": self.dataset_id,
#             "format": self.format,
#         }

#         if self.custom_output_fields:
#             params["custom_output_fields"] = self.custom_output_fields
#         if self.collection_type:
#             params["type"] = self.collection_type
#         if self.discover_by:
#             params["discover_by"] = self.discover_by
#         if self.include_errors:
#             params["include_errors"] = "true"
#         if self.limit_per_input:
#             params["limit_per_input"] = self.limit_per_input
#         if self.limit_multiple_results:
#             params["limit_multiple_results"] = self.limit_multiple_results
#         if self.notify:
#             params["notify"] = self.notify
#         if self.endpoint:
#             params["endpoint"] = self.endpoint
#         if self.auth_header:
#             params["auth_header"] = self.auth_header
#         if self.uncompressed_webhook:
#             params["uncompressed_webhook"] = "true"

#         return params

#     def trigger_collection(self, urls: List[str]) -> requests.Response:
#         params = self.trigger_collection_params()

#         # Prepare input data
#         input_data = [{"url": url} for url in urls]

#         max_retries = 3
#         for attempt in range(max_retries):
#             try:
#                 response = self._session.post(
#                     self.trigger_url,
#                     params=params,
#                     json=input_data,
#                     timeout=120,
#                 )

#                 response.raise_for_status()
#                 return response

#             except requests.exceptions.HTTPError as e:
#                 if e.response.status_code == 429:
#                     if attempt < max_retries - 1:
#                         wait_time = 2**attempt
#                         time.sleep(wait_time)
#                         continue
#                 raise e
#             except Exception as e:
#                 if attempt < max_retries - 1:
#                     wait_time = 2**attempt
#                     time.sleep(wait_time)
#                     continue
#                 raise

#     def get_snapshot_results(self, snapshot_id: str, max_wait_time: int = 300) -> List[Dict[str, Any]]:
#         """
#         Poll for snapshot results with exponential backoff
#         """
#         wait_time = 5
#         total_wait_time = 0

#         while total_wait_time < max_wait_time:
#             try:
#                 response = self._session.get(
#                     f"{self.snapshot_url}{snapshot_id}",
#                     params={"format": "json"},
#                     timeout=30,
#                 )

#                 if response.status_code == 200:
#                     data = response.json()
#                     # Check if results are ready (not empty array)
#                     if data and len(data) > 0:
#                         return data
#                     else:
#                         # Results not ready yet, wait and retry
#                         time.sleep(wait_time)
#                         total_wait_time += wait_time
#                         wait_time = min(wait_time * 1.5, 30)  # Exponential backoff with max 30 seconds
#                 else:
#                     response.raise_for_status()

#             except requests.exceptions.HTTPError as e:
#                 if e.response.status_code == 404:
#                     # Snapshot not ready yet
#                     time.sleep(wait_time)
#                     total_wait_time += wait_time
#                     wait_time = min(wait_time * 1.5, 30)
#                 else:
#                     raise e
#             except Exception as e:
#                 raise e

#         raise TimeoutError(f"Snapshot {snapshot_id} not ready after {max_wait_time} seconds")
