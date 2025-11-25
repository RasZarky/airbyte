# Copyright (c) 2025 Airbyte, Inc., all rights reserved.

import json

import requests


# Your config
api_key = "8d6e4d1c20c89d26cd89a4a4e32bda872dbc1c0c39582c6a9c14ab978aa312cd"
dataset_id = "gd_l1viktl72bvl7bjuj0"
urls = ["https://www.linkedin.com/in/kylelacy/"]

# Test the API directly
headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}

data = [{"url": urls[0]}]
params = {"dataset_id": dataset_id, "format": "json"}

print("Testing Bright Data Web Scraper API...")
print(f"URL: https://api.brightdata.com/datasets/v3/trigger")
print(f"Dataset ID: {dataset_id}")

try:
    response = requests.post("https://api.brightdata.com/datasets/v3/trigger", params=params, json=data, headers=headers, timeout=30)

    print(f"Status Code: {response.status_code}")
    print(f"Response Headers: {dict(response.headers)}")
    print(f"Response Body: {response.text}")

    if response.status_code == 200:
        print("✅ API call successful!")
        data = response.json()
        print(f"Snapshot ID: {data.get('snapshot_id')}")
    else:
        print("❌ API call failed!")

except Exception as e:
    print(f"❌ Error: {str(e)}")
