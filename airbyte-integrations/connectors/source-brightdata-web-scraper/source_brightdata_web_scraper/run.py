# Copyright (c) 2025 Airbyte, Inc., all rights reserved.

import sys

from airbyte_cdk.entrypoint import launch
from source_brightdata_web_scraper.source import SourceBrightDataWebScraper


def run():
    source = SourceBrightDataWebScraper()
    launch(source, sys.argv[1:])
