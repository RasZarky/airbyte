# Copyright (c) 2025 Airbyte, Inc., all rights reserved.

import sys

from airbyte_cdk.entrypoint import launch
from source_brightdata_serp.source import SourceBrightDataSerp


def run():
    source = SourceBrightDataSerp()
    launch(source, sys.argv[1:])
