# Copyright (c) 2025 Airbyte, Inc., all rights reserved.

from setuptools import find_packages, setup


setup(
    name="source-bright-data-serp",
    version="0.1.0",
    description="Airbyte source connector for Bright Data SERP API",
    author="Your Name",
    author_email="your.email@example.com",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    install_requires=[
        "airbyte-cdk>=0.51.0",
        "requests>=2.28.0",
    ],
    extras_require={
        "dev": [
            "pytest>=7.0.0",
            "pytest-mock>=3.10.0",
            "freezegun>=1.2.0",
        ],
    },
    entry_points={
        "console_scripts": [
            "bright-data-serp=source_bright_data_serp.main:main",
        ],
    },
    python_requires=">=3.9",
)
