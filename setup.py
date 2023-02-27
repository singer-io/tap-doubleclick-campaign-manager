#!/usr/bin/env python

from setuptools import setup, find_packages

setup(
    name="tap-doubleclick-campaign-manager",
    version="1.3.0",
    description="Singer.io tap for extracting data from the DoubleClick for Campaign Managers API",
    author="Stitch",
    url="http://singer.io",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    py_modules=["tap_doubleclick_campaign_manager"],
    install_requires=[
        "singer-python>=5.1.1",
        "pendulum",
        "google-api-python-client",
        "oauth2client==4.1.2"
    ],
    extras_require={
        "dev": [
            "beautifulsoup4==4.9.3",
            "bs4==0.0.1",
            "soupsieve==2.2.1",
            "pytest==6.2.4",
        ],
    },
    entry_points="""
    [console_scripts]
    tap-doubleclick-campaign-manager=tap_doubleclick_campaign_manager:main
    """,
    packages=find_packages(),
    include_package_data=True
)
