#!/usr/bin/env python

from setuptools import setup, find_packages

setup(
    name="tap-doubleclick-campaign-manager",
    version="1.4.1",
    description="Singer.io tap for extracting data from the DoubleClick for Campaign Managers API",
    author="Stitch",
    url="http://singer.io",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    py_modules=["tap_doubleclick_campaign_manager"],
    install_requires=[
        "singer-python==6.1.1",
        "pendulum==3.1.0",
        "google-api-python-client==2.172.0",
        "oauth2client==4.1.3"
    ],
    extras_require={
        "dev": [
            "beautifulsoup4==4.13.4",
            "bs4==0.0.2",
            "soupsieve==2.7",
            "pytest==8.4.1"
        ],
    },
    entry_points="""
    [console_scripts]
    tap-doubleclick-campaign-manager=tap_doubleclick_campaign_manager:main
    """,
    packages=find_packages(),
    include_package_data=True
)
