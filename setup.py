#!/usr/bin/env python

from setuptools import setup, find_packages

setup(
    name="tap-doubleclick-campaign-manager",
    version="0.2.5",
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
    entry_points="""
    [console_scripts]
    tap-doubleclick-campaign-manager=tap_doubleclick_campaign_manager:main
    """,
    packages=find_packages(),
    include_package_data=True
)
