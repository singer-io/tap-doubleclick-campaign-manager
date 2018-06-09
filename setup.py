#!/usr/bin/env python

from setuptools import setup, find_packages

setup(
    name="tap-doubleclick-campaign-managers",
    version="0.1.0",
    description="Singer.io tap for extracting data from the DoubleClick for Campaign Managers API",
    author="Stitch",
    url="http://singer.io",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    py_modules=["tap_doubleclick_campaign_managers"],
    install_requires=[
        "singer-python>=5.1.1",
        "pendulum",
        "google-api-python-client",
    ],
    entry_points="""
    [console_scripts]
    tap-doubleclick-campaign-managers=tap_doubleclick_campaign_managers:main
    """,
    packages=find_packages()
)
