#!/usr/bin/env python3

import json
import sys

import httplib2
import singer
from singer import metadata
from googleapiclient import discovery
from googleapiclient.http import set_user_agent
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from google_auth_httplib2 import AuthorizedHttp
from singer.transform import Transformer

from tap_doubleclick_campaign_manager.discover import discover_streams
from tap_doubleclick_campaign_manager.sync_reports import sync_reports

LOGGER = singer.get_logger()

REQUIRED_CONFIG_KEYS = [
    "client_id",
    "client_secret",
    "refresh_token",
    "profile_id"
]

GOOGLE_TOKEN_URI = "https://oauth2.googleapis.com/token"


def get_service(config):
    """
    Creates and returns a Campaign Manager 360 service object using google-auth.
    """
    creds = Credentials(
        token=None,
        refresh_token=config["refresh_token"],
        token_uri=GOOGLE_TOKEN_URI,
        client_id=config["client_id"],
        client_secret=config["client_secret"],
    )

    try:
        creds.refresh(Request())
    except Exception:
        LOGGER.error("Failed to refresh OAuth2 credentials; aborting. ", exc_info=True)
        raise

    authed_http = AuthorizedHttp(creds, http=httplib2.Http())

    if config.get("user_agent"):
        authed_http = set_user_agent(authed_http, config["user_agent"])

    return discovery.build(
        "dfareporting",
        "v4",
        http=authed_http,
        cache_discovery=False
    )


def do_discover(service, config):
    LOGGER.info("Starting discover")
    catalog = discover_streams(service, config)
    json.dump(catalog, sys.stdout, indent=2)
    LOGGER.info("Finished discover")


def get_selected_streams(catalog):
    """
    Get selected streams by checking top-level metadata
    """
    selected_streams = []
    for stream in catalog['streams']:
        for entry in stream['metadata']:
            if not entry['breadcrumb'] and entry['metadata'].get('selected'):
                selected_streams.append(stream['tap_stream_id'])
    return selected_streams


def get_selected_fields(catalog, stream_name):
    """
    Get selected fields for a given stream.
    """
    for stream in catalog['streams']:
        if stream['tap_stream_id'] == stream_name:
            selected_fields = []
            for entry in stream['metadata']:
                if len(entry['breadcrumb']) == 2 and entry['breadcrumb'][0] == 'properties':
                    field = entry['breadcrumb'][1]
                    if entry['metadata'].get('selected', False):
                        selected_fields.append(field)
            return selected_fields
    return []


def do_sync(service, config, catalog, state):
    transformer = Transformer()
    selected_streams = []
    for stream in catalog['streams']:
        for entry in stream['metadata']:
            if not entry['breadcrumb'] and entry['metadata'].get('selected'):
                selected_streams.append(stream['tap_stream_id'])
    if not selected_streams:
        LOGGER.warning("No streams selected. Exiting.")
        return
    for stream in catalog['streams']:
        if stream['tap_stream_id'] in selected_streams:
            singer.write_schema(stream['tap_stream_id'], stream['schema'], stream.get('key_properties', []))
            sync_reports(service, config, catalog, state, stream['tap_stream_id'], transformer)
    singer.write_state(state)
    LOGGER.info("Finished sync")

@singer.utils.handle_top_exception(LOGGER)
def main():
    parsed_args = singer.utils.parse_args(REQUIRED_CONFIG_KEYS)
    config = parsed_args.config

    service = get_service(config)

    if parsed_args.discover:
        do_discover(service, config)
    elif parsed_args.catalog:
        state = parsed_args.state
        do_sync(service, config, parsed_args.catalog, state)
