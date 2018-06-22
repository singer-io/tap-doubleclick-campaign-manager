#!/usr/bin/env python3

import json
import sys

import httplib2
import singer
from singer import metadata
from googleapiclient import discovery
from googleapiclient.http import set_user_agent
from oauth2client import client, GOOGLE_TOKEN_URI, GOOGLE_REVOKE_URI

from tap_doubleclick_campaign_manager.discover import discover_streams
from tap_doubleclick_campaign_manager.sync_reports import sync_reports

LOGGER = singer.get_logger()

REQUIRED_CONFIG_KEYS = [
    "client_id",
    "client_secret",
    "refresh_token",
    "profile_id"
]

def get_service(config):
    credentials = client.OAuth2Credentials(
        None,
        config.get('client_id'),
        config.get('client_secret'),
        config.get('refresh_token'),
        None,
        GOOGLE_TOKEN_URI,
        None,
        revoke_uri=GOOGLE_REVOKE_URI)
    http = credentials.authorize(httplib2.Http())
    user_agent = config.get('user_agent')
    if user_agent:
        http = set_user_agent(http, user_agent)
    return discovery.build('dfareporting', 'v3.1', http=http, cache_discovery=False)

def do_discover(service, config):
    LOGGER.info("Starting discover")
    catalog = discover_streams(service, config)
    json.dump(catalog, sys.stdout, indent=2)
    LOGGER.info("Finished discover")

def stream_is_selected(mdata):
    return mdata.get((), {}).get('selected', False)

def do_sync(service, config, catalog, state):
    sync_reports(service, config, catalog, state)

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
