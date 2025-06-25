#!/usr/bin/env python3

import json
import sys

import backoff
import httplib2
import singer
from singer import metadata
from googleapiclient import discovery
from googleapiclient.errors import HttpError
from googleapiclient.http import set_user_agent
from oauth2client import client, GOOGLE_TOKEN_URI, GOOGLE_REVOKE_URI

from tap_doubleclick_campaign_manager.discover import discover_streams
from tap_doubleclick_campaign_manager.sync_reports import sync_reports

LOGGER = singer.get_logger()


class Server5xxError(Exception):
    pass


REQUIRED_CONFIG_KEYS = [
    "client_id",
    "client_secret",
    "refresh_token",
    "profile_id"
]


def execute_with_retries(func):
    """
      • 400: parse & log message, return None
      • 5xx: exponential backoff via decorator
    """
    @backoff.on_exception(
        backoff.expo,
        HttpError,
        max_tries=5,
    )
    def _execute():
        try:
            return func()
        except HttpError as e:
            status_code = e.status_code
            if status_code == 400:
                try:
                    payload = json.loads(e.content.decode("utf-8"))
                    msg = payload.get("error", {}).get("message", str(e))
                except Exception:
                    msg = str(e)
                LOGGER.error(msg)
                return None
            elif 500 <= status_code < 600:
                raise Server5xxError()
            # re-raise so backoff decorator can catch 429/5xx
            raise

    return _execute()


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
    return discovery.build('dfareporting', 'v4', http=http, cache_discovery=False)

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
