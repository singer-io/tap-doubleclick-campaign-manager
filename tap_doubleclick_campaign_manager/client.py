import json
import backoff
import singer

from googleapiclient.errors import HttpError

LOGGER = singer.get_logger()


class Server429Error(Exception):
    pass


class Server5xxError(Exception):
    pass


class DoubleclickCampaignManagerClient:

    def __init__(self):
        pass

    def make_request(self, func):
        """
        400: parse & log message
        429: constant backoff
        5xx: exponential backoff
        """
        @backoff.on_exception(
            backoff.constant,
            Server429Error,
            interval=60,  # Reference: https://developers.google.com/doubleclick-advertisers/quotas#quota_limits
            max_tries=5,
        )
        @backoff.on_exception(
            backoff.expo,
            Server5xxError,
            max_tries=5,
        )
        def _call():
            try:
                return func()
            except HttpError as e:
                status = e.resp.status
                if status == 400:
                    try:
                        payload = json.loads(e.content.decode("utf-8"))
                        msg = payload.get("error", {}).get("message", str(e))
                    except Exception:
                        msg = str(e)
                    LOGGER.error(msg)
                elif status == 429:
                    raise Server429Error()
                elif 500 <= status < 600:
                    raise Server5xxError()
                raise

        return _call()
