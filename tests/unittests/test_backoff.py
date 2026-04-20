import unittest
import json
import time
from types import SimpleNamespace
from unittest.mock import patch

from parameterized import parameterized
from googleapiclient.errors import HttpError

import tap_doubleclick_campaign_manager.client as dcm


class TestDoubleclickCampaignManagerClient(unittest.TestCase):
    def setUp(self):
        self.client = dcm.DoubleclickCampaignManagerClient()

    def make_http_error(self, status, content=b""):
        """
        Helper to construct a googleapiclient.errors.HttpError
        with the given status code and raw content.
        """
        resp = SimpleNamespace(status=status, reason="", uri="")
        return HttpError(resp, content, uri="")

    def test_successful_request_returns_value(self):
        """Test that make_request returns the function result when no error occurs."""
        func = lambda: {"data": 123}
        result = self.client.make_request(func)
        self.assertEqual(result, {"data": 123})

    @patch.object(dcm.LOGGER, "error")
    def test_400_error_with_valid_json_logs_message(self, mock_error):
        """Test that a 400 HttpError with valid JSON logs the error message."""
        payload = {
            "error": {"message": "Active schedules can't have a past expiration date."}
        }
        error_content = json.dumps(payload).encode("utf-8")
        func = lambda: (_ for _ in ()).throw(self.make_http_error(400, error_content))

        with self.assertRaises(HttpError):
            self.client.make_request(func)

        mock_error.assert_called_once_with(
            "Active schedules can't have a past expiration date."
        )

    @patch.object(dcm.LOGGER, "error")
    def test_400_error_with_invalid_json_logs_exception_string(self, mock_error):
        """Test that a 400 HttpError with invalid JSON logs the exception string using exception block."""
        error_content = b"not-a-json"
        func = lambda: (_ for _ in ()).throw(self.make_http_error(400, error_content))

        with self.assertRaises(HttpError):
            self.client.make_request(func)

        called_args = mock_error.call_args[0]
        self.assertTrue(isinstance(called_args[0], str))

    def test_non_handled_status_raises_http_error(self):
        """Test that HttpError with status not 400, 429, or 5xx is propagated unchanged."""
        func = lambda: (_ for _ in ()).throw(self.make_http_error(404, b""))
        with self.assertRaises(HttpError):
            self.client.make_request(func)

    @parameterized.expand(
        [
            ("rate_limit_retry", 429, dcm.Server429Error),
        ]
    )
    @patch("time.sleep", return_value=None)
    def test_backoff_max_retries_then_raises_for_429_error(
        self, name, status_code, expected_exception, mock_sleep
    ):
        """
        Test that make_request retries up to max_tries and then raises the expected exception
        for 429 (rate limit) status codes.
        """
        calls = {"count": 0}

        def func():
            calls["count"] += 1
            raise self.make_http_error(status_code, b"{}")

        with self.assertRaises(expected_exception):
            self.client.make_request(func)

        self.assertEqual(calls["count"], 2)

    @parameterized.expand(
        [
            ("server_error_retry_500", 500, dcm.Server5xxError),
            ("server_error_retry_503", 503, dcm.Server5xxError),
        ]
    )
    @patch("time.sleep", return_value=None)
    def test_backoff_max_retries_then_raises_for_5xx_errors(
        self, name, status_code, expected_exception, mock_sleep
    ):
        """
        Test that make_request retries up to max_tries and then raises the expected exception
        for 5xx (server error) status codes.
        """
        calls = {"count": 0}

        def func():
            calls["count"] += 1
            raise self.make_http_error(status_code, b"{}")

        with self.assertRaises(expected_exception):
            self.client.make_request(func)

        # make_request should have called the function exactly 5 times
        self.assertEqual(calls["count"], 5)

    @patch.object(dcm.LOGGER, "error")
    def test_400_error_json_with_no_message_key_logs_str_fallback(self, mock_error):
        """Test that a 400 HttpError where JSON error object has no 'message' key falls back to str(e)."""
        payload = {"error": {"code": 400}}  # valid JSON but no 'message' key
        error_content = json.dumps(payload).encode("utf-8")
        func = lambda: (_ for _ in ()).throw(self.make_http_error(400, error_content))

        with self.assertRaises(HttpError):
            self.client.make_request(func)

        mock_error.assert_called_once()
        call_arg = mock_error.call_args[0][0]
        self.assertIsInstance(call_arg, str)

    @patch.object(dcm.LOGGER, "error")
    def test_400_error_empty_payload_falls_back_to_str_e(self, mock_error):
        """Test that a 400 error with empty JSON object logs str(e)."""
        payload = {}
        error_content = json.dumps(payload).encode("utf-8")
        func = lambda: (_ for _ in ()).throw(self.make_http_error(400, error_content))

        with self.assertRaises(HttpError):
            self.client.make_request(func)

        mock_error.assert_called_once()

    def test_401_raises_http_error_directly(self):
        """Test that a 401 HttpError is raised directly without special handling."""
        func = lambda: (_ for _ in ()).throw(self.make_http_error(401, b""))
        with self.assertRaises(HttpError):
            self.client.make_request(func)

    def test_403_raises_http_error_directly(self):
        """Test that a 403 HttpError is raised directly without special handling."""
        func = lambda: (_ for _ in ()).throw(self.make_http_error(403, b""))
        with self.assertRaises(HttpError):
            self.client.make_request(func)

    def test_500_raises_server_5xx_error(self):
        """Test that a 500 HttpError raises Server5xxError (after exhausting retries)."""
        with patch("time.sleep", return_value=None):
            func = lambda: (_ for _ in ()).throw(self.make_http_error(500, b""))
            with self.assertRaises(dcm.Server5xxError):
                self.client.make_request(func)

    def test_503_raises_server_5xx_error(self):
        """Test that a 503 HttpError raises Server5xxError (after exhausting retries)."""
        with patch("time.sleep", return_value=None):
            func = lambda: (_ for _ in ()).throw(self.make_http_error(503, b""))
            with self.assertRaises(dcm.Server5xxError):
                self.client.make_request(func)

    def test_successful_request_with_none_return(self):
        """Test that make_request correctly returns None from the function."""
        func = lambda: None
        result = self.client.make_request(func)
        self.assertIsNone(result)

    def test_successful_request_with_list_return(self):
        """Test that make_request correctly returns a list from the function."""
        func = lambda: [1, 2, 3]
        result = self.client.make_request(func)
        self.assertEqual(result, [1, 2, 3])
