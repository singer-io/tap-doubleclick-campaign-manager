"""
Unit tests for tap_doubleclick_campaign_manager/__init__.py

Covers:
- stream_is_selected
- do_discover
- do_sync
- get_service (credential refresh error path)
"""
import io
import json
import sys
import unittest
from unittest.mock import patch, MagicMock

import tap_doubleclick_campaign_manager as tap_module
from tap_doubleclick_campaign_manager import (
    stream_is_selected,
    do_discover,
    do_sync,
    get_service,
)


# ---------------------------------------------------------------------------
# stream_is_selected
# ---------------------------------------------------------------------------

class TestStreamIsSelected(unittest.TestCase):

    def test_selected_true(self):
        """metadata with selected=True at root should return True."""
        mdata = {(): {'selected': True}}
        self.assertTrue(stream_is_selected(mdata))

    def test_selected_false(self):
        """metadata with selected=False at root should return False."""
        mdata = {(): {'selected': False}}
        self.assertFalse(stream_is_selected(mdata))

    def test_selected_missing_returns_false(self):
        """metadata with no 'selected' key at root should return False."""
        mdata = {(): {'inclusion': 'automatic'}}
        self.assertFalse(stream_is_selected(mdata))

    def test_empty_root_metadata_returns_false(self):
        """metadata with empty root dict should return False."""
        mdata = {(): {}}
        self.assertFalse(stream_is_selected(mdata))

    def test_completely_empty_mdata_returns_false(self):
        """Totally empty metadata dict should return False without raising."""
        mdata = {}
        self.assertFalse(stream_is_selected(mdata))


# ---------------------------------------------------------------------------
# do_discover
# ---------------------------------------------------------------------------

class TestDoDiscover(unittest.TestCase):

    @patch('tap_doubleclick_campaign_manager.discover_streams')
    def test_do_discover_writes_catalog_to_stdout(self, mock_discover_streams):
        """do_discover should JSON-dump the catalog to sys.stdout."""
        fake_catalog = {'streams': [{'stream': 'my_stream'}]}
        mock_discover_streams.return_value = fake_catalog

        captured = io.StringIO()
        with patch('sys.stdout', captured):
            do_discover(MagicMock(), {'profile_id': '123'})

        output = captured.getvalue()
        parsed = json.loads(output)
        self.assertEqual(parsed, fake_catalog)

    @patch('tap_doubleclick_campaign_manager.discover_streams')
    def test_do_discover_passes_service_and_config(self, mock_discover_streams):
        """do_discover should forward service and config to discover_streams."""
        mock_discover_streams.return_value = {'streams': []}
        service = MagicMock()
        config = {'profile_id': '42'}

        captured = io.StringIO()
        with patch('sys.stdout', captured):
            do_discover(service, config)

        mock_discover_streams.assert_called_once_with(service, config)


# ---------------------------------------------------------------------------
# do_sync
# ---------------------------------------------------------------------------

class TestDoSync(unittest.TestCase):

    @patch('tap_doubleclick_campaign_manager.sync_reports')
    @patch('singer.write_state')
    def test_do_sync_calls_sync_reports(self, mock_write_state, mock_sync_reports):
        """do_sync should call sync_reports with service, config, catalog, and state."""
        service = MagicMock()
        config = {'profile_id': '1'}
        catalog = MagicMock()
        state = {}

        do_sync(service, config, catalog, state)

        mock_sync_reports.assert_called_once_with(service, config, catalog, state)

    @patch('tap_doubleclick_campaign_manager.sync_reports')
    @patch('singer.write_state')
    def test_do_sync_writes_state_after_sync(self, mock_write_state, mock_sync_reports):
        """do_sync should call singer.write_state with the state after sync."""
        service = MagicMock()
        config = {'profile_id': '1'}
        catalog = MagicMock()
        state = {'current_report': None}

        do_sync(service, config, catalog, state)

        mock_write_state.assert_called_once_with(state)


# ---------------------------------------------------------------------------
# get_service
# ---------------------------------------------------------------------------

class TestGetService(unittest.TestCase):

    @patch('tap_doubleclick_campaign_manager.discovery.build')
    @patch('tap_doubleclick_campaign_manager.AuthorizedHttp')
    @patch('tap_doubleclick_campaign_manager.Credentials')
    def test_get_service_builds_dfareporting_v4(
        self, mock_credentials_class, mock_authed_http_class, mock_build
    ):
        """get_service should build the 'dfareporting' v4 API."""
        mock_creds = MagicMock()
        mock_credentials_class.return_value = mock_creds

        mock_http = MagicMock()
        mock_authed_http_class.return_value = mock_http

        config = {
            'client_id': 'client_id_value',
            'client_secret': 'client_secret_value',
            'refresh_token': 'refresh_token_value',
        }

        get_service(config)

        mock_build.assert_called_once()
        call_args, call_kwargs = mock_build.call_args
        self.assertEqual(call_args[0], 'dfareporting')
        self.assertEqual(call_args[1], 'v4')

    @patch('tap_doubleclick_campaign_manager.Credentials')
    def test_get_service_raises_on_credential_refresh_failure(self, mock_credentials_class):
        """get_service should re-raise when credential refresh fails."""
        mock_creds = MagicMock()
        mock_credentials_class.return_value = mock_creds
        mock_creds.refresh.side_effect = Exception("No network")

        config = {
            'client_id': 'client_id_value',
            'client_secret': 'client_secret_value',
            'refresh_token': 'refresh_token_value',
        }

        with self.assertRaises(Exception) as ctx:
            get_service(config)

        self.assertIn("No network", str(ctx.exception))

    @patch('tap_doubleclick_campaign_manager.discovery.build')
    @patch('tap_doubleclick_campaign_manager.set_user_agent')
    @patch('tap_doubleclick_campaign_manager.AuthorizedHttp')
    @patch('tap_doubleclick_campaign_manager.Credentials')
    def test_get_service_applies_user_agent_when_present(
        self, mock_credentials_class, mock_authed_http_class, mock_set_ua, mock_build
    ):
        """get_service should call set_user_agent when user_agent is in config."""
        mock_creds = MagicMock()
        mock_credentials_class.return_value = mock_creds
        mock_authed_http_class.return_value = MagicMock()
        mock_set_ua.return_value = MagicMock()

        config = {
            'client_id': 'cid',
            'client_secret': 'csecret',
            'refresh_token': 'rtoken',
            'user_agent': 'my-tap/1.0',
        }

        get_service(config)

        mock_set_ua.assert_called_once()
        self.assertEqual(mock_set_ua.call_args[0][1], 'my-tap/1.0')

    @patch('tap_doubleclick_campaign_manager.discovery.build')
    @patch('tap_doubleclick_campaign_manager.set_user_agent')
    @patch('tap_doubleclick_campaign_manager.AuthorizedHttp')
    @patch('tap_doubleclick_campaign_manager.Credentials')
    def test_get_service_no_user_agent_does_not_call_set_user_agent(
        self, mock_credentials_class, mock_authed_http_class, mock_set_ua, mock_build
    ):
        """get_service should NOT call set_user_agent when user_agent is absent from config."""
        mock_creds = MagicMock()
        mock_credentials_class.return_value = mock_creds
        mock_authed_http_class.return_value = MagicMock()

        config = {
            'client_id': 'cid',
            'client_secret': 'csecret',
            'refresh_token': 'rtoken',
        }

        get_service(config)

        mock_set_ua.assert_not_called()
