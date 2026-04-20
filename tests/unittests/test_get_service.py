import unittest
from unittest.mock import patch, MagicMock

from tap_doubleclick_campaign_manager import get_service


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
        mock_authed_http_class.return_value = MagicMock()

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
