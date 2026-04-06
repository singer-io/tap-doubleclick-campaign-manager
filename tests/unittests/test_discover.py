import unittest
from unittest.mock import patch, MagicMock

from tap_doubleclick_campaign_manager.discover import discover_streams, sanitize_name


class TestDiscoverFunctions(unittest.TestCase):

    def setUp(self):
        self.maxDiff = None

    @patch('tap_doubleclick_campaign_manager.discover.DoubleclickCampaignManagerClient')
    @patch('tap_doubleclick_campaign_manager.discover.get_field_type_lookup')
    def test_discover_streams_includes_forced_replication_method(self, mock_get_field_type_lookup, mock_client_class):
        """Test that discover_streams includes forced-replication-method in metadata"""
        
        # Mock the service and client
        mock_service = MagicMock()
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        
        # Mock report data
        mock_reports = [
            {
                'id': 123,
                'name': 'Test Report',
                'type': 'STANDARD',
                'criteria': {
                    'dimensions': ['date'],
                    'metricNames': ['impressions']
                }
            }
        ]
        
        # Configure mock responses
        mock_reports_list = MagicMock()
        mock_reports_list.list.return_value.execute.return_value.get.return_value = mock_reports
        mock_service.reports.return_value = mock_reports_list
        mock_client.make_request.return_value = mock_reports
        
        # Mock field type lookup
        mock_get_field_type_lookup.return_value = {
            'date': 'string',
            'impressions': 'long'
        }
        
        # Test config
        config = {'profile_id': '12345'}
        
        # Call discover_streams
        result = discover_streams(mock_service, config)
        
        # Verify the result structure
        self.assertIsInstance(result, dict)
        self.assertIn('streams', result)
        self.assertEqual(len(result['streams']), 1)
        
        stream = result['streams'][0]
        
        # Check basic stream properties
        self.assertEqual(stream['stream'], 'test_report')
        self.assertEqual(stream['tap_stream_id'], 'test_report_123')
        
        # Check metadata structure
        self.assertIn('metadata', stream)
        metadata = stream['metadata']
        
        # Find the root metadata entry (breadcrumb: [])
        root_metadata = None
        for meta in metadata:
            if meta['breadcrumb'] == []:
                root_metadata = meta
                break
        
        self.assertIsNotNone(root_metadata, "Root metadata entry not found")
        
        # Check that forced-replication-method is present in root metadata
        self.assertIn('metadata', root_metadata)
        self.assertIn('forced-replication-method', root_metadata['metadata'])
        self.assertEqual(root_metadata['metadata']['forced-replication-method'], 'FULL_TABLE')
        
        # Check that report-id is also present (existing functionality)
        self.assertIn('tap-doubleclick-campaign-manager.report-id', root_metadata['metadata'])
        self.assertEqual(root_metadata['metadata']['tap-doubleclick-campaign-manager.report-id'], 123)


class TestSanitizeName(unittest.TestCase):

    def test_lowercase(self):
        self.assertEqual(sanitize_name('Report'), 'report')

    def test_spaces_replaced_with_underscore(self):
        self.assertEqual(sanitize_name('my report name'), 'my_report_name')

    def test_hyphens_replaced_with_underscore(self):
        self.assertEqual(sanitize_name('my-report'), 'my_report')

    def test_slashes_replaced_with_underscore(self):
        self.assertEqual(sanitize_name('my/report'), 'my_report')

    def test_special_chars_removed(self):
        self.assertEqual(sanitize_name('report!@#$%'), 'report')

    def test_mixed_transformations(self):
        self.assertEqual(sanitize_name('My Report/Name-2024!'), 'my_report_name_2024')

    def test_numbers_preserved(self):
        self.assertEqual(sanitize_name('report123'), 'report123')

    def test_already_clean_name(self):
        self.assertEqual(sanitize_name('clean_name'), 'clean_name')

    def test_all_lowercase(self):
        self.assertEqual(sanitize_name('ABC'), 'abc')


class TestDiscoverStreamsExtended(unittest.TestCase):

    def setUp(self):
        self.maxDiff = None

    @patch('tap_doubleclick_campaign_manager.discover.DoubleclickCampaignManagerClient')
    @patch('tap_doubleclick_campaign_manager.discover.get_field_type_lookup')
    def test_discover_streams_multiple_reports_sorted_by_id(self, mock_get_field_type_lookup, mock_client_class):
        """discover_streams should sort reports by id ascending."""
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client

        mock_reports = [
            {'id': 200, 'name': 'Beta Report', 'type': 'STANDARD',
             'criteria': {'dimensions': [], 'metricNames': []}},
            {'id': 100, 'name': 'Alpha Report', 'type': 'STANDARD',
             'criteria': {'dimensions': [], 'metricNames': []}},
        ]
        mock_client.make_request.return_value = mock_reports
        mock_get_field_type_lookup.return_value = {}

        result = discover_streams(MagicMock(), {'profile_id': '99'})
        stream_ids = [s['tap_stream_id'] for s in result['streams']]

        self.assertEqual(stream_ids, ['alpha_report_100', 'beta_report_200'])

    @patch('tap_doubleclick_campaign_manager.discover.DoubleclickCampaignManagerClient')
    @patch('tap_doubleclick_campaign_manager.discover.get_field_type_lookup')
    def test_discover_streams_empty_reports_returns_empty_catalog(self, mock_get_field_type_lookup, mock_client_class):
        """discover_streams with no reports should return a catalog with no streams."""
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        mock_client.make_request.return_value = []
        mock_get_field_type_lookup.return_value = {}

        result = discover_streams(MagicMock(), {'profile_id': '99'})
        self.assertEqual(result['streams'], [])

    @patch('tap_doubleclick_campaign_manager.discover.DoubleclickCampaignManagerClient')
    @patch('tap_doubleclick_campaign_manager.discover.get_field_type_lookup')
    def test_discover_streams_tap_stream_id_format(self, mock_get_field_type_lookup, mock_client_class):
        """tap_stream_id should be '<sanitized_name>_<report_id>'."""
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        mock_client.make_request.return_value = [
            {'id': 42, 'name': 'My Report', 'type': 'STANDARD',
             'criteria': {'dimensions': [], 'metricNames': []}}
        ]
        mock_get_field_type_lookup.return_value = {}

        result = discover_streams(MagicMock(), {'profile_id': '99'})
        self.assertEqual(result['streams'][0]['tap_stream_id'], 'my_report_42')

    @patch('tap_doubleclick_campaign_manager.discover.DoubleclickCampaignManagerClient')
    @patch('tap_doubleclick_campaign_manager.discover.get_field_type_lookup')
    def test_discover_streams_all_properties_have_inclusion_automatic(self, mock_get_field_type_lookup, mock_client_class):
        """All field-level metadata entries should have inclusion=automatic."""
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        mock_client.make_request.return_value = [
            {'id': 1, 'name': 'Test', 'type': 'STANDARD',
             'criteria': {'dimensions': ['date'], 'metricNames': ['impressions']}}
        ]
        mock_get_field_type_lookup.return_value = {'date': 'string', 'impressions': 'long'}

        result = discover_streams(MagicMock(), {'profile_id': '99'})
        stream = result['streams'][0]

        for meta in stream['metadata']:
            if meta['breadcrumb'] != []:
                self.assertEqual(meta['metadata']['inclusion'], 'automatic')

    @patch('tap_doubleclick_campaign_manager.discover.DoubleclickCampaignManagerClient')
    @patch('tap_doubleclick_campaign_manager.discover.get_field_type_lookup')
    def test_discover_streams_stream_alias_equals_stream_name(self, mock_get_field_type_lookup, mock_client_class):
        """stream and stream_alias should both equal the sanitized report name."""
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        mock_client.make_request.return_value = [
            {'id': 7, 'name': 'My Stream', 'type': 'STANDARD',
             'criteria': {'dimensions': [], 'metricNames': []}}
        ]
        mock_get_field_type_lookup.return_value = {}

        result = discover_streams(MagicMock(), {'profile_id': '99'})
        stream = result['streams'][0]
        self.assertEqual(stream['stream'], 'my_stream')

    @patch('tap_doubleclick_campaign_manager.discover.DoubleclickCampaignManagerClient')
    @patch('tap_doubleclick_campaign_manager.discover.get_field_type_lookup')
    def test_discover_streams_key_properties_is_empty_list(self, mock_get_field_type_lookup, mock_client_class):
        """key_properties should be an empty list."""
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        mock_client.make_request.return_value = [
            {'id': 99, 'name': 'Test Report', 'type': 'STANDARD',
             'criteria': {'dimensions': [], 'metricNames': []}}
        ]
        mock_get_field_type_lookup.return_value = {}

        result = discover_streams(MagicMock(), {'profile_id': '99'})
        self.assertEqual(result['streams'][0]['key_properties'], [])
