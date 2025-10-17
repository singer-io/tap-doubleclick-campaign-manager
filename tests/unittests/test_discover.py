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
