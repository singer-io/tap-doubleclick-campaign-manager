import unittest
from unittest.mock import patch, MagicMock

from tap_doubleclick_campaign_manager.discover import discover_streams, sanitize_name


class TestDiscoverFunctions(unittest.TestCase):

    def setUp(self):
        self.maxDiff = None

    def test_sanitize_name(self):
        """Test that sanitize_name correctly formats report names"""
        # Test spaces and dashes are replaced with underscores
        actual = sanitize_name("Test Report Name")
        expected = "test_report_name"
        self.assertEqual(expected, actual)

        # Test special characters are removed
        actual = sanitize_name("Test-Report/Name!")
        expected = "test_report_name"
        self.assertEqual(expected, actual)

        # Test mixed case and symbols
        actual = sanitize_name("Complex_Report-Name 2023!")
        expected = "complex_report_name_2023"
        self.assertEqual(expected, actual)

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

    @patch('tap_doubleclick_campaign_manager.discover.DoubleclickCampaignManagerClient')
    @patch('tap_doubleclick_campaign_manager.discover.get_field_type_lookup')
    def test_discover_streams_handles_multiple_reports(self, mock_get_field_type_lookup, mock_client_class):
        """Test that discover_streams correctly handles multiple reports with forced-replication-method"""
        
        # Mock the service and client
        mock_service = MagicMock()
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        
        # Mock multiple report data
        mock_reports = [
            {
                'id': 123,
                'name': 'First Report',
                'type': 'STANDARD',
                'criteria': {
                    'dimensions': ['date'],
                    'metricNames': ['impressions']
                }
            },
            {
                'id': 456,
                'name': 'Second Report',
                'type': 'FLOODLIGHT',
                'floodlightCriteria': {
                    'dimensions': ['conversionId'],
                    'metricNames': ['totalConversions']
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
            'impressions': 'long',
            'conversionId': 'long',
            'totalConversions': 'long'
        }
        
        # Test config
        config = {'profile_id': '12345'}
        
        # Call discover_streams
        result = discover_streams(mock_service, config)
        
        # Verify we have 2 streams
        self.assertEqual(len(result['streams']), 2)
        
        # Check both streams have forced-replication-method
        for stream in result['streams']:
            metadata = stream['metadata']
            root_metadata = None
            for meta in metadata:
                if meta['breadcrumb'] == []:
                    root_metadata = meta
                    break
            
            self.assertIsNotNone(root_metadata)
            self.assertIn('forced-replication-method', root_metadata['metadata'])
            self.assertEqual(root_metadata['metadata']['forced-replication-method'], 'FULL_TABLE')

    @patch('tap_doubleclick_campaign_manager.discover.DoubleclickCampaignManagerClient')
    @patch('tap_doubleclick_campaign_manager.discover.get_field_type_lookup')
    def test_discover_streams_metadata_structure(self, mock_get_field_type_lookup, mock_client_class):
        """Test that discover_streams creates correct metadata structure for all fields"""
        
        # Mock the service and client
        mock_service = MagicMock()
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        
        # Mock report data
        mock_reports = [
            {
                'id': 789,
                'name': 'Metadata Test Report',
                'type': 'STANDARD',
                'criteria': {
                    'dimensions': ['date', 'campaignId'],
                    'metricNames': ['impressions', 'clicks']
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
            'campaignId': 'long',
            'impressions': 'long',
            'clicks': 'long'
        }
        
        # Test config
        config = {'profile_id': '12345'}
        
        # Call discover_streams
        result = discover_streams(mock_service, config)
        
        stream = result['streams'][0]
        metadata = stream['metadata']
        
        # Should have metadata for: root + 6 fields (date, campaignId, impressions, clicks, _sdc_report_time, _sdc_report_id)
        expected_metadata_count = 7  # 1 root + 6 field metadata entries
        self.assertEqual(len(metadata), expected_metadata_count)
        
        # Check that all field metadata entries have inclusion: automatic
        field_metadata_entries = [meta for meta in metadata if len(meta['breadcrumb']) == 2]
        for meta in field_metadata_entries:
            self.assertEqual(meta['metadata']['inclusion'], 'automatic')
            self.assertEqual(meta['breadcrumb'][0], 'properties')
