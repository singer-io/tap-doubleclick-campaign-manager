import unittest
from unittest.mock import patch, MagicMock

from tap_doubleclick_campaign_manager.discover import discover_streams


class TestDiscoverEdgeCases(unittest.TestCase):
    """Test edge cases for the forced-replication-method functionality"""

    def setUp(self):
        self.maxDiff = None

    @patch('tap_doubleclick_campaign_manager.discover.DoubleclickCampaignManagerClient')
    @patch('tap_doubleclick_campaign_manager.discover.get_field_type_lookup')
    def test_discover_streams_different_report_types(self, mock_get_field_type_lookup, mock_client_class):
        """Test that forced-replication-method is included for all report types"""
        
        # Mock the service and client
        mock_service = MagicMock()
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        
        # Mock reports of different types
        mock_reports = [
            {
                'id': 1,
                'name': 'Standard Report',
                'type': 'STANDARD',
                'criteria': {
                    'dimensions': ['date'],
                    'metricNames': ['impressions']
                }
            },
            {
                'id': 2,
                'name': 'Floodlight Report',
                'type': 'FLOODLIGHT',
                'floodlightCriteria': {
                    'dimensions': ['conversionId'],
                    'metricNames': ['totalConversions']
                }
            },
            {
                'id': 3,
                'name': 'Cross Dimension Report',
                'type': 'CROSS_DIMENSION_REACH',
                'crossDimensionReachCriteria': {
                    'breakdown': ['age'],
                    'metricNames': ['reach'],
                    'overlapMetricNames': ['overlapReach']
                }
            },
            {
                'id': 4,
                'name': 'Path to Conversion Report',
                'type': 'PATH_TO_CONVERSION',
                'pathToConversionCriteria': {
                    'conversionDimensions': ['conversionId'],
                    'perInteractionDimensions': ['date'],
                    'customFloodlightVariables': ['u1'],
                    'metricNames': ['totalConversions']
                }
            },
            {
                'id': 5,
                'name': 'Reach Report',
                'type': 'REACH',
                'reachCriteria': {
                    'dimensions': ['date'],
                    'metricNames': ['impressions'],
                    'reachByFrequencyMetricNames': ['reach1Plus']
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
            'totalConversions': 'long',
            'age': 'string',
            'reach': 'long',
            'overlapReach': 'long',
            'u1': 'string',
            'reach1Plus': 'long'
        }
        
        # Test config
        config = {'profile_id': '12345'}
        
        # Call discover_streams
        result = discover_streams(mock_service, config)
        
        # Verify we have 5 streams
        self.assertEqual(len(result['streams']), 5)
        
        # Check that all streams have forced-replication-method in metadata
        for i, stream in enumerate(result['streams']):
            with self.subTest(report_type=mock_reports[i]['type'], stream_name=stream['stream']):
                metadata = stream['metadata']
                
                # Find the root metadata entry
                root_metadata = None
                for meta in metadata:
                    if meta['breadcrumb'] == []:
                        root_metadata = meta
                        break
                
                self.assertIsNotNone(root_metadata, f"Root metadata not found for {stream['stream']}")
                
                # Check forced-replication-method is present
                self.assertIn('forced-replication-method', root_metadata['metadata'])
                self.assertEqual(root_metadata['metadata']['forced-replication-method'], 'FULL_TABLE')
                
                # Check report-id is also present
                self.assertIn('tap-doubleclick-campaign-manager.report-id', root_metadata['metadata'])
                self.assertEqual(root_metadata['metadata']['tap-doubleclick-campaign-manager.report-id'], mock_reports[i]['id'])

    @patch('tap_doubleclick_campaign_manager.discover.DoubleclickCampaignManagerClient')
    @patch('tap_doubleclick_campaign_manager.discover.get_field_type_lookup')
    def test_discover_streams_empty_reports_list(self, mock_get_field_type_lookup, mock_client_class):
        """Test that discover_streams handles empty reports list gracefully"""
        
        # Mock the service and client
        mock_service = MagicMock()
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        
        # Mock empty reports list
        mock_reports = []
        
        # Configure mock responses
        mock_reports_list = MagicMock()
        mock_reports_list.list.return_value.execute.return_value.get.return_value = mock_reports
        mock_service.reports.return_value = mock_reports_list
        mock_client.make_request.return_value = mock_reports
        
        # Mock field type lookup
        mock_get_field_type_lookup.return_value = {}
        
        # Test config
        config = {'profile_id': '12345'}
        
        # Call discover_streams
        result = discover_streams(mock_service, config)
        
        # Verify we have no streams
        self.assertIsInstance(result, dict)
        self.assertIn('streams', result)
        self.assertEqual(len(result['streams']), 0)

    @patch('tap_doubleclick_campaign_manager.discover.DoubleclickCampaignManagerClient')
    @patch('tap_doubleclick_campaign_manager.discover.get_field_type_lookup')
    def test_discover_streams_with_special_characters_in_names(self, mock_get_field_type_lookup, mock_client_class):
        """Test that discover_streams handles report names with special characters"""
        
        # Mock the service and client
        mock_service = MagicMock()
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        
        # Mock reports with special character names
        mock_reports = [
            {
                'id': 100,
                'name': 'Report with Spaces & Special-Characters!',
                'type': 'STANDARD',
                'criteria': {
                    'dimensions': ['date'],
                    'metricNames': ['impressions']
                }
            },
            {
                'id': 101,
                'name': 'Report/With\\Slashes',
                'type': 'STANDARD',
                'criteria': {
                    'dimensions': ['campaignId'],
                    'metricNames': ['clicks']
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
            'campaignId': 'long',
            'clicks': 'long'
        }
        
        # Test config
        config = {'profile_id': '12345'}
        
        # Call discover_streams
        result = discover_streams(mock_service, config)
        
        # Verify we have 2 streams
        self.assertEqual(len(result['streams']), 2)
        
        # Check stream names are properly sanitized
        stream_names = [stream['stream'] for stream in result['streams']]
        self.assertIn('report_with_spaces__special_characters', stream_names)
        self.assertIn('report_withslashes', stream_names)
        
        # Check that both streams have forced-replication-method
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
