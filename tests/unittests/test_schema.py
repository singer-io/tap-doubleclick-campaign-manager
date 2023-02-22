from tap_doubleclick_campaign_manager.schema import get_fields
from tap_doubleclick_campaign_manager.schema import get_schema
from tap_doubleclick_campaign_manager.schema import get_field_type_lookup
from tap_doubleclick_campaign_manager.schema import REPORT_ID_FIELD, SINGER_REPORT_FIELD
from tap_doubleclick_campaign_manager.schema import convert_to_json_schema_types
from tap_doubleclick_campaign_manager.schema import convert_to_json_schema_type

import unittest

class TestSchemaFunctions(unittest.TestCase):

    def setUp(self):
        self.maxDiff = None

    def test_get_fields_finds_fields(self):
        """Create a fake 'STANDARD' report and assert fields come out correctly'"""

        fake_report = {
            'type': 'STANDARD',
            'criteria': {
                'dimensions': ['cookieReachTotalReach'],
                'metricNames': ['clickCount']
            }
        }

        field_type_lookup = get_field_type_lookup()

        actual = get_fields(field_type_lookup, fake_report)

        expected = [
            {'name': 'cookieReachTotalReach',
             'type': 'long'},
            {'name': 'clickCount',
             'type': 'long'},

        ]

        self.assertEqual(expected, actual)

    def test_get_fields_does_not_finds_fields(self):
        """Create a fake 'STANDARD' report and assert fields come out correctly'"""

        fake_report = {
            'type': 'STANDARD',
            'criteria': {
                'dimensions': ['some_dimension'],
                'metricNames': ['some_metric']
            }
        }

        field_type_lookup = get_field_type_lookup()

        actual = get_fields(field_type_lookup, fake_report)

        expected = [
            {'name': 'some_dimension',
             'type': 'string'},
            {'name': 'some_metric',
             'type': 'string'},

        ]

        self.assertEqual(expected, actual)

    def test_get_fields_handles_multiple_types(self):
        """Create a fake 'STANDARD' report and assert fields come out correctly'"""

        fake_report = {
            'type': 'STANDARD',
            'criteria': {
                'dimensions': ['uniqueReachAverageImpressionFrequency',
                               'uniqueReachClickReach',
                               'uniqueReachImpressionReach',
                               'conversionId (required)'],
                'metricNames': ['uniqueReachTotalReach']
            }
        }

        field_type_lookup = get_field_type_lookup()

        actual = get_fields(field_type_lookup, fake_report)

        expected = [
            {'name': 'uniqueReachAverageImpressionFrequency',
             'type': ['double', 'string']},
            {'name': 'uniqueReachClickReach',
             'type': ['long', 'string']},
            {'name': 'uniqueReachImpressionReach',
             'type': ['long', 'string']},
            {'name': 'conversionId (required)',
             'type': ['long', 'string']},
            {'name': 'uniqueReachTotalReach',
             'type': ['long', 'string']},
        ]

        self.assertEqual(expected, actual)


    def test_convert_to_json_schema_types_single(self):

        actual = convert_to_json_schema_types("string")

        expected = ["string"]

        self.assertEqual(expected, actual)


    def test_convert_to_json_schema_types_multiple(self):

        actual = convert_to_json_schema_types(["long", "string"])

        expected = ["integer", "string"]

        self.assertEqual(expected, actual)


    def test_get_schema_handles_single_types(self):
        fieldmap = [
            {'name': 'uniqueReachAverageImpressionFrequency',
             'type': 'string'},
        ]

        actual = get_schema("some_stream", fieldmap)

        expected = {
            'type': 'object',
            'properties': {
                'uniqueReachAverageImpressionFrequency': {
                    'type': ['null', 'string']
                },
                REPORT_ID_FIELD: {
                    'type': 'integer'
                },
                SINGER_REPORT_FIELD: {
                    'type': 'string',
                    'format': 'date-time'
                },
            },
            'addtionalProperties': False
        }

        self.assertDictEqual(expected, actual)


    def test_get_schema_handles_multiple_types(self):
        fieldmap = [
            {'name': 'uniqueReachAverageImpressionFrequency',
             'type': ['double', 'string']},
            {'name': 'uniqueReachClickReach',
             'type': ['long', 'string']},
            {'name': 'uniqueReachImpressionReach',
             'type': ['long', 'string']},
            {'name': 'conversionId (required)',
             'type': ['long', 'string']},
            {'name': 'uniqueReachTotalReach',
             'type': ['long', 'string']},
        ]

        actual = get_schema("some_stream", fieldmap)

        expected = {
            'type': 'object',
            'properties': {
                'uniqueReachAverageImpressionFrequency': {
                    'type': ['null', 'number', 'string']
                },
                'uniqueReachClickReach': {
                    'type': ['null', 'integer', 'string']
                },
                'uniqueReachImpressionReach': {
                    'type': ['null', 'integer', 'string']
                },
                'conversionId (required)': {
                    'type': ['null', 'integer', 'string']
                },
                'uniqueReachTotalReach': {
                    'type': ['null', 'integer', 'string']
                },
                REPORT_ID_FIELD: {
                    'type': 'integer'
                },
                SINGER_REPORT_FIELD: {
                    'type': 'string',
                    'format': 'date-time'
                },
            },
            'addtionalProperties': False
        }

        self.assertDictEqual(expected, actual)
