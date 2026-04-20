import unittest

from tap_doubleclick_campaign_manager.schema import get_fields
from tap_doubleclick_campaign_manager.schema import get_schema
from tap_doubleclick_campaign_manager.schema import get_field_type_lookup
from tap_doubleclick_campaign_manager.schema import REPORT_ID_FIELD, SINGER_REPORT_FIELD
from tap_doubleclick_campaign_manager.schema import convert_to_json_schema_types
from tap_doubleclick_campaign_manager.schema import convert_to_json_schema_type
from tap_doubleclick_campaign_manager.schema import report_dimension_fn

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


class TestReportDimensionFn(unittest.TestCase):

    def test_string_dimension_returns_string(self):
        """A plain string dimension should be returned unchanged."""
        self.assertEqual(report_dimension_fn('date'), 'date')

    def test_dict_dimension_returns_name(self):
        """A dict dimension should return the value of its 'name' key."""
        self.assertEqual(report_dimension_fn({'name': 'campaign'}), 'campaign')

    def test_invalid_type_raises_exception(self):
        """Any type other than str or dict should raise an Exception."""
        with self.assertRaises(Exception):
            report_dimension_fn(12345)

    def test_invalid_none_raises_exception(self):
        """None should raise an Exception."""
        with self.assertRaises(Exception):
            report_dimension_fn(None)


class TestConvertToJsonSchemaType(unittest.TestCase):

    def test_long_maps_to_integer(self):
        self.assertEqual(convert_to_json_schema_type('long'), 'integer')

    def test_double_maps_to_number(self):
        self.assertEqual(convert_to_json_schema_type('double'), 'number')

    def test_string_passthrough(self):
        self.assertEqual(convert_to_json_schema_type('string'), 'string')

    def test_boolean_passthrough(self):
        self.assertEqual(convert_to_json_schema_type('boolean'), 'boolean')

    def test_unknown_type_passthrough(self):
        self.assertEqual(convert_to_json_schema_type('unknown_type'), 'unknown_type')


class TestGetFieldTypeLookup(unittest.TestCase):

    def test_returns_dict(self):
        """get_field_type_lookup should return a dict loaded from JSON."""
        result = get_field_type_lookup()
        self.assertIsInstance(result, dict)

    def test_non_empty(self):
        """Lookup dict should not be empty."""
        result = get_field_type_lookup()
        self.assertGreater(len(result), 0)

    def test_known_field_present(self):
        """Known fields such as 'clickCount' should be present."""
        result = get_field_type_lookup()
        self.assertIn('clickCount', result)


class TestGetFieldsReportTypes(unittest.TestCase):

    def setUp(self):
        self.field_type_lookup = get_field_type_lookup()

    def test_floodlight_report(self):
        """FLOODLIGHT report should use floodlightCriteria."""
        fake_report = {
            'type': 'FLOODLIGHT',
            'floodlightCriteria': {
                'dimensions': ['date'],
                'metricNames': ['impressions']
            }
        }
        result = get_fields(self.field_type_lookup, fake_report)
        names = [f['name'] for f in result]
        self.assertIn('date', names)
        self.assertIn('impressions', names)

    def test_cross_dimension_reach_report(self):
        """CROSS_DIMENSION_REACH report should combine breakdown + metricNames + overlapMetricNames."""
        fake_report = {
            'type': 'CROSS_DIMENSION_REACH',
            'crossDimensionReachCriteria': {
                'breakdown': ['date'],
                'metricNames': ['impressions'],
                'overlapMetricNames': ['clicks']
            }
        }
        result = get_fields(self.field_type_lookup, fake_report)
        names = [f['name'] for f in result]
        self.assertIn('date', names)
        self.assertIn('impressions', names)
        self.assertIn('clicks', names)

    def test_path_to_conversion_report(self):
        """PATH_TO_CONVERSION report should combine conversion/perInteraction/custom dimensions."""
        fake_report = {
            'type': 'PATH_TO_CONVERSION',
            'pathToConversionCriteria': {
                'conversionDimensions': ['date'],
                'perInteractionDimensions': ['campaign'],
                'customFloodlightVariables': [],
                'metricNames': ['impressions']
            }
        }
        result = get_fields(self.field_type_lookup, fake_report)
        names = [f['name'] for f in result]
        self.assertIn('date', names)
        self.assertIn('campaign', names)
        self.assertIn('impressions', names)

    def test_reach_report(self):
        """REACH report should combine metricNames + reachByFrequencyMetricNames."""
        fake_report = {
            'type': 'REACH',
            'reachCriteria': {
                'dimensions': ['date'],
                'metricNames': ['impressions'],
                'reachByFrequencyMetricNames': ['cookieReachTotalReach']
            }
        }
        result = get_fields(self.field_type_lookup, fake_report)
        names = [f['name'] for f in result]
        self.assertIn('date', names)
        self.assertIn('impressions', names)
        self.assertIn('cookieReachTotalReach', names)

    def test_dict_dimension_in_report(self):
        """Dimensions can be dicts with a 'name' key (report_dimension_fn handles them)."""
        fake_report = {
            'type': 'STANDARD',
            'criteria': {
                'dimensions': [{'name': 'date'}],
                'metricNames': []
            }
        }
        result = get_fields(self.field_type_lookup, fake_report)
        self.assertEqual(result[0]['name'], 'date')


class TestGetSchema(unittest.TestCase):

    def test_sdc_report_time_field_present(self):
        """Schema must include the _sdc_report_time datetime field."""
        schema = get_schema('my_stream', [])
        self.assertIn(SINGER_REPORT_FIELD, schema['properties'])
        self.assertEqual(schema['properties'][SINGER_REPORT_FIELD]['type'], 'string')
        self.assertEqual(schema['properties'][SINGER_REPORT_FIELD]['format'], 'date-time')

    def test_report_id_field_present(self):
        """Schema must include the _sdc_report_id integer field."""
        schema = get_schema('my_stream', [])
        self.assertIn(REPORT_ID_FIELD, schema['properties'])
        self.assertEqual(schema['properties'][REPORT_ID_FIELD]['type'], 'integer')

    def test_schema_type_is_object(self):
        """Schema root type should be 'object'."""
        schema = get_schema('my_stream', [])
        self.assertEqual(schema['type'], 'object')

    def test_field_type_null_prefix(self):
        """Every custom field should have 'null' prepended to its types."""
        fieldmap = [{'name': 'myField', 'type': 'string'}]
        schema = get_schema('my_stream', fieldmap)
        self.assertEqual(schema['properties']['myField']['type'][0], 'null')

    def test_long_field_becomes_integer_in_schema(self):
        """A 'long' type field should become 'integer' in JSON schema."""
        fieldmap = [{'name': 'myCount', 'type': 'long'}]
        schema = get_schema('my_stream', fieldmap)
        self.assertIn('integer', schema['properties']['myCount']['type'])

    def test_double_field_becomes_number_in_schema(self):
        """A 'double' type field should become 'number' in JSON schema."""
        fieldmap = [{'name': 'myRate', 'type': 'double'}]
        schema = get_schema('my_stream', fieldmap)
        self.assertIn('number', schema['properties']['myRate']['type'])
