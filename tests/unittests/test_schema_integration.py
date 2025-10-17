import unittest
from unittest.mock import patch, MagicMock

from tap_doubleclick_campaign_manager.schema import get_schema, SINGER_REPORT_FIELD, REPORT_ID_FIELD


class TestSchemaIntegration(unittest.TestCase):
    """Integration tests for schema functionality related to metadata changes"""

    def setUp(self):
        self.maxDiff = None

    def test_get_schema_includes_required_singer_fields(self):
        """Test that get_schema includes the required Singer fields regardless of report type"""
        
        fieldmap = [
            {'name': 'customField', 'type': 'string'}
        ]
        
        actual = get_schema('test_stream', fieldmap)
        
        # Verify Singer required fields are always present
        self.assertIn(SINGER_REPORT_FIELD, actual['properties'])
        self.assertIn(REPORT_ID_FIELD, actual['properties'])
        
        # Verify their types are correct
        self.assertEqual(actual['properties'][SINGER_REPORT_FIELD]['type'], 'string')
        self.assertEqual(actual['properties'][SINGER_REPORT_FIELD]['format'], 'date-time')
        self.assertEqual(actual['properties'][REPORT_ID_FIELD]['type'], 'integer')
        
        # Verify custom field is also present
        self.assertIn('customField', actual['properties'])
        self.assertEqual(actual['properties']['customField']['type'], ['null', 'string'])

    def test_get_schema_with_complex_fieldmap(self):
        """Test schema generation with complex field types to ensure compatibility with metadata changes"""
        
        fieldmap = [
            {'name': 'stringField', 'type': 'string'},
            {'name': 'longField', 'type': 'long'},
            {'name': 'doubleField', 'type': 'double'},
            {'name': 'multiTypeField', 'type': ['long', 'string']}
        ]
        
        actual = get_schema('complex_stream', fieldmap)
        
        # Verify all field types are correctly converted
        expected_properties = {
            'stringField': {'type': ['null', 'string']},
            'longField': {'type': ['null', 'integer']},
            'doubleField': {'type': ['null', 'number']},
            'multiTypeField': {'type': ['null', 'integer', 'string']},
            SINGER_REPORT_FIELD: {'type': 'string', 'format': 'date-time'},
            REPORT_ID_FIELD: {'type': 'integer'}
        }
        
        for field_name, expected_field in expected_properties.items():
            self.assertIn(field_name, actual['properties'])
            self.assertEqual(actual['properties'][field_name], expected_field)

    def test_schema_structure_compatibility(self):
        """Test that schema structure remains compatible with catalog metadata"""
        
        fieldmap = [{'name': 'testField', 'type': 'string'}]
        schema = get_schema('test_stream', fieldmap)
        
        # Verify schema has required top-level structure
        self.assertIn('type', schema)
        self.assertEqual(schema['type'], 'object')
        self.assertIn('properties', schema)
        self.assertIn('addtionalProperties', schema)  # Note: This appears to be a typo in the original code
        self.assertEqual(schema['addtionalProperties'], False)
        
        # Verify properties is a dict
        self.assertIsInstance(schema['properties'], dict)
