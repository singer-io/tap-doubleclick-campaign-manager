from tap_doubleclick_campaign_manager.sync_reports import transform_field

import unittest

class TestSyncReports(unittest.TestCase):

    def setUp(self):
        self.maxDiff = None

    def test_transform_field_handles_single_type_string(self):
        actual = transform_field("string", "some_field")
        expected = "some_field"
        self.assertEqual(expected, actual)

    def test_transform_field_handles_single_type_double(self):
        actual = transform_field("double", "1.23")
        expected = 1.23
        self.assertEqual(expected, actual)

    def test_transform_field_handles_single_type_long(self):
        actual = transform_field("long", "123")
        expected = 123
        self.assertEqual(expected, actual)

    def test_transform_field_handles_single_type_boolean(self):
        actual = transform_field("boolean", "true")
        expected = True
        self.assertEqual(expected, actual)

    def test_transform_field_handles_single_type_empty(self):
        actual = transform_field("double", "")
        expected = None
        self.assertEqual(expected, actual)


    def test_transform_field_handles_multiple_type_uses_first_type(self):
        actual = transform_field(["double", "string"], "123")
        expected = 123.0
        self.assertEqual(expected, actual)

        actual = transform_field(["long", "string"], "123")
        expected = 123
        self.assertEqual(expected, actual)

        actual = transform_field(["long", "string"], "some_field")
        expected = None
        self.assertEqual(expected, actual)

    def test_transform_field_handles_multiple_type_uses_second_type(self):
        actual = transform_field(["double", "string"], "some_field")
        expected = "some_field"
        self.assertEqual(expected, actual)
