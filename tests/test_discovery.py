"""Mock integration tests for tap-doubleclick-campaign-manager stream discovery.

Calls discover_streams() with a mocked Google API service — no real credentials
or network access required.  Verifies catalog structure for all five DCM report
types: STANDARD, FLOODLIGHT, CROSS_DIMENSION_REACH, PATH_TO_CONVERSION, REACH.
"""
import unittest
from singer import metadata

try:
    from .base import (
        DcmBaseTest,
        STANDARD_REPORT,
        FLOODLIGHT_REPORT,
        CROSS_DIMENSION_REACH_REPORT,
        PATH_TO_CONVERSION_REPORT,
        REACH_REPORT,
        build_mock_service,
        _expected_tap_stream_id,
    )
except ImportError:
    from base import (
        DcmBaseTest,
        STANDARD_REPORT,
        FLOODLIGHT_REPORT,
        CROSS_DIMENSION_REACH_REPORT,
        PATH_TO_CONVERSION_REPORT,
        REACH_REPORT,
        build_mock_service,
        _expected_tap_stream_id,
    )

from tap_doubleclick_campaign_manager.discover import discover_streams
from tap_doubleclick_campaign_manager.schema import SINGER_REPORT_FIELD, REPORT_ID_FIELD


class DcmDiscoveryTest(DcmBaseTest, unittest.TestCase):
    """Verify discover_streams() builds the correct Singer Catalog."""

    # ── Stream presence ──────────────────────────────────────────────────

    def test_discovery_returns_all_five_report_type_streams(self):
        """discover_streams must return one entry for each mocked report."""
        catalog_dict = self._discover()
        discovered_ids = {s["tap_stream_id"] for s in catalog_dict["streams"]}
        self.assertEqual(discovered_ids, self.expected_tap_stream_ids())

    def test_discovery_stream_count_matches_report_count(self):
        """Catalog entry count must equal the number of mock reports."""
        catalog_dict = self._discover()
        self.assertEqual(len(catalog_dict["streams"]), self.expected_stream_count())

    def test_discovery_stream_name_equals_tap_stream_id_without_suffix(self):
        """stream field (display name) must be the sanitized report name only."""
        catalog_dict = self._discover()
        for stream in catalog_dict["streams"]:
            with self.subTest(tap_stream_id=stream["tap_stream_id"]):
                # tap_stream_id = {stream}_{report_id}, so strip the numeric suffix
                expected_stream = stream["tap_stream_id"].rsplit("_", 1)[0]
                self.assertEqual(stream["stream"], expected_stream)

    def test_discovery_tap_stream_id_contains_report_id(self):
        """tap_stream_id must end with the numeric report id."""
        catalog_dict = self._discover()
        report_ids = {str(r["id"]) for r in self.MOCK_REPORTS}
        for stream in catalog_dict["streams"]:
            with self.subTest(tap_stream_id=stream["tap_stream_id"]):
                suffix = stream["tap_stream_id"].rsplit("_", 1)[-1]
                self.assertIn(suffix, report_ids)

    def test_discovery_sorts_streams_by_report_id(self):
        """Streams in the returned catalog must be sorted ascending by report id."""
        catalog_dict = self._discover()
        stream_ids_in_order = [s["tap_stream_id"] for s in catalog_dict["streams"]]
        # extract numeric suffix and verify ascending order
        suffixes = [int(sid.rsplit("_", 1)[-1]) for sid in stream_ids_in_order]
        self.assertEqual(suffixes, sorted(suffixes))

    # ── Schema integrity ─────────────────────────────────────────────────

    def test_discovery_schema_has_properties(self):
        """Every stream schema must have at least one property."""
        catalog_dict = self._discover()
        for stream in catalog_dict["streams"]:
            with self.subTest(tap_stream_id=stream["tap_stream_id"]):
                props = stream["schema"].get("properties", {})
                self.assertGreater(len(props), 0)

    def test_discovery_schema_contains_sdc_report_time(self):
        """Every stream schema must include the _sdc_report_time SdC field."""
        catalog_dict = self._discover()
        for stream in catalog_dict["streams"]:
            with self.subTest(tap_stream_id=stream["tap_stream_id"]):
                self.assertIn(SINGER_REPORT_FIELD, stream["schema"]["properties"])

    def test_discovery_schema_contains_sdc_report_id(self):
        """Every stream schema must include the _sdc_report_id SdC field."""
        catalog_dict = self._discover()
        for stream in catalog_dict["streams"]:
            with self.subTest(tap_stream_id=stream["tap_stream_id"]):
                self.assertIn(REPORT_ID_FIELD, stream["schema"]["properties"])

    def test_discovery_sdc_report_time_is_datetime(self):
        """_sdc_report_time property must have format: date-time."""
        catalog_dict = self._discover()
        for stream in catalog_dict["streams"]:
            with self.subTest(tap_stream_id=stream["tap_stream_id"]):
                prop = stream["schema"]["properties"][SINGER_REPORT_FIELD]
                self.assertEqual(prop.get("format"), "date-time")

    def test_discovery_sdc_report_id_is_integer(self):
        """_sdc_report_id property must have type: integer."""
        catalog_dict = self._discover()
        for stream in catalog_dict["streams"]:
            with self.subTest(tap_stream_id=stream["tap_stream_id"]):
                prop = stream["schema"]["properties"][REPORT_ID_FIELD]
                self.assertIn("integer", prop.get("type", []))

    def test_discovery_schema_includes_dimension_and_metric_fields(self):
        """Schema properties must include every dimension and metric from the report."""
        catalog_dict = self._discover()
        # Check the STANDARD report as the reference case
        std_stream = next(
            s for s in catalog_dict["streams"]
            if str(STANDARD_REPORT["id"]) in s["tap_stream_id"]
        )
        props = std_stream["schema"]["properties"]
        for col in ("date", "campaign", "impressions", "clicks"):
            with self.subTest(column=col):
                self.assertIn(col, props)

    # ── Metadata assertions ───────────────────────────────────────────────

    def test_discovery_forced_replication_method_is_full_table(self):
        """forced-replication-method must be FULL_TABLE for every stream."""
        catalog_dict = self._discover()
        for stream in catalog_dict["streams"]:
            with self.subTest(tap_stream_id=stream["tap_stream_id"]):
                root_meta = next(
                    m for m in stream["metadata"] if m["breadcrumb"] == []
                )
                self.assertEqual(
                    root_meta["metadata"]["forced-replication-method"],
                    "FULL_TABLE",
                )

    def test_all_streams_are_full_table(self):
        """expected_metadata() must report FULL_TABLE for every DCM stream."""
        for stream_id, meta in self.expected_metadata().items():
            with self.subTest(stream=stream_id):
                self.assertEqual(meta[self.REPLICATION_METHOD], self.FULL_TABLE)

    def test_all_streams_have_no_replication_keys(self):
        """All DCM streams are FULL_TABLE — no replication keys expected."""
        for stream_id, meta in self.expected_metadata().items():
            with self.subTest(stream=stream_id):
                self.assertEqual(meta[self.REPLICATION_KEYS], set())

    def test_full_table_streams_do_not_obey_start_date(self):
        """All DCM streams must have OBEYS_START_DATE=False in expected_metadata."""
        for stream_id, meta in self.expected_metadata().items():
            with self.subTest(stream=stream_id):
                self.assertFalse(meta[self.OBEYS_START_DATE])

    def test_full_table_streams_set_matches_all_streams(self):
        """full_table_streams() must return every expected stream."""
        self.assertEqual(self.full_table_streams(), self.expected_stream_names())

    def test_discovery_report_id_in_root_metadata(self):
        """The DCM report-id must be stored in root metadata for each stream."""
        catalog_dict = self._discover()
        report_ids = {r["id"] for r in self.MOCK_REPORTS}
        for stream in catalog_dict["streams"]:
            with self.subTest(tap_stream_id=stream["tap_stream_id"]):
                root_meta = next(
                    m for m in stream["metadata"] if m["breadcrumb"] == []
                )
                rid = root_meta["metadata"][
                    "tap-doubleclick-campaign-manager.report-id"
                ]
                self.assertIn(rid, report_ids)

    def test_discovery_every_property_has_inclusion_automatic(self):
        """Every schema property must have inclusion=automatic in metadata."""
        catalog_dict = self._discover()
        for stream in catalog_dict["streams"]:
            with self.subTest(tap_stream_id=stream["tap_stream_id"]):
                prop_meta = [
                    m for m in stream["metadata"]
                    if len(m["breadcrumb"]) == 2 and m["breadcrumb"][0] == "properties"
                ]
                for m in prop_meta:
                    with self.subTest(breadcrumb=m["breadcrumb"]):
                        self.assertEqual(m["metadata"]["inclusion"], "automatic")

    def test_discovery_metadata_list_is_not_empty(self):
        """Every stream catalog entry must have at least one metadata dict."""
        catalog_dict = self._discover()
        for stream in catalog_dict["streams"]:
            with self.subTest(tap_stream_id=stream["tap_stream_id"]):
                self.assertGreater(len(stream["metadata"]), 0)

    def test_discovery_no_primary_keys(self):
        """DCM report streams have no natural primary keys (empty key_properties)."""
        catalog_dict = self._discover()
        for stream in catalog_dict["streams"]:
            with self.subTest(tap_stream_id=stream["tap_stream_id"]):
                self.assertEqual(stream.get("key_properties", []), [])

    # ── All five report types produce streams ─────────────────────────────

    def test_standard_report_produces_stream(self):
        catalog_dict = self._discover()
        ids = {s["tap_stream_id"] for s in catalog_dict["streams"]}
        self.assertIn(_expected_tap_stream_id(STANDARD_REPORT), ids)

    def test_floodlight_report_produces_stream(self):
        catalog_dict = self._discover()
        ids = {s["tap_stream_id"] for s in catalog_dict["streams"]}
        self.assertIn(_expected_tap_stream_id(FLOODLIGHT_REPORT), ids)

    def test_cross_dimension_reach_report_produces_stream(self):
        catalog_dict = self._discover()
        ids = {s["tap_stream_id"] for s in catalog_dict["streams"]}
        self.assertIn(_expected_tap_stream_id(CROSS_DIMENSION_REACH_REPORT), ids)

    def test_path_to_conversion_report_produces_stream(self):
        catalog_dict = self._discover()
        ids = {s["tap_stream_id"] for s in catalog_dict["streams"]}
        self.assertIn(_expected_tap_stream_id(PATH_TO_CONVERSION_REPORT), ids)

    def test_reach_report_produces_stream(self):
        catalog_dict = self._discover()
        ids = {s["tap_stream_id"] for s in catalog_dict["streams"]}
        self.assertIn(_expected_tap_stream_id(REACH_REPORT), ids)

    # ── Field-type schema mapping ─────────────────────────────────────────

    def test_long_fields_have_integer_type_in_schema(self):
        """Fields with DCM type 'long' must map to JSON Schema 'integer'."""
        catalog_dict = self._discover()
        # 'impressions' is type 'long' in the field lookup
        std_stream = next(
            s for s in catalog_dict["streams"]
            if str(STANDARD_REPORT["id"]) in s["tap_stream_id"]
        )
        impressions_type = std_stream["schema"]["properties"]["impressions"]["type"]
        self.assertIn("integer", impressions_type)

    def test_string_fields_have_string_type_in_schema(self):
        """Fields with DCM type 'string' must map to JSON Schema 'string'."""
        catalog_dict = self._discover()
        std_stream = next(
            s for s in catalog_dict["streams"]
            if str(STANDARD_REPORT["id"]) in s["tap_stream_id"]
        )
        date_type = std_stream["schema"]["properties"]["date"]["type"]
        self.assertIn("string", date_type)

    def test_schema_types_include_null(self):
        """All user-defined field types must allow null (Singer convention)."""
        catalog_dict = self._discover()
        for stream in catalog_dict["streams"]:
            with self.subTest(tap_stream_id=stream["tap_stream_id"]):
                props = stream["schema"]["properties"]
                for field_name, field_schema in props.items():
                    # SdC system fields use a plain non-null type; skip them
                    if field_name in (SINGER_REPORT_FIELD, REPORT_ID_FIELD):
                        continue
                    with self.subTest(field=field_name):
                        self.assertIn("null", field_schema.get("type", []))

    # ── Empty report list edge case ───────────────────────────────────────

    def test_empty_report_list_returns_empty_catalog(self):
        """discover_streams with no reports should return an empty catalog."""
        service = build_mock_service(reports_pages=[[]])
        catalog_dict = discover_streams(service, self.MOCK_CONFIG)
        self.assertEqual(len(catalog_dict["streams"]), 0)

    # ── Name sanitization ─────────────────────────────────────────────────

    def test_report_name_with_spaces_sanitized(self):
        """Spaces in report names must be replaced with underscores."""
        catalog_dict = self._discover()
        for stream in catalog_dict["streams"]:
            self.assertNotIn(" ", stream["stream"])

    def test_report_name_with_hyphens_sanitized(self):
        """Hyphens in report names must be replaced with underscores."""
        from tap_doubleclick_campaign_manager.discover import sanitize_name
        self.assertEqual(sanitize_name("My-Report-Name"), "my_report_name")

    def test_stream_name_lowercase(self):
        """All stream names must be lowercase."""
        catalog_dict = self._discover()
        for stream in catalog_dict["streams"]:
            with self.subTest(tap_stream_id=stream["tap_stream_id"]):
                self.assertEqual(stream["stream"], stream["stream"].lower())

    def test_stream_alias_equals_stream(self):
        """stream_alias must equal stream (used by Singer for table naming)."""
        catalog_dict = self._discover()
        for stream in catalog_dict["streams"]:
            with self.subTest(tap_stream_id=stream["tap_stream_id"]):
                self.assertEqual(stream.get("stream_alias"), stream["stream"])

