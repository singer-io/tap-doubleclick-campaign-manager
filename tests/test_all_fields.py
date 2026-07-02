"""Mock integration tests — all report fields replicated for every DCM stream.

Patches MediaIoBaseDownload and singer.write_record to capture written records
and verify every dimension/metric column plus SdC system fields are present.
No live credentials or network access required.
"""
from datetime import datetime
import unittest
from unittest.mock import patch, MagicMock

try:
    from .base import (
        DcmBaseTest,
        STANDARD_REPORT,
        FLOODLIGHT_REPORT,
        CROSS_DIMENSION_REACH_REPORT,
        PATH_TO_CONVERSION_REPORT,
        REACH_REPORT,
        get_report_headers,
        build_sample_rows_for_report,
        build_dcm_csv,
        make_mock_downloader_class,
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
        get_report_headers,
        build_sample_rows_for_report,
        build_dcm_csv,
        make_mock_downloader_class,
        _expected_tap_stream_id,
    )

from tap_doubleclick_campaign_manager.schema import (
    SINGER_REPORT_FIELD,
    REPORT_ID_FIELD,
    get_field_type_lookup,
)


# ---------------------------------------------------------------------------
# Known missing / skipped fields
# Add entries here only if a field is intentionally absent from mock data.
# ---------------------------------------------------------------------------
KNOWN_MISSING_FIELDS: dict[str, set[str]] = {}


class DcmAllFieldsTest(DcmBaseTest, unittest.TestCase):
    """Ensure every schema field appears in the records written during sync."""

    @staticmethod
    def _assert_rfc3339_datetime(value: str):
        datetime.fromisoformat(value.replace("Z", "+00:00"))

    # ── Generic assertion helper ─────────────────────────────────────────

    def _assert_all_report_fields_present(self, report: dict, n_rows: int = 2):
        """
        Sync *report* with mocked data and assert every schema field appears
        in at least one of the written Singer records.
        """
        tap_stream_id = _expected_tap_stream_id(report)
        written = self._run_sync_for_report(report, n_rows=n_rows)

        self.assertGreater(len(written), 0,
                           f"No records written for stream '{tap_stream_id}'")

        # Expected fields = report dimensions + metrics + SdC system fields
        headers = get_report_headers(report)
        expected_fields = set(headers) | {SINGER_REPORT_FIELD, REPORT_ID_FIELD}
        known_missing = KNOWN_MISSING_FIELDS.get(tap_stream_id, set())
        required = expected_fields - known_missing

        actual_fields = set().union(*(set(r.keys()) for r in written))
        self.assertSetEqual(
            actual_fields,
            required,
            msg=(
                f"Stream '{tap_stream_id}': expected exact fields {required}, "
                f"got {actual_fields}"
            ),
        )

    def _assert_datetime_fields_are_rfc3339(self, report: dict):
        """Fields with JSON Schema format=date-time must contain RFC3339 strings."""
        field_type_lookup = get_field_type_lookup()
        schema = self._schema_for_report(report, field_type_lookup)
        datetime_fields = [
            name for name, props in schema["properties"].items()
            if props.get("format") == "date-time"
        ]
        written = self._run_sync_for_report(report)
        for rec in written:
            for field in datetime_fields:
                value = rec.get(field)
                with self.subTest(report_id=report["id"], field=field):
                    self.assertIsNotNone(value)
                    self.assertIsInstance(value, str)
                    self._assert_rfc3339_datetime(value)

    @staticmethod
    def _schema_for_report(report: dict, field_type_lookup: dict):
        from tap_doubleclick_campaign_manager.schema import get_fields, get_schema

        fieldmap = get_fields(field_type_lookup, report)
        return get_schema(_expected_tap_stream_id(report), fieldmap)

    # ── SdC system fields — all streams ──────────────────────────────────

    def _assert_sdc_fields_present(self, report: dict):
        written = self._run_sync_for_report(report)
        for rec in written:
            with self.subTest(report_id=report["id"]):
                self.assertIn(SINGER_REPORT_FIELD, rec,
                              msg=f"{SINGER_REPORT_FIELD} missing from record")
                self.assertIn(REPORT_ID_FIELD, rec,
                              msg=f"{REPORT_ID_FIELD} missing from record")

    # ── STANDARD report ───────────────────────────────────────────────────

    def test_standard_report_all_fields_present(self):
        """STANDARD report: every dimension + metric + SdC field in records."""
        self._assert_all_report_fields_present(STANDARD_REPORT)

    def test_standard_report_sdc_fields_present(self):
        self._assert_sdc_fields_present(STANDARD_REPORT)

    def test_standard_report_sdc_report_id_is_integer(self):
        """_sdc_report_id written to STANDARD records must be an integer."""
        written = self._run_sync_for_report(STANDARD_REPORT)
        for rec in written:
            with self.subTest():
                self.assertIsInstance(rec[REPORT_ID_FIELD], int)

    def test_standard_report_sdc_report_time_is_string(self):
        """_sdc_report_time written to STANDARD records must be a datetime string."""
        written = self._run_sync_for_report(STANDARD_REPORT)
        for rec in written:
            with self.subTest():
                self.assertIsInstance(rec[SINGER_REPORT_FIELD], str)

    def test_standard_report_correct_number_of_records(self):
        """sync_report must write exactly as many records as rows in the CSV."""
        n = 3
        written = self._run_sync_for_report(STANDARD_REPORT, n_rows=n)
        self.assertEqual(len(written), n)

    def test_standard_report_dimensions_present(self):
        """STANDARD: 'date' and 'campaign' dimensions appear in every record."""
        written = self._run_sync_for_report(STANDARD_REPORT)
        for rec in written:
            self.assertIn("date", rec)
            self.assertIn("campaign", rec)

    def test_standard_report_metrics_present(self):
        """STANDARD: 'impressions' and 'clicks' metrics appear in every record."""
        written = self._run_sync_for_report(STANDARD_REPORT)
        for rec in written:
            self.assertIn("impressions", rec)
            self.assertIn("clicks", rec)

    # ── FLOODLIGHT report ─────────────────────────────────────────────────

    def test_floodlight_report_all_fields_present(self):
        """FLOODLIGHT report: every dimension + metric + SdC field in records."""
        self._assert_all_report_fields_present(FLOODLIGHT_REPORT)

    def test_floodlight_report_sdc_fields_present(self):
        self._assert_sdc_fields_present(FLOODLIGHT_REPORT)

    def test_floodlight_report_dimensions_and_metrics_present(self):
        """FLOODLIGHT: 'date', 'activity', 'totalConversions' in records."""
        written = self._run_sync_for_report(FLOODLIGHT_REPORT)
        for rec in written:
            self.assertIn("date", rec)
            self.assertIn("activity", rec)
            self.assertIn("totalConversions", rec)

    def test_floodlight_report_correct_record_count(self):
        n = 2
        written = self._run_sync_for_report(FLOODLIGHT_REPORT, n_rows=n)
        self.assertEqual(len(written), n)

    # ── CROSS_DIMENSION_REACH report ──────────────────────────────────────

    def test_cross_dimension_reach_report_all_fields_present(self):
        """CROSS_DIMENSION_REACH: every field in records."""
        self._assert_all_report_fields_present(CROSS_DIMENSION_REACH_REPORT)

    def test_cross_dimension_reach_report_sdc_fields_present(self):
        self._assert_sdc_fields_present(CROSS_DIMENSION_REACH_REPORT)

    def test_cross_dimension_reach_includes_overlap_metrics(self):
        """CROSS_DIMENSION_REACH: 'overlapMetricNames' merged into columns."""
        written = self._run_sync_for_report(CROSS_DIMENSION_REACH_REPORT)
        for rec in written:
            self.assertIn("uniqueReachIncrementalReach", rec)

    # ── PATH_TO_CONVERSION report ─────────────────────────────────────────

    def test_path_to_conversion_report_all_fields_present(self):
        """PATH_TO_CONVERSION: every field in records."""
        self._assert_all_report_fields_present(PATH_TO_CONVERSION_REPORT)

    def test_path_to_conversion_report_sdc_fields_present(self):
        self._assert_sdc_fields_present(PATH_TO_CONVERSION_REPORT)

    def test_path_to_conversion_combines_conversion_and_interaction_dims(self):
        """PATH_TO_CONVERSION: conversionDimensions + perInteractionDimensions merged."""
        written = self._run_sync_for_report(PATH_TO_CONVERSION_REPORT)
        for rec in written:
            self.assertIn("date", rec)
            self.assertIn("campaign", rec)

    # ── REACH report ──────────────────────────────────────────────────────

    def test_reach_report_all_fields_present(self):
        """REACH report: every field in records."""
        self._assert_all_report_fields_present(REACH_REPORT)

    def test_reach_report_sdc_fields_present(self):
        self._assert_sdc_fields_present(REACH_REPORT)

    def test_reach_report_includes_reach_by_frequency_metrics(self):
        """REACH: 'reachByFrequencyMetricNames' merged into columns."""
        written = self._run_sync_for_report(REACH_REPORT)
        for rec in written:
            self.assertIn("reachByFrequency", rec)

    # ── Data type integrity ───────────────────────────────────────────────

    def test_standard_report_impressions_is_none_or_integer(self):
        """'impressions' (long) in STANDARD records must be int or None."""
        written = self._run_sync_for_report(STANDARD_REPORT)
        for rec in written:
            val = rec.get("impressions")
            with self.subTest():
                self.assertTrue(
                    val is None or isinstance(val, int),
                    msg=f"Expected int|None for impressions, got {type(val)}: {val!r}",
                )

    def test_standard_report_clicks_is_none_or_integer(self):
        """'clicks' (long) in STANDARD records must be int or None."""
        written = self._run_sync_for_report(STANDARD_REPORT)
        for rec in written:
            val = rec.get("clicks")
            with self.subTest():
                self.assertTrue(
                    val is None or isinstance(val, int),
                    msg=f"Expected int|None for clicks, got {type(val)}: {val!r}",
                )

    def test_sdc_report_id_matches_mock_report_id(self):
        """_sdc_report_id in records must equal the actual mock report id."""
        for report in self.MOCK_REPORTS:
            written = self._run_sync_for_report(report)
            for rec in written:
                with self.subTest(report_id=report["id"]):
                    self.assertEqual(rec[REPORT_ID_FIELD], report["id"])

    # ── All 5 reports in a single parameterised sweep ─────────────────────

    def test_all_report_types_write_at_least_one_record(self):
        """Every supported DCM report type must produce ≥1 written record."""
        for report in self.MOCK_REPORTS:
            with self.subTest(report_type=report["type"], report_id=report["id"]):
                written = self._run_sync_for_report(report, n_rows=1)
                self.assertGreaterEqual(len(written), 1)

    def test_all_report_types_include_sdc_fields(self):
        """_sdc_report_time and _sdc_report_id must appear in all report types."""
        for report in self.MOCK_REPORTS:
            with self.subTest(report_type=report["type"]):
                written = self._run_sync_for_report(report, n_rows=1)
                for rec in written:
                    self.assertIn(SINGER_REPORT_FIELD, rec)
                    self.assertIn(REPORT_ID_FIELD, rec)

    def test_datetime_fields_are_rfc3339_for_all_report_types(self):
        """Any schema field marked date-time must emit RFC3339 values."""
        for report in self.MOCK_REPORTS:
            with self.subTest(report_type=report["type"]):
                self._assert_datetime_fields_are_rfc3339(report)

