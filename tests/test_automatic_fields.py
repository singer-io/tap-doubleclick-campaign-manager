"""Mock integration tests — automatic (SdC system) fields always present.

Verifies that _sdc_report_time and _sdc_report_id are always injected into
every written record regardless of the report type or field selection config.
These SdC fields are the DCM equivalent of Singer 'automatic' fields and must
be present even when a downstream target has suppressed other columns.
"""
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
        build_mock_service,
        make_mock_downloader_class,
        build_dcm_csv,
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
        make_mock_downloader_class,
        build_dcm_csv,
        _expected_tap_stream_id,
    )

from tap_doubleclick_campaign_manager.schema import (
    SINGER_REPORT_FIELD,
    REPORT_ID_FIELD,
    get_field_type_lookup,
    get_fields,
)
from tap_doubleclick_campaign_manager.sync_reports import sync_report


class DcmAutomaticFieldsTest(DcmBaseTest, unittest.TestCase):
    """
    Verify that _sdc_report_time and _sdc_report_id (automatic / system fields)
    are always injected into records for every report type.
    """

    # SdC system fields that must always be present
    AUTOMATIC_FIELDS = frozenset({SINGER_REPORT_FIELD, REPORT_ID_FIELD})

    # ── Generic assertion helper ─────────────────────────────────────────

    def _assert_automatic_fields_always_present(self, report: dict):
        """
        Sync *report* and assert that every written record contains both
        automatic SdC system fields regardless of any other field selection.
        """
        written = self._run_sync_for_report(report, n_rows=2)
        self.assertGreater(
            len(written), 0,
            msg=f"No records written for report type '{report['type']}'",
        )
        for rec in written:
            with self.subTest(report_type=report["type"],
                              record_fields=list(rec.keys())):
                for auto_field in self.AUTOMATIC_FIELDS:
                    self.assertIn(
                        auto_field,
                        rec,
                        msg=(
                            f"Automatic field '{auto_field}' missing from "
                            f"'{report['type']}' record"
                        ),
                    )

    # ── Per-report-type tests ─────────────────────────────────────────────

    def test_standard_report_sdc_report_time_always_present(self):
        """STANDARD: _sdc_report_time injected into every record."""
        written = self._run_sync_for_report(STANDARD_REPORT)
        for rec in written:
            self.assertIn(SINGER_REPORT_FIELD, rec)

    def test_standard_report_sdc_report_id_always_present(self):
        """STANDARD: _sdc_report_id injected into every record."""
        written = self._run_sync_for_report(STANDARD_REPORT)
        for rec in written:
            self.assertIn(REPORT_ID_FIELD, rec)

    def test_floodlight_report_automatic_fields_always_present(self):
        self._assert_automatic_fields_always_present(FLOODLIGHT_REPORT)

    def test_cross_dimension_reach_report_automatic_fields_always_present(self):
        self._assert_automatic_fields_always_present(CROSS_DIMENSION_REACH_REPORT)

    def test_path_to_conversion_report_automatic_fields_always_present(self):
        self._assert_automatic_fields_always_present(PATH_TO_CONVERSION_REPORT)

    def test_reach_report_automatic_fields_always_present(self):
        self._assert_automatic_fields_always_present(REACH_REPORT)

    # ── SdC field value contracts ─────────────────────────────────────────

    def test_sdc_report_id_is_int_for_all_report_types(self):
        """_sdc_report_id must be an integer for every report type."""
        for report in self.MOCK_REPORTS:
            written = self._run_sync_for_report(report, n_rows=1)
            for rec in written:
                with self.subTest(report_type=report["type"]):
                    self.assertIsInstance(
                        rec[REPORT_ID_FIELD], int,
                        msg=f"_sdc_report_id should be int, got {type(rec[REPORT_ID_FIELD])}",
                    )

    def test_sdc_report_time_is_str_for_all_report_types(self):
        """_sdc_report_time must be a string (ISO-8601 datetime) for every report type."""
        for report in self.MOCK_REPORTS:
            written = self._run_sync_for_report(report, n_rows=1)
            for rec in written:
                with self.subTest(report_type=report["type"]):
                    self.assertIsInstance(
                        rec[SINGER_REPORT_FIELD], str,
                        msg=f"_sdc_report_time should be str, got "
                            f"{type(rec[SINGER_REPORT_FIELD])}",
                    )

    def test_sdc_report_id_matches_report_id(self):
        """_sdc_report_id must equal the actual report id in the config."""
        for report in self.MOCK_REPORTS:
            written = self._run_sync_for_report(report, n_rows=1)
            for rec in written:
                with self.subTest(report_id=report["id"]):
                    self.assertEqual(
                        rec[REPORT_ID_FIELD],
                        report["id"],
                        msg=(
                            f"_sdc_report_id {rec[REPORT_ID_FIELD]} != "
                            f"report id {report['id']}"
                        ),
                    )

    def test_sdc_report_time_contains_date_component(self):
        """_sdc_report_time must look like a datetime (contains 'T' separator)."""
        written = self._run_sync_for_report(STANDARD_REPORT, n_rows=1)
        for rec in written:
            self.assertIn(
                "T",
                rec[SINGER_REPORT_FIELD],
                msg="_sdc_report_time does not look like an ISO-8601 datetime",
            )

    # ── Empty-CSV edge case: SdC fields still defined in schema ──────────

    def test_sdc_fields_in_schema_for_empty_csv(self):
        """Even if no data rows are emitted, schema must declare SdC fields."""
        from tap_doubleclick_campaign_manager.schema import get_schema

        for report in self.MOCK_REPORTS:
            field_type_lookup = get_field_type_lookup()
            fieldmap = get_fields(field_type_lookup, report)
            schema = get_schema(_expected_tap_stream_id(report), fieldmap)
            with self.subTest(report_type=report["type"]):
                self.assertIn(SINGER_REPORT_FIELD, schema["properties"])
                self.assertIn(REPORT_ID_FIELD, schema["properties"])

    # ── Minimal CSV (one row): automatic fields survive all transforms ────

    def test_automatic_fields_survive_single_row_csv(self):
        """With a 1-row CSV, automatic fields must still appear in the record."""
        for report in self.MOCK_REPORTS:
            with self.subTest(report_type=report["type"]):
                written = self._run_sync_for_report(report, n_rows=1)
                self.assertEqual(len(written), 1)
                for auto_field in self.AUTOMATIC_FIELDS:
                    self.assertIn(auto_field, written[0])

    # ── Grand Total row not emitted as a record ───────────────────────────

    def test_grand_total_row_excluded_from_records(self):
        """The 'Grand Total:' CSV footer row must never appear as a Singer record."""
        for report in self.MOCK_REPORTS:
            with self.subTest(report_type=report["type"]):
                written = self._run_sync_for_report(report, n_rows=2)
                # If Grand Total were included, the first field value would start with
                # "Grand Total:" — this must not occur.
                for rec in written:
                    for val in rec.values():
                        if isinstance(val, str):
                            self.assertFalse(
                                val.startswith("Grand Total:"),
                                msg="Grand Total marker appeared in a record",
                            )


if __name__ == "__main__":
    unittest.main()
