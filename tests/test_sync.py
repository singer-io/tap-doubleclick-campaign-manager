"""Mock integration tests — full end-to-end sync pipeline for tap-dcm.

Covers the complete sync lifecycle:
  1. discover_streams() → build catalog
  2. sync_reports() → selects streams, calls sync_report() per stream
  3. sync_report() → run report, poll status, download CSV, write records

All Google API calls are replaced by MagicMock objects; no real credentials
or network access are required.  CSV data is injected via a mock
MediaIoBaseDownload class.
"""
import unittest
from unittest.mock import patch, MagicMock, call

from singer import metadata as singer_metadata
from singer.catalog import Catalog, CatalogEntry, Schema

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
        get_report_headers,
        build_sample_rows_for_report,
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
        get_report_headers,
        build_sample_rows_for_report,
        _expected_tap_stream_id,
    )

from tap_doubleclick_campaign_manager.sync_reports import sync_report, sync_reports
from tap_doubleclick_campaign_manager.schema import (
    SINGER_REPORT_FIELD,
    REPORT_ID_FIELD,
    get_field_type_lookup,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_catalog_entry(report: dict, selected: bool = True) -> CatalogEntry:
    """Build a single CatalogEntry for the given mock report."""
    tap_stream_id = _expected_tap_stream_id(report)
    meta = [
        {
            "breadcrumb": [],
            "metadata": {
                "tap-doubleclick-campaign-manager.report-id": report["id"],
                "forced-replication-method": "FULL_TABLE",
                "selected": selected,
            },
        }
    ]
    return CatalogEntry(
        stream=tap_stream_id.rsplit("_", 1)[0],
        stream_alias=tap_stream_id.rsplit("_", 1)[0],
        tap_stream_id=tap_stream_id,
        key_properties=[],
        schema=Schema.from_dict({"type": "object", "properties": {}}),
        metadata=meta,
    )


def _make_catalog(reports: list[dict], selected: bool = True) -> Catalog:
    return Catalog([_make_catalog_entry(r, selected=selected) for r in reports])


# ---------------------------------------------------------------------------
# sync_report() tests
# ---------------------------------------------------------------------------

class DcmSyncReportTest(DcmBaseTest, unittest.TestCase):
    """Test the sync_report() function in isolation with mocked service."""

    # ── Records are written ───────────────────────────────────────────────

    def test_sync_report_writes_records_for_standard_report(self):
        """sync_report() must write at least one Singer record for STANDARD."""
        written = self._run_sync_for_report(STANDARD_REPORT, n_rows=2)
        self.assertGreaterEqual(len(written), 1)

    def test_sync_report_writes_records_for_floodlight_report(self):
        written = self._run_sync_for_report(FLOODLIGHT_REPORT, n_rows=2)
        self.assertGreaterEqual(len(written), 1)

    def test_sync_report_writes_records_for_cross_dimension_reach(self):
        written = self._run_sync_for_report(CROSS_DIMENSION_REACH_REPORT, n_rows=2)
        self.assertGreaterEqual(len(written), 1)

    def test_sync_report_writes_records_for_path_to_conversion(self):
        written = self._run_sync_for_report(PATH_TO_CONVERSION_REPORT, n_rows=2)
        self.assertGreaterEqual(len(written), 1)

    def test_sync_report_writes_records_for_reach(self):
        written = self._run_sync_for_report(REACH_REPORT, n_rows=2)
        self.assertGreaterEqual(len(written), 1)

    # ── Singer schema is emitted ─────────────────────────────────────────

    def test_sync_report_writes_schema_before_records(self):
        """sync_report() must call singer.write_schema before any write_record."""
        field_type_lookup = get_field_type_lookup()
        csv_bytes = self._csv_for_report(STANDARD_REPORT, n_rows=1)
        service = build_mock_service(
            reports_pages=[[STANDARD_REPORT]],
            report_details=STANDARD_REPORT,
        )
        report_config = {
            "report_id": STANDARD_REPORT["id"],
            "stream_name": _expected_tap_stream_id(STANDARD_REPORT),
            "stream_alias": _expected_tap_stream_id(STANDARD_REPORT),
            "metadata": {},
        }
        call_order = []

        with patch("tap_doubleclick_campaign_manager.sync_reports.http.MediaIoBaseDownload",
                   make_mock_downloader_class(csv_bytes)), \
             patch("singer.write_schema",
                   side_effect=lambda *a, **kw: call_order.append("schema")), \
             patch("singer.write_record",
                   side_effect=lambda *a, **kw: call_order.append("record")), \
             patch("singer.metrics.record_counter") as mc, \
             patch("singer.metrics.job_timer") as mt:

            mc.return_value.__enter__ = lambda s: s
            mc.return_value.__exit__ = MagicMock(return_value=False)
            mt.return_value.__enter__ = lambda s: s
            mt.return_value.__exit__ = MagicMock(return_value=False)

            sync_report(service, field_type_lookup, self.MOCK_PROFILE_ID, report_config)

        schema_idx = call_order.index("schema")
        first_record_idx = call_order.index("record")
        self.assertLess(schema_idx, first_record_idx,
                        "write_schema must be called before the first write_record")

    # ── File status polling ───────────────────────────────────────────────

    @patch("time.sleep", return_value=None)
    def test_queued_status_retries_until_available(self, mock_sleep):
        """QUEUED status triggers a sleep/retry loop before the file is processed."""
        field_type_lookup = get_field_type_lookup()
        csv_bytes = self._csv_for_report(STANDARD_REPORT, n_rows=1)

        service = build_mock_service(
            reports_pages=[[STANDARD_REPORT]],
            report_details=STANDARD_REPORT,
        )
        # Override files().get().execute() to return QUEUED then REPORT_AVAILABLE
        statuses = iter(["QUEUED", "REPORT_AVAILABLE"])
        service.files.return_value.get.return_value.execute.side_effect = (
            lambda: {"status": next(statuses), "id": "file_001"}
        )

        report_config = {
            "report_id": STANDARD_REPORT["id"],
            "stream_name": _expected_tap_stream_id(STANDARD_REPORT),
            "stream_alias": _expected_tap_stream_id(STANDARD_REPORT),
            "metadata": {},
        }
        written = []
        with patch("tap_doubleclick_campaign_manager.sync_reports.http.MediaIoBaseDownload",
                   make_mock_downloader_class(csv_bytes)), \
             patch("singer.write_schema"), \
             patch("singer.write_record",
                   side_effect=lambda s, r, **kw: written.append(r)), \
             patch("singer.metrics.record_counter") as mc, \
             patch("singer.metrics.job_timer") as mt:

            mc.return_value.__enter__ = lambda s: s
            mc.return_value.__exit__ = MagicMock(return_value=False)
            mt.return_value.__enter__ = lambda s: s
            mt.return_value.__exit__ = MagicMock(return_value=False)

            sync_report(service, field_type_lookup, self.MOCK_PROFILE_ID, report_config)

        mock_sleep.assert_called_once()
        self.assertGreaterEqual(len(written), 1)

    @patch("time.sleep", return_value=None)
    def test_processing_status_continues_polling(self, mock_sleep):
        """PROCESSING status keeps polling without sleeping (unlike QUEUED)."""
        field_type_lookup = get_field_type_lookup()
        csv_bytes = self._csv_for_report(STANDARD_REPORT, n_rows=1)

        service = build_mock_service(
            reports_pages=[[STANDARD_REPORT]],
            report_details=STANDARD_REPORT,
        )
        statuses = iter(["PROCESSING", "PROCESSING", "REPORT_AVAILABLE"])
        service.files.return_value.get.return_value.execute.side_effect = (
            lambda: {"status": next(statuses), "id": "file_002"}
        )

        report_config = {
            "report_id": STANDARD_REPORT["id"],
            "stream_name": _expected_tap_stream_id(STANDARD_REPORT),
            "stream_alias": _expected_tap_stream_id(STANDARD_REPORT),
            "metadata": {},
        }
        written = []
        with patch("tap_doubleclick_campaign_manager.sync_reports.http.MediaIoBaseDownload",
                   make_mock_downloader_class(csv_bytes)), \
             patch("singer.write_schema"), \
             patch("singer.write_record",
                   side_effect=lambda s, r, **kw: written.append(r)), \
             patch("singer.metrics.record_counter") as mc, \
             patch("singer.metrics.job_timer") as mt:

            mc.return_value.__enter__ = lambda s: s
            mc.return_value.__exit__ = MagicMock(return_value=False)
            mt.return_value.__enter__ = lambda s: s
            mt.return_value.__exit__ = MagicMock(return_value=False)

            sync_report(service, field_type_lookup, self.MOCK_PROFILE_ID, report_config)

        # PROCESSING status does NOT trigger sleep — the loop polls immediately.
        # sleep is only called for QUEUED status; here we just verify
        # that the file was eventually processed after the PROCESSING poll loop.
        mock_sleep.assert_not_called()
        self.assertGreaterEqual(len(written), 1)

    def test_unknown_file_status_raises_exception(self):
        """An unexpected file status (FAILED, CANCELLED, …) must raise an exception."""
        field_type_lookup = get_field_type_lookup()
        service = build_mock_service(
            reports_pages=[[STANDARD_REPORT]],
            report_details=STANDARD_REPORT,
            file_status="FAILED",
        )
        report_config = {
            "report_id": STANDARD_REPORT["id"],
            "stream_name": _expected_tap_stream_id(STANDARD_REPORT),
            "stream_alias": _expected_tap_stream_id(STANDARD_REPORT),
            "metadata": {},
        }
        with patch("singer.write_schema"), \
             patch("singer.metrics.record_counter") as mc, \
             patch("singer.metrics.job_timer") as mt:

            mc.return_value.__enter__ = lambda s: s
            mc.return_value.__exit__ = MagicMock(return_value=False)
            mt.return_value.__enter__ = lambda s: s
            mt.return_value.__exit__ = MagicMock(return_value=False)

            with self.assertRaises(Exception):
                sync_report(service, field_type_lookup, self.MOCK_PROFILE_ID, report_config)

    # ── CSV parsing edge cases ────────────────────────────────────────────

    def test_empty_csv_writes_no_records(self):
        """A CSV with no data rows produces zero written records."""
        field_type_lookup = get_field_type_lookup()
        headers = get_report_headers(STANDARD_REPORT)
        csv_bytes = build_dcm_csv(headers, rows=[])  # headers only, no data rows

        service = build_mock_service(
            reports_pages=[[STANDARD_REPORT]],
            report_details=STANDARD_REPORT,
        )
        report_config = {
            "report_id": STANDARD_REPORT["id"],
            "stream_name": _expected_tap_stream_id(STANDARD_REPORT),
            "stream_alias": _expected_tap_stream_id(STANDARD_REPORT),
            "metadata": {},
        }
        written = []
        with patch("tap_doubleclick_campaign_manager.sync_reports.http.MediaIoBaseDownload",
                   make_mock_downloader_class(csv_bytes)), \
             patch("singer.write_schema"), \
             patch("singer.write_record",
                   side_effect=lambda s, r, **kw: written.append(r)), \
             patch("singer.metrics.record_counter") as mc, \
             patch("singer.metrics.job_timer") as mt:

            mc.return_value.__enter__ = lambda s: s
            mc.return_value.__exit__ = MagicMock(return_value=False)
            mt.return_value.__enter__ = lambda s: s
            mt.return_value.__exit__ = MagicMock(return_value=False)

            sync_report(service, field_type_lookup, self.MOCK_PROFILE_ID, report_config)

        self.assertEqual(len(written), 0)

    def test_grand_total_row_not_written_as_record(self):
        """The Grand Total CSV footer row must never be written as a Singer record."""
        written = self._run_sync_for_report(STANDARD_REPORT, n_rows=2)
        for rec in written:
            values = [str(v) for v in rec.values() if v is not None]
            for val in values:
                self.assertFalse(val.startswith("Grand Total:"),
                                 msg="Grand Total appeared in a written record")

    def test_correct_record_count_for_multiple_rows(self):
        """sync_report must write exactly N records for N CSV data rows."""
        for n in (1, 3, 5):
            with self.subTest(n_rows=n):
                written = self._run_sync_for_report(STANDARD_REPORT, n_rows=n)
                self.assertEqual(len(written), n)


# ---------------------------------------------------------------------------
# sync_reports() orchestration tests
# ---------------------------------------------------------------------------

class DcmSyncReportsTest(DcmBaseTest, unittest.TestCase):
    """Test the sync_reports() orchestration layer with mocked sub-calls."""

    @patch("tap_doubleclick_campaign_manager.sync_reports.sync_report")
    @patch("tap_doubleclick_campaign_manager.sync_reports.get_field_type_lookup")
    @patch("singer.write_state")
    def test_selected_streams_are_synced(self, mock_ws, mock_lookup, mock_sr):
        """sync_reports calls sync_report once for each selected stream."""
        mock_lookup.return_value = {}
        catalog = _make_catalog([STANDARD_REPORT, FLOODLIGHT_REPORT], selected=True)
        sync_reports(MagicMock(), self.MOCK_CONFIG, catalog, self.state)
        self.assertEqual(mock_sr.call_count, 2)

    @patch("tap_doubleclick_campaign_manager.sync_reports.sync_report")
    @patch("tap_doubleclick_campaign_manager.sync_reports.get_field_type_lookup")
    @patch("singer.write_state")
    def test_unselected_streams_are_skipped(self, mock_ws, mock_lookup, mock_sr):
        """Streams with selected=False must not be passed to sync_report."""
        mock_lookup.return_value = {}
        catalog = Catalog([
            _make_catalog_entry(STANDARD_REPORT, selected=True),
            _make_catalog_entry(FLOODLIGHT_REPORT, selected=False),
        ])
        sync_reports(MagicMock(), self.MOCK_CONFIG, catalog, self.state)
        synced_ids = [c[0][3]["report_id"] for c in mock_sr.call_args_list]
        self.assertIn(STANDARD_REPORT["id"], synced_ids)
        self.assertNotIn(FLOODLIGHT_REPORT["id"], synced_ids)

    @patch("tap_doubleclick_campaign_manager.sync_reports.sync_report")
    @patch("tap_doubleclick_campaign_manager.sync_reports.get_field_type_lookup")
    @patch("singer.write_state")
    def test_streams_synced_in_ascending_report_id_order(self, mock_ws, mock_lookup, mock_sr):
        """sync_reports must call sync_report in ascending report-id order."""
        mock_lookup.return_value = {}
        # Pass reports in reverse order — expect sorted output
        catalog = _make_catalog(
            [FLOODLIGHT_REPORT, STANDARD_REPORT],  # 1002, 1001 → sort to 1001, 1002
            selected=True,
        )
        sync_reports(MagicMock(), self.MOCK_CONFIG, catalog, self.state)
        call_order = [c[0][3]["report_id"] for c in mock_sr.call_args_list]
        self.assertEqual(call_order, sorted(call_order))

    @patch("tap_doubleclick_campaign_manager.sync_reports.sync_report")
    @patch("tap_doubleclick_campaign_manager.sync_reports.get_field_type_lookup")
    @patch("singer.write_state")
    def test_state_cleared_after_full_sync(self, mock_ws, mock_lookup, mock_sr):
        """After sync completes, state.current_report and state.reports must be None."""
        mock_lookup.return_value = {}
        catalog = _make_catalog([STANDARD_REPORT], selected=True)
        sync_reports(MagicMock(), self.MOCK_CONFIG, catalog, self.state)
        self.assertIsNone(self.state.get("current_report"))
        self.assertIsNone(self.state.get("reports"))

    @patch("tap_doubleclick_campaign_manager.sync_reports.sync_report")
    @patch("tap_doubleclick_campaign_manager.sync_reports.get_field_type_lookup")
    @patch("singer.write_state")
    def test_full_table_sync_does_not_create_bookmarks_or_currently_syncing(
        self, mock_ws, mock_lookup, mock_sr
    ):
        """FULL_TABLE DCM streams must not populate Singer bookmarks/currently_syncing."""
        mock_lookup.return_value = {}
        catalog = _make_catalog([STANDARD_REPORT, FLOODLIGHT_REPORT], selected=True)
        sync_reports(MagicMock(), self.MOCK_CONFIG, catalog, self.state)

        self.assertIsNone(self.state.get("bookmarks"))
        self.assertIsNone(self.state.get("currently_syncing"))

    @patch("tap_doubleclick_campaign_manager.sync_reports.sync_report")
    @patch("tap_doubleclick_campaign_manager.sync_reports.get_field_type_lookup")
    @patch("singer.write_state")
    def test_state_reset_when_report_list_changes(self, mock_ws, mock_lookup, mock_sr):
        """If the report list in state differs from current, current_report resets."""
        mock_lookup.return_value = {}
        catalog = _make_catalog([STANDARD_REPORT], selected=True)
        state = {"reports": [{"report_id": 9999}], "current_report": 9999}
        sync_reports(MagicMock(), self.MOCK_CONFIG, catalog, state)
        # current_report was reset to None before syncing
        mock_sr.assert_called()

    @patch("tap_doubleclick_campaign_manager.sync_reports.sync_report")
    @patch("tap_doubleclick_campaign_manager.sync_reports.get_field_type_lookup")
    @patch("singer.write_state")
    def test_no_selected_streams_never_calls_sync_report(self, mock_ws, mock_lookup, mock_sr):
        """With no selected streams sync_report must never be called."""
        mock_lookup.return_value = {}
        catalog = _make_catalog(
            [STANDARD_REPORT, FLOODLIGHT_REPORT], selected=False
        )
        sync_reports(MagicMock(), self.MOCK_CONFIG, catalog, self.state)
        mock_sr.assert_not_called()

    @patch("tap_doubleclick_campaign_manager.sync_reports.sync_report")
    @patch("tap_doubleclick_campaign_manager.sync_reports.get_field_type_lookup")
    @patch("singer.write_state")
    def test_current_report_checkpoint_skips_earlier_reports(
        self, mock_ws, mock_lookup, mock_sr
    ):
        """When current_report is set, reports with smaller IDs must be skipped."""
        mock_lookup.return_value = {}
        catalog = _make_catalog(
            [STANDARD_REPORT, FLOODLIGHT_REPORT, CROSS_DIMENSION_REACH_REPORT],
            selected=True,
        )

        # Build state['reports'] exactly as sync_reports() would: use the
        # singer metadata map from each catalog entry so the equality check passes.
        expected_reports = sorted(
            [
                {
                    "report_id": singer_metadata.to_map(entry.metadata)[()][
                        "tap-doubleclick-campaign-manager.report-id"
                    ],
                    "stream_name": entry.tap_stream_id,
                    "stream_alias": entry.stream_alias,
                    "metadata": singer_metadata.to_map(entry.metadata),
                }
                for entry in catalog.streams
            ],
            key=lambda x: x["report_id"],
        )
        state = {
            "current_report": FLOODLIGHT_REPORT["id"],
            "reports": expected_reports,
        }
        sync_reports(MagicMock(), self.MOCK_CONFIG, catalog, state)
        synced_ids = [c[0][3]["report_id"] for c in mock_sr.call_args_list]
        self.assertNotIn(STANDARD_REPORT["id"], synced_ids)
        self.assertIn(FLOODLIGHT_REPORT["id"], synced_ids)
        self.assertIn(CROSS_DIMENSION_REACH_REPORT["id"], synced_ids)

    @patch("tap_doubleclick_campaign_manager.sync_reports.sync_report")
    @patch("tap_doubleclick_campaign_manager.sync_reports.get_field_type_lookup")
    @patch("singer.write_state")
    def test_all_five_report_types_synced(self, mock_ws, mock_lookup, mock_sr):
        """sync_reports must invoke sync_report for all five DCM report types."""
        mock_lookup.return_value = {}
        catalog = _make_catalog(self.MOCK_REPORTS, selected=True)
        sync_reports(MagicMock(), self.MOCK_CONFIG, catalog, self.state)
        self.assertEqual(mock_sr.call_count, 5)

    # ── Full pipeline: discover → sync ───────────────────────────────────

    def test_end_to_end_discover_then_sync_writes_records(self):
        """discover_streams() + sync_reports() end-to-end with fully mocked service."""
        from tap_doubleclick_campaign_manager.discover import discover_streams

        report = STANDARD_REPORT
        csv_bytes = self._csv_for_report(report, n_rows=2)

        service = build_mock_service(
            reports_pages=[[report]],
            report_details=report,
            file_status="REPORT_AVAILABLE",
        )

        # 1. Discover
        catalog_dict = discover_streams(service, self.MOCK_CONFIG)
        catalog = Catalog.from_dict(catalog_dict)

        # Select the stream
        for entry in catalog.streams:
            mdata_map = singer_metadata.to_map(entry.metadata)
            mdata_map[()]["selected"] = True
            entry.metadata = singer_metadata.to_list(mdata_map)

        # 2. Sync
        written = []
        with patch("tap_doubleclick_campaign_manager.sync_reports.http.MediaIoBaseDownload",
                   make_mock_downloader_class(csv_bytes)), \
             patch("singer.write_schema"), \
             patch("singer.write_state"), \
             patch("singer.write_record",
                   side_effect=lambda s, r, **kw: written.append(r)), \
             patch("singer.metrics.record_counter") as mc, \
             patch("singer.metrics.job_timer") as mt:

            mc.return_value.__enter__ = lambda s: s
            mc.return_value.__exit__ = MagicMock(return_value=False)
            mt.return_value.__enter__ = lambda s: s
            mt.return_value.__exit__ = MagicMock(return_value=False)

            sync_reports(service, self.MOCK_CONFIG, catalog, self.state)

        self.assertEqual(len(written), 2)
        for rec in written:
            self.assertIn(SINGER_REPORT_FIELD, rec)
            self.assertIn(REPORT_ID_FIELD, rec)

