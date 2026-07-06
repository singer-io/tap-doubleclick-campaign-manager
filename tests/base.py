"""Base test mixin for tap-doubleclick-campaign-manager mock integration tests.

Not a TestCase itself — mix with unittest.TestCase in each test file.
All Google API calls are patched at the service level; no live credentials required.

Mock report fixtures cover all five DCM report types:
  STANDARD, FLOODLIGHT, CROSS_DIMENSION_REACH, PATH_TO_CONVERSION, REACH
"""
from __future__ import annotations

import io
import unittest
from unittest.mock import MagicMock, patch

from singer.catalog import Catalog

from tap_doubleclick_campaign_manager.discover import discover_streams, sanitize_name
from tap_doubleclick_campaign_manager.schema import (
    SINGER_REPORT_FIELD,
    REPORT_ID_FIELD,
    get_field_type_lookup,
    get_fields,
    get_schema,
)

# ---------------------------------------------------------------------------
# Static mock report definitions (one per supported DCM report type)
# Payloads mirror the structure returned by reports.get / reports.list.
# ---------------------------------------------------------------------------

STANDARD_REPORT = {
    "id": 1001,
    "name": "Standard Report",
    "type": "STANDARD",
    "criteria": {
        "dimensions": [{"name": "date"}, {"name": "campaign"}],
        "metricNames": ["impressions", "clicks"],
    },
}

FLOODLIGHT_REPORT = {
    "id": 1002,
    "name": "Floodlight Report",
    "type": "FLOODLIGHT",
    "floodlightCriteria": {
        "dimensions": [{"name": "date"}, {"name": "activity"}],
        "metricNames": ["totalConversions"],
    },
}

CROSS_DIMENSION_REACH_REPORT = {
    "id": 1003,
    "name": "Cross Dimension Reach Report",
    "type": "CROSS_DIMENSION_REACH",
    "crossDimensionReachCriteria": {
        "breakdown": [{"name": "date"}],
        "metricNames": ["uniqueReachTotalReach"],
        "overlapMetricNames": ["uniqueReachIncrementalReach"],
    },
}

PATH_TO_CONVERSION_REPORT = {
    "id": 1004,
    "name": "Path To Conversion Report",
    "type": "PATH_TO_CONVERSION",
    "pathToConversionCriteria": {
        "conversionDimensions": [{"name": "date"}],
        "perInteractionDimensions": [{"name": "campaign"}],
        "customFloodlightVariables": [],
        "metricNames": ["totalConversions"],
    },
}

REACH_REPORT = {
    "id": 1005,
    "name": "Reach Report",
    "type": "REACH",
    "reachCriteria": {
        "dimensions": [{"name": "date"}],
        "metricNames": ["uniqueReachTotalReach"],
        "reachByFrequencyMetricNames": ["reachByFrequency"],
    },
}

ALL_MOCK_REPORTS = [
    STANDARD_REPORT,
    FLOODLIGHT_REPORT,
    CROSS_DIMENSION_REACH_REPORT,
    PATH_TO_CONVERSION_REPORT,
    REACH_REPORT,
]

# Mapping from a report dict to its expected tap_stream_id
def _expected_tap_stream_id(report):
    stream_name = sanitize_name(report["name"])
    return f"{stream_name}_{report['id']}"

EXPECTED_TAP_STREAM_IDS = {_expected_tap_stream_id(r) for r in ALL_MOCK_REPORTS}

# Expected stream-name → report mapping (tap_stream_id → report)
REPORT_BY_TAP_STREAM_ID = {_expected_tap_stream_id(r): r for r in ALL_MOCK_REPORTS}


# ---------------------------------------------------------------------------
# CSV helpers
# ---------------------------------------------------------------------------

def build_dcm_csv(headers: list[str], rows: list[list]) -> bytes:
    """Return mock DCM CSV bytes in the format expected by process_file().

    Format:
        Report Fields
        col1,col2,...
        val1,val2,...
        Grand Total:,...
    """
    lines = ["Report Fields", ",".join(headers)]
    for row in rows:
        lines.append(",".join(str(v) for v in row))
    lines.append("Grand Total:" + "," * (len(headers) - 1))
    return ("\n".join(lines) + "\n").encode("utf-8")


def get_report_headers(report: dict) -> list[str]:
    """Return the ordered list of column names for a report (dimensions + metrics)."""
    field_type_lookup = get_field_type_lookup()
    fieldmap = get_fields(field_type_lookup, report)
    return [f["name"] for f in fieldmap]


def build_sample_rows_for_report(report: dict, n: int = 2) -> list[list]:
    """
    Build *n* sample data rows whose values match the report's field types.

    Integers → 100, 200, …; date-like strings → RFC3339 UTC values.
    """
    field_type_lookup = get_field_type_lookup()
    fieldmap = get_fields(field_type_lookup, report)
    rows = []
    for i in range(n):
        row = []
        for field in fieldmap:
            ft = field["type"]
            if isinstance(ft, list):
                ft = [t for t in ft if t != "null"][0] if ft else "string"
            if ft in ("long", "integer"):
                row.append(str((i + 1) * 100))
            elif ft in ("double", "number"):
                row.append(f"{(i + 1) * 1.5:.1f}")
            elif ft == "boolean":
                row.append("true" if i % 2 == 0 else "false")
            else:
                # Use RFC3339 for date-like columns to match checklist expectations.
                field_name = field["name"]
                if (
                    field_name.endswith("_time")
                    or field_name.endswith("_date")
                    or "date" in field_name
                    or "time" in field_name
                ):
                    row.append(f"2024-01-{i + 1:02d}T00:00:00Z")
                else:
                    row.append(f"value_{i + 1}")
        rows.append(row)
    return rows


def make_mock_downloader_class(csv_bytes: bytes):
    """
    Return a mock MediaIoBaseDownload class that writes *csv_bytes* to the
    stream fd when next_chunk() is first called.

    Usage::

        with patch("tap_doubleclick_campaign_manager.sync_reports.http.MediaIoBaseDownload",
                   make_mock_downloader_class(csv_bytes)):
            ...
    """
    class _MockDownloader:
        def __init__(self, fd, request, chunksize=None):
            self._fd = fd
            self._done = False

        def next_chunk(self):
            if not self._done:
                self._fd.write(csv_bytes)
                self._done = True
            return (MagicMock(), True)

    return _MockDownloader


# ---------------------------------------------------------------------------
# Mock Google API service builder
# ---------------------------------------------------------------------------

def build_mock_service(
    reports_pages: list[list[dict]] | None = None,
    report_details: dict | None = None,
    file_status: str = "REPORT_AVAILABLE",
    file_id: str = "mock_file_id_1",
) -> MagicMock:
    """
    Build a MagicMock Google API service with pre-configured responses.

    Parameters
    ----------
    reports_pages:
        List of pages; each page is a list of report dicts.  For single-page
        results pass ``[[r1, r2, ...]]``.  Subsequent pages automatically
        receive a ``nextPageToken`` in the mocked response so that
        ``discover_streams`` keeps fetching.
    report_details:
        What ``service.reports().get().execute()`` returns.  Defaults to the
        first report in *reports_pages*.
    file_status:
        Status returned by ``service.files().get().execute()``.
    file_id:
        File id returned by ``service.reports().run().execute()``.
    """
    service = MagicMock()

    # ── reports().list() (multi-page aware) ─────────────────────────────
    if reports_pages is not None:
        execute_responses = []
        for idx, page in enumerate(reports_pages):
            resp = {"items": page}
            if idx < len(reports_pages) - 1:
                resp["nextPageToken"] = f"page_token_{idx}"
            execute_responses.append(resp)
        service.reports.return_value.list.return_value.execute.side_effect = (
            execute_responses
        )

    # ── reports().get() ──────────────────────────────────────────────────
    if report_details is not None:
        service.reports.return_value.get.return_value.execute.return_value = (
            report_details
        )

    # ── reports().run() ──────────────────────────────────────────────────
    service.reports.return_value.run.return_value.execute.return_value = {
        "id": file_id
    }

    # ── files().get() (status polling) ───────────────────────────────────
    service.files.return_value.get.return_value.execute.return_value = {
        "status": file_status,
        "id": file_id,
    }

    # ── files().get_media() ───────────────────────────────────────────────
    # Returns a plain MagicMock; the MediaIoBaseDownload is patched in tests.
    service.files.return_value.get_media.return_value = MagicMock()

    return service


# ---------------------------------------------------------------------------
# Catalog builder
# ---------------------------------------------------------------------------

def build_catalog_from_reports(reports: list[dict], selected: bool = True) -> Catalog:
    """
    Build a Singer Catalog from a list of mock report dicts — identical to
    what discover_streams() would produce for those reports.
    """
    mock_service = build_mock_service(reports_pages=[reports])
    mock_config = {"profile_id": "12345"}
    catalog_dict = discover_streams(mock_service, mock_config)
    catalog = Catalog.from_dict(catalog_dict)

    if selected:
        import singer.metadata as meta_mod
        for entry in catalog.streams:
            mdata_map = meta_mod.to_map(entry.metadata)
            mdata_map[()]["selected"] = True
            entry.metadata = meta_mod.to_list(mdata_map)

    return catalog


# ---------------------------------------------------------------------------
# Base mixin
# ---------------------------------------------------------------------------

class DcmBaseTest:
    """Base test mixin for tap-doubleclick-campaign-manager mock integration tests.

    Not a TestCase itself — mix with unittest.TestCase in each test file.
    All Google API calls are patched at the service level; no live credentials required.
    """

    # ── Metadata constants ───────────────────────────────────────────────
    PRIMARY_KEYS = "primary_keys"
    REPLICATION_METHOD = "replication_method"
    REPLICATION_KEYS = "replication_keys"
    OBEYS_START_DATE = "obeys_start_date"

    FULL_TABLE = "FULL_TABLE"

    default_start_date = "2024-01-01T00:00:00Z"

    MOCK_PROFILE_ID = "12345"
    MOCK_CONFIG = {
        "client_id": "mock-client-id",
        "client_secret": "mock-client-secret",
        "refresh_token": "mock-refresh-token",
        "profile_id": MOCK_PROFILE_ID,
    }

    # All five DCM report types surfaced as mock fixtures
    MOCK_REPORTS = ALL_MOCK_REPORTS
    REPORT_BY_ID = {r["id"]: r for r in ALL_MOCK_REPORTS}

    # ── Stream metadata ──────────────────────────────────────────────────

    @classmethod
    def expected_metadata(cls):
        """Expected streams and metadata — all DCM report streams are FULL_TABLE."""
        return {
            _expected_tap_stream_id(r): {
                cls.PRIMARY_KEYS: set(),
                cls.REPLICATION_METHOD: cls.FULL_TABLE,
                cls.REPLICATION_KEYS: set(),
                cls.OBEYS_START_DATE: False,
            }
            for r in ALL_MOCK_REPORTS
        }

    @classmethod
    def expected_stream_names(cls):
        """Return the set of all expected tap_stream_ids."""
        return set(cls.expected_metadata().keys())

    @classmethod
    def expected_tap_stream_ids(cls) -> set[str]:
        return EXPECTED_TAP_STREAM_IDS

    @classmethod
    def expected_stream_count(cls) -> int:
        return len(cls.MOCK_REPORTS)

    @classmethod
    def full_table_streams(cls):
        """Return all streams that use FULL_TABLE replication (all DCM streams)."""
        return {
            s for s, m in cls.expected_metadata().items()
            if m[cls.REPLICATION_METHOD] == cls.FULL_TABLE
        }

    # ── Setup / teardown ────────────────────────────────────────────────

    def setUp(self):
        """Set up test fixtures with dummy config and empty state."""
        self.config = self.get_mock_config()
        self.state = {}

    def tearDown(self):
        """Clean up after tests."""
        pass

    # ── Config helpers ───────────────────────────────────────────────────

    @staticmethod
    def get_mock_config():
        """Return mock configuration with dummy values — no real credentials."""
        return {
            "client_id": "mock-client-id",
            "client_secret": "mock-client-secret",
            "refresh_token": "mock-refresh-token",
            "profile_id": "12345",
        }

    @staticmethod
    def get_mock_state():
        """Return initial mock state."""
        return {}

    # ── Service + catalog helpers ────────────────────────────────────────

    @classmethod
    def _build_service(cls, reports=None, report_details=None,
                       file_status="REPORT_AVAILABLE"):
        """Return a mock service pre-loaded with *reports* (defaults to all 5)."""
        reports = reports if reports is not None else cls.MOCK_REPORTS
        return build_mock_service(
            reports_pages=[reports],
            report_details=report_details,
            file_status=file_status,
        )

    @classmethod
    def _discover(cls, reports=None):
        """Run discover_streams() against a mocked service; return catalog dict."""
        service = cls._build_service(reports=reports)
        return discover_streams(service, cls.MOCK_CONFIG)

    @classmethod
    def _build_catalog(cls, reports=None, selected=True) -> Catalog:
        return build_catalog_from_reports(
            reports if reports is not None else cls.MOCK_REPORTS,
            selected=selected,
        )

    # ── CSV / sync helpers ───────────────────────────────────────────────

    @staticmethod
    def _csv_for_report(report: dict, n_rows: int = 2) -> bytes:
        headers = get_report_headers(report)
        rows = build_sample_rows_for_report(report, n=n_rows)
        return build_dcm_csv(headers, rows)

    @staticmethod
    def _downloader_class(csv_bytes: bytes):
        return make_mock_downloader_class(csv_bytes)

    def _run_sync_for_report(self, report: dict, n_rows: int = 2):
        """
        Full-stack mock sync of a single report.

        Returns the list of Singer records written by singer.write_record.
        """
        import singer.metrics
        from tap_doubleclick_campaign_manager.sync_reports import sync_report

        csv_bytes = self._csv_for_report(report, n_rows=n_rows)
        field_type_lookup = get_field_type_lookup()

        service = build_mock_service(
            reports_pages=[[report]],
            report_details=report,
            file_status="REPORT_AVAILABLE",
        )

        report_config = {
            "report_id": report["id"],
            "stream_name": _expected_tap_stream_id(report),
            "stream_alias": _expected_tap_stream_id(report),
            "metadata": {},
        }

        written = []

        with patch("tap_doubleclick_campaign_manager.sync_reports.http.MediaIoBaseDownload",
                   make_mock_downloader_class(csv_bytes)), \
             patch("singer.write_schema"), \
             patch("singer.write_state"), \
             patch("singer.write_record",
                   side_effect=lambda s, r, **kw: written.append(r)), \
             patch("singer.metrics.record_counter") as mock_counter, \
             patch("singer.metrics.job_timer") as mock_timer:

            mock_counter.return_value.__enter__ = lambda s: s
            mock_counter.return_value.__exit__ = MagicMock(return_value=False)
            mock_timer.return_value.__enter__ = lambda s: s
            mock_timer.return_value.__exit__ = MagicMock(return_value=False)

            sync_report(service, field_type_lookup, self.MOCK_PROFILE_ID, report_config)

        return written
