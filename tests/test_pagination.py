"""Mock integration tests — pagination for tap-doubleclick-campaign-manager.

Verifies that discover_streams() correctly fetches ALL pages from the
reports.list endpoint when the API returns a nextPageToken, and that every
report from every page is registered as a stream in the final catalog.

DCM's reports.list API uses cursor-based pagination (nextPageToken / pageToken).
The current implementation (discover.py) has been updated to page through all
results.
"""
import unittest
from unittest.mock import MagicMock

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


class DcmPaginationTest(DcmBaseTest, unittest.TestCase):
    """
    Verify discover_streams() pages through all available reports via
    nextPageToken, building a complete catalog.
    """

    # ── Two-page discovery ────────────────────────────────────────────────

    def test_two_page_discovery_returns_all_streams(self):
        """Reports split across 2 pages must all appear in the catalog."""
        page1 = [STANDARD_REPORT, FLOODLIGHT_REPORT]
        page2 = [CROSS_DIMENSION_REACH_REPORT]

        service = build_mock_service(reports_pages=[page1, page2])
        catalog_dict = discover_streams(service, self.MOCK_CONFIG)

        discovered = {s["tap_stream_id"] for s in catalog_dict["streams"]}
        expected = {
            _expected_tap_stream_id(STANDARD_REPORT),
            _expected_tap_stream_id(FLOODLIGHT_REPORT),
            _expected_tap_stream_id(CROSS_DIMENSION_REACH_REPORT),
        }
        self.assertEqual(discovered, expected)

    def test_two_page_discovery_stream_count(self):
        """Catalog must contain exactly 3 entries when 3 reports span 2 pages."""
        page1 = [STANDARD_REPORT, FLOODLIGHT_REPORT]
        page2 = [CROSS_DIMENSION_REACH_REPORT]

        service = build_mock_service(reports_pages=[page1, page2])
        catalog_dict = discover_streams(service, self.MOCK_CONFIG)

        self.assertEqual(len(catalog_dict["streams"]), 3)

    def test_two_page_discovery_calls_list_twice(self):
        """Two pages must result in exactly 2 calls to list().execute()."""
        page1 = [STANDARD_REPORT]
        page2 = [FLOODLIGHT_REPORT]

        service = build_mock_service(reports_pages=[page1, page2])
        discover_streams(service, self.MOCK_CONFIG)

        execute_mock = service.reports.return_value.list.return_value.execute
        self.assertEqual(execute_mock.call_count, 2)

    # ── Three-page discovery ──────────────────────────────────────────────

    def test_three_page_discovery_returns_all_five_streams(self):
        """Reports split across 3 pages — all five must appear in catalog."""
        page1 = [STANDARD_REPORT, FLOODLIGHT_REPORT]
        page2 = [CROSS_DIMENSION_REACH_REPORT, PATH_TO_CONVERSION_REPORT]
        page3 = [REACH_REPORT]

        service = build_mock_service(reports_pages=[page1, page2, page3])
        catalog_dict = discover_streams(service, self.MOCK_CONFIG)

        discovered = {s["tap_stream_id"] for s in catalog_dict["streams"]}
        self.assertEqual(discovered, self.expected_tap_stream_ids())

    def test_three_page_discovery_calls_list_three_times(self):
        """Three pages must result in exactly 3 calls to list().execute()."""
        page1 = [STANDARD_REPORT]
        page2 = [FLOODLIGHT_REPORT]
        page3 = [CROSS_DIMENSION_REACH_REPORT]

        service = build_mock_service(reports_pages=[page1, page2, page3])
        discover_streams(service, self.MOCK_CONFIG)

        execute_mock = service.reports.return_value.list.return_value.execute
        self.assertEqual(execute_mock.call_count, 3)

    # ── Single-page (no pagination) ───────────────────────────────────────

    def test_single_page_calls_list_once(self):
        """When there is no nextPageToken one call to list().execute() is enough."""
        service = build_mock_service(reports_pages=[[STANDARD_REPORT]])
        discover_streams(service, self.MOCK_CONFIG)

        execute_mock = service.reports.return_value.list.return_value.execute
        self.assertEqual(execute_mock.call_count, 1)

    def test_single_page_discovery_returns_correct_stream(self):
        """Single-page discovery must still build the correct catalog entry."""
        service = build_mock_service(reports_pages=[[STANDARD_REPORT]])
        catalog_dict = discover_streams(service, self.MOCK_CONFIG)

        self.assertEqual(len(catalog_dict["streams"]), 1)
        self.assertEqual(
            catalog_dict["streams"][0]["tap_stream_id"],
            _expected_tap_stream_id(STANDARD_REPORT),
        )

    # ── Empty-page handling ───────────────────────────────────────────────

    def test_empty_first_page_returns_empty_catalog(self):
        """An empty first page with no nextPageToken produces an empty catalog."""
        service = build_mock_service(reports_pages=[[]])
        catalog_dict = discover_streams(service, self.MOCK_CONFIG)
        self.assertEqual(len(catalog_dict["streams"]), 0)

    def test_page_with_no_items_key_returns_empty(self):
        """A response dict missing the 'items' key is handled gracefully."""
        service = MagicMock()
        # Simulate a page with no 'items' key (empty response)
        service.reports.return_value.list.return_value.execute.return_value = {}
        catalog_dict = discover_streams(service, self.MOCK_CONFIG)
        self.assertEqual(len(catalog_dict["streams"]), 0)

    # ── Sorting preserved across pages ───────────────────────────────────

    def test_streams_sorted_by_report_id_across_pages(self):
        """Streams must be globally sorted by report id even when split across pages."""
        # Deliberately put the higher-id report on page 1
        page1 = [FLOODLIGHT_REPORT]   # id=1002
        page2 = [STANDARD_REPORT]     # id=1001 — lower id on page 2

        service = build_mock_service(reports_pages=[page1, page2])
        catalog_dict = discover_streams(service, self.MOCK_CONFIG)

        ids_in_order = [s["tap_stream_id"] for s in catalog_dict["streams"]]
        numeric_ids = [int(sid.rsplit("_", 1)[-1]) for sid in ids_in_order]
        self.assertEqual(numeric_ids, sorted(numeric_ids))

    # ── nextPageToken pass-through ────────────────────────────────────────

    def test_next_page_token_passed_to_second_request(self):
        """The pageToken returned on page 1 must be forwarded to the page 2 request."""
        page1 = [STANDARD_REPORT]
        page2 = [FLOODLIGHT_REPORT]
        token = "abc_test_token"

        service = MagicMock()
        execute_mock = service.reports.return_value.list.return_value.execute
        execute_mock.side_effect = [
            {"items": page1, "nextPageToken": token},
            {"items": page2},
        ]

        discover_streams(service, self.MOCK_CONFIG)

        # Verify list() was called twice
        list_mock = service.reports.return_value.list
        self.assertEqual(list_mock.call_count, 2)

        # Second call must include pageToken
        second_call_kwargs = list_mock.call_args_list[1][1]
        self.assertIn("pageToken", second_call_kwargs,
                      msg="Second call to list() must include pageToken kwarg")
        self.assertEqual(second_call_kwargs["pageToken"], token)

    def test_first_request_does_not_include_page_token(self):
        """The first request to list() must NOT include a pageToken."""
        service = build_mock_service(reports_pages=[[STANDARD_REPORT]])
        discover_streams(service, self.MOCK_CONFIG)

        list_mock = service.reports.return_value.list
        first_call_kwargs = list_mock.call_args_list[0][1]
        self.assertNotIn("pageToken", first_call_kwargs)

    # ── Large page simulation ─────────────────────────────────────────────

    def test_many_reports_on_single_page(self):
        """A single page with 20 reports produces exactly 20 catalog entries."""
        reports = []
        for i in range(20):
            reports.append({
                "id": 2000 + i,
                "name": f"Bulk Report {i:02d}",
                "type": "STANDARD",
                "criteria": {
                    "dimensions": [{"name": "date"}],
                    "metricNames": ["impressions"],
                },
            })
        service = build_mock_service(reports_pages=[reports])
        catalog_dict = discover_streams(service, self.MOCK_CONFIG)
        self.assertEqual(len(catalog_dict["streams"]), 20)

    def test_reports_spread_across_five_single_item_pages(self):
        """5 pages of 1 report each must produce exactly 5 catalog entries."""
        pages = [[r] for r in self.MOCK_REPORTS]
        service = build_mock_service(reports_pages=pages)
        catalog_dict = discover_streams(service, self.MOCK_CONFIG)
        self.assertEqual(len(catalog_dict["streams"]), 5)

        execute_mock = service.reports.return_value.list.return_value.execute
        self.assertEqual(execute_mock.call_count, 5)

    # ── Duplicate report names across pages don't collide ─────────────────

    def test_duplicate_name_reports_get_unique_tap_stream_ids(self):
        """Two reports with the same name but different IDs must produce 2 streams."""
        report_a = {
            "id": 5001,
            "name": "Sales Report",
            "type": "STANDARD",
            "criteria": {
                "dimensions": [{"name": "date"}],
                "metricNames": ["impressions"],
            },
        }
        report_b = {
            "id": 5002,
            "name": "Sales Report",
            "type": "STANDARD",
            "criteria": {
                "dimensions": [{"name": "date"}],
                "metricNames": ["clicks"],
            },
        }
        service = build_mock_service(reports_pages=[[report_a], [report_b]])
        catalog_dict = discover_streams(service, self.MOCK_CONFIG)

        tap_stream_ids = [s["tap_stream_id"] for s in catalog_dict["streams"]]
        self.assertEqual(len(set(tap_stream_ids)), 2,
                         msg="Same-name reports must have distinct tap_stream_ids")
        self.assertIn("sales_report_5001", tap_stream_ids)
        self.assertIn("sales_report_5002", tap_stream_ids)

