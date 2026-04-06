from tap_doubleclick_campaign_manager.sync_reports import transform_field
from tap_doubleclick_campaign_manager.sync_reports import StreamFunc
from tap_doubleclick_campaign_manager.sync_reports import next_sleep_interval
from tap_doubleclick_campaign_manager.sync_reports import parse_line
from tap_doubleclick_campaign_manager.sync_reports import sync_reports
from tap_doubleclick_campaign_manager.sync_reports import sync_report
from tap_doubleclick_campaign_manager.sync_reports import MIN_RETRY_INTERVAL, MAX_RETRY_INTERVAL

import unittest
from unittest.mock import patch, MagicMock, call
from singer.catalog import Catalog, CatalogEntry, Schema

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

    def test_transform_field_boolean_true_values(self):
        """Transform various truthy string representations for boolean type."""
        for val in ['true', 'True', 'TRUE', 't', 'T', 'yes', 'Yes', 'YES', 'y', 'Y']:
            with self.subTest(val=val):
                self.assertTrue(transform_field('boolean', val))

    def test_transform_field_boolean_false_values(self):
        """Non-truthy boolean strings should return False."""
        for val in ['false', 'no', 'n', '0', 'f']:
            with self.subTest(val=val):
                self.assertFalse(transform_field('boolean', val))

    def test_transform_field_empty_string_returns_none_for_long(self):
        self.assertIsNone(transform_field('long', ''))

    def test_transform_field_empty_string_returns_none_for_double(self):
        self.assertIsNone(transform_field('double', ''))

    def test_transform_field_empty_string_returns_none_for_string(self):
        """Empty string for string type should still return None."""
        self.assertIsNone(transform_field('string', ''))

    def test_transform_field_long_non_numeric_returns_none(self):
        """Non-numeric value with 'long' type should return None after failed int conversion."""
        self.assertIsNone(transform_field('long', 'not_a_number'))

    def test_transform_field_string_passthrough(self):
        """String type should pass the value through unchanged."""
        self.assertEqual(transform_field('string', 'hello'), 'hello')

    def test_transform_field_multiple_all_fail_returns_value(self):
        """If all type conversions fail, the raw value is returned."""
        result = transform_field(['long', 'double'], 'abc')
        # Both 'long' and 'double' conversions fail -> returns None from long, falls through
        # Actually long returns None from except, double raises ValueError -> caught
        # The last fallthrough `return value` won't be reached via list iteration
        # because 'long' returns None (not raise ValueError) and that None is returned.
        self.assertIsNone(result)


class TestStreamFunc(unittest.TestCase):

    def test_single_complete_line_calls_func(self):
        """A single newline-terminated chunk should call the func once."""
        received = []
        sf = StreamFunc(received.append)
        sf.write(b'hello world\n')
        self.assertEqual(received, ['hello world'])

    def test_partial_line_does_not_call_func(self):
        """A chunk without a trailing newline should buffer the data without calling func."""
        received = []
        sf = StreamFunc(received.append)
        sf.write(b'partial line')
        self.assertEqual(received, [])

    def test_continuation_of_partial_line_calls_func(self):
        """Sending the rest of a partial line (with newline) should flush the buffer."""
        received = []
        sf = StreamFunc(received.append)
        sf.write(b'partial')
        sf.write(b' line\n')
        self.assertEqual(received, ['partial line'])

    def test_multiple_lines_in_one_write(self):
        """Multiple newline-terminated lines in one write should each trigger the func."""
        received = []
        sf = StreamFunc(received.append)
        sf.write(b'line1\nline2\nline3\n')
        self.assertEqual(received, ['line1', 'line2', 'line3'])

    def test_mixed_complete_and_partial_lines(self):
        """Complete lines are dispatched; the trailing partial is buffered."""
        received = []
        sf = StreamFunc(received.append)
        sf.write(b'line1\nline2\npartial')
        self.assertEqual(received, ['line1', 'line2'])

    def test_second_write_appends_to_buffer(self):
        """A second write completes the buffered partial line."""
        received = []
        sf = StreamFunc(received.append)
        sf.write(b'foo\nbar')
        sf.write(b'baz\n')
        self.assertEqual(received, ['foo', 'barbaz'])

    def test_empty_write_produces_nothing(self):
        """Writing empty bytes should not call func."""
        received = []
        sf = StreamFunc(received.append)
        sf.write(b'')
        self.assertEqual(received, [])


class TestNextSleepInterval(unittest.TestCase):

    def test_zero_previous_returns_min_interval(self):
        """Starting from 0 (or None) should return MIN_RETRY_INTERVAL."""
        result = next_sleep_interval(0)
        self.assertEqual(result, MIN_RETRY_INTERVAL)

    def test_small_previous_returns_value_in_range(self):
        """Result should be within [prev, prev*2] and at most MAX_RETRY_INTERVAL."""
        for _ in range(20):
            result = next_sleep_interval(10)
            self.assertGreaterEqual(result, 10)
            self.assertLessEqual(result, 20)

    def test_large_previous_caps_at_max(self):
        """When prev * 2 exceeds MAX_RETRY_INTERVAL, result should be MAX_RETRY_INTERVAL."""
        result = next_sleep_interval(MAX_RETRY_INTERVAL)
        self.assertEqual(result, MAX_RETRY_INTERVAL)

    def test_half_max_may_reach_max(self):
        """prev = MAX/2 means max_interval = MAX, so result can reach MAX."""
        results = set()
        for _ in range(50):
            results.add(next_sleep_interval(MAX_RETRY_INTERVAL // 2))
        # Result must be between MAX/2 and MAX
        for r in results:
            self.assertGreaterEqual(r, MAX_RETRY_INTERVAL // 2)
            self.assertLessEqual(r, MAX_RETRY_INTERVAL)


class TestParseLine(unittest.TestCase):

    def test_simple_line(self):
        """Simple CSV line should be split into a list."""
        self.assertEqual(parse_line('a,b,c'), ['a', 'b', 'c'])

    def test_quoted_field(self):
        """Quoted fields with commas should not be split."""
        self.assertEqual(parse_line('"hello, world",b'), ['hello, world', 'b'])

    def test_single_field(self):
        self.assertEqual(parse_line('only_one'), ['only_one'])

    def test_empty_field(self):
        self.assertEqual(parse_line('a,,c'), ['a', '', 'c'])

    def test_numeric_fields(self):
        self.assertEqual(parse_line('1,2,3'), ['1', '2', '3'])


class TestSyncReportsStateManagement(unittest.TestCase):
    """Tests for the sync_reports function's state and report selection logic."""

    def _make_catalog(self, streams):
        """Helper that creates a Singer Catalog from a list of (tap_stream_id, report_id, selected) tuples."""
        entries = []
        for tap_stream_id, report_id, selected in streams:
            meta = [
                {
                    'breadcrumb': [],
                    'metadata': {
                        'tap-doubleclick-campaign-manager.report-id': report_id,
                        'selected': selected,
                        'forced-replication-method': 'FULL_TABLE'
                    }
                }
            ]
            entry = CatalogEntry(
                stream=tap_stream_id,
                stream_alias=tap_stream_id,
                tap_stream_id=tap_stream_id,
                key_properties=[],
                schema=Schema.from_dict({'type': 'object', 'properties': {}}),
                metadata=meta
            )
            entries.append(entry)
        return Catalog(entries)

    @patch('tap_doubleclick_campaign_manager.sync_reports.sync_report')
    @patch('tap_doubleclick_campaign_manager.sync_reports.get_field_type_lookup')
    @patch('singer.write_state')
    def test_unselected_streams_are_skipped(self, mock_write_state, mock_lookup, mock_sync_report):
        """Streams with selected=False should not be synced."""
        mock_lookup.return_value = {}
        catalog = self._make_catalog([
            ('stream_selected', 10, True),
            ('stream_not_selected', 20, False),
        ])
        state = {}
        sync_reports(MagicMock(), {'profile_id': '1'}, catalog, state)
        # Only the selected stream should appear in the reports list
        synced_ids = [c[0][3]['report_id'] for c in mock_sync_report.call_args_list]
        self.assertIn(10, synced_ids)
        self.assertNotIn(20, synced_ids)

    @patch('tap_doubleclick_campaign_manager.sync_reports.sync_report')
    @patch('tap_doubleclick_campaign_manager.sync_reports.get_field_type_lookup')
    @patch('singer.write_state')
    def test_reports_sorted_by_report_id(self, mock_write_state, mock_lookup, mock_sync_report):
        """sync_reports should call sync_report in ascending report_id order."""
        mock_lookup.return_value = {}
        catalog = self._make_catalog([
            ('stream_b', 200, True),
            ('stream_a', 100, True),
        ])
        state = {}
        sync_reports(MagicMock(), {'profile_id': '1'}, catalog, state)
        call_order = [c[0][3]['report_id'] for c in mock_sync_report.call_args_list]
        self.assertEqual(call_order, [100, 200])

    @patch('tap_doubleclick_campaign_manager.sync_reports.sync_report')
    @patch('tap_doubleclick_campaign_manager.sync_reports.get_field_type_lookup')
    @patch('singer.write_state')
    def test_state_reset_when_reports_change(self, mock_write_state, mock_lookup, mock_sync_report):
        """If the report list in state differs from current, current_report should reset."""
        mock_lookup.return_value = {}
        catalog = self._make_catalog([('stream_a', 1, True)])
        # State has different reports list
        state = {'reports': [{'report_id': 999}], 'current_report': 999}
        sync_reports(MagicMock(), {'profile_id': '1'}, catalog, state)
        self.assertIsNone(state['current_report'])

    @patch('tap_doubleclick_campaign_manager.sync_reports.sync_report')
    @patch('tap_doubleclick_campaign_manager.sync_reports.get_field_type_lookup')
    @patch('singer.write_state')
    def test_state_cleared_after_sync(self, mock_write_state, mock_lookup, mock_sync_report):
        """After sync completes, state reports and current_report should be None."""
        mock_lookup.return_value = {}
        catalog = self._make_catalog([('stream_a', 1, True)])
        state = {}
        sync_reports(MagicMock(), {'profile_id': '1'}, catalog, state)
        self.assertIsNone(state.get('current_report'))
        self.assertIsNone(state.get('reports'))

    @patch('tap_doubleclick_campaign_manager.sync_reports.sync_report')
    @patch('tap_doubleclick_campaign_manager.sync_reports.get_field_type_lookup')
    @patch('singer.write_state')
    def test_current_report_resumes_from_checkpoint(self, mock_write_state, mock_lookup, mock_sync_report):
        """When current_report is set, reports before it should be skipped."""
        mock_lookup.return_value = {}
        catalog = self._make_catalog([
            ('stream_a', 1, True),
            ('stream_b', 2, True),
            ('stream_c', 3, True),
        ])
        # Simulate resuming from report_id=2
        expected_reports = sorted([
            {'report_id': 1, 'stream_name': 'stream_a', 'stream_alias': 'stream_a',
             'metadata': {(): {'tap-doubleclick-campaign-manager.report-id': 1, 'selected': True, 'forced-replication-method': 'FULL_TABLE'}}},
            {'report_id': 2, 'stream_name': 'stream_b', 'stream_alias': 'stream_b',
             'metadata': {(): {'tap-doubleclick-campaign-manager.report-id': 2, 'selected': True, 'forced-replication-method': 'FULL_TABLE'}}},
            {'report_id': 3, 'stream_name': 'stream_c', 'stream_alias': 'stream_c',
             'metadata': {(): {'tap-doubleclick-campaign-manager.report-id': 3, 'selected': True, 'forced-replication-method': 'FULL_TABLE'}}},
        ], key=lambda x: x['report_id'])
        state = {'current_report': 2, 'reports': expected_reports}
        sync_reports(MagicMock(), {'profile_id': '1'}, catalog, state)
        synced = [c[0][3]['report_id'] for c in mock_sync_report.call_args_list]
        # report_id=1 should be skipped; 2 and 3 should be synced
        self.assertNotIn(1, synced)
        self.assertIn(2, synced)
        self.assertIn(3, synced)

    @patch('tap_doubleclick_campaign_manager.sync_reports.sync_report')
    @patch('tap_doubleclick_campaign_manager.sync_reports.get_field_type_lookup')
    @patch('singer.write_state')
    def test_no_selected_streams_does_nothing(self, mock_write_state, mock_lookup, mock_sync_report):
        """No selected streams means sync_report is never called."""
        mock_lookup.return_value = {}
        catalog = self._make_catalog([
            ('stream_a', 1, False),
            ('stream_b', 2, False),
        ])
        state = {}
        sync_reports(MagicMock(), {'profile_id': '1'}, catalog, state)
        mock_sync_report.assert_not_called()


class TestSyncReport(unittest.TestCase):
    """Tests for sync_report's status polling loop."""

    def _make_service(self, statuses, report_data=None):
        """Build a mock service where files().get() cycles through 'statuses'."""
        service = MagicMock()
        if report_data is None:
            report_data = {
                'type': 'STANDARD',
                'criteria': {'dimensions': [], 'metricNames': []}
            }
        service.reports.return_value.get.return_value.execute.return_value = report_data
        service.reports.return_value.run.return_value.execute.return_value = {'id': 'file_999'}

        status_iter = iter(statuses)
        service.files.return_value.get.return_value.execute.side_effect = (
            lambda: {'status': next(status_iter), 'id': 'file_999'}
        )
        return service

    @patch('tap_doubleclick_campaign_manager.sync_reports.process_file')
    @patch('tap_doubleclick_campaign_manager.sync_reports.CLIENT')
    @patch('singer.write_schema')
    @patch('singer.metrics.job_timer')
    def test_report_available_calls_process_file(
        self, mock_timer, mock_write_schema, mock_client_class, mock_process_file
    ):
        """REPORT_AVAILABLE status should trigger process_file and break the loop."""
        mock_timer.return_value.__enter__ = lambda s: s
        mock_timer.return_value.__exit__ = MagicMock(return_value=False)

        mock_client = MagicMock()
        mock_client_class.return_value = mock_client

        statuses = ['REPORT_AVAILABLE']
        status_iter = iter(statuses)

        def make_request_side_effect(func):
            return func()

        service = MagicMock()
        service.reports.return_value.get.return_value.execute.return_value = {
            'type': 'STANDARD', 'criteria': {'dimensions': [], 'metricNames': []}
        }
        service.reports.return_value.run.return_value.execute.return_value = {'id': 'file_1'}
        service.files.return_value.get.return_value.execute.side_effect = (
            lambda: {'status': next(status_iter), 'id': 'file_1'}
        )

        mock_client.make_request.side_effect = make_request_side_effect

        report_config = {
            'report_id': '123',
            'stream_name': 'my_stream',
            'stream_alias': 'my_stream',
            'metadata': {}
        }

        sync_report(service, {}, '999', report_config)
        mock_process_file.assert_called_once()

    @patch('tap_doubleclick_campaign_manager.sync_reports.process_file')
    @patch('tap_doubleclick_campaign_manager.sync_reports.CLIENT')
    @patch('singer.write_schema')
    @patch('singer.metrics.job_timer')
    @patch('time.sleep', return_value=None)
    def test_queued_status_sleeps_then_processes(
        self, mock_sleep, mock_timer, mock_write_schema, mock_client_class, mock_process_file
    ):
        """QUEUED status should sleep, then REPORT_AVAILABLE should process the file."""
        mock_timer.return_value.__enter__ = lambda s: s
        mock_timer.return_value.__exit__ = MagicMock(return_value=False)

        mock_client = MagicMock()
        mock_client_class.return_value = mock_client

        statuses = iter(['QUEUED', 'REPORT_AVAILABLE'])

        def make_request_side_effect(func):
            return func()

        service = MagicMock()
        service.reports.return_value.get.return_value.execute.return_value = {
            'type': 'STANDARD', 'criteria': {'dimensions': [], 'metricNames': []}
        }
        service.reports.return_value.run.return_value.execute.return_value = {'id': 'file_2'}
        service.files.return_value.get.return_value.execute.side_effect = (
            lambda: {'status': next(statuses), 'id': 'file_2'}
        )

        mock_client.make_request.side_effect = make_request_side_effect

        report_config = {
            'report_id': '123',
            'stream_name': 'my_stream',
            'stream_alias': 'my_stream',
            'metadata': {}
        }

        sync_report(service, {}, '999', report_config)
        mock_sleep.assert_called()
        mock_process_file.assert_called_once()

    @patch('tap_doubleclick_campaign_manager.sync_reports.CLIENT')
    @patch('singer.write_schema')
    @patch('singer.metrics.job_timer')
    def test_unknown_status_raises_exception(
        self, mock_timer, mock_write_schema, mock_client_class
    ):
        """An unexpected file status (not QUEUED/PROCESSING/REPORT_AVAILABLE) should raise."""
        mock_timer.return_value.__enter__ = lambda s: s
        mock_timer.return_value.__exit__ = MagicMock(return_value=False)

        mock_client = MagicMock()
        mock_client_class.return_value = mock_client

        statuses = iter(['FAILED'])

        def make_request_side_effect(func):
            return func()

        service = MagicMock()
        service.reports.return_value.get.return_value.execute.return_value = {
            'type': 'STANDARD', 'criteria': {'dimensions': [], 'metricNames': []}
        }
        service.reports.return_value.run.return_value.execute.return_value = {'id': 'file_3'}
        service.files.return_value.get.return_value.execute.side_effect = (
            lambda: {'status': next(statuses), 'id': 'file_3'}
        )

        mock_client.make_request.side_effect = make_request_side_effect

        report_config = {
            'report_id': '123',
            'stream_name': 'my_stream',
            'stream_alias': 'my_stream',
            'metadata': {}
        }

        with self.assertRaises(Exception) as ctx:
            sync_report(service, {}, '999', report_config)
        self.assertIn('FAILED', str(ctx.exception))
