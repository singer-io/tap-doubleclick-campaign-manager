import unittest
from unittest.mock import patch, MagicMock, call
from singer.catalog import Catalog, CatalogEntry, Schema

from tap_doubleclick_campaign_manager import do_sync
from tap_doubleclick_campaign_manager.sync_reports import transform_field
from tap_doubleclick_campaign_manager.sync_reports import StreamFunc
from tap_doubleclick_campaign_manager.sync_reports import next_sleep_interval
from tap_doubleclick_campaign_manager.sync_reports import parse_line
from tap_doubleclick_campaign_manager.sync_reports import sync_reports
from tap_doubleclick_campaign_manager.sync_reports import sync_report
from tap_doubleclick_campaign_manager.sync_reports import MIN_RETRY_INTERVAL, MAX_RETRY_INTERVAL


class TestDoSync(unittest.TestCase):

    @patch('tap_doubleclick_campaign_manager.sync_reports')
    @patch('singer.write_state')
    def test_do_sync_calls_sync_reports(self, mock_write_state, mock_sync_reports):
        """do_sync should call sync_reports with service, config, catalog, and state."""
        service = MagicMock()
        config = {'profile_id': '1'}
        catalog = MagicMock()
        state = {}

        do_sync(service, config, catalog, state)

        mock_sync_reports.assert_called_once_with(service, config, catalog, state)

    @patch('tap_doubleclick_campaign_manager.sync_reports')
    @patch('singer.write_state')
    def test_do_sync_writes_state_after_sync(self, mock_write_state, mock_sync_reports):
        """do_sync should call singer.write_state with the state after sync."""
        service = MagicMock()
        config = {'profile_id': '1'}
        catalog = MagicMock()
        state = {'current_report': None}

        do_sync(service, config, catalog, state)

        mock_write_state.assert_called_once_with(state)


class TestTransformField(unittest.TestCase):

    def setUp(self):
        self.maxDiff = None

    def test_handles_single_type_string(self):
        self.assertEqual(transform_field("string", "some_field"), "some_field")

    def test_handles_single_type_double(self):
        self.assertEqual(transform_field("double", "1.23"), 1.23)

    def test_handles_single_type_long(self):
        self.assertEqual(transform_field("long", "123"), 123)

    def test_handles_single_type_boolean(self):
        self.assertTrue(transform_field("boolean", "true"))

    def test_handles_single_type_empty(self):
        self.assertIsNone(transform_field("double", ""))

    def test_handles_multiple_type_uses_first_type(self):
        self.assertEqual(transform_field(["double", "string"], "123"), 123.0)
        self.assertEqual(transform_field(["long", "string"], "123"), 123)
        self.assertIsNone(transform_field(["long", "string"], "some_field"))

    def test_handles_multiple_type_uses_second_type(self):
        self.assertEqual(transform_field(["double", "string"], "some_field"), "some_field")

    def test_boolean_true_values(self):
        """Transform various truthy string representations for boolean type."""
        for val in ['true', 'True', 'TRUE', 't', 'T', 'yes', 'Yes', 'YES', 'y', 'Y']:
            with self.subTest(val=val):
                self.assertTrue(transform_field('boolean', val))

    def test_boolean_false_values(self):
        """Non-truthy boolean strings should return False."""
        for val in ['false', 'no', 'n', '0', 'f']:
            with self.subTest(val=val):
                self.assertFalse(transform_field('boolean', val))

    def test_empty_string_returns_none_for_long(self):
        self.assertIsNone(transform_field('long', ''))

    def test_empty_string_returns_none_for_double(self):
        self.assertIsNone(transform_field('double', ''))

    def test_empty_string_returns_none_for_string(self):
        self.assertIsNone(transform_field('string', ''))

    def test_long_non_numeric_returns_none(self):
        self.assertIsNone(transform_field('long', 'not_a_number'))

    def test_string_passthrough(self):
        self.assertEqual(transform_field('string', 'hello'), 'hello')

    def test_multiple_all_fail_returns_none(self):
        """long returns None (not ValueError), so the first type wins and None is returned."""
        self.assertIsNone(transform_field(['long', 'double'], 'abc'))


class TestStreamFunc(unittest.TestCase):

    def test_single_complete_line_calls_func(self):
        received = []
        sf = StreamFunc(received.append)
        sf.write(b'hello world\n')
        self.assertEqual(received, ['hello world'])

    def test_partial_line_does_not_call_func(self):
        received = []
        sf = StreamFunc(received.append)
        sf.write(b'partial line')
        self.assertEqual(received, [])

    def test_continuation_of_partial_line_calls_func(self):
        received = []
        sf = StreamFunc(received.append)
        sf.write(b'partial')
        sf.write(b' line\n')
        self.assertEqual(received, ['partial line'])

    def test_multiple_lines_in_one_write(self):
        received = []
        sf = StreamFunc(received.append)
        sf.write(b'line1\nline2\nline3\n')
        self.assertEqual(received, ['line1', 'line2', 'line3'])

    def test_mixed_complete_and_partial_lines(self):
        received = []
        sf = StreamFunc(received.append)
        sf.write(b'line1\nline2\npartial')
        self.assertEqual(received, ['line1', 'line2'])

    def test_second_write_appends_to_buffer(self):
        received = []
        sf = StreamFunc(received.append)
        sf.write(b'foo\nbar')
        sf.write(b'baz\n')
        self.assertEqual(received, ['foo', 'barbaz'])

    def test_empty_write_produces_nothing(self):
        received = []
        sf = StreamFunc(received.append)
        sf.write(b'')
        self.assertEqual(received, [])


class TestNextSleepInterval(unittest.TestCase):

    def test_zero_previous_returns_min_interval(self):
        self.assertEqual(next_sleep_interval(0), MIN_RETRY_INTERVAL)

    def test_small_previous_returns_value_in_range(self):
        for _ in range(20):
            result = next_sleep_interval(10)
            self.assertGreaterEqual(result, 10)
            self.assertLessEqual(result, 20)

    def test_large_previous_caps_at_max(self):
        self.assertEqual(next_sleep_interval(MAX_RETRY_INTERVAL), MAX_RETRY_INTERVAL)

    def test_half_max_stays_within_bounds(self):
        for _ in range(50):
            result = next_sleep_interval(MAX_RETRY_INTERVAL // 2)
            self.assertGreaterEqual(result, MAX_RETRY_INTERVAL // 2)
            self.assertLessEqual(result, MAX_RETRY_INTERVAL)


class TestParseLine(unittest.TestCase):

    def test_simple_line(self):
        self.assertEqual(parse_line('a,b,c'), ['a', 'b', 'c'])

    def test_quoted_field(self):
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

        statuses = iter(['REPORT_AVAILABLE'])
        service = MagicMock()
        service.reports.return_value.get.return_value.execute.return_value = {
            'type': 'STANDARD', 'criteria': {'dimensions': [], 'metricNames': []}
        }
        service.reports.return_value.run.return_value.execute.return_value = {'id': 'file_1'}
        service.files.return_value.get.return_value.execute.side_effect = (
            lambda: {'status': next(statuses), 'id': 'file_1'}
        )
        mock_client.make_request.side_effect = lambda func: func()

        sync_report(service, {}, '999', {
            'report_id': '123', 'stream_name': 'my_stream',
            'stream_alias': 'my_stream', 'metadata': {}
        })
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
        service = MagicMock()
        service.reports.return_value.get.return_value.execute.return_value = {
            'type': 'STANDARD', 'criteria': {'dimensions': [], 'metricNames': []}
        }
        service.reports.return_value.run.return_value.execute.return_value = {'id': 'file_2'}
        service.files.return_value.get.return_value.execute.side_effect = (
            lambda: {'status': next(statuses), 'id': 'file_2'}
        )
        mock_client.make_request.side_effect = lambda func: func()

        sync_report(service, {}, '999', {
            'report_id': '123', 'stream_name': 'my_stream',
            'stream_alias': 'my_stream', 'metadata': {}
        })
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
        service = MagicMock()
        service.reports.return_value.get.return_value.execute.return_value = {
            'type': 'STANDARD', 'criteria': {'dimensions': [], 'metricNames': []}
        }
        service.reports.return_value.run.return_value.execute.return_value = {'id': 'file_3'}
        service.files.return_value.get.return_value.execute.side_effect = (
            lambda: {'status': next(statuses), 'id': 'file_3'}
        )
        mock_client.make_request.side_effect = lambda func: func()

        with self.assertRaises(Exception) as ctx:
            sync_report(service, {}, '999', {
                'report_id': '123', 'stream_name': 'my_stream',
                'stream_alias': 'my_stream', 'metadata': {}
            })
        self.assertIn('FAILED', str(ctx.exception))
