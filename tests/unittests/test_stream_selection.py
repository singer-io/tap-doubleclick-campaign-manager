import unittest

from tap_doubleclick_campaign_manager import stream_is_selected


class TestStreamIsSelected(unittest.TestCase):

    def test_selected_true(self):
        """metadata with selected=True at root should return True."""
        mdata = {(): {'selected': True}}
        self.assertTrue(stream_is_selected(mdata))

    def test_selected_false(self):
        """metadata with selected=False at root should return False."""
        mdata = {(): {'selected': False}}
        self.assertFalse(stream_is_selected(mdata))

    def test_selected_missing_returns_false(self):
        """metadata with no 'selected' key at root should return False."""
        mdata = {(): {'inclusion': 'automatic'}}
        self.assertFalse(stream_is_selected(mdata))

    def test_empty_root_metadata_returns_false(self):
        """metadata with empty root dict should return False."""
        mdata = {(): {}}
        self.assertFalse(stream_is_selected(mdata))

    def test_completely_empty_mdata_returns_false(self):
        """Totally empty metadata dict should return False without raising."""
        mdata = {}
        self.assertFalse(stream_is_selected(mdata))
