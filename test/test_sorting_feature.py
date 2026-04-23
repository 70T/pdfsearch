import database as db
import search_logic
import unittest
import os
import tempfile


class BaseSortingTest(unittest.TestCase):
    def setUp(self):
        self.db_fd, self.db_path = tempfile.mkstemp()
        db.init_db(self.db_path)

    def tearDown(self):
        # Properly close all database connections before unlinking (Windows compatibility)
        import gc
        import database as db_module

        db_module.close_db()
        gc.collect()
        os.close(self.db_fd)
        # Small retry loop for Windows file locks
        import time

        for _ in range(5):
            try:
                os.unlink(self.db_path)
                break
            except PermissionError:
                time.sleep(0.1)
        else:
            try:
                os.unlink(self.db_path)
            except Exception:
                pass

    def _add_test_content(self, filename, relative_path, file_hash, pages):
        """Helper to add a file with page content for search testing."""
        file_id = db.add_or_update_file(
            self.db_path, filename, relative_path, 1000.0, file_hash
        )
        db.bulk_insert_pages(self.db_path, file_id, pages)
        return file_id


class TestSortingFeature(BaseSortingTest):
    def test_sort_by_filename(self):
        """Verify explicit sort by filename."""
        # Setup: A has 1 match, B has 10.
        pages_a = [(1, "common_term")]
        self._add_test_content("A_file.pdf", "folder/A_file.pdf", "h_a", pages_a)

        pages_b = [(i, "common_term") for i in range(1, 11)]
        self._add_test_content("B_file.pdf", "folder/B_file.pdf", "h_b", pages_b)

        # Action: Search with sort_by='filename'
        results, has_more, error, _ = search_logic.perform_search(
            self.db_path, "common_term", sort_by="filename"
        )

        # Assertion
        self.assertIsNone(error)
        self.assertEqual(len(results), 2)
        self.assertEqual(results[0][0], "A_file")
        self.assertEqual(results[1][0], "B_file")

    def test_sort_by_relevance(self):
        """Verify explicit sort by relevance (match count)."""
        # Setup: A has 1 match, B has 10.
        pages_a = [(1, "common_term")]
        self._add_test_content("A_file.pdf", "folder/A_file.pdf", "h_a", pages_a)

        pages_b = [(i, "common_term") for i in range(1, 11)]
        self._add_test_content("B_file.pdf", "folder/B_file.pdf", "h_b", pages_b)

        # Action: Search with sort_by='relevance'
        results, has_more, error, _ = search_logic.perform_search(
            self.db_path, "common_term", sort_by="relevance"
        )

        # Assertion: B should be first because it has more matches
        self.assertIsNone(error)
        self.assertEqual(len(results), 2)
        self.assertEqual(results[0][0], "B_file")
        self.assertEqual(results[1][0], "A_file")
