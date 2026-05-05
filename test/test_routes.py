import unittest
import os
import tempfile
import database as db
from app import app


class BaseRouteTest(unittest.TestCase):
    def setUp(self):
        self.db_fd, self.db_path = tempfile.mkstemp()
        db.init_db(self.db_path)

        # Configure app to use temp db
        app.config["TESTING"] = True
        app.config["DATABASE"] = self.db_path
        self.client = app.test_client()

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
        return db.commit_indexed_pdf(
            self.db_path,
            filename,
            relative_path,
            1000.0,
            file_hash,
            pages=pages,
            chapters=[],
        )


class TestRoutes(BaseRouteTest):
    def test_search_form_get(self):
        """GET / should return 200 with the search form."""
        response = self.client.get("/")
        self.assertEqual(response.status_code, 200)
        self.assertIn(b"Search Query", response.data)

    def test_search_empty_db(self):
        """GET /?search_query=test on empty DB should redirect to index builder."""
        response = self.client.get("/?search_query=test")
        self.assertEqual(response.status_code, 302)
        self.assertIn("/index", response.headers["Location"])

    def test_search_no_results(self):
        """Search for a term that doesn't exist should show no results page."""
        self._add_test_content(
            "dummy.pdf", "folder/dummy.pdf", "hash1", [(1, "some content here")]
        )

        response = self.client.get("/?search_query=zzzznonexistent")
        self.assertEqual(response.status_code, 200)
        self.assertIn(b"No Results Found", response.data)

    def test_search_with_results(self):
        """Search for a term that exists should show results page."""
        self._add_test_content(
            "found.pdf",
            "folder/found.pdf",
            "hash2",
            [(1, "The emperor protects the faithful.")],
        )

        response = self.client.get("/?search_query=emperor")
        self.assertEqual(response.status_code, 200)
        # The visible filename should be clean, but the path metadata should still have .pdf
        self.assertIn(b"found", response.data)
        self.assertIn(b"folder/found.pdf", response.data)

    def test_back_to_search_link(self):
        """Results page should have a 'Back to Search' link pointing to /."""
        self._add_test_content(
            "link.pdf", "folder/link.pdf", "hash3", [(1, "navigation test content")]
        )

        response = self.client.get("/?search_query=navigation")
        self.assertEqual(response.status_code, 200)
        self.assertIn(b'href="/"', response.data)

    def test_index_builder_get(self):
        """GET /index should return 200."""
        response = self.client.get("/index")
        self.assertEqual(response.status_code, 200)
        self.assertIn(b"Index Builder", response.data)
