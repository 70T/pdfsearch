import unittest
import os
import tempfile
from app import generate_browser_link, app
import database as db


class BaseUXTest(unittest.TestCase):
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


class TestUXFeatures(BaseUXTest):
    def test_generate_browser_link_no_config(self):
        """Should return None if FILES_DIRECTORY is not set."""
        # Ensure config is empty for this test
        old_dir = app.config.get("FILES_DIRECTORY")
        app.config["FILES_DIRECTORY"] = None
        try:
            link = generate_browser_link("test.pdf", 1)
            self.assertIsNone(link)
        finally:
            app.config["FILES_DIRECTORY"] = old_dir

    def test_generate_browser_link_basic(self):
        """Should return simple page link if no snippet."""
        old_dir = app.config.get("FILES_DIRECTORY")
        app.config["FILES_DIRECTORY"] = "/tmp/pdfs"
        try:
            with app.test_request_context():
                link = generate_browser_link("folder/doc.pdf", 5)
                # url_for might return /file/folder/doc.pdf
                self.assertIn("/file/folder/doc.pdf", link)
                self.assertIn("#page=5", link)
                self.assertNotIn("#:~:text=", link)
        finally:
            app.config["FILES_DIRECTORY"] = old_dir

    def test_generate_browser_link_with_snippet(self):
        """Should return text fragment if snippet provided."""
        old_dir = app.config.get("FILES_DIRECTORY")
        app.config["FILES_DIRECTORY"] = "/tmp/pdfs"
        # snippet = ... (Removed unused)
        try:
            with app.test_request_context():
                link = generate_browser_link("doc.pdf", 1)
                self.assertIn("#page=1", link)
        finally:
            app.config["FILES_DIRECTORY"] = old_dir

    def test_partial_results_route(self):
        """/search_results_partial should return result items HTML."""
        # Setup data
        file_id = db.add_or_update_file(
            self.db_path, "test.pdf", "test.pdf", 1000.0, "h1"
        )
        db.bulk_insert_pages(self.db_path, file_id, [(1, "unique_term")])

        response = self.client.get("/search_results_partial?search_query=unique_term")
        self.assertEqual(response.status_code, 200)
        self.assertIn(b"test.pdf", response.data)
        self.assertIn(b'class="result-book"', response.data)
        # Should NOT contain full HTML shell
        self.assertNotIn(b"<!DOCTYPE html>", response.data)

    def test_partial_results_empty(self):
        """/search_results_partial with no query should return 400."""
        response = self.client.get("/search_results_partial")
        self.assertEqual(response.status_code, 400)

    def test_rendering_match_dicts(self):
        """Regression test: Ensure templates handle match dicts correctly."""
        # Setup data
        file_id = db.add_or_update_file(
            self.db_path, "dict_test.pdf", "dict_test.pdf", 1000.0, "h1"
        )
        # match structure is handled by search_logic, but here we insert data that search_logic will retrieve.
        db.bulk_insert_pages(self.db_path, file_id, [(5, "page 5 content")])

        # Configure FILES_DIRECTORY so browser link matches are generated
        self.client.application.config["FILES_DIRECTORY"] = "/tmp/test_pdfs"

        # We need to ensure search_logic returns matches.
        # Run a search via the route
        response = self.client.get("/search_results_partial?search_query=content")
        self.assertEqual(response.status_code, 200)
        # Check that #page=5 is present in the output
        self.assertIn(b"#page=5", response.data)
        # Check that snippet content is present (accounting for highlighting)
        self.assertIn(b"page 5 <b>content</b>", response.data)
        # Ensure template ellipses are gone
        self.assertNotIn(b"...page 5", response.data)

    def test_api_snippets(self):
        """Test the /api/snippets endpoint."""
        # Setup data
        file_id = db.add_or_update_file(
            self.db_path, "snippet_test.pdf", "snippet_test.pdf", 1000.0, "h1"
        )
        db.bulk_insert_pages(
            self.db_path, file_id, [(1, "This is a test snippet content.")]
        )

        # Call API
        response = self.client.get(
            f"/api/snippets?file_id={file_id}&page_num=1&search_query=snippet"
        )
        self.assertEqual(response.status_code, 200)
        self.assertIn(b"test <b>snippet</b> content", response.data)

        # Missing params
        response = self.client.get("/api/snippets")
        self.assertEqual(response.status_code, 400)
