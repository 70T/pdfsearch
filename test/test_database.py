import unittest
import os
import tempfile
import database as db


class BaseDBTest(unittest.TestCase):
    def setUp(self):
        self.db_fd, self.db_path = tempfile.mkstemp()
        db.init_db(self.db_path)

    def tearDown(self):
        # Properly close all database connections before unlinking (Windows compatibility)
        import gc
        import database as db_module

        db_module.close_db()  # Close Flask g connection if any
        conn = getattr(db_module._worker_local, "conn", None)
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass
            db_module._worker_local.conn = None
        gc.collect()  # Force garbage collection to close any lingering connections
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
            # Final attempt
            try:
                os.unlink(self.db_path)
            except Exception:
                pass  # Ignore if still locked


class TestInitDb(BaseDBTest):
    def test_creates_tables(self):
        """Verify init_db creates the expected tables."""
        rows = db.query_db(
            self.db_path,
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name",
        )
        table_names = sorted([r[0] for r in rows])
        self.assertIn("chapters", table_names)
        self.assertIn("files", table_names)
        self.assertIn("pdf_text_fts", table_names)


class TestFileOperations(BaseDBTest):
    def test_add_and_query_file(self):
        """Insert a file record via commit_indexed_pdf and verify retrieval."""
        file_id = db.commit_indexed_pdf(
            self.db_path,
            "test.pdf",
            "folder/test.pdf",
            1000.0,
            "abc123",
            pages=[(1, "sample text")],
            chapters=[],
        )
        self.assertIsNotNone(file_id)
        rows = db.query_db(
            self.db_path,
            "SELECT filename, relative_path FROM files WHERE filename = ?",
            ("test.pdf",),
        )
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0][0], "test.pdf")
        self.assertEqual(rows[0][1], "folder/test.pdf")

    def test_delete_file(self):
        """Insert then delete a file, verify FTS content is also removed."""
        db.commit_indexed_pdf(
            self.db_path,
            "delete_me.pdf",
            "folder/delete_me.pdf",
            1000.0,
            "def456",
            pages=[(1, "hello world")],
            chapters=[],
        )

        db.delete_file(self.db_path, "folder/delete_me.pdf")

        # Verify file record is gone
        rows = db.query_db(
            self.db_path, "SELECT * FROM files WHERE filename = ?", ("delete_me.pdf",)
        )
        self.assertEqual(len(rows), 0)

        # Verify FTS content is gone
        fts_rows = db.query_db(
            self.db_path,
            "SELECT * FROM pdf_text_fts WHERE pdf_text_fts MATCH ?",
            ("hello",),
        )
        self.assertEqual(len(fts_rows), 0)

    def test_commit_indexed_pdf_pages(self):
        """Insert pages via commit_indexed_pdf and verify FTS content."""
        pages = [
            (1, "alpha bravo charlie"),
            (2, "delta echo foxtrot"),
        ]
        db.commit_indexed_pdf(
            self.db_path,
            "bulk.pdf",
            "folder/bulk.pdf",
            1000.0,
            "ghi789",
            pages=pages,
            chapters=[],
        )

        # Verify FTS search works
        rows = db.query_db(
            self.db_path,
            "SELECT page_num, text FROM pdf_text_fts WHERE pdf_text_fts MATCH ?",
            ("bravo",),
        )
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0][0], 1)

    def test_get_unique_folders(self):
        """Insert files with different paths and verify folder extraction."""
        db.commit_indexed_pdf(
            self.db_path,
            "a.pdf",
            "fiction/a.pdf",
            1000.0,
            "aaa",
            pages=[(1, "text")],
            chapters=[],
        )
        db.commit_indexed_pdf(
            self.db_path,
            "b.pdf",
            "science/b.pdf",
            1000.0,
            "bbb",
            pages=[(1, "text")],
            chapters=[],
        )
        db.commit_indexed_pdf(
            self.db_path,
            "c.pdf",
            "fiction/c.pdf",
            1000.0,
            "ccc",
            pages=[(1, "text")],
            chapters=[],
        )

        folders = db.get_unique_folders(self.db_path)
        self.assertIn("fiction", folders)
        self.assertIn("science", folders)


class TestWipeDb(BaseDBTest):
    def test_wipe_and_rebuild(self):
        """Wipe the database and verify tables are recreated empty."""
        db.commit_indexed_pdf(
            self.db_path,
            "wipe.pdf",
            "folder/wipe.pdf",
            1000.0,
            "xxx",
            pages=[(1, "text")],
            chapters=[],
        )
        db.wipe_db(self.db_path)

        # Tables should exist but be empty
        rows = db.query_db(self.db_path, "SELECT * FROM files")
        self.assertEqual(len(rows), 0)
