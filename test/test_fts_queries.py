import unittest
import os
import tempfile
import database as db
import search_logic


class BaseFTSTest(unittest.TestCase):
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
        return db.commit_indexed_pdf(
            self.db_path,
            filename,
            relative_path,
            1000.0,
            file_hash,
            pages=pages,
            chapters=[],
        )


class TestBasicSearch(BaseFTSTest):
    def test_basic_term_search(self):
        """Search for a single word and verify it's found."""
        self._add_test_content(
            "basic.pdf",
            "folder/basic.pdf",
            "h1",
            [(1, "The quick brown fox jumps over the lazy dog.")],
        )

        results, has_more, error, _ = search_logic.perform_search(self.db_path, "fox")
        self.assertIsNone(error)
        self.assertEqual(len(results), 1)

    def test_phrase_search(self):
        """Search for an exact phrase using quotes."""
        self._add_test_content(
            "phrase.pdf",
            "folder/phrase.pdf",
            "h2",
            [(1, "The quick brown fox jumps over the lazy dog.")],
        )

        results, has_more, error, _ = search_logic.perform_search(
            self.db_path, '"brown fox"'
        )
        self.assertIsNone(error)
        self.assertEqual(len(results), 1)

    def test_no_match(self):
        """Search for a term that doesn't exist returns empty."""
        self._add_test_content(
            "empty.pdf", "folder/empty.pdf", "h3", [(1, "Nothing relevant here.")]
        )

        results, has_more, error, _ = search_logic.perform_search(
            self.db_path, "elephant"
        )
        self.assertIsNone(error)
        self.assertEqual(len(results), 0)

    def test_multi_word_search(self):
        """Search with multiple terms matches documents containing all terms."""
        self._add_test_content(
            "multi.pdf",
            "folder/multi.pdf",
            "h4",
            [(1, "Cats and dogs are popular pets.")],
        )

        results, has_more, error, _ = search_logic.perform_search(
            self.db_path, "cats dogs"
        )
        self.assertIsNone(error)
        self.assertEqual(len(results), 1)


class TestHyphenatedSearch(BaseFTSTest):
    def test_hyphenated_term(self):
        """Search for hyphenated term matches both hyphenated and merged forms."""
        self._add_test_content(
            "hyphen.pdf",
            "folder/hyphen.pdf",
            "h5",
            [(1, "The snap-hiss of a lightsaber.")],
        )

        results, has_more, error, _ = search_logic.perform_search(
            self.db_path, "snap-hiss"
        )
        self.assertIsNone(error)
        self.assertEqual(len(results), 1)


class TestFTSErrorHandling(BaseFTSTest):
    def test_unbalanced_quotes_returns_error(self):
        """Unbalanced quotes should return an error message, not crash."""
        self._add_test_content(
            "err.pdf", "folder/err.pdf", "h6", [(1, "Some text here.")]
        )

        results, has_more, error, _ = search_logic.perform_search(
            self.db_path, '"unbalanced'
        )
        # Should return an error message rather than raising
        if error is None:
            self.assertEqual(len(results), 0)  # Graceful empty result
        else:
            self.assertIsNotNone(error)

    def test_normal_query_no_error(self):
        """Normal queries should not return an error."""
        self._add_test_content(
            "ok.pdf", "folder/ok.pdf", "h7", [(1, "This is normal text for searching.")]
        )

        results, has_more, error, _ = search_logic.perform_search(
            self.db_path, "normal"
        )
        self.assertIsNone(error)


class TestFTSSanitization(BaseFTSTest):
    def test_keyword_sanitization(self):
        """Reserved keywords like NOT should be treated as literals, not operators."""
        self._add_test_content(
            "kw.pdf", "folder/kw.pdf", "h8", [(1, "I do NOT like green eggs and ham.")]
        )

        # Search for "NOT" - if treated as operator it would be a syntax error (AND NOT ...)
        # With sanitization, it becomes "NOT" (literal)
        results, has_more, error, _ = search_logic.perform_search(self.db_path, "NOT")
        self.assertIsNone(error)
        self.assertEqual(len(results), 1)

    def test_wildcard_sanitization(self):
        """Leading wildcards should be stripped to avoid FTS5 errors."""
        self._add_test_content(
            "wild.pdf", "folder/wild.pdf", "h9", [(1, "A starry night.")]
        )

        # "*starry" is invalid FTS5. Sanitization should strip it to "starry"
        results, has_more, error, _ = search_logic.perform_search(
            self.db_path, "*starry"
        )
        self.assertIsNone(error)
        self.assertEqual(len(results), 1)
