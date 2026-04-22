import unittest
import sys
import os

# Ensure the parent directory is in the path so we can import shared_utils
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from shared_utils import natural_sort_key, UNWANTED_PATTERNS, GARBAGE_PATTERNS


class TestSharedUtils(unittest.TestCase):
    def test_natural_sort_key(self):
        # Test standard sorting vs natural sorting
        filenames = ["Book 1.pdf", "Book 10.pdf", "Book 2.pdf"]

        # Standard sort would be 1, 10, 2
        self.assertEqual(sorted(filenames), ["Book 1.pdf", "Book 10.pdf", "Book 2.pdf"])

        # Natural sort should be 1, 2, 10
        self.assertEqual(
            sorted(filenames, key=natural_sort_key),
            ["Book 1.pdf", "Book 2.pdf", "Book 10.pdf"],
        )

    def test_unwanted_patterns_oceanofpdf(self):
        # Test removal of watermark
        text = "This is a book. OceanofPDF.com"
        for pattern in UNWANTED_PATTERNS:
            text = pattern.sub("", text)
        self.assertEqual(text.strip(), "This is a book.")

    def test_garbage_patterns_repeated_chars(self):
        # GARBAGE_PATTERNS[6] detects words with 5+ repeated characters (e.g. "eeeee")
        pattern = GARBAGE_PATTERNS[6]

        # Should match (5 'e's)
        self.assertTrue(pattern.search("Screeeeeam"))

        # Should not match (3 'e's)
        self.assertFalse(pattern.search("Screeam"))


if __name__ == "__main__":
    unittest.main()
