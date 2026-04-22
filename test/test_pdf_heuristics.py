import unittest
import os
import fitz
import logging
import random
import sys

# Configure logging to suppress output during tests
logging.basicConfig(level=logging.DEBUG, format="%(message)s")

# Add parent directory to path so tests can import app modules
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from indexing_logic import clean_and_normalize_text, _determine_ocr_status  # noqa: E402


class TestPdfHeuristics(unittest.TestCase):
    def setUp(self):
        self.pdf_path = "test_curveballs.pdf"
        doc = fitz.open()
        page = doc.new_page()

        print("\n" + "=" * 50)
        print("TEST SETUP: Generating PDF with curveball text...")

        # Insert text that mimics the "curveball" scenarios.
        # We use a simple text insertion.
        # Note: 'fitz' extracts text roughly as inserted.

        lines = [
            "This is fne stus.",
            "The |mperium.",
            'W a r h a m m e r : The |mperium\' s pow- er , said Horus . " It is un- limited . "',
        ]
        random.shuffle(lines)
        text_content = "\n".join(lines)

        # Insert text at position (50, 50)
        page.insert_text((50, 50), text_content, fontsize=12)
        print(f"Inserted text (Randomized Order):\n{text_content}")

        doc.save(self.pdf_path)
        doc.close()

    def tearDown(self):
        if os.path.exists(self.pdf_path):
            try:
                os.remove(self.pdf_path)
            except PermissionError:
                pass

    def test_pdf_extraction_and_cleaning(self):
        print("\n>>> STEP 1: Opening PDF and extracting raw text...")
        doc = fitz.open(self.pdf_path)
        full_text = ""

        # Replicate extraction logic from indexing_logic.py index_pdf_file
        # This ensures we are testing the exact pipeline used in production
        for page in doc:
            page_dict = page.get_text("dict")
            blocks = page_dict.get("blocks", [])
            text_blocks = [b for b in blocks if b.get("type") == 0]
            text_blocks.sort(key=lambda b: (b["bbox"][1], b["bbox"][0]))

            block_texts = []
            for b in text_blocks:
                lines = b.get("lines", [])

                para = ""
                for line in lines:
                    line_text = ""
                    prev_span = None
                    for span in line.get("spans", []):
                        span_text = span.get("text", "")

                        if (
                            prev_span
                            and span_text
                            and not span_text.startswith(" ")
                            and prev_span.get("text")
                            and not prev_span["text"].endswith(" ")
                        ):
                            dist = span["bbox"][0] - prev_span["bbox"][2]
                            if dist > (span["size"] * 0.15):
                                if span_text[0] not in ".,!?:;'\"’”]":
                                    line_text += " "

                        line_text += span_text
                        prev_span = span

                    line_text = line_text.strip()
                    if not line_text:
                        continue
                    if not para:
                        para = line_text
                    elif para.endswith("-") and len(para) > 1 and para[-2].isalpha():
                        para += line_text
                    else:
                        para += " " + line_text

                if para:
                    block_texts.append(para)
            full_text += "\n\n".join(block_texts)

        doc.close()

        print(f">>> Raw Extracted Text:\n{full_text!r}")

        print("\n>>> STEP 2: Running Heuristics (clean_and_normalize_text)...")
        cleaned = clean_and_normalize_text(full_text)
        print(f">>> Cleaned Text:\n{cleaned}")

        # Assertions
        # 1. OCR fixes
        self.assertIn("This is fine stuff.", cleaned)
        self.assertIn("The Imperium.", cleaned)

        # 2. Complex sentence
        # Expected: Warhammer: The Imperium's power, said Horus. "It is unlimited."
        expected_complex = (
            'Warhammer: The Imperium\'s power, said Horus. "It is unlimited."'
        )
        # Normalize for comparison
        clean_normalized = (
            cleaned.replace("\u201c", '"')
            .replace("\u201d", '"')
            .replace("\u2018", "'")
            .replace("\u2019", "'")
        )

        print("\n>>> STEP 3: Verifying assertions...")
        # Use regex to allow for an occasional stray space before closing quote caused by block joining
        import re as std_re

        pattern = std_re.escape(expected_complex).replace('"', r'\s*"')
        self.assertTrue(
            std_re.search(pattern, clean_normalized),
            f"Pattern '{pattern}' not found in '{clean_normalized}'",
        )

    def test_ocr_trigger(self):
        print("\n>>> STEP 4: Testing OCR Trigger (Garbage Detection)...")
        # Create a garbage PDF to simulate bad OCR
        garbage_pdf_path = "test_garbage.pdf"
        doc = fitz.open()
        page = doc.new_page()
        # Insert garbage pattern defined in GARBAGE_PATTERNS
        # e.g. repeated tokens "S] S] S]"
        garbage_text = ("S] " * 10 + "ER " * 10 + "\n") * 10
        page.insert_text((50, 50), garbage_text)
        doc.save(garbage_pdf_path)
        doc.close()

        doc_check = fitz.open(garbage_pdf_path)
        status = _determine_ocr_status(doc_check)
        doc_check.close()

        print(f"OCR Status for garbage PDF: {status}")
        self.assertEqual(status, "redo")

        if os.path.exists(garbage_pdf_path):
            os.remove(garbage_pdf_path)

    def test_ocr_false_positive_numbers(self):
        print("\n>>> STEP 5: Testing OCR False Positive (Tabletop Stats)...")
        # Create a PDF with repeated numbers (common in stat blocks)
        # e.g. "4 4 4 4 5+"
        stat_pdf_path = "test_stats.pdf"
        doc = fitz.open()
        page = doc.new_page()

        # Simulate a stat block row
        # M WS BS S T W A Ld Sv
        # 4 4 4 4 4 1 1 7 5+
        stat_text = "M WS BS S T W A Ld Sv\n" + ("4 " * 8 + "5+\n") * 5

        page.insert_text((50, 50), stat_text)
        doc.save(stat_pdf_path)
        doc.close()

        doc_check = fitz.open(stat_pdf_path)
        status = _determine_ocr_status(doc_check)
        doc_check.close()

        print(f"OCR Status for stats PDF: {status}")
        # This should NOT be redo. It should be None or 'keep' depending on implementation.
        self.assertNotEqual(
            status,
            "redo",
            "Repeated numbers in stat blocks should not trigger OCR redo",
        )

        if os.path.exists(stat_pdf_path):
            os.remove(stat_pdf_path)


if __name__ == "__main__":
    unittest.main()
