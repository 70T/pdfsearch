# c:\Users\Miro\pdfsearch\tools\debug_ocr.py
import sys
import os

# Add parent directory to path so we can import app modules
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import fitz  # PyMuPDF
import indexing_logic
import re
from shared_utils import STOP_WORDS, GARBAGE_PATTERNS


def analyze_file(file_path):
    print(f"--- Analyzing: {file_path} ---")
    try:
        doc = fitz.open(file_path)
    except Exception as e:
        print(f"Error opening file: {e}")
        return

    # 1. Run the standard check
    status = indexing_logic._determine_ocr_status(doc)
    print(f"Resulting Status: {status}\n")

    # 2. Deep Dive into the Stats (Replicating logic for display)
    page_count = doc.page_count
    if page_count > 10:
        indices = [
            int(page_count * p)
            for p in [0.10, 0.15, 0.25, 0.35, 0.45, 0.55, 0.65, 0.75, 0.85, 0.90]
        ]
        pages_to_check = sorted(list(set(indices)))
    else:
        pages_to_check = sorted({0, page_count // 2, page_count - 1})

    full_text_sample = ""
    for p_idx in pages_to_check:
        full_text_sample += doc[p_idx].get_text() + " "

    # Check Stop Words
    words = re.findall(r"\b[a-z]+\b", full_text_sample.lower())
    common_words = STOP_WORDS["eng"]
    common_count = sum(1 for w in words if w in common_words)
    ratio = common_count / len(words) if words else 0
    print(f"Stop Word Ratio: {ratio:.2%} (Threshold is 10.00%)")
    if ratio < 0.10:
        print(" -> FAIL: Text does not look like English.")

    # Check Repeated Characters
    matches_chars = GARBAGE_PATTERNS[1].findall(full_text_sample)
    print(f"Repeated Char Patterns Found: {len(matches_chars)}")
    if matches_chars:
        print(f" -> Samples: {matches_chars[:5]}")

    # Check Repeated Tokens
    matches_tokens = GARBAGE_PATTERNS[3].findall(full_text_sample)
    print(f"Repeated Token Patterns Found (Raw): {len(matches_tokens)}")
    if matches_tokens:
        print(f" -> Samples: {matches_tokens[:5]}")

    real_matches = []
    for m in matches_tokens:
        alnum_content = "".join(c for c in m if c.isalnum())
        if not alnum_content or alnum_content.isdigit():
            continue
        real_matches.append(m)

    print(f"Repeated Token Patterns Found (Filtered): {len(real_matches)}")
    if real_matches:
        print(f" -> Samples: {real_matches[:5]}")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python debug_ocr.py <path_to_pdf>")
    else:
        analyze_file(sys.argv[1])
