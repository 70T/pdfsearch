import sys
import os
import argparse
import logging
from difflib import SequenceMatcher

# Add parent directory to path so we can import app modules
parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, parent_dir)

import database as db  # noqa: E402

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)


def get_local_library(db_name):
    """Fetches all indexed filenames from the database."""
    if not os.path.exists(db_name):
        logger.error(f"Database not found at {db_name}")
        return set()

    sql = "SELECT filename FROM files"
    rows = db.query_db(db_name, sql)
    return {row[0] for row in rows}


def normalize_string(text):
    """
    Normalizes a string for comparison:
    - Lowercase
    - Remove extensions
    - Remove apostrophes
    - Replace separators with spaces
    """
    text = text.lower()
    for ext in [".pdf", ".epub", ".mobi", ".azw3", ".txt"]:
        if text.endswith(ext):
            text = text[: -len(ext)]

    # Remove apostrophes entirely (e.g. "It's" -> "its")
    text = text.replace("'", "")

    # Replace common separators with spaces
    for char in ["-", "_", ".", "[", "]", "(", ")", ":", ",", "&"]:
        text = text.replace(char, " ")

    # Collapse multiple spaces
    return " ".join(text.split())


def is_similar(a, b, threshold=0.85):
    """
    Checks if two strings are similar using Python's built-in difflib.
    """
    return SequenceMatcher(None, a, b).ratio() > threshold


def check_file_list(input_file, db_path):
    if not os.path.exists(input_file):
        logger.error(f"Input file not found: {input_file}")
        return

    logger.info(f"Loading local library from {db_path}...")
    local_library = get_local_library(db_path)
    if not local_library:
        logger.error("Local library is empty or database could not be read.")
        return

    logger.info(f"Loaded {len(local_library)} files from local index.")

    # Pre-normalize local library and prepare token lists for flexible matching
    normalized_local_data = []
    for f in local_library:
        norm = normalize_string(f)
        normalized_local_data.append((norm, sorted(norm.split())))

    # Set for O(1) exact lookups
    normalized_local_set = {x[0] for x in normalized_local_data}

    logger.info(f"Reading list from {input_file}...")
    target_works = []
    valid_codes = {"ON", "OJR", "NA", "JRA", "YR", "GB"}
    is_tabular = False

    with open(input_file, "r", encoding="utf-8") as f:
        lines = f.readlines()

    for line in lines:
        parts = line.split("\t")
        if len(parts) >= 3 and parts[1].strip() in valid_codes:
            is_tabular = True
            break

    for line in lines:
        line_str = line.strip()
        if not line_str:
            continue

        if is_tabular:
            parts = line.split("\t")
            if len(parts) >= 3:
                code = parts[1].strip()
                if code in valid_codes:
                    title = parts[2].strip()
                    if title:
                        target_works.append(title)
        else:
            target_works.append(line_str)

    missing = []
    found = []

    total = len(target_works)
    logger.info(f"Comparing {total} items against library...")

    for i, work in enumerate(target_works):
        norm_work = normalize_string(work)
        work_tokens = sorted(norm_work.split())

        # 1. Exact match (normalized)
        if norm_work in normalized_local_set:
            found.append(work)
            continue

        # 2. Fuzzy match
        is_found = False
        for local_norm, local_tokens in normalized_local_data:
            if norm_work in local_norm or local_norm in norm_work:
                is_found = True
                break

            # Check for reordered words (e.g. "Author - Title" vs "Title - Author")
            if work_tokens == local_tokens:
                is_found = True
                break

            # Check if work tokens are a subset of local tokens (fuzzy)
            # Handles: "Ephrael Stern: Heretic Saint" -> "Sisters of Battle - Ephrael Stern - Heretical Saint"
            if len(work_tokens) > 0:
                match_count = 0
                for wt in work_tokens:
                    if wt in local_tokens:
                        match_count += 1
                        continue
                    # Fuzzy token check
                    for lt in local_tokens:
                        if is_similar(wt, lt, threshold=0.8):
                            match_count += 1
                            break

                if match_count == len(work_tokens):
                    is_found = True
                    break

            if is_similar(norm_work, local_norm):
                is_found = True
                break

        if is_found:
            found.append(work)
        else:
            missing.append(work)

        if (i + 1) % 10 == 0:
            print(f"Processed {i + 1}/{total}...", end="\r")

    print(f"Processed {total}/{total}    ")

    print("\n" + "=" * 60)
    print(f"REPORT: {len(missing)} MISSING / {len(found)} FOUND")
    print("=" * 60)

    if missing:
        print("--- MISSING WORKS ---")
        missing.sort()
        for m in missing:
            print(f"[MISSING] {m}")
    else:
        print("All works from the list were found in the library!")
    print("=" * 60)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Check a text file list of works against the PDF library."
    )
    parser.add_argument(
        "input_file",
        nargs="?",
        default="list.txt",
        help="Path to the text file containing the list of works (one per line, or tabular text).",
    )
    parser.add_argument(
        "--db",
        default="pdf_search.db",
        help="Name of the database file (default: pdf_search.db)",
    )

    args = parser.parse_args()

    # Resolve DB path relative to parent dir if not absolute
    db_path = args.db
    if not os.path.isabs(db_path):
        db_path = os.path.join(parent_dir, db_path)

    check_file_list(args.input_file, db_path)
