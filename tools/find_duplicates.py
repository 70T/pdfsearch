import argparse
import logging
import sys
import os
import sqlite3
from collections import defaultdict

# Add parent directory to path to allow importing database from project root
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import database as db

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)


def get_file_metadata(db_name):
    # Retrieves metadata for all files to map IDs to paths.
    sql = "SELECT id, filename, relative_path FROM files"
    rows = db.query_db(db_name, sql)
    return {row[0]: {"filename": row[1], "path": row[2]} for row in rows}


def analyze_duplicates(db_name, min_length, threshold, max_occurrences, output_file):
    # Analyzes the database for duplicate lines and reports file overlaps.
    #
    # Args:
    #     db_name (str): Path to the database.
    #     min_length (int): Ignore lines shorter than this (filters short headers/dialogue).
    #     threshold (float): Report file pairs that share more than this ratio of content.
    #     max_occurrences (int): Ignore lines appearing in more than this many files (boilerplate).
    #     output_file (str): Path to write the detailed report.
    # Safety check: Ensure output file is not the database file
    if os.path.realpath(db_name) == os.path.realpath(output_file):
        logger.error(
            "Critical Error: Output file path matches database path. Aborting to prevent database overwrite."
        )
        return

    if not db.check_db_has_content(db_name):
        if os.path.exists(db_name) and os.path.getsize(db_name) == 0:
            logger.error(
                "Database is empty (0 bytes). This often happens if you accidentally overwrote it (e.g., using '>' shell redirection)."
            )
        else:
            logger.error("Database is empty or does not exist.")
        return

    logger.info(f"Loading file metadata from {db_name}...")
    file_meta = get_file_metadata(db_name)
    logger.info(f"Found {len(file_meta)} files.")

    logger.info("Scanning content for duplicate lines...")

    # Data structures
    # line_content -> set(file_ids)
    line_map = defaultdict(set)

    # file_id -> count of valid lines processed
    file_line_counts = defaultdict(int)

    # Fetch all text
    # We use a direct connection here to iterate via cursor rather than loading
    # the entire DB into memory with fetchall(), which is safer for large libraries.
    sql = "SELECT file_id, text FROM pdf_text_fts"

    try:
        with sqlite3.connect(db_name, timeout=30.0) as conn:
            cursor = conn.cursor()
            cursor.execute(sql)

            count = 0
            for file_id, text in cursor:
                if not text:
                    continue

                # Your indexing logic stores text as paragraphs separated by newlines.
                # This makes 'lines' effectively 'paragraphs', which are excellent fingerprints.
                lines = text.split("\n")
                for line in lines:
                    clean = line.strip()
                    if len(clean) < min_length:
                        continue

                    line_map[clean].add(file_id)
                    file_line_counts[file_id] += 1

                count += 1
                if count % 500 == 0:
                    print(f"\rScanned {count} pages...", end="", flush=True)

            print(f"\rScanned {count} pages. Done.")

    except sqlite3.Error as e:
        logger.error(f"Database error: {e}")
        return

    # Analyze overlaps
    logger.info("Analyzing overlaps...")

    # (file_id_a, file_id_b) -> list of shared lines
    pair_overlaps = defaultdict(list)
    duplicate_line_count = 0
    boilerplate_line_count = 0

    for line, file_ids in line_map.items():
        # Heuristic: If a line appears in too many files, it's likely boilerplate (header/footer/copyright)
        if len(file_ids) > max_occurrences:
            boilerplate_line_count += 1
            continue

        if len(file_ids) > 1:
            duplicate_line_count += 1
            # Create pairs for every combination of files sharing this line
            ids = sorted(list(file_ids))
            for i in range(len(ids)):
                for j in range(i + 1, len(ids)):
                    pair_overlaps[(ids[i], ids[j])].append(line)

    logger.info(
        f"Found {duplicate_line_count} unique lines/paragraphs that appear in multiple files."
    )
    logger.info(
        f"Skipped {boilerplate_line_count} lines appearing in > {max_occurrences} files (boilerplate)."
    )

    # Filter and Report
    significant_overlaps = []

    for (id_a, id_b), shared_lines in pair_overlaps.items():
        shared_count = len(shared_lines)
        total_a = file_line_counts[id_a]
        total_b = file_line_counts[id_b]

        if total_a == 0 or total_b == 0:
            continue

        # Calculate overlap ratio relative to each file
        ratio_a = shared_count / total_a
        ratio_b = shared_count / total_b

        # If either file is significantly composed of content from the other, flag it.
        if ratio_a >= threshold or ratio_b >= threshold:
            significant_overlaps.append(
                {
                    "a": file_meta[id_a],
                    "b": file_meta[id_b],
                    "shared": shared_count,
                    "shared_lines": shared_lines,
                    "total_a": total_a,
                    "total_b": total_b,
                    "ratio_a": ratio_a,
                    "ratio_b": ratio_b,
                }
            )

    # Sort by highest overlap ratio
    significant_overlaps.sort(
        key=lambda x: max(x["ratio_a"], x["ratio_b"]), reverse=True
    )

    # Write to file and print summary
    with open(output_file, "w", encoding="utf-8") as f:
        if not significant_overlaps:
            msg = f"No file pairs found with overlap > {threshold:.0%}."
            logger.info(msg)
            f.write(msg + "\n")
        else:
            header = f"POTENTIAL DUPLICATES REPORT (Threshold > {threshold:.0%})"
            print("\n" + "=" * 80)
            print(header)
            print("=" * 80)

            f.write("=" * 80 + "\n")
            f.write(header + "\n")
            f.write("=" * 80 + "\n")

            for item in significant_overlaps:
                path_a = item["a"]["path"]
                path_b = item["b"]["path"]
                match_pct = max(item["ratio_a"], item["ratio_b"]) * 100

                # Console Output (Summary)
                print(f"Match: {match_pct:.1f}% overlap")
                print(f"File A: {path_a}")
                print(f"File B: {path_b}")
                print("-" * 80)

                # File Output (Detailed)
                f.write(f"Match: {match_pct:.1f}% overlap\n")
                f.write(
                    f"File A: {path_a} ({item['shared']} shared paragraphs out of {item['total_a']} total)\n"
                )
                f.write(
                    f"File B: {path_b} ({item['shared']} shared paragraphs out of {item['total_b']} total)\n"
                )
                f.write("Offending Sentences (Shared Content):\n")

                # Limit output to avoid massive files for full duplicates
                for i, line in enumerate(item["shared_lines"]):
                    if i >= 10:
                        f.write(
                            f"  ... [and {len(item['shared_lines']) - 10} more shared lines]\n"
                        )
                        break
                    # Truncate very long lines for readability
                    display_line = (line[:147] + "...") if len(line) > 150 else line
                    f.write(f"  - {display_line}\n")
                f.write("-" * 80 + "\n")

            print(f"\nFull report written to: {output_file}")


def main():
    # Default to looking for the database in the project root (parent of 'tools')
    default_db_path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "pdf_search.db"
    )

    parser = argparse.ArgumentParser(
        description="Find duplicate content across indexed PDF files."
    )
    parser.add_argument(
        "--database", "-d", default=default_db_path, help="Path to the database file"
    )
    parser.add_argument(
        "--min-length",
        type=int,
        default=40,
        help="Minimum line length to consider (default: 40)",
    )
    parser.add_argument(
        "--threshold",
        type=float,
        default=0.2,
        help="Overlap threshold (0.0 - 1.0) (default: 0.2)",
    )
    parser.add_argument(
        "--max-occurrences",
        type=int,
        default=5,
        help="Ignore lines appearing in more than this many files (boilerplate filter) (default: 5)",
    )
    parser.add_argument(
        "--output",
        "-o",
        default="duplicates_report.txt",
        help="Output text file for the report (default: duplicates_report.txt)",
    )

    args = parser.parse_args()

    try:
        analyze_duplicates(
            args.database,
            args.min_length,
            args.threshold,
            args.max_occurrences,
            args.output,
        )
    except KeyboardInterrupt:
        print("\nOperation cancelled by user.")
        sys.exit(0)


if __name__ == "__main__":
    main()
