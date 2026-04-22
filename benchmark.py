#!/usr/bin/env python3
"""
benchmark.py — Performance profiler for the PDF search engine.

Usage:
    python benchmark.py [options]

Options:
    --db PATH           Path to the SQLite database (default: pdf_search.db)
    --pdf PATH          Path to a PDF file or directory for indexing benchmarks
    --queries FILE      Path to a text file of search queries, one per line
    --suite SUITE,...   Comma-separated list of suites to run.
                        Choices: cleaning, db, search, snippet, all (default: all)
    --cleaning-pages N  Number of pages to use for the cleaning benchmark (default: 200)
    --search-runs N     Number of times to run each search query (default: 5)
    --runs N            Number of times to run the entire benchmark suite (default: 3)
    --output FILE       Write results to a JSON file for tracking over time
    --compare FILE      Compare current run against a previous JSON output
    --verbose           Print per-query and per-pass timing details

Suites:
    cleaning    Profiles clean_and_normalize_text() pass-by-pass using cProfile.
                Shows which regex passes are consuming the most time.
    db          Benchmarks SQLite connection overhead (old-style vs thread-local),
                bulk insert throughput, and query latency.
    search      Times perform_search() end-to-end, split into DB portion vs
                snippet generation. Requires --db and --queries.
    snippet     Microbenchmarks _get_full_sentence_snippet() with synthetic fixtures
                covering normal text, long dialogue, nested quotes, and cross-page stitches.

Examples:
    # Full benchmark with a real database and PDF
    python benchmark.py --db pdf_search.db --pdf ~/books/ --queries queries.txt

    # Just profile the cleaning pipeline against a specific PDF
    python benchmark.py --suite cleaning --pdf ~/books/somebook.pdf

    # Just benchmark search against an existing DB
    python benchmark.py --suite search --db pdf_search.db --queries queries.txt

    # Save results and compare against a previous baseline
    python benchmark.py --db pdf_search.db --output results_after.json --compare results_before.json
"""

import argparse
import contextlib
import cProfile
import io
import json
import os
import pstats
import re
import sqlite3
import sys
import tempfile
import time
from pathlib import Path

RE_PATH_TRIM = re.compile(r"[^/\\\\]+[/\\\\]([^/\\\\]+[/\\\\][^/\\\\]+)$")

#  Terminal colours
# Auto-detect: disable colours if stdout is not a real TTY (e.g. Windows cmd
# without VT mode, output piped to a file, or IDEs that don't emulate ANSI).
# Also respects the NO_COLOR env var (https://no-color.org).
_COLOURS_ON = (
    hasattr(sys.stdout, "isatty")
    and sys.stdout.isatty()
    and os.environ.get("NO_COLOR", "") == ""
    and os.environ.get("TERM", "").lower() != "dumb"
)

# On Windows, attempt to enable VT processing; fall back gracefully if it fails.
if _COLOURS_ON and sys.platform == "win32":
    try:
        import ctypes

        kernel32 = ctypes.windll.kernel32
        # ENABLE_VIRTUAL_TERMINAL_PROCESSING = 0x0004
        kernel32.SetConsoleMode(kernel32.GetStdHandle(-11), 7)
    except Exception:
        _COLOURS_ON = False

RESET = "\033[0m" if _COLOURS_ON else ""
BOLD = "\033[1m" if _COLOURS_ON else ""
DIM = "\033[2m" if _COLOURS_ON else ""
RED = "\033[91m" if _COLOURS_ON else ""
GREEN = "\033[92m" if _COLOURS_ON else ""
YELLOW = "\033[93m" if _COLOURS_ON else ""
BLUE = "\033[94m" if _COLOURS_ON else ""
CYAN = "\033[96m" if _COLOURS_ON else ""
WHITE = "\033[97m" if _COLOURS_ON else ""


def clr(text, *codes):
    if not _COLOURS_ON:
        return str(text)
    return "".join(codes) + str(text) + RESET


def hr(char="-", width=72, color=DIM):
    return clr(char * width, color)


def section(title):
    print()
    print(clr(f"  {title}", BOLD, CYAN))
    print(clr("  " + "-" * (len(title) + 2), DIM))


def row(label, value, unit="", note="", label_width=38):
    label_str = clr(f"  {label:<{label_width}}", WHITE)
    value_str = clr(f"{value:>10}", BOLD, GREEN)
    unit_str = clr(f" {unit:<6}", DIM)
    note_str = clr(f"  {note}", DIM, YELLOW) if note else ""
    print(f"{label_str}{value_str}{unit_str}{note_str}")


def warn(msg):
    print(clr(f"  [!] {msg}", YELLOW))


def info(msg):
    print(clr(f"  . {msg}", DIM))


def ok(msg):
    print(clr(f"  [OK] {msg}", GREEN))


def fail(msg):
    print(clr(f"  [FAIL] {msg}", RED))


#  Import guards


def _import_app_modules():
    """Import project modules, adding the script's directory to sys.path."""
    script_dir = Path(__file__).resolve().parent
    if str(script_dir) not in sys.path:
        sys.path.insert(0, str(script_dir))
    try:
        import database as db_mod
        import indexing_logic as idx_mod
        import search_logic as srch_mod
        import shared_utils as utils_mod

        return db_mod, idx_mod, srch_mod, utils_mod
    except ImportError as e:
        fail(f"Cannot import project modules: {e}")
        fail("Make sure benchmark.py lives in the same directory as database.py etc.")
        sys.exit(1)


#  Timing helpers


class Timer:
    """Context manager that records wall-clock elapsed time in seconds."""

    def __init__(self):
        self.elapsed = 0.0

    def __enter__(self):
        self._start = time.perf_counter()
        return self

    def __exit__(self, *_):
        self.elapsed = time.perf_counter() - self._start


def _repeat(fn, n=5):
    """Run fn n times, return (min, mean, max, all_times)."""
    times = []
    for _ in range(n):
        t0 = time.perf_counter()
        fn()
        times.append(time.perf_counter() - t0)
    return min(times), sum(times) / n, max(times), times


def _percentile(values, p):
    if not values:
        return 0.0
    s = sorted(values)
    idx = (len(s) - 1) * p / 100
    lo, hi = int(idx), min(int(idx) + 1, len(s) - 1)
    return s[lo] + (s[hi] - s[lo]) * (idx - lo)


#  Suite: cleaning


def bench_cleaning(db_mod, idx_mod, srch_mod, utils_mod, pdf_path, n_pages, verbose):
    section("Cleaning Pipeline  (clean_and_normalize_text)")

    # Gather sample page texts
    pages = _gather_page_texts(pdf_path, n_pages)
    if not pages:
        warn(
            "No page texts available. Provide --pdf to benchmark the cleaning pipeline."
        )
        return {}

    ok(f"Loaded {len(pages)} page samples (total {sum(len(p) for p in pages):,} chars)")

    #  1. Overall timing
    fn = idx_mod.clean_and_normalize_text

    min_t, mean_t, max_t, all_times = _repeat(lambda: [fn(p) for p in pages], n=3)
    total_chars = sum(len(p) for p in pages)

    row("Total time (mean, 3 runs)", f"{mean_t * 1000:.1f}", "ms")
    row("Per page (mean)", f"{mean_t / len(pages) * 1000:.2f}", "ms/page")
    row("Throughput", f"{total_chars / mean_t / 1000:.0f}", "k ch/s")
    row("Min / Max run", f"{min_t * 1000:.1f} / {max_t * 1000:.1f}", "ms")

    #  2. cProfile breakdown
    section("  Cleaning — cProfile breakdown (top 20 by cumulative time)")
    pr = cProfile.Profile()
    pr.enable()
    for p in pages:
        fn(p)
    pr.disable()

    buf = io.StringIO()
    ps = pstats.Stats(pr, stream=buf).sort_stats("cumulative")
    ps.print_stats(20)
    raw = buf.getvalue()

    if verbose:
        print(raw)
    else:
        # Parse and re-render the table cleanly
        _render_profile_table(raw)

    #  3. Pass-by-pass isolation
    section("  Cleaning — per-pass timing (isolated, 3 runs each)")
    passes = _build_cleaning_passes(utils_mod, idx_mod)
    pass_results = {}
    for name, fn_pass in passes:
        min_t, mean_t, _, _ = _repeat(lambda fn=fn_pass: [fn(p) for p in pages], n=3)
        row(
            f"  {name}",
            f"{mean_t * 1000:.2f}",
            "ms",
            f"({mean_t / len(pages) * 1000:.3f} ms/pg)",
        )
        pass_results[name] = mean_t

    results = {
        "pages_sampled": len(pages),
        "total_chars": total_chars,
        "mean_ms": mean_t * 1000,
        "per_page_ms": mean_t / len(pages) * 1000,
        "throughput_kchps": total_chars / mean_t / 1000,
        "passes": {k: v * 1000 for k, v in pass_results.items()},
    }
    return results


def _gather_page_texts(pdf_path, n_pages):
    """Extract up to n_pages texts from a PDF or directory of PDFs."""
    if not pdf_path:
        return []
    try:
        import fitz
    except ImportError:
        warn("PyMuPDF (fitz) not installed — cannot extract page texts.")
        return []

    pdf_files = []
    p = Path(pdf_path)
    if p.is_file() and p.suffix.lower() == ".pdf":
        pdf_files = [p]
    elif p.is_dir():
        pdf_files = sorted(p.rglob("*.pdf"))[:10]  # Limit to first 10 files

    pages = []
    for pdf in pdf_files:
        try:
            doc = fitz.open(str(pdf))
            # Sample evenly across the document, skipping first/last pages
            page_count = doc.page_count
            if page_count > 4:
                indices = [
                    int(page_count * f)
                    for f in [i / (n_pages - 1) for i in range(n_pages)]
                    if 0 <= int(page_count * f) < page_count
                ]
                indices = sorted(set(indices))
            else:
                indices = list(range(page_count))

            for i in indices:
                text = doc[i].get_text()
                if text.strip():
                    pages.append(text)
                if len(pages) >= n_pages:
                    break
            doc.close()
        except Exception:
            pass
        if len(pages) >= n_pages:
            break
    return pages[:n_pages]


def _build_cleaning_passes(utils_mod, idx_mod):
    """Return a list of (name, fn) pairs that mirror clean_and_normalize_text's steps."""
    # Each function takes a string and returns a string — isolated passes
    RE_NON_STANDARD_WHITESPACE = utils_mod.RE_NON_STANDARD_WHITESPACE
    RE_SPACED_ELLIPSIS = utils_mod.RE_SPACED_ELLIPSIS
    RE_MULTIPLE_SPACES = utils_mod.RE_MULTIPLE_SPACES
    TRANS_TABLE = utils_mod.TRANS_TABLE
    BOILERPLATE_PATTERNS = utils_mod.BOILERPLATE_PATTERNS
    GARBAGE_PATTERNS = utils_mod.GARBAGE_PATTERNS
    UNWANTED_PATTERNS = utils_mod.UNWANTED_PATTERNS
    RE_HYPHEN_FIX = idx_mod.RE_HYPHEN_FIX

    def pass_whitespace(t):
        return RE_NON_STANDARD_WHITESPACE.sub(" ", t)

    def pass_spaced_text(t):
        return idx_mod._normalize_spaced_text(t)

    def pass_unwanted(t):
        for p in UNWANTED_PATTERNS:
            t = p.sub("", t)
        return t

    def pass_boilerplate(t):
        t = RE_SPACED_ELLIPSIS.sub("...", t)
        for p in BOILERPLATE_PATTERNS:
            t = p.sub("", t)
        return t

    def pass_garbage(t):
        for p in GARBAGE_PATTERNS:
            t = p.sub("", t)
        return t

    def pass_trans_table(t):
        return t.translate(TRANS_TABLE)

    def pass_quote_spacing(t):
        t = utils_mod.RE_OPEN_QUOTE_SPACE_FIX.sub(r"\1", t)
        t = utils_mod.RE_CLOSE_QUOTE_SPACE.sub(r"\1 ", t)
        return t

    def pass_display_fixes(t):
        return utils_mod.apply_display_fixes(t)

    def pass_hyphen_fix(t):
        return RE_HYPHEN_FIX.sub(idx_mod._fix_hyphen, t)

    def pass_final_cleanup(t):
        return RE_MULTIPLE_SPACES.sub(" ", t)

    return [
        ("Whitespace normalization", pass_whitespace),
        ("Spaced-text normalizer", pass_spaced_text),
        ("Unwanted patterns (3)", pass_unwanted),
        ("Boilerplate patterns (7+)", pass_boilerplate),
        ("Garbage patterns (6)", pass_garbage),
        ("OCR + ctrl char translate", pass_trans_table),
        ("Quote spacing", pass_quote_spacing),
        ("Display fixes (contractions etc.)", pass_display_fixes),
        ("Hyphen fix", pass_hyphen_fix),
        ("Final whitespace collapse", pass_final_cleanup),
    ]


def _render_profile_table(raw):
    """Parse cProfile output and print a clean table."""
    lines = raw.splitlines()
    printed = 0
    for line in lines:
        # Skip header boilerplate
        if not line.strip() or line.startswith("   ncalls") or "function calls" in line:
            continue
        parts = line.split()
        if len(parts) >= 6 and parts[0].replace(",", "").isdigit():
            try:
                ncalls = parts[0].replace(",", "")
                tottime = float(parts[1])
                cumtime = float(parts[3])
                fname = " ".join(parts[5:])
                # Trim long paths
                fname = RE_PATH_TRIM.sub(r".../\1", fname)
                print(
                    f"  {int(ncalls):>8,}  {tottime * 1000:>8.2f}ms  {cumtime * 1000:>8.2f}ms  {fname}"
                )
                printed += 1
                if printed >= 20:
                    break
            except (ValueError, IndexError):
                pass
    if not printed:
        # Fallback: print raw
        for line in lines[5:25]:
            if line.strip():
                print(f"  {line}")


#  Suite: db


def bench_db(db_mod, idx_mod, srch_mod, utils_mod, db_path, verbose):
    section("Database  (connection overhead, write throughput, query latency)")

    #  1. Connection overhead: new connection each call vs thread-local
    section("  DB — connection overhead (100 open/PRAGMA/close cycles)")

    def _open_close():
        conn = sqlite3.connect(db_path, timeout=60.0)
        conn.execute("PRAGMA synchronous = NORMAL")
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute("PRAGMA journal_mode = WAL")
        conn.execute("PRAGMA cache_size = -200000")
        conn.execute("PRAGMA mmap_size = 30000000000")
        conn.execute("PRAGMA temp_store = MEMORY")
        conn.close()

    def _hundred_open_close():
        for _ in range(100):
            _open_close()

    min_t, mean_t, _, _ = _repeat(_hundred_open_close, n=3)
    row(
        "100× open+PRAGMA+close (mean)",
        f"{mean_t * 1000:.1f}",
        "ms",
        f"({mean_t * 10:.2f} ms/conn)",
    )

    # Simulate thread-local: reuse one connection for 100 queries
    conn = sqlite3.connect(db_path, timeout=60.0)

    def _hundred_reused():
        for _ in range(100):
            conn.execute("SELECT 1").fetchone()

    min_t2, mean_t2, _, _ = _repeat(_hundred_reused, n=3)
    conn.close()
    row(
        "100× reused connection query",
        f"{mean_t2 * 1000:.1f}",
        "ms",
        f"({mean_t2 * 10:.2f} ms/query)",
    )

    overhead_factor = mean_t / max(mean_t2, 1e-9)
    row(
        "Overhead factor (open vs reuse)",
        f"{overhead_factor:.1f}",
        "×",
        "higher = more benefit from thread-local fix",
    )

    #  2. Write throughput: bulk_insert_pages
    section("  DB — bulk insert throughput")

    # Synthetic page data: 50 and 500 page batches
    sample_text = (
        "The Emperor stood at the precipice of his golden throne, his once-mighty form "
        "now bound by the psychic matrix that kept the Astronomican burning. "
        "Ten thousand years of silent vigil had not diminished his will. "
    ) * 5  # ~340 chars

    for n_pages_batch in [50, 200, 500]:
        page_data = [(i + 1, sample_text) for i in range(n_pages_batch)]

        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            tmp_db = f.name
        try:
            db_mod.init_db(tmp_db)
            # Pre-insert a file record so we have a valid file_id
            file_id = db_mod.add_or_update_file(
                tmp_db, "bench.pdf", "bench.pdf", 0.0, "deadbeef"
            )

            min_t, mean_t, _, _ = _repeat(
                lambda fid=file_id, pd=page_data: db_mod.bulk_insert_pages(
                    tmp_db, fid, pd
                ),
                n=3,
            )
            throughput = n_pages_batch / mean_t
            row(
                f"bulk_insert_pages ({n_pages_batch:>3} pages, mean)",
                f"{mean_t * 1000:.1f}",
                "ms",
                f"({throughput:.0f} pages/s)",
            )
        finally:
            # Close the thread-local connection before unlinking — on Windows the OS
            # holds an exclusive file lock for any open connection, so unlink() fails
            # with PermissionError if the connection is still alive in thread-local storage.
            conn = getattr(db_mod._worker_local, "conn", None)
            if conn is not None:
                try:
                    conn.close()
                except Exception:
                    pass
                db_mod._worker_local.conn = None
            try:
                os.unlink(tmp_db)
            except OSError:
                pass  # Already gone or still locked — not fatal for benchmark purposes

    #  3. Query latency on the real database
    section("  DB — query latency (real database)")

    if not os.path.exists(db_path):
        warn(f"Database not found at {db_path!r} — skipping query latency tests.")
        return {}

    # Count rows so we know the size
    try:
        conn = sqlite3.connect(db_path)
        n_files = conn.execute("SELECT COUNT(*) FROM files").fetchone()[0]
        n_pages = conn.execute("SELECT COUNT(*) FROM pdf_text_fts").fetchone()[0]
        conn.close()
        info(f"Database: {n_files:,} files, {n_pages:,} indexed pages")
    except Exception:
        n_files, n_pages = 0, 0

    # get_indexed_files — full table scan (used on index page)
    min_t, mean_t, _, _ = _repeat(lambda: db_mod.get_indexed_files(db_path), n=5)
    row("get_indexed_files (full scan)", f"{mean_t * 1000:.2f}", "ms")

    # get_unique_folders
    min_t, mean_t, _, _ = _repeat(lambda: db_mod.get_unique_folders(db_path), n=5)
    row("get_unique_folders", f"{mean_t * 1000:.2f}", "ms")

    # check_db_has_content
    min_t, mean_t, _, _ = _repeat(lambda: db_mod.check_db_has_content(db_path), n=10)
    row("check_db_has_content (LIMIT 1)", f"{mean_t * 1000:.2f}", "ms")

    results = {
        "connection_overhead_ms": mean_t * 1000,
        "n_files": n_files,
        "n_pages": n_pages,
    }
    return results


#  Suite: search


def bench_search(
    db_mod, idx_mod, srch_mod, utils_mod, db_path, queries, n_runs, verbose
):
    section("Search  (perform_search end-to-end, DB vs snippet split)")

    if not os.path.exists(db_path):
        warn(f"Database not found at {db_path!r} — skipping search benchmarks.")
        return {}

    if not queries:
        queries = _default_queries()
        info(f"No --queries file provided. Using {len(queries)} built-in test queries.")

    # Attempt to count pages for context
    try:
        conn = sqlite3.connect(db_path)
        n_files = conn.execute("SELECT COUNT(*) FROM files").fetchone()[0]
        n_pages = conn.execute("SELECT COUNT(*) FROM pdf_text_fts").fetchone()[0]
        conn.close()
        info(f"Database: {n_files:,} files, {n_pages:,} indexed pages")
    except Exception:
        pass

    all_results = []
    per_query = []

    for query in queries:
        times = []
        db_times = []
        snippet_times = []
        result_count = 0

        for _ in range(n_runs):
            # Instrument perform_search by timing the DB call and the snippet loop separately
            t_total_start = time.perf_counter()

            # We time perform_search as a whole
            results, has_more, error, terms = srch_mod.perform_search(
                db_path, query, limit=20, offset=0
            )
            t_total = time.perf_counter() - t_total_start
            times.append(t_total)
            result_count = len(results) if results else 0

            # Approximate DB-only time by re-running just the SQL portion
            t_db = _time_search_db_only(db_path, query, utils_mod)
            db_times.append(t_db)
            snippet_times.append(max(0.0, t_total - t_db))

        mean_total = sum(times) / len(times)
        mean_db = sum(db_times) / len(db_times)
        mean_snippet = sum(snippet_times) / len(snippet_times)
        p95 = _percentile(times, 95)

        per_query.append(
            {
                "query": query,
                "results": result_count,
                "mean_ms": mean_total * 1000,
                "db_ms": mean_db * 1000,
                "snippet_ms": mean_snippet * 1000,
                "p95_ms": p95 * 1000,
            }
        )
        all_results.extend(times)

        if verbose or len(queries) <= 10:
            q_display = query[:40].ljust(42)
            row(
                f"  {q_display}",
                f"{mean_total * 1000:.1f}",
                "ms",
                f"({result_count} books, db={mean_db * 1000:.1f}ms snip={mean_snippet * 1000:.1f}ms)",
            )

    section("  Search — aggregate stats")
    if all_results:
        global_mean = sum(all_results) / len(all_results)
        global_p50 = _percentile(all_results, 50)
        global_p95 = _percentile(all_results, 95)
        global_p99 = _percentile(all_results, 99)
        global_min = min(all_results)
        global_max = max(all_results)

        mean_db_all = sum(q["db_ms"] for q in per_query) / len(per_query)
        mean_snippet_all = sum(q["snippet_ms"] for q in per_query) / len(per_query)

        row("Mean latency", f"{global_mean * 1000:.1f}", "ms")
        row("p50 latency", f"{global_p50 * 1000:.1f}", "ms")
        row("p95 latency", f"{global_p95 * 1000:.1f}", "ms")
        row("p99 latency", f"{global_p99 * 1000:.1f}", "ms")
        row("Min / Max", f"{global_min * 1000:.1f} / {global_max * 1000:.1f}", "ms")
        row("DB time (mean)", f"{mean_db_all:.1f}", "ms", "FTS5 query + JOIN")
        row(
            "Snippet time (mean)", f"{mean_snippet_all:.1f}", "ms", "per 20-result page"
        )

        # Slowest queries
        if verbose and len(per_query) > 1:
            section("  Search — slowest 5 queries")
            for q in sorted(per_query, key=lambda x: -x["mean_ms"])[:5]:
                row(f"  {q['query'][:44]}", f"{q['mean_ms']:.1f}", "ms")

    return {
        "queries_run": len(queries),
        "runs_each": n_runs,
        "mean_ms": global_mean * 1000,
        "p50_ms": global_p50 * 1000,
        "p95_ms": global_p95 * 1000,
        "p99_ms": global_p99 * 1000,
        "db_mean_ms": mean_db_all,
        "snippet_mean_ms": mean_snippet_all,
        "per_query": per_query,
    }


def _time_search_db_only(db_path, query, utils_mod):
    """Time the SQL portion of a search query without snippet generation."""
    try:
        # Normalize smart quotes
        q = query.replace("\u201c", '"').replace("\u201d", '"')
        raw_terms = utils_mod.RE_QUERY_TERMS.findall(q)
        search_terms = []
        for term in raw_terms:
            if not term.startswith('"') and term.upper() in {
                "AND",
                "OR",
                "NOT",
                "NEAR",
            }:
                term = f'"{term}"'
            search_terms.append(term)
        sql_query = " AND ".join(search_terms)

        conn = sqlite3.connect(db_path)
        t0 = time.perf_counter()
        conn.execute(
            "SELECT file_id, COUNT(*) FROM pdf_text_fts WHERE pdf_text_fts MATCH ? GROUP BY file_id",
            (sql_query,),
        ).fetchall()
        elapsed = time.perf_counter() - t0
        conn.close()
        return elapsed
    except Exception:
        return 0.0


def _default_queries():
    return [
        "the emperor",
        "battle",
        "chapter one",
        "death",
        '"in the beginning"',
        "space marine",
        "war AND peace",
        "darkness light shadow",
        "NOT magic",
        "sword shield armor battle",
    ]


#  Suite: snippet


def bench_snippet(db_mod, idx_mod, srch_mod, utils_mod, verbose):
    section("Snippet Generation  (_get_full_sentence_snippet)")

    fn = srch_mod._get_full_sentence_snippet

    fixtures = _build_snippet_fixtures()
    all_times = []
    fixture_results = {}

    for name, text, terms in fixtures:
        min_t, mean_t, max_t, times = _repeat(lambda t=text, tr=terms: fn(t, tr), n=200)
        all_times.extend(times)
        row(
            f"  {name}",
            f"{mean_t * 1000:.3f}",
            "ms",
            f"(min={min_t * 1000:.3f} max={max_t * 1000:.3f})",
        )
        fixture_results[name] = mean_t * 1000

    section("  Snippet — SENT_BOUNDARY regex isolated")
    # Benchmark just the sentence boundary regex since it's the suspected bottleneck
    from search_logic import SENT_BOUNDARY

    sample_texts = [text for _, text, _ in fixtures]
    min_t, mean_t, _, _ = _repeat(
        lambda: [list(SENT_BOUNDARY.finditer(t)) for t in sample_texts], n=200
    )
    row("  SENT_BOUNDARY.finditer (all fixtures)", f"{mean_t * 1000:.3f}", "ms")

    overall_mean = sum(all_times) / len(all_times)
    row("Overall mean (all fixtures)", f"{overall_mean * 1000:.3f}", "ms")

    return {
        "overall_mean_ms": overall_mean * 1000,
        "fixtures": fixture_results,
        "sent_boundary_ms": mean_t * 1000,
    }


def _build_snippet_fixtures():
    """Return (name, text, search_terms) tuples for snippet benchmarks."""

    normal = (
        "The primarch raised his hand and the chamber fell silent. "
        "His warriors, clad in the blue-grey of their Legion, stood at attention. "
        "The battle had been long and costly. Many had fallen in the Emperor's name. "
        "But they had prevailed. The fortress was theirs. "
        "Tomorrow they would push further into the traitor's territory. "
        "Tonight, they would mourn their dead. "
    ) * 3

    long_dialogue = (
        '"I have seen the warp tear open and swallow entire battlegroups," the old warrior said. '
        '"I have watched fortress worlds fall in a single night. '
        'I have seen things that would shatter a lesser mind." '
        "He paused, letting the words settle over the assembled warriors. "
        '"And yet here I stand. Do you know why?" '
        "No one spoke. The firelight guttered. "
        '"Because faith is not a shield. It is a weapon. '
        'The greatest weapon ever forged." '
        "He turned and walked from the room without another word. "
        "Behind him, the silence stretched on for a very long time. "
    ) * 2

    nested_quotes = (
        "The interrogator leaned forward. "
        '"He told me," she said, "the password is blood and iron." '
        "I didn't believe him at first. "
        "\"Then he said, 'Every secret has its price,' and that was when I knew he was serious.\" "
        "The room was very quiet after that. "
        "Outside, rain hammered the windows of the precinct house. "
    )

    cross_page_stitch = (
        "The last page of chapter twelve ended mid-sentence, as if the author had been "
        "called away suddenly. The narrative resumed here, on what was notionally a new "
        "page in the index, with the protagonist still mid-stride through the ruined city. "
        "Rubble crunched under his boots. The air tasted of ash and promethium. "
        "Somewhere ahead, through the smoke, an autocannon was firing in three-round bursts. "
        "He counted the intervals. Regular. Disciplined. Not orks, then. "
    ) + normal[:200]

    short = "He died. The end."

    long_no_sentences = "word " * 300 + "target " + "word " * 300

    return [
        ("Normal prose (3× para)", normal, ["battle", "Emperor"]),
        ("Long dialogue (2× para)", long_dialogue, ["faith", "weapon"]),
        ("Nested quotes", nested_quotes, ["password", "secret"]),
        ("Cross-page stitch (long)", cross_page_stitch, ["rubble", "autocannon"]),
        ("Short text (edge case)", short, ["died"]),
        ("Long text, no sentence boundaries", long_no_sentences, ["target"]),
    ]


#  Comparison


def _compare_results(current, previous_path):
    """Print a before/after comparison table."""
    try:
        with open(previous_path) as f:
            previous = json.load(f)
    except Exception as e:
        warn(f"Could not load comparison file: {e}")
        return

    section("Comparison  (current vs previous)")

    def _diff_row(label, curr_val, prev_val, unit="ms", lower_is_better=True):
        if prev_val is None or prev_val == 0:
            row(label, f"{curr_val:.1f}", unit, "(no baseline)")
            return
        delta = curr_val - prev_val
        pct = delta / prev_val * 100
        direction = "v" if delta < 0 else "^"
        if lower_is_better:
            color = GREEN if delta < 0 else (RED if delta > 0 else "")
        else:
            color = GREEN if delta > 0 else (RED if delta < 0 else "")
        note = clr(
            f"{direction} {abs(pct):.1f}%  ({prev_val:.1f} -> {curr_val:.1f})", color
        )
        print(f"  {'  ' + label:<40}{curr_val:>10.1f} {unit:<6}  {note}")

    def _get(d, *keys, default=None):
        for k in keys:
            if isinstance(d, dict) and k in d:
                d = d[k]
            else:
                return default
        return d

    if "cleaning" in current and "cleaning" in previous:
        print(clr("\n  cleaning:", BOLD))
        _diff_row(
            "mean_ms",
            _get(current, "cleaning", "mean_ms", default=0),
            _get(previous, "cleaning", "mean_ms"),
        )
        _diff_row(
            "per_page_ms",
            _get(current, "cleaning", "per_page_ms", default=0),
            _get(previous, "cleaning", "per_page_ms"),
        )

    if "search" in current and "search" in previous:
        print(clr("\n  search:", BOLD))
        _diff_row(
            "mean_ms",
            _get(current, "search", "mean_ms", default=0),
            _get(previous, "search", "mean_ms"),
        )
        _diff_row(
            "p95_ms",
            _get(current, "search", "p95_ms", default=0),
            _get(previous, "search", "p95_ms"),
        )
        _diff_row(
            "db_mean_ms",
            _get(current, "search", "db_mean_ms", default=0),
            _get(previous, "search", "db_mean_ms"),
        )
        _diff_row(
            "snippet_ms",
            _get(current, "search", "snippet_mean_ms", default=0),
            _get(previous, "search", "snippet_mean_ms"),
        )

    if "snippet" in current and "snippet" in previous:
        print(clr("\n  snippet:", BOLD))
        _diff_row(
            "overall_mean_ms",
            _get(current, "snippet", "overall_mean_ms", default=0),
            _get(previous, "snippet", "overall_mean_ms"),
        )
        _diff_row(
            "sent_boundary_ms",
            _get(current, "snippet", "sent_boundary_ms", default=0),
            _get(previous, "snippet", "sent_boundary_ms"),
        )


def _aggregate_benchmark_runs(runs):
    """Deep-merge multiple result dictionaries by averaging numerical leaf values."""
    if not runs:
        return {}

    def _avg(vals):
        vals = [v for v in vals if v is not None]
        return sum(vals) / len(vals) if vals else 0.0

    def _recursive_aggregate(items):
        if not items:
            return None
        first = items[0]

        if isinstance(first, (int, float)) and not isinstance(first, bool):
            return _avg(items)
        elif isinstance(first, dict):
            keys = set()
            for item in items:
                if isinstance(item, dict):
                    keys.update(item.keys())
            res = {}
            for k in keys:
                res[k] = _recursive_aggregate(
                    [item.get(k) for item in items if item is not None]
                )
            return res
        elif isinstance(first, list):
            # For lists, we only average if they are lists of dicts with numerical fields
            # or if we can maintain structural alignment. For simplicity in benchmark results:
            if (
                all(isinstance(x, list) for x in items)
                and len(set(len(x) for x in items)) == 1
            ):
                return [
                    _recursive_aggregate([run[i] for run in items])
                    for i in range(len(first))
                ]
            return first  # Fallback
        return first

    # We start aggregation from the first run's structure
    base = runs[0].copy()
    # Fields to keep from first run only
    meta = {"timestamp": base.get("timestamp"), "db": base.get("db")}

    aggregated = _recursive_aggregate(runs)
    if aggregated:
        aggregated.update(meta)
    return aggregated


#  Main


def main():
    parser = argparse.ArgumentParser(
        description="Benchmark tool for the PDF search engine.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("--db", default="pdf_search.db", help="Path to SQLite database")
    parser.add_argument(
        "--pdf", default=None, help="PDF file or directory for cleaning benchmarks"
    )
    parser.add_argument(
        "--queries", default=None, help="Text file of search queries (one per line)"
    )
    parser.add_argument(
        "--suite",
        default="all",
        help="Comma-separated suites: cleaning,db,search,snippet,all",
    )
    parser.add_argument(
        "--cleaning-pages",
        type=int,
        default=200,
        help="Pages to sample for cleaning benchmark",
    )
    parser.add_argument(
        "--search-runs", type=int, default=5, help="Runs per query for search benchmark"
    )
    parser.add_argument(
        "--runs",
        type=int,
        default=3,
        help="Number of times to run the entire benchmark suite for averaging",
    )
    parser.add_argument(
        "--output", default=None, help="Write JSON results to this file"
    )
    parser.add_argument(
        "--compare", default=None, help="Compare against a previous JSON output"
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Print detailed per-query / per-pass output",
    )
    args = parser.parse_args()

    #  Header
    print()
    print(
        clr(
            "  +------------------------------------------------------------------+",
            CYAN,
        )
    )
    print(
        clr(
            "  |         PDF Search Engine — Performance Benchmark Tool           |",
            BOLD,
            CYAN,
        )
    )
    print(
        clr(
            "  +------------------------------------------------------------------+",
            CYAN,
        )
    )
    print()
    print(clr("  database:  ", DIM) + clr(args.db, WHITE))
    print(
        clr("  pdf path:  ", DIM)
        + clr(str(args.pdf or "(none — cleaning suite will be skipped)"), WHITE)
    )
    print(clr("  suites:    ", DIM) + clr(args.suite, WHITE))
    print(clr("  timestamp: ", DIM) + clr(time.strftime("%Y-%m-%d %H:%M:%S"), WHITE))

    #  Imports
    db_mod, idx_mod, srch_mod, utils_mod = _import_app_modules()

    suites_raw = [s.strip().lower() for s in args.suite.split(",")]
    run_all = "all" in suites_raw

    def run(s):
        return run_all or s in suites_raw

    #  Load queries
    queries = None
    if args.queries:
        try:
            with open(args.queries) as f:
                queries = [
                    line.strip()
                    for line in f
                    if line.strip() and not line.startswith("#")
                ]
            info(f"Loaded {len(queries)} queries from {args.queries}")
        except Exception as e:
            warn(f"Could not load queries file: {e}")

    #  Run suites
    all_runs_results = []

    for run_idx in range(args.runs):
        is_last = run_idx == args.runs - 1
        if args.runs > 1:
            print(
                clr(f"  [ RUN {run_idx + 1} / {args.runs} ]", BOLD, YELLOW),
                end="\r",
                flush=True,
            )

        run_results = {"timestamp": time.strftime("%Y-%m-%dT%H:%M:%S"), "db": args.db}

        # Create a context that suppresses output for intermediate runs
        out_ctx = (
            contextlib.nullcontext()
            if is_last
            else contextlib.redirect_stdout(io.StringIO())
        )

        with out_ctx:
            if run("cleaning"):
                r = bench_cleaning(
                    db_mod,
                    idx_mod,
                    srch_mod,
                    utils_mod,
                    args.pdf,
                    args.cleaning_pages,
                    args.verbose,
                )
                run_results["cleaning"] = r

            if run("db"):
                r = bench_db(
                    db_mod, idx_mod, srch_mod, utils_mod, args.db, args.verbose
                )
                run_results["db_bench"] = r

            if run("search"):
                r = bench_search(
                    db_mod,
                    idx_mod,
                    srch_mod,
                    utils_mod,
                    args.db,
                    queries,
                    args.search_runs,
                    args.verbose,
                )
                run_results["search"] = r

            if run("snippet"):
                r = bench_snippet(db_mod, idx_mod, srch_mod, utils_mod, args.verbose)
                run_results["snippet"] = r

        all_runs_results.append(run_results)

    if args.runs > 1:
        print(" " * 40, end="\r")  # Clear the [ RUN X/N ] line

    #  Aggregate results if multiple runs
    if args.runs > 1:
        all_results = _aggregate_benchmark_runs(all_runs_results)
        section("Averaged Results (over %d runs)" % args.runs)
        # Re-print the comparison or summary if needed, but for now we focus on the data.
    else:
        all_results = all_runs_results[0]

    #  Comparison
    if args.compare:
        _compare_results(all_results, args.compare)

    #  JSON output
    if args.output:
        try:
            with open(args.output, "w") as f:
                json.dump(all_results, f, indent=2)
            print()
            ok(f"Results written to {args.output}")
        except Exception as e:
            warn(f"Could not write output file: {e}")

    #  Footer
    print()
    print(hr())
    print()


if __name__ == "__main__":
    main()
