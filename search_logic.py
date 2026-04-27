import logging
import html
import re
import sqlite3
from collections import defaultdict
from functools import lru_cache
import database as db
from shared_utils import (
    natural_sort_key,
    TRANS_TABLE,
    RE_EXCESS_SPACES,
    RE_TRAILING_SENTENCE_PUNCT,
    RE_QUERY_TERMS,
    SENT_BOUNDARY,
    RE_HYPHEN_FIX,
    _fix_hyphen,
)

logger = logging.getLogger(__name__)


class SearchResults(list):
    def __init__(self, data, total_books=0, total_pages=0):
        super().__init__(data)
        self.total_books = total_books
        self.total_pages = total_pages


# --- Constants & Configuration ---

# How close (in chars) a match must be to a page edge for stitching to be worthwhile.
_STITCH_BOUNDARY_CHARS = 400
# Maximum number of rich snippets to generate per book on initial search.
SNIPPETS_PER_BOOK_INITIAL = 3
# Target character budget for snippet windows.
_TARGET_SNIPPET_CHARS = 600

RE_MULTIPLE_QUOTES = re.compile(r'"+')


# --- Cache & Pattern Compilation ---


@lru_cache(maxsize=1024)
def _get_cleaned_text(page_text: str) -> str:
    if not page_text:
        return ""
    # Text in the database is already cleaned (translate + apply_display_fixes).
    # We only need to escape it for safe HTML display.
    return html.escape(page_text, quote=False)


@lru_cache(maxsize=4096)
def _cached_snippet(
    text: str, terms_tuple: tuple, highlight_patterns: tuple, min_match_pos: int = 0
) -> str:
    cleaned_text = _get_cleaned_text(text)
    return _get_full_sentence_snippet(
        cleaned_text, list(terms_tuple), min_match_pos, list(highlight_patterns)
    )


@lru_cache(maxsize=256)
def _compile_highlight_patterns(terms_tuple):
    patterns = []
    for term in terms_tuple:
        clean = term.strip('"').translate(TRANS_TABLE)
        clean = html.escape(clean, quote=False)
        if not clean:
            continue
        prefix_b = r"\b" if clean[0].isalnum() else ""
        suffix_b = r"\b" if clean[-1].isalnum() else ""
        # Restricted spacer: matches spaces, punctuation, etc. (allowed to cross newlines)
        # Length limited to 15 to catch common formatting but avoid massive "whack" spans.
        spacer = r"[^\w]{0,15}"
        if clean.endswith("*") and len(clean) > 1:
            clean_escaped = (
                re.escape(clean[:-1])
                .replace(r"\ ", spacer)
                .replace(r"\'", r"['\u2019]\s*")
            )
            patterns.append(re.compile(r"(\b" + clean_escaped + r"\w*)", re.IGNORECASE))
        else:
            clean_escaped = (
                re.escape(clean).replace(r"\ ", spacer).replace(r"\'", r"['\u2019]\s*")
            )
            patterns.append(
                re.compile(f"({prefix_b}{clean_escaped}{suffix_b})", re.IGNORECASE)
            )
    return tuple(patterns)


@lru_cache(maxsize=256)
def _compile_match_probes(terms_tuple):
    probes = []
    spacer = r"[^\w]{0,15}"
    for term in terms_tuple:
        clean = term.strip('"').translate(TRANS_TABLE).lower()
        if not clean:
            continue
        if clean.endswith("*"):
            clean_escaped = (
                re.escape(clean[:-1])
                .replace(r"\ ", spacer)
                .replace(r"\'", r"['\u2019]\s*")
            )
            probes.append(re.compile(r"\b" + clean_escaped))
        else:
            prefix_b = r"\b" if clean[0].isalnum() else ""
            suffix_b = r"\b" if clean[-1].isalnum() else ""
            clean_escaped = (
                re.escape(clean).replace(r"\ ", spacer).replace(r"\'", r"['\u2019]\s*")
            )
            probes.append(re.compile(f"{prefix_b}{clean_escaped}{suffix_b}"))
    return tuple(probes)


@lru_cache(maxsize=256)
def _compile_offset_patterns(terms_tuple):
    compiled = []
    spacer = r"[^\w]{0,15}"
    for term in terms_tuple:
        clean = term.strip('"').strip().translate(TRANS_TABLE).lower()
        if not clean:
            continue
        term_pats = []
        if clean.endswith("*") and len(clean) > 1:
            prefix = clean[:-1]
            clean_escaped = (
                re.escape(prefix).replace(r"\ ", spacer).replace(r"\'", r"['\u2019]\s*")
            )
            term_pats.append(re.compile(r"\b" + clean_escaped))
        else:
            variations = [clean]
            if "-" in clean:
                variations.append(clean.replace("-", ""))
                variations.append(clean.replace("-", " "))
            for var in variations:
                prefix_b = r"\b" if var and var[0].isalnum() else ""
                suffix_b = r"\b" if var and var[-1].isalnum() else ""
                clean_escaped = (
                    re.escape(var)
                    .replace(r"\ ", spacer)
                    .replace(r"\'", r"['\u2019]\s*")
                )
                term_pats.append(re.compile(f"{prefix_b}{clean_escaped}{suffix_b}"))
        compiled.append(tuple(term_pats))
    return tuple(compiled)



# --- Relevance Filtering ---


def _is_similar_snippet(s1, s2):
    if not s1 or not s2:
        return False
    c1, c2 = (
        s1.replace("<b>", "").replace("</b>", ""),
        s2.replace("<b>", "").replace("</b>", ""),
    )
    if c1 in c2 or c2 in c1:
        return True
    words1, words2 = set(c1.lower().split()), set(c2.lower().split())
    if not words1 or not words2:
        return False
    overlap = len(words1.intersection(words2))
    # Threshold lowered from 0.6 to 0.45 to account for 300-char overlap in 600-char windows.
    # An overlap of 50% is common due to indexing logic, and should be considered a duplicate.
    return overlap / max(len(words1), len(words2)) > 0.45


# --- Page Stitching ---


def _merge_overlapping_pages(prev_text, next_text):
    if not next_text:
        return prev_text, 0
    if not prev_text:
        return next_text, 0
    prefix, suffix = next_text[: min(50, len(next_text))], prev_text[-400:]
    idx = suffix.find(prefix)
    if idx != -1:
        clean_prev = prev_text[: len(prev_text) - len(suffix) + idx]
        combined = clean_prev + next_text
    else:
        combined = prev_text + " " + next_text

    healed = RE_HYPHEN_FIX.sub(_fix_hyphen, combined)
    return healed, len(healed) - len(next_text)


# --- Snippet Generation ---


def _find_match_offset(lower_text, compiled_patterns, min_match_pos):
    match_pos = None
    for term_pats in compiled_patterns:
        for pattern in term_pats:
            m = pattern.search(lower_text, min_match_pos)
            if m:
                pos = m.start()
                if match_pos is None or pos < match_pos:
                    match_pos = pos
                break
    return match_pos if match_pos is not None else min_match_pos


def _calculate_sentence_window(para_text, rel_match):
    if rel_match is None:
        end_match = SENT_BOUNDARY.search(para_text)
        return 0, (end_match.end() if end_match else 300)
    ends = [m.end() for m in SENT_BOUNDARY.finditer(para_text)]
    if not ends or ends[-1] != len(para_text):
        ends.append(len(para_text))
    starts = [0] + ends[:-1]
    match_idx = 0
    for i, (s, e) in enumerate(zip(starts, ends)):
        if s <= rel_match < e:
            match_idx = i
            break
    l_idx, r_idx = match_idx, match_idx
    while True:
        expanded = False
        if l_idx > 0 and (ends[r_idx] - starts[l_idx - 1]) <= _TARGET_SNIPPET_CHARS:
            l_idx -= 1
            expanded = True
        if (
            r_idx < len(ends) - 1
            and (ends[r_idx + 1] - starts[l_idx]) <= _TARGET_SNIPPET_CHARS
        ):
            r_idx += 1
            expanded = True
        if not expanded:
            break
    # Final cleanup: skip any leading fragments (starts with lowercase)
    # UNLESS it's the match segment itself (which we can't skip entirely).
    while l_idx < match_idx:
        seg = para_text[starts[l_idx] : ends[l_idx]].lstrip()
        if seg and seg[0].islower():
            l_idx += 1
        else:
            break
    while r_idx > match_idx:
        seg = para_text[starts[r_idx] : ends[r_idx]].strip()
        if not RE_TRAILING_SENTENCE_PUNCT.search(seg):
            r_idx -= 1
            continue
        has_open = any(c in seg for c in "“‘")
        has_close = any(c in seg for c in "”’")
        if (has_open and not has_close) or (seg.count('"') % 2 != 0):
            r_idx -= 1
            continue
        break
    window_start, window_end = starts[l_idx], ends[r_idx]

    # Hard safety limit for unstructured text (e.g. giant stat blocks without punctuation)
    # If the window is more than 2x the target, forcefully truncate it around the match.
    if (window_end - window_start) > (
        _TARGET_SNIPPET_CHARS * 2
    ) and rel_match is not None:
        half_budget = _TARGET_SNIPPET_CHARS // 2
        # Try to align with the original paragraph boundaries if possible
        window_start = max(window_start, rel_match - half_budget)
        window_end = min(window_end, rel_match + half_budget)

    return window_start, window_end


def _expand_quoted_dialogue(para_text, window_start, window_end, rel_match):
    if rel_match is None:
        return window_start, window_end
    for q_start, q_end in [('"', '"'), ("'", "'"), ("“", "”"), ("‘", "’")]:
        curr, open_pos = rel_match, -1
        while True:
            curr = para_text.rfind(q_start, 0, curr)
            if curr == -1:
                break
            is_opener = False
            if q_start in ["“", "‘"] or curr == 0:
                is_opener = True
            elif not para_text[curr - 1].isalnum():
                if para_text[curr - 1] not in ".,!?;":
                    is_opener = True
                elif curr + 1 < len(para_text) and para_text[curr + 1].isalnum():
                    is_opener = True
            if is_opener:
                is_closed, search_pos = False, curr + 1
                while search_pos < rel_match:
                    next_q = para_text.find(q_end, search_pos, rel_match)
                    if next_q == -1:
                        break
                    if (
                        q_end in ["”", "’"]
                        or (next_q > 0 and para_text[next_q - 1] in ".,!?;")
                        or (
                            next_q + 1 >= len(para_text)
                            or not para_text[next_q + 1].isalnum()
                        )
                    ):
                        is_closed = True
                        break
                    search_pos = next_q + 1
                if not is_closed:
                    open_pos = curr
                    break
        if open_pos != -1:
            close_pos, search_pos = -1, rel_match
            while True:
                next_q = para_text.find(q_end, search_pos)
                if next_q == -1:
                    break
                if (
                    q_end in ["”", "’"]
                    or (next_q > 0 and para_text[next_q - 1] in ".,!?;")
                    or (
                        next_q + 1 >= len(para_text)
                        or not para_text[next_q + 1].isalnum()
                    )
                ):
                    close_pos = next_q
                    break
                search_pos = next_q + 1
            if (
                close_pos != -1
                and len(
                    list(SENT_BOUNDARY.finditer(para_text[open_pos : close_pos + 1]))
                )
                <= 12
            ):
                window_start, window_end = (
                    min(window_start, open_pos),
                    max(window_end, close_pos + 1),
                )
    return window_start, window_end


def _apply_html_highlights(snippet, highlight_patterns):
    if highlight_patterns:
        for pattern in highlight_patterns:
            snippet = pattern.sub(r"<b>\g<1></b>", snippet)
    return snippet


def _get_full_sentence_snippet(
    cleaned_text, search_terms, min_match_pos=0, highlight_patterns=None
):
    if not cleaned_text:
        return ""
    if highlight_patterns is None:
        highlight_patterns = _compile_highlight_patterns(
            tuple(t.strip('"') for t in search_terms)
        )
    lower_text = cleaned_text.lower()
    offset_patterns = _compile_offset_patterns(tuple(search_terms))
    match_pos = _find_match_offset(lower_text, offset_patterns, min_match_pos)
    window_start, window_end = _calculate_sentence_window(cleaned_text, match_pos)
    window_start, window_end = _expand_quoted_dialogue(
        cleaned_text, window_start, window_end, match_pos
    )
    snippet = cleaned_text[window_start:window_end].strip()
    snippet = RE_EXCESS_SPACES.sub(" ", snippet)
    return _apply_html_highlights(snippet, highlight_patterns)


def get_snippet_for_page(db_name, file_id, page_num, search_query):
    try:
        # Fetch the target page and its immediate neighbours in a single indexed query.
        # pages has PRIMARY KEY (file_id, page_num): each point lookup is O(log n).
        rows = db.query_db(
            db_name,
            """SELECT pg.page_num, t.text
               FROM pages pg
               JOIN pdf_text_fts t ON t.rowid = pg.rowid_fts
               WHERE pg.file_id = ? AND pg.page_num IN (?, ?, ?)""",
            (file_id, page_num - 1, page_num, page_num + 1),
        )
        page_map = {r[0]: r[1] for r in rows}
        if page_num not in page_map:
            return None
        page_text = page_map[page_num]
        search_query = search_query.replace("“", '"').replace("”", '"')
        raw_terms = RE_QUERY_TERMS.findall(search_query)
        highlight_terms = [t.strip('"') for t in raw_terms if t.strip('"')]
        matchers = _compile_match_probes(tuple(highlight_terms))
        approx_pos = next(
            (
                m.start()
                for pattern in matchers
                if (m := pattern.search(page_text.lower()))
            ),
            None,
        )
        full_text, match_offset = page_text, approx_pos or 0
        if page_num - 1 in page_map and (
            approx_pos is None or approx_pos < _STITCH_BOUNDARY_CHARS
        ):
            merged, start_of_new = _merge_overlapping_pages(
                page_map[page_num - 1], full_text
            )
            full_text = merged
            if approx_pos is not None:
                match_offset = start_of_new + approx_pos
            else:
                match_offset = start_of_new
        if page_num + 1 in page_map and (
            approx_pos is None or (len(page_text) - approx_pos) < _STITCH_BOUNDARY_CHARS
        ):
            full_text, _ = _merge_overlapping_pages(full_text, page_map[page_num + 1])
        highlight_patterns = _compile_highlight_patterns(tuple(highlight_terms))
        return _cached_snippet(
            full_text, tuple(highlight_terms), highlight_patterns, match_offset
        )
    except Exception as e:
        logger.error(f"Error on-demand snippet: {e}")
        return None


# --- Main Search Logic ---


def perform_search(
    db_name,
    search_query,
    limit=20,
    offset=0,
    selected_folders=None,
    sort_by="filename",
    max_matches_per_book=None,
):
    try:
        search_query = search_query.replace("“", '"').replace("”", '"')
        search_query = RE_MULTIPLE_QUOTES.sub('"', search_query)
        raw_terms = RE_QUERY_TERMS.findall(search_query)
        # Filter out empty terms and decorative empty quotes like ""
        raw_terms = [t for t in raw_terms if t.replace('"', "").strip()]
        if not raw_terms:
            return [], False, None, []
        search_terms, highlight_terms = [], []
        for term in raw_terms:
            # For highlighting, we must use the unquoted version for the regex to match
            h_term = term[1:-1] if term.startswith('"') and term.endswith('"') else term

            if not term.startswith('"'):
                if term.upper() in {"AND", "OR", "NOT", "NEAR"}:
                    term = f'"{term}"'
                elif term.startswith("*"):
                    term = term.lstrip("*")
                    if not term:
                        continue
            if term.startswith('"') and term.endswith('*"') and len(term) > 3:
                term = term[1:-2] + "*"
            if "-" in term and not term.startswith('"') and not term.startswith("-"):
                search_terms.append(f'("{term}" OR "{term.replace("-", "")}")')
                highlight_terms.extend([term, term.replace("-", "")])
            elif "'" in term and not term.startswith('"'):
                smart = term.replace("'", "’")
                search_terms.append(
                    f'("{term}" OR "{smart}")' if smart != term else f'"{term}"'
                )
                highlight_terms.append(h_term)
            else:
                search_terms.append(term)
                highlight_terms.append(h_term)
        highlight_terms.sort(key=len, reverse=True)
        sql_search_query = " AND ".join(search_terms)
        folder_clause, folder_params = "", []
        if selected_folders:
            conditions = []
            for folder in selected_folders:
                if folder == "(Root)":
                    conditions.append("(f.relative_path NOT LIKE '%/%')")
                else:
                    conditions.append("(f.relative_path LIKE ?)")
                    folder_params.append(f"{folder.replace('\\', '/')}/%")
            folder_clause = "AND (" + " OR ".join(conditions) + ")"
        sql_books = f"""
            SELECT pg.file_id, f.filename, f.relative_path, COUNT(*) as match_count 
            FROM pdf_text_fts t
            JOIN pages pg ON t.rowid = pg.rowid_fts
            JOIN files f ON pg.file_id = f.id 
            WHERE t.pdf_text_fts MATCH ? {folder_clause}
            GROUP BY pg.file_id
        """
        all_book_rows = db.query_db(
            db_name, sql_books, tuple([sql_search_query] + folder_params)
        )
        all_book_rows.sort(
            key=lambda x: (
                (-x[3], natural_sort_key(x[1]))
                if sort_by == "relevance"
                else natural_sort_key(x[1])
            )
        )
        total_books, total_pages = (
            len(all_book_rows),
            sum(row[3] for row in all_book_rows),
        )
        has_more = (offset + limit) < total_books
        book_rows = all_book_rows[offset : offset + limit]
        if not book_rows:
            return [], False, None, search_terms
        target_file_ids = [r[0] for r in book_rows]
        book_total_map = {r[0]: r[3] for r in book_rows}
        sql_pages = f"""
            SELECT p.rowid, p.file_id, f.filename, f.relative_path, p.page_num, 
                   CASE WHEN p.rn <= ? THEN p.text ELSE '' END as text
            FROM (
                SELECT t.rowid, pg.file_id, pg.page_num, t.text,
                       row_number() OVER (PARTITION BY pg.file_id ORDER BY pg.page_num) as rn
                FROM pdf_text_fts t
                JOIN pages pg ON t.rowid = pg.rowid_fts
                WHERE t.pdf_text_fts MATCH ?
                  AND pg.file_id IN ({",".join("?" for _ in target_file_ids)})
            ) p 
            JOIN files f ON p.file_id = f.id 
            ORDER BY p.file_id, p.page_num
        """
        db_results = db.query_db(
            db_name,
            sql_pages,
            tuple(
                [max_matches_per_book or SNIPPETS_PER_BOOK_INITIAL, sql_search_query]
                + target_file_ids
            ),
        )
        matchers = _compile_match_probes(tuple(highlight_terms))
        # Use the pages shadow table for neighbour lookups.
        # PRIMARY KEY (file_id, page_num) makes each pair an O(log n) point lookup.
        neighbor_map = {}
        if db_results:
            neighbor_pairs = sorted(
                {
                    (r[1], r[4] + delta)
                    for r in db_results
                    for delta in (-1, 1)
                    if r[4] + delta > 0
                }
            )
            # 450 pairs * 2 params each = 900, safely under SQLite's 999 limit.
            for i in range(0, len(neighbor_pairs), 450):
                chunk = neighbor_pairs[i : i + 450]
                conditions = " OR ".join(
                    ["(pg.file_id = ? AND pg.page_num = ?)"] * len(chunk)
                )
                params = tuple(item for pair in chunk for item in pair)
                sql_neighbors = f"""SELECT pg.file_id, pg.page_num, t.text
                    FROM pages pg
                    JOIN pdf_text_fts t ON t.rowid = pg.rowid_fts
                    WHERE {conditions}"""
                for r in db.query_db(db_name, sql_neighbors, params):
                    neighbor_map[(r[0], r[1])] = r[2]

        chapters_by_file = db.get_chapters_for_files(
            db_name, set(row[1] for row in db_results)
        )
        highlight_patterns = _compile_highlight_patterns(tuple(highlight_terms))
        grouped_results, seen_snippets, snippet_count_per_book, last_page_data = (
            defaultdict(lambda: {"filename": "", "matches": [], "total_matches": 0}),
            defaultdict(set),
            defaultdict(int),
            {},
        )
        full_text_registry = {}  # Object deduplication to speed up lru_cache hashing

        for row in db_results:
            rowid, file_id, filename, relative_path, page_num, page_text = row
            filename = filename.removesuffix(".pdf")
            lower_page = page_text.lower()
            approx_pos = next(
                (m.start() for p in matchers if (m := p.search(lower_page))), None
            )

            full_text, match_offset = page_text, approx_pos or 0

            # Lookup neighbors using our new (file_id, page_num) neighbor_map
            prev_text = neighbor_map.get((file_id, page_num - 1))
            if prev_text and (
                approx_pos is None or approx_pos < _STITCH_BOUNDARY_CHARS
            ):
                merged, start_of_new = _merge_overlapping_pages(prev_text, full_text)
                full_text = merged
                if approx_pos is not None:
                    match_offset = start_of_new + approx_pos
                else:
                    match_offset = start_of_new

            nxt_text = neighbor_map.get((file_id, page_num + 1))
            if nxt_text and (
                approx_pos is None
                or (len(page_text) - approx_pos) < _STITCH_BOUNDARY_CHARS
            ):
                full_text, _ = _merge_overlapping_pages(full_text, nxt_text)

            # Use the deduplication registry to ensure we pass the same string object for the same text
            # This allows lru_cache to skip re-hashing the string if it has already been hashed.
            if full_text in full_text_registry:
                full_text = full_text_registry[full_text]
            else:
                full_text_registry[full_text] = full_text

            breadcrumb, current_level = [], float("inf")
            for ch_page, ch_title, ch_lvl in chapters_by_file.get(file_id, []):
                if ch_page <= page_num:
                    if ch_lvl < current_level:
                        breadcrumb.append(ch_title)
                        current_level = ch_lvl
                    if ch_lvl == 1:
                        break
            chapter_title = " > ".join(reversed(breadcrumb)) if breadcrumb else None
            rich_snippet = _cached_snippet(
                full_text, tuple(highlight_terms), highlight_patterns, match_offset
            )

            if rich_snippet:
                is_duplicate = False
                last = last_page_data.get(file_id)
                if (
                    last
                    and last["page"] == page_num - 1
                    and _is_similar_snippet(last["snippet"], rich_snippet)
                ):
                    is_duplicate = True
                if not is_duplicate:
                    last_page_data[file_id] = {
                        "page": page_num,
                        "snippet": rich_snippet,
                    }
                    is_duplicate_text = rich_snippet in seen_snippets[relative_path]
                    if not is_duplicate_text:
                        seen_snippets[relative_path].add(rich_snippet)
                    snippet_to_store = (
                        rich_snippet
                        if snippet_count_per_book[file_id] < SNIPPETS_PER_BOOK_INITIAL
                        and not is_duplicate_text
                        else None
                    )
                    if snippet_to_store:
                        snippet_count_per_book[file_id] += 1
                    grouped_results[relative_path]["filename"] = filename
                    grouped_results[relative_path]["total_matches"] = (
                        book_total_map.get(file_id, 0)
                    )
                    grouped_results[relative_path]["matches"].append(
                        {
                            "page": page_num,
                            "snippet": snippet_to_store,
                            "chapter": chapter_title,
                            "file_id": file_id,
                        }
                    )

        for path in grouped_results:
            grouped_results[path]["matches"].sort(key=lambda m: m["page"])
        processed_results = [
            (data["filename"], path, data["matches"], data["total_matches"])
            for path, data in grouped_results.items()
        ]
        if sort_by == "relevance":
            order_map = {row[2]: i for i, row in enumerate(book_rows)}
            processed_results.sort(key=lambda x: order_map.get(x[1], 999999))
        else:
            # Consistent sorting: Natural Filename
            processed_results.sort(key=lambda x: natural_sort_key(x[0]))
        return (
            SearchResults(
                processed_results, total_books=total_books, total_pages=total_pages
            ),
            has_more,
            None,
            search_terms,
        )
    except sqlite3.OperationalError as e:
        if "fts5: syntax error" in str(e):
            return (
                [],
                False,
                "Search syntax error: check quotes or special characters.",
                [],
            )
        logger.error(f"DB error search: {e}")
        return [], False, "Database error.", []
    except Exception as e:
        logger.error(f"Unexpected search error: {e}")
        raise
