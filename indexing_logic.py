import logging
import os
import time
from collections import defaultdict
from typing import Any, TypedDict
import hashlib
import fitz  # PyMuPDF
import database as db
from shared_utils import (
    STOP_WORDS,
    UNWANTED_PATTERNS,
    BOILERPLATE_PATTERNS,
    GARBAGE_PATTERNS,
    GARBAGE_REPEATED_CHARS,
    GARBAGE_REPEATED_TOKENS,
    BOILERPLATE_CHAPTER_TITLES,
    TRANS_TABLE,
    OCR_EXEMPTIONS,
    OCR_REDO_EXEMPTIONS,
    RE_NON_STANDARD_WHITESPACE,
    RE_MULTIPLE_SPACES,
    RE_SPACED_ELLIPSIS,
    RE_SPACED_TEXT_CHECK,
    RE_SPACED_TEXT_FIX,
    RE_PAGE_NUMBER_PATTERN,
    RE_NUMBERS_TO_TOKEN,
    RE_HYPHEN_FIX,
    RE_VISUAL_TOC_LINE,
    RE_OCR_WORDS,
    _fix_hyphen,
    GARBAGE_PATTERNS_OCR_CHECK,
    apply_display_fixes,
)

logger = logging.getLogger(__name__)


class WorkerResult(TypedDict, total=False):
    """Uniform return type for process_file worker tasks."""

    status: (
        str  # indexed | renamed | skipped | failed | needs_ocr_missing | needs_ocr_redo
    )
    filename: str
    file_path: str  # absolute path on disk
    data: dict[str, Any]  # only present when status == 'indexed'


# --- Constants & State ---

# Configuration for Heuristics
MIN_PAGES_FOR_HEADER_FOOTER_SCAN = 8
MIN_OCCURRENCES_FOR_COMMON_LINE = 3
OCR_CHAR_THRESHOLD = 15  # Avg chars per page below this triggers OCR
BATCH_SIZE = 200  # Pages per bulk DB insert during indexing


# --- Text Normalization Helpers ---


def _normalize_spaced_text(text):
    # Detects and corrects text that has been formatted with extra spaces
    # between characters (e.g., "s o m e t e x t").
    # Optimization: Use high-speed regex check BEFORE attempting regex sub.
    if not RE_SPACED_TEXT_CHECK.search(text):
        return text

    # Regex protection: lookbehinds/lookaheads across massive strings can freeze Python.
    # We chunk by newline to safely constrain regex traversal.
    if len(text) > 2000:
        lines = text.split("\n")
        processed_lines = []
        for line in lines:
            # If a single line is still massive (OCR missing newlines), bail out on that specific line
            if len(line) > 2000:
                processed_lines.append(line)
            else:
                processed_lines.append(RE_SPACED_TEXT_FIX.sub("", line))
        return "\n".join(processed_lines)

    return RE_SPACED_TEXT_FIX.sub("", text)


# --- Heuristics & Analysis ---


def _recover_visual_toc(doc):
    # Scans the first few pages of a PDF to reconstruct a TOC from text if metadata is missing.
    recovered_toc = []
    # Only scan first 15 pages for potential TOC contents
    for p_num in range(min(doc.page_count, 15)):
        page = doc[p_num]
        text = page.get_text("text", sort=True)
        # Keep leading spaces in raw lines to detect indentation levels for hierarchy
        lines = [line.rstrip() for line in text.split("\n") if line.strip()]

        page_toc_items = []
        for line in lines:
            stripped_line = line.lstrip()
            indent = len(line) - len(stripped_line)

            match = RE_VISUAL_TOC_LINE.match(stripped_line)
            if match:
                title, page_ref = match.groups()
                try:
                    # Heuristic: Indentation usually signals hierarchy in visual TOCs
                    level = 1 + (indent // 3)
                    page_toc_items.append([level, title.strip(), int(page_ref)])
                except ValueError:
                    continue

        # Heuristic: If a page has 5+ lines matching the TOC pattern, it is almost certainly a TOC page.
        if len(page_toc_items) >= 5:
            recovered_toc.extend(page_toc_items)

    # Remove duplicates and sort by page number
    unique_items = []
    seen = set()
    for item in recovered_toc:
        tup = tuple(item)
        if tup not in seen:
            unique_items.append(item)
            seen.add(tup)

    unique_items.sort(key=lambda x: x[2])
    return unique_items


def _determine_ocr_status(doc, char_threshold=OCR_CHAR_THRESHOLD):
    """Efficiently checks if a PDF needs OCR and why.
    Returns: "missing" (no text), "redo" (bad text), or None."""
    page_count = doc.page_count
    if page_count == 0:
        return None

    # Improved sampling: Check 10 pages distributed across the document.
    # For larger docs, this avoids the very first/last pages (often covers).
    if page_count > 10:
        indices = [
            int(page_count * p)
            for p in [0.10, 0.15, 0.25, 0.35, 0.45, 0.55, 0.65, 0.75, 0.85, 0.90]
        ]
        pages_to_check = sorted(list(set(indices)))
    else:
        # For short docs, check start, middle, end
        pages_to_check = sorted({0, page_count // 2, page_count - 1})

    total_chars = 0
    max_chars_on_page = 0
    full_text_sample = ""

    page_texts = []
    for p_idx in pages_to_check:
        page = doc[p_idx]
        # Use fast text extraction for sampling
        text = page.get_text().strip()
        page_texts.append(text)

        text_len = len(text)
        total_chars += text_len
        if text_len > max_chars_on_page:
            max_chars_on_page = text_len
        full_text_sample += text + " "

    # Low Text Density Check
    avg_chars = total_chars / len(pages_to_check)

    is_missing = False

    # Only check for images if the text stats are suspicious.
    # This avoids the performance hit of get_images() for standard text documents.
    if avg_chars < char_threshold and max_chars_on_page < 50:
        has_images = False
        for p_idx in pages_to_check:
            if doc[p_idx].get_images():
                has_images = True
                break

        # If images are present, be more lenient to support slide decks, but not too lenient.
        # 10 chars allows for very sparse slides, but catches noisy scans (e.g. "P a g e 1").
        effective_threshold = 10 if has_images else char_threshold
        if avg_chars < effective_threshold:
            is_missing = True

    # Garbage / Bad OCR Check
    # Heuristic 1: Density of empty or junk-filled pages (>40%)
    garbage_pages = 0
    for p_text in page_texts:
        if not p_text:
            garbage_pages += 1
            continue
        if any(p.search(p_text) for p in GARBAGE_PATTERNS_OCR_CHECK):
            garbage_pages += 1

    if len(pages_to_check) > 0 and (garbage_pages / len(pages_to_check)) > 0.4:
        return "redo"

    # Heuristic 2: Check for common English stop words. If text is English, ~30-40% should be stop words.
    # If we find almost none (<5%), the text is likely garbled (e.g. "T h i s" or "l!ke~^").
    is_redo = False
    ratio = 0.0
    words = RE_OCR_WORDS.findall(full_text_sample.lower())
    if len(words) > 50:
        # Use the stop words defined for English
        common_words = STOP_WORDS["eng"]
        common_count = sum(1 for w in words if w in common_words)
        ratio = common_count / len(words)

        # Lowered threshold from 0.10 to 0.055 (~5.5% density) as our stop word list is intentionally small (15 words).
        if ratio < 0.055:
            is_redo = True

    # Heuristic 3: Check for "symbol soup" (high character count but very few valid words)
    elif total_chars > 500 and len(words) < 10:
        logger.info(
            f"OCR trigger: High char count ({total_chars}) but few words ({len(words)})."
        )
        return "redo"

    matches_repeated_chars = GARBAGE_REPEATED_CHARS.findall(full_text_sample)

    # If the text has a healthy ratio of stop words, it's likely valid English.
    # In that case, we significantly raise the threshold for repeated characters
    # to accommodate stylistic choices like sound effects (e.g. "Rrrroar", "Beeeeeep").
    repeated_char_threshold = 10
    if ratio >= 0.10:
        repeated_char_threshold = 50

    # Filter out underscore-only sequences (common separators)
    matches_repeated_chars = [m for m in matches_repeated_chars if m.replace("_", "")]

    if len(matches_repeated_chars) > repeated_char_threshold:
        logger.info(
            f"OCR trigger: Detected multiple instances of repeated characters ({len(matches_repeated_chars)}). Samples: {matches_repeated_chars[:5]}"
        )
        return "redo"

    # Increased threshold to 8 to avoid flagging "Ha ha ha" or "No no no".
    matches_repeated_tokens = GARBAGE_REPEATED_TOKENS.findall(full_text_sample)
    # Filter out punctuation-only repeats (e.g. ". . ." or "- - -") which are common in formatting
    # Also filter out numeric repeats (e.g. "4 4 4" or "5+ 5+") common in tabletop stat blocks
    real_matches = []
    for m in matches_repeated_tokens:
        # Must have alphanumeric content (filters ". . .")
        alnum_content = "".join(c for c in m if c.isalnum())
        if not alnum_content:
            continue
        # If alphanumeric content is purely digits, assume it's a number/stat and ignore
        if alnum_content.isdigit():
            continue
        real_matches.append(m)

    if len(real_matches) > 8:
        logger.info(
            f"OCR trigger: Detected repeated token patterns. Samples: {real_matches[:5]}"
        )
        return "redo"

    if is_missing:
        logger.info(
            f"OCR trigger: Low text density detected ({avg_chars:.1f} chars/page)."
        )
        return "missing"

    if is_redo:
        logger.info(
            f"OCR trigger: Low common word ratio ({ratio:.1%}). Text quality suspect."
        )
        return "redo"

    return None


# --- Main Text Normalization Pipeline ---


def clean_and_normalize_text(text):
    # Applies a comprehensive series of cleaning and normalization steps to raw text
    # before it is inserted into the database.
    if logger.isEnabledFor(logging.DEBUG):
        logger.debug(f"Starting text cleaning. Input length: {len(text)}")

    # Normalize non-standard whitespace (e.g. tabs, non-breaking spaces) to ordinary spaces.
    # We preserve newlines (\n) because they are needed for paragraph reconstruction later.
    text = RE_NON_STANDARD_WHITESPACE.sub(" ", text)

    # Control characters (ASCII 0-31 except tab/LF/CR, plus 0x7F) are removed later
    # by the translate table in a single C-level pass along with OCR normalization.

    # Handle spaced-out text like "s o m e t e x t".
    text = _normalize_spaced_text(text)

    if logger.isEnabledFor(logging.DEBUG):
        logger.debug("Finished whitespace and spaced-text normalization.")

    # Remove known watermark patterns.
    for pattern in UNWANTED_PATTERNS:
        text = pattern.sub("", text)

    # Remove specific multi-line boilerplate sections.
    # Using re.DOTALL to make '.' match newlines.
    for pattern in BOILERPLATE_PATTERNS:
        text = pattern.sub("", text)

    # Normalize spaced ellipses . . . to ... BEFORE garbage collection
    # to prevent them from being flagged as repeated single-character garbage (e.g. ". . .")
    text = RE_SPACED_ELLIPSIS.sub("...", text)

    # Remove garbled text patterns (OCR noise).
    for pattern in GARBAGE_PATTERNS:
        text = pattern.sub("", text)

    if logger.isEnabledFor(logging.DEBUG):
        logger.debug("Finished boilerplate and garbage removal.")

    # Normalize characters using a translation table.
    # This handles quotes, apostrophes, common OCR artifacts (Long S, Esh), and ligatures.
    text = text.translate(TRANS_TABLE)

    # Note: OCR artifact fixes (fne -> fine) are now consolidated in apply_display_fixes.

    # Note: Character normalization and abbreviation protection are now
    # consolidated in apply_display_fixes.

    if logger.isEnabledFor(logging.DEBUG):
        logger.debug("Finished character normalization and abbreviation protection.")

    # Normalize detached quotes: remove space between punctuation and quote (e.g. "text. ' Next" -> "text.' Next")
    # This logic was simplified to avoid gluing proper opening quotes.
    # We rely on SENT_BOUNDARY to consume detached closing quotes if they appear as "text. ' "

    # Apply shared display fixes (contractions, quotes, possessives, quote/period swap, and closing quote spacing)
    text = apply_display_fixes(text)

    # Fix hyphenated words broken across lines. e.g. "Emper- or" -> "Emperor"
    # The text extraction joins lines with spaces, so we look for "word- word".
    # We restrict to letters to avoid merging number ranges.
    text = RE_HYPHEN_FIX.sub(_fix_hyphen, text)

    # Final cleanup: collapse multiple spaces into one (leftover from newline replacements).
    text = RE_MULTIPLE_SPACES.sub(" ", text)

    if logger.isEnabledFor(logging.DEBUG):
        logger.debug(f"Text cleaning complete. Output length: {len(text)}")

    return text


# --- File Hashing ---


def compute_file_hash(file_path):
    """Calculates SHA-256 hash of a file for duplicate/rename detection."""
    sha256_hash = hashlib.sha256()
    try:
        with open(file_path, "rb") as f:
            for byte_block in iter(lambda: f.read(65536), b""):
                sha256_hash.update(byte_block)
        return sha256_hash.hexdigest()
    except Exception:
        return None


def _reading_order_key(bbox, strip_width):
    """Column-aware sorting key for multi-column PDF layouts."""
    return (int(bbox[0] / strip_width), bbox[1])


def index_pdf_file(
    db_name, file_path, filename, relative_path, last_modified, file_hash=None
):
    """Extracts text from each page of a PDF and inserts it into the database.

    Returns:
        dict: {status: "indexed", pages: [(p, text)], chapters: [(p, title, level)]}
        or str: "failed", "needs_ocr_missing", "needs_ocr_redo"
    """
    pages_content = []
    line_counts = defaultdict(int)
    fuzzy_header_counts = defaultdict(int)
    fuzzy_footer_counts = defaultdict(int)

    try:
        doc = fitz.open(file_path)
        if doc.is_encrypted:
            logger.warning(f"Skipping encrypted PDF: {filename}")
            return "failed"

        # OCR Check
        ocr_status = _determine_ocr_status(doc)
        if ocr_status:
            # Check specific file exemptions (always skip OCR)
            if relative_path in OCR_EXEMPTIONS:
                logger.info(
                    f"OCR trigger for {filename}: {ocr_status}, but file is in OCR_EXEMPTIONS. Skipping OCR."
                )
                ocr_status = None

            # Check specific file exemptions for redo (only skip 'redo', keep 'missing')
            elif ocr_status == "redo":
                if relative_path in OCR_REDO_EXEMPTIONS:
                    logger.info(
                        f"OCR trigger for {filename}: {ocr_status}, but file is in OCR_REDO_EXEMPTIONS. Skipping OCR."
                    )
                    ocr_status = None
                else:
                    # Dynamically check .pdfsearchignore for OCR redo exemption
                    # Find .pdfsearchignore file
                    ignore_file = None
                    current_check_dir = os.path.dirname(os.path.abspath(file_path))
                    max_depth = 10
                    while current_check_dir and max_depth > 0:
                        potential_ignore = os.path.join(
                            current_check_dir, ".pdfsearchignore"
                        )
                        if os.path.exists(potential_ignore):
                            ignore_file = potential_ignore
                            break
                        parent_dir = os.path.dirname(current_check_dir)
                        if parent_dir == current_check_dir:  # root folder logic
                            break
                        current_check_dir = parent_dir
                        max_depth -= 1

                    if ignore_file:
                        try:
                            # Parse .pdfsearchignore relative paths to the root dir
                            root_path_for_ignore = os.path.dirname(ignore_file)
                            # Get the relative path of the file itself
                            rel_file_path = os.path.relpath(
                                file_path, root_path_for_ignore
                            ).replace("\\", "/")
                            # Current dir relative path
                            rel_check_path = os.path.dirname(rel_file_path)

                            with open(ignore_file, "r", encoding="utf-8") as f:
                                for line in f:
                                    cleaned = line.strip()
                                    if cleaned and not cleaned.startswith("#"):
                                        norm_pattern = cleaned.replace("\\", "/")

                                        # Match if the pattern matches a directory exactly (from anywhere)
                                        dir_match_simple = (
                                            os.path.basename(rel_check_path) == cleaned
                                        )
                                        # Match if the pattern matches the relative directory path
                                        dir_match_rel = (
                                            rel_check_path == norm_pattern
                                            or rel_check_path.startswith(
                                                norm_pattern + "/"
                                            )
                                        )
                                        # Match if the pattern matches the exact file
                                        file_match_exact = (
                                            os.path.basename(file_path) == cleaned
                                            or rel_file_path == norm_pattern
                                        )

                                        if (
                                            dir_match_simple
                                            or dir_match_rel
                                            or file_match_exact
                                        ):
                                            logger.info(
                                                f"OCR trigger for {filename}: {ocr_status}, but matches .pdfsearchignore '{cleaned}'. Skipping OCR redo."
                                            )
                                            ocr_status = None
                                            break
                        except Exception as e:
                            logger.warning(
                                f"Failed to check .pdfsearchignore during OCR status generation: {e}"
                            )

        if ocr_status:
            logger.info(
                f"OCR trigger for {filename}: {ocr_status}. Marking for batch processing."
            )
            return f"needs_ocr_{ocr_status}"

        logger.info(f"Indexing: {filename}")
        page_count = doc.page_count
        logger.info(f"Processing {page_count} pages from {filename}")

        #  Pre-process TOC to identify boilerplate pages
        boilerplate_pages = set()
        _raw_toc = []
        try:
            _raw_toc = doc.get_toc(simple=True)
            if not _raw_toc:
                _raw_toc = _recover_visual_toc(doc)
                if _raw_toc:
                    logger.info(
                        f"No metadata TOC found. Recovered {len(_raw_toc)} entries via visual scan."
                    )

            toc = _raw_toc
            # Filter valid items and sort by page number
            valid_toc = sorted(
                [
                    item
                    for item in toc
                    if len(item) >= 3 and isinstance(item[2], int) and item[2] > 0
                ],
                key=lambda x: x[2],
            )

            current_boilerplate_level = -1

            for i, (lvl, title, page, *_) in enumerate(valid_toc):
                # Check if we are exiting a boilerplate block (nested logic)
                if current_boilerplate_level != -1:
                    if lvl <= current_boilerplate_level:
                        current_boilerplate_level = -1

                # Check if this is a new boilerplate root
                if current_boilerplate_level == -1:
                    for pattern in BOILERPLATE_CHAPTER_TITLES:
                        if pattern.match(title):
                            current_boilerplate_level = lvl
                            break

                # If we are in boilerplate (either continued or just started), mark pages
                if current_boilerplate_level != -1:
                    start_page = page
                    if i + 1 < len(valid_toc):
                        end_page = valid_toc[i + 1][2]
                    else:
                        end_page = page_count + 1

                    for p in range(start_page, end_page):
                        boilerplate_pages.add(p)

            if boilerplate_pages:
                logger.info(
                    f"Skipping {len(boilerplate_pages)} pages identified as boilerplate via TOC."
                )
        except Exception as e:
            logger.warning(f"Failed to process TOC for boilerplate detection: {e}")

        #  Extraction Configuration
        # Exclude fitz.TEXT_PRESERVE_IMAGES (16) to avoid processing heavy image data
        extraction_flags = (
            fitz.TEXT_PRESERVE_LIGATURES
            | fitz.TEXT_PRESERVE_WHITESPACE
            | fitz.TEXT_MEDIABOX_CLIP
        )

        start_time = time.time()
        heavy_mode = False

        for page_num_zero_based, page in enumerate(doc):
            current_page_num = page_num_zero_based + 1
            if current_page_num in boilerplate_pages:
                pages_content.append("")
                continue

            # Performance bailout for heavy/fragmented PDFs
            if not heavy_mode and current_page_num == 11:
                elapsed = time.time() - start_time
                if elapsed > 8.0:
                    logger.warning(
                        f"Heavy PDF: {filename} is slow ({elapsed:.1f}s/10p). Using high-speed mode."
                    )
                    heavy_mode = True

            block_texts = []
            strip_width = page.rect.width / 2.5

            if heavy_mode:
                # Fast block-level extraction
                blocks = page.get_text("blocks", flags=extraction_flags)
                # Apply custom column-aware sort
                sorted_blocks = sorted(blocks, key=lambda b: _reading_order_key(b[:4], strip_width))

                for b in sorted_blocks:
                    if b[6] == 0:  # Text block
                        text = b[4].strip()
                        if text:
                            block_texts.append(text)
            else:
                # Balanced extraction: use dict for spacing checks
                page_dict = page.get_text("dict", flags=extraction_flags)
                blocks = page_dict.get("blocks", [])
                text_blocks = [b for b in blocks if b.get("type") == 0]

                # Apply custom column-aware sort
                sorted_blocks = sorted(
                    text_blocks, key=lambda b: _reading_order_key(b["bbox"], strip_width)
                )

                for b in sorted_blocks:
                    para_lines = []
                    for line in b.get("lines", []):
                        line_text = ""
                        prev_span = None
                        for span in line.get("spans", []):
                            span_text = span.get("text", "")
                            # Structural spacing check
                            if (
                                prev_span
                                and span_text
                                and not span_text.startswith(" ")
                                and prev_span.get("text")
                                and not prev_span["text"].endswith(" ")
                                and span_text[0] not in ".,!?:;'\"’”] "
                            ):
                                dist = span["bbox"][0] - prev_span["bbox"][2]
                                if dist > (span.get("size", 10) * 0.15):
                                    line_text += " "
                            line_text += span_text
                            prev_span = span

                        line_text = line_text.strip()
                        if line_text:
                            para_lines.append(line_text)

                    if para_lines:
                        block_texts.append(" ".join(para_lines))

            page_text = "\n\n".join(block_texts)
            pages_content.append(page_text)

            # If the doc is long enough, analyze first/last lines for commonality.
            if page_count > MIN_PAGES_FOR_HEADER_FOOTER_SCAN and page_text:
                lines = [line.strip() for line in page_text.split("\n") if line.strip()]
                if lines:
                    # Header analysis (Exact and Fuzzy) - Check top 3 lines
                    for i in range(min(len(lines), 3)):
                        line_counts[lines[i]] += 1
                        # Fuzzy: replace digits with generic token to catch "Page 1", "Page 2"
                        fuzzy_header = RE_NUMBERS_TO_TOKEN.sub("<NUM>", lines[i])
                        fuzzy_header_counts[fuzzy_header] += 1

                    # Footer analysis - Check bottom 3 lines
                    start_footer = max(len(lines) - 3, 0)
                    for i in range(start_footer, len(lines)):
                        line_counts[lines[i]] += 1
                        fuzzy_footer = RE_NUMBERS_TO_TOKEN.sub("<NUM>", lines[i])
                        fuzzy_footer_counts[fuzzy_footer] += 1

            if (page_num_zero_based + 1) % 100 == 0:
                logger.info(f"Extracted {page_num_zero_based + 1}/{page_count} pages")

        #  Identify and Remove Common Lines
        common_lines = set()
        common_fuzzy_headers = set()
        common_fuzzy_footers = set()

        if page_count > MIN_PAGES_FOR_HEADER_FOOTER_SCAN:
            common_lines = {
                line
                for line, count in line_counts.items()
                if count >= MIN_OCCURRENCES_FOR_COMMON_LINE
            }

            # Fuzzy threshold: needs to appear on a significant portion of pages (e.g., > 20%)
            # to avoid false positives on similar looking sentences.
            # We cap the max threshold at 25 to ensure headers in very large books (where headers might change per chapter) are still caught.
            fuzzy_threshold = max(
                MIN_OCCURRENCES_FOR_COMMON_LINE, min(page_count // 10, 25)
            )
            common_fuzzy_headers = {
                k for k, c in fuzzy_header_counts.items() if c >= fuzzy_threshold
            }
            common_fuzzy_footers = {
                k for k, c in fuzzy_footer_counts.items() if c >= fuzzy_threshold
            }

            if common_lines or common_fuzzy_headers or common_fuzzy_footers:
                logger.info(
                    f"Identified common headers/footers (Exact: {len(common_lines)}, Fuzzy: {len(common_fuzzy_headers) + len(common_fuzzy_footers)})"
                )

        #  Process and Prepare Data
        # The database writes are now deferred to the master thread to avoid lock contention.
        pages_to_return = []
        clean_toc = []

        prev_cleaned_text = None
        prev_page_num = None

        for i, page_text in enumerate(pages_content):
            raw_lines = page_text.split("\n")

            # Identify indices of non-empty lines to determine first and last "content" lines
            non_empty_indices = [
                idx for idx, line in enumerate(raw_lines) if line.strip()
            ]

            filtered_lines = []
            if non_empty_indices:
                first_content_idx = non_empty_indices[0]
                last_content_idx = non_empty_indices[-1]

                for idx, line in enumerate(raw_lines):
                    stripped = line.strip()
                    # Keep empty lines to preserve paragraph structure, and skip common headers/footers
                    if not stripped or stripped in common_lines:
                        if stripped in common_lines:
                            continue
                        filtered_lines.append(line)
                        continue

                    # Check fuzzy headers/footers (only on first/last few lines of the block to be safe)
                    # Use non_empty_indices to target actual content boundaries
                    is_header_candidate = idx <= first_content_idx + 2
                    is_footer_candidate = idx >= last_content_idx - 2

                    if is_header_candidate:
                        if (
                            RE_NUMBERS_TO_TOKEN.sub("<NUM>", stripped)
                            in common_fuzzy_headers
                        ):
                            continue

                    if is_footer_candidate:
                        if (
                            RE_NUMBERS_TO_TOKEN.sub("<NUM>", stripped)
                            in common_fuzzy_footers
                        ):
                            continue

                    # Check for page numbers on first/last content lines
                    if (
                        idx == first_content_idx or idx == last_content_idx
                    ) and RE_PAGE_NUMBER_PATTERN.match(stripped):
                        continue

                    filtered_lines.append(line)
            else:
                filtered_lines = raw_lines

            page_content = "\n".join(filtered_lines)
            cleaned_text = clean_and_normalize_text(page_content)

            if prev_cleaned_text is not None:
                overlap_text = ""
                if cleaned_text.strip():
                    overlap_len = 300
                    # Trim a bit to avoid matching whitespace-only differences
                    current_start = cleaned_text[:overlap_len].strip()

                    # Deduplication Logic:
                    # In some PDFs, the last words of a page are repeated at the beginning of the next.
                    # We check for this to avoid "doubling" the text in the index.
                    raw_overlap_found = False
                    if current_start:
                        # Check if the start of this page is already at the end of the previous page
                        # We check the last 150 chars of the previous page for the first 50 of this one.
                        check_sample = current_start[:50]
                        if check_sample in prev_cleaned_text[-200:]:
                            raw_overlap_found = True

                    if not raw_overlap_found:
                        overlap_text = cleaned_text[:overlap_len]
                        if len(cleaned_text) > overlap_len:
                            last_space = overlap_text.rfind(" ")
                            if last_space != -1:
                                overlap_text = overlap_text[:last_space]
                    else:
                        # If raw overlap exists, adding more is redundant and causes bloat/duplicate hits
                        overlap_text = ""

                if overlap_text:
                    # Join with a space and run the hyphen fixer to heal words split across pages (e.g. "word-" + "word")
                    joined_temp = prev_cleaned_text + " " + overlap_text.strip()
                    final_prev_text = RE_HYPHEN_FIX.sub(_fix_hyphen, joined_temp)
                else:
                    final_prev_text = prev_cleaned_text

                pages_to_return.append((prev_page_num, final_prev_text))

            if cleaned_text.strip():
                prev_cleaned_text = cleaned_text
                prev_page_num = i + 1
            else:
                prev_cleaned_text = None
                prev_page_num = None

        if prev_cleaned_text is not None:
            pages_to_return.append((prev_page_num, prev_cleaned_text))

        #  Extract and Store Chapters (TOC)
        try:
            # get_toc() returns [lvl, title, page, dest].
            toc = _raw_toc
            # Filter for valid entries with page numbers
            clean_toc = []
            for item in toc:
                if len(item) < 3 or not isinstance(item[2], int) or item[2] <= 0:
                    continue
                level = item[0]
                title = item[1]
                # Filter out boilerplate chapters from the database
                if not any(p.match(title) for p in BOILERPLATE_CHAPTER_TITLES):
                    clean_toc.append((item[2], title, level))

        except Exception as e:
            logger.warning(f"Failed to extract TOC for {filename}: {e}")

        logger.info(
            f"Worker finished processing: {filename} ({len(pages_to_return)} pages)"
        )
        return {
            "status": "indexed",
            "pages": pages_to_return,
            "chapters": clean_toc,
            "file_hash": file_hash,
        }
    except Exception as e:
        logger.error(f"Error indexing {filename}: {e}")
        return "failed"
    finally:
        if "doc" in locals() and doc:
            doc.close()


def _index_and_wrap(
    db_name, file_path, file, relative_path, current_last_modified, file_hash
) -> "WorkerResult":
    """Run index_pdf_file and wrap the result as a uniform WorkerResult."""
    res = index_pdf_file(
        db_name, file_path, file, relative_path, current_last_modified, file_hash
    )
    if isinstance(res, dict):
        return WorkerResult(
            status="indexed",
            filename=file,
            file_path=file_path,
            data={
                "filename": file,
                "file_path": file_path,
                "pages": res.get("pages"),
                "chapters": res.get("chapters"),
                "relative_path": relative_path,
                "last_modified": current_last_modified,
                "file_hash": res.get("file_hash"),
            },
        )
    # res is a status string like "needs_ocr_missing", "needs_ocr_redo", "failed"
    return WorkerResult(status=str(res), filename=file, file_path=file_path)


def process_file(args_tuple):
    # Wrapper function for multiprocessing. Checks if a file needs indexing
    # and calls the indexing function.
    if not logging.getLogger().hasHandlers():
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s [%(levelname)s] %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
    db_name, file_path, file, relative_path, current_last_modified, root_dir = (
        args_tuple
    )

    try:
        db_last_modified = db.get_last_modified_from_db(db_name, relative_path)

        if db_last_modified is None:
            # File not found in DB by path. Check if it's a rename (same hash, old path missing).
            file_hash = compute_file_hash(file_path)
            if file_hash:
                existing_files = db.get_files_by_hash(db_name, file_hash)
                for old_id, old_fname, old_rel_path in existing_files:
                    if old_rel_path == relative_path:
                        continue
                    old_full_path = os.path.join(root_dir, old_rel_path)
                    if not os.path.exists(old_full_path):
                        logger.info(
                            f"Rename detected: {old_fname} -> {file}. Updating index without re-processing."
                        )
                        db.update_file_path(
                            db_name, old_id, relative_path, file, current_last_modified
                        )
                        return WorkerResult(
                            status="renamed", filename=file, file_path=file_path
                        )

            # Not a rename, index as new
            return _index_and_wrap(
                db_name,
                file_path,
                file,
                relative_path,
                current_last_modified,
                file_hash,
            )

        elif current_last_modified > db_last_modified:
            file_hash = compute_file_hash(file_path)
            return _index_and_wrap(
                db_name,
                file_path,
                file,
                relative_path,
                current_last_modified,
                file_hash,
            )

        logger.debug(f"Skipping existing file (unchanged): {file}")
        return WorkerResult(status="skipped", filename=file, file_path=file_path)
    except Exception as e:
        logger.error(f"Critical error in worker process for {file}: {e}")
        return WorkerResult(status="failed", filename=file, file_path=file_path)
