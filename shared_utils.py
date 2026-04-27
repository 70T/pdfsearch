import re
from functools import lru_cache

# --- Configuration ---
# --- OCR Exemptions ---
# Files that should be excluded from OCR processing (e.g. due to low scan quality).
OCR_EXEMPTIONS = {}
# --- OCR Redo Exemptions ---
# Files that should be exempt from "redo" OCR (garbage text),
# but still processed if they have "missing" text (no OCR at all).
OCR_REDO_EXEMPTIONS = {}
# --- Stop Words ---
# Used to determine if text is garbage (bad OCR) or valid content.
STOP_WORDS = {
    "eng": {
        "the",
        "and",
        "that",
        "have",
        "with",
        "this",
        "from",
        "they",
        "there",
        "which",
        "their",
        "about",
        "could",
        "would",
        "should",
        "after",
        "before",
        "people",
        "would",
    }
}
# --- Constants ---
# --- Contraction Patterns ---
CONTRACTION_S_WORDS = r"that|it|he|she|there|here|what|let|who|how|where|when|why|someone|anyone|everyone|nobody|anybody|somebody|one"
CONTRACTION_T_WORDS = r"can|don|won|shan|isn|aren|wasn|weren|hasn|haven|hadn|doesn|didn|wouldn|shouldn|couldn|mustn|ain|needn|oughtn|mightn|daren"
CONTRACTION_VE_WORDS = r"i|you|we|they|who|what|where|how|would|should|could|might|must"
CONTRACTION_LL_WORDS = r"i|you|he|she|it|we|they|who|that|what|where|how"
CONTRACTION_RE_WORDS = r"you|we|they|who|what|where|how"
CONTRACTION_M_WORDS = r"i"
CONTRACTION_D_WORDS = r"i|you|he|she|it|we|they|who|that|what|where|how"
# --- Compiled Contraction Regexes ---
# Merge all 7 contraction word sets into a combined high-performance regex.
_ALL_CONTRACTION_WORDS = "|".join(
    sorted(
        set(
            (
                CONTRACTION_S_WORDS
                + "|"
                + CONTRACTION_T_WORDS
                + "|"
                + CONTRACTION_VE_WORDS
                + "|"
                + CONTRACTION_LL_WORDS
                + "|"
                + CONTRACTION_RE_WORDS
                + "|"
                + CONTRACTION_M_WORDS
                + "|"
                + CONTRACTION_D_WORDS
            ).split("|")
        ),
        key=len,
        reverse=True,
    )
)
RE_GLUED_CONTRACTION = re.compile(
    r"(?i)\b(" + _ALL_CONTRACTION_WORDS + r")'\s*(s|t|ve|ll|re|m|d)([a-z])"
)
# --- Natural Sort Helper ---
_NATURAL_SORT_SPLIT_RE = re.compile(r"(\d+)")


@lru_cache(maxsize=1024)
def natural_sort_key(s):
    # Create a sort key for natural sorting (e.g., '2' before '10').
    # Splits the string into a list of strings and numbers.
    return [
        int(text) if text.isdigit() else text.lower()
        for text in _NATURAL_SORT_SPLIT_RE.split(s)
    ]


# --- Text Cleaning Patterns ---
UNWANTED_PATTERNS = [
    re.compile(r"(?:oceanofpdf(?: |)\.com|angrygotfan\.com)", re.IGNORECASE)
]
BOILERPLATE_PATTERNS = [
    # Horus Heresy introduction text
    re.compile(
        r"THE HORUS HERESY\s+It is a time of legend.*?how far can a star rise before it falls\?",
        re.IGNORECASE | re.DOTALL,
    ),
    # Table of Contents sections (now ignores Dramatis Personae)
    re.compile(
        r"(?:Table of Contents|CONTENTS)\s*(?:(?:PART|Part) (?:ONE|TWO|THREE|FOUR|FIVE|One|Two|Three|Four|Five)\s*)+(?:[A-Z-]+(?![a-z])\s*)*",
        re.DOTALL,
    ),
    # Additional boilerplate sections (headers that consume the rest of the page)
    re.compile(
        r"(?:^|\n)\s*(?:An?\s+(?:[\w-]+\s+){1,5}Publication|Copyright|Dedication|Acknowledgements?|Legal|About the (?:Authors?|Publisher|Illustrator)|By (?:George R\. ?R\. Martin|Guy Haley|Dan Abnett|Gav Thorpe|John French)|eBook license|Afterword|Further reading from The Horus Heresy|Introduction to the (?:Old Republic|Rise of the Empire|Rebellion|New Republic|Legacy Era)|Praise for (?:George R\. ?R\. Martin's )?A GAME OF THRONES|Backlist|Other Titles|Also by this Author|Excerpt from).*",
        re.IGNORECASE | re.DOTALL,
    ),
    # Contents header (stricter to avoid false positives with the word 'contents')
    re.compile(r"(?:^|\n)\s*Contents\s*(?:\n|$).*", re.IGNORECASE | re.DOTALL),
    # Star Wars Timeline
    re.compile(
        r"(?:^|\n)\s*The STAR WARS Novels Timeline.*", re.IGNORECASE | re.DOTALL
    ),
    # Chapter headers (e.g. Chapter 1, Chapter One, CHAPTER I)
    re.compile(
        r"(?:^|\n)\s*Chapter\s+(?:[0-9]+|[IVXLCDM]+|[a-zA-Z]+)\s*(?=\n|$)",
        re.IGNORECASE,
    ),
]
GARBAGE_PATTERNS = [
    # 1. Lines containing high-confidence OCR noise characters: broken bar (¦)
    # Tildes are checked per-token below to be more surgical.
    re.compile(r"^[^\n]*[¦][^\n]*(?:\n|$)", re.MULTILINE),
    # 2. Long sequences of decorative characters / line separators (4+ repetitions)
    re.compile(r"[#=~*_\-]{4,}"),
    # 3. Tokens containing tilde (~) or broken bar (¦) - almost always OCR junk
    # Uses negative lookbehind/lookahead for whitespace to target whole "words".
    re.compile(r"(?<!\S)\S*[~¦]\S*(?!\S)"),
    # 4. Interleaved noise in words (e.g., r~i:: a:.:. F:.: le;;: e;:.. t.: P:. r,;;)
    # Matches words with multiple symbol injections.
    re.compile(r"\b[A-Za-z0-9]*(?:[~¦:;,.]{1,}[A-Za-z0-9]{1,}){2,}[~¦:;,.!]*\b"),
    # 5. Tokens consisting primarily of symbols (e.g., :':;  or  r,;;)
    re.compile(r"(?<!\S)(?![.]{2,})[~:.;,¦'\"\"“]{2,}(?!\S)"),
    # 6. Sparse interleaved noise (e.g., u:,; r:... a:.:. F:.: le;;: )
    # Matches tokens where symbols are more prominent or weirdly clustered.
    re.compile(r"(?<!\S)\w{0,2}[~:;,.¦]{2,}\w?\S*(?!\S)"),
    # 7. Words with 5+ repeated characters (e.g. "eeeee"). Optimized: lazy matching.
    re.compile(r"\b\w*?(\w)\1{4,}\w*\b"),
    # 8. Repeated sequences of 2+ characters within a word (e.g. "thththat") - 3+ repetitions
    re.compile(r"\b\w*?(\w{2,})\1{2,}\w*\b"),
    # 9. Repeated words/tokens (e.g. "ER ER ER", "S] S] S]") - 3+ repetitions
    re.compile(r"(?<!\S)(\S+)(?:\s+\1){2,}(?!\S)"),
    # 10. Repeated hash marks or long streams of dots (e.g. in TOCs)
    re.compile(r"#{4,}|\.{5,}"),
]

GARBAGE_PATTERNS_OCR_CHECK = (
    GARBAGE_PATTERNS[0],  # broken-bar lines
    GARBAGE_PATTERNS[6],  # 5+ repeated alpha chars
    GARBAGE_PATTERNS[7],  # repeated 2+ char sequences
)

# --- Common Text Cleaning Regexes ---
RE_NON_STANDARD_WHITESPACE = re.compile(r"[^\S\n]+")
RE_MULTIPLE_SPACES = re.compile(r" +")
RE_SPACED_ELLIPSIS = re.compile(r"\.(?:\s+\.){2,}")
# Spaced out text detection (s o m e t e x t) - Requires at least 3 chars separated by spaces
RE_SPACED_TEXT_CHECK = re.compile(r"\b\w\s\w\s\w\b")
RE_SPACED_TEXT_FIX = re.compile(r"(?<=\b\w) (?=\w\b)")
RE_OPEN_QUOTE_SPACE_FIX = re.compile(r"((?:^|(?<=\s))[\u2018\u201c'\"])\s+(?=\w)")
# Remove stray space before closing single quote (common OCR artifact in dialogue)
# e.g. "Europa, ' said" -> "Europa,' said"  |  "free. '" -> "free.'"
RE_SPACE_BEFORE_CLOSE_QUOTE = re.compile(
    r"""([\w.,!?;:]) '(?!')(?=\s+[a-zA-Z]|\s+['"]|\s*$|[A-Z])"""
)
RE_CLOSE_QUOTE_SPACE_FIX = re.compile(
    r"(?<=\S)\s+(['\"\"\u201d\u2019])(?=\s|[.,!?;]|$)"
)
RE_SPACE_BEFORE_APOSTROPHE = re.compile(r"(?i)\b([a-z]+)\s+'(s|t|d|m|ll|re|ve)\b")
RE_POSSESSIVE_S_FIX = re.compile(r"([sS])'(?![sS])(\w)")
# RE_POSSESSIVE_SPACE_FIX is strictly a subset of RE_CONTRACTION_SPACE_FIX, obsolete.
RE_CONTRACTION_SPACE_FIX = re.compile(r"(?i)(\w)'\s+(t|d|m|ll|re|ve|s)\b")
RE_PUNCT_QUOTE_CAP = re.compile(r'([.!?])"([A-Z])')
# Split single quotes: two patterns with different spacing.
# 1) Punct+'quote+letter: space goes BEFORE the quote (opening quote after punctuation)
RE_SINGLE_QUOTE_SPACER_BEFORE = re.compile(r"(?<=[^\w\s])'(?=[A-Za-z])")
# --- Abbreviation Patterns ---
RE_ABBREV_PROTECT = re.compile(r"(?i)\b(e\.g|i\.e)\b")
# --- URL Patterns ---
# Targets http:// and www. links to protect them from punctuation spacing rules.
# Matches until next whitespace or markdown delimiter.
RE_URL_PROTECT = re.compile(r"((?:https?://|www\.)[^\s\[\](){}]+)", re.IGNORECASE)
# --- Entities ---
# Protects HTML entities from being mangled by punctuation spacing rules.
RE_ENTITY_PROTECT = re.compile(r"(&[a-z0-9#]{2,10};)", re.IGNORECASE)
RE_SURROGATES = re.compile(r"[\uD800-\uDFFF]")
# --- OCR Artifact Fixes ---
RE_FNE_FIX = re.compile(r"\bfne\b")
RE_STUS_FIX = re.compile(r"\bstus\b")
# --- Punctuation Spacing ---
RE_FIX_PUNCT_SPACING_1 = re.compile(r"\s+([:,.!?;])")
# Updated: Ignore digits and punctuation following single letters to preserve initials (J.K.) and abbreviations.
RE_FIX_PUNCT_SPACING_2 = re.compile(r"(?<!\b[A-Za-z])([:,.!?;])(?=[A-Za-z])")
# --- Ellipsis Spacing ---
# Inserts a space between a closing ellipsis and the next sentence start
# (e.g. "...Next" -> "... Next"). MUST run before RE_FIX_PUNCT_SPACING_2,
# which would corrupt "...Word" into ".. . Word" by matching the trailing dot.
RE_ELLIPSIS_SPACE = re.compile(r"\.\.\.(?=[A-Z])")
# --- Dialogue & Quote Spacing ---
# Combines both single and double quotes into a single optimized pass
RE_QUOTE_PERIOD_SWAP_ALL = re.compile(
    r'([a-zA-Z]{3,})([\'"\u201c\u201d\u2018\u2019])\.\s*([A-Z])'
)
# --- Shared Quote Spacing Patterns ---
# Used by both indexing (clean_and_normalize_text) and search (snippet generation).
RE_CLOSE_QUOTE_SPACE = re.compile(r'(?<=[^\s"])(")(?=\w)')
RE_CLOSE_SINGLE_QUOTE_SPACE = re.compile(
    r"(?i)(?<=[a-zA-Z]{3})(')(?=[a-zA-Z]{2,})(?!(?:re|ve|ll|em)\b)"
)
RE_JOINED_DOUBLE_QUOTES = re.compile(r'([.,!?-])""\s*')
RE_JOINED_SINGLE_QUOTES = re.compile(r"([.,!?-])''\s*")
RE_DOT_COMMA_QUOTE_LOWERCASE = re.compile(r'([.,])\s+"(?:\s+)?(?=[a-z])')
# Consolidated from indexing_logic.py
RE_PUNCT_SINGLE_QUOTE_SPACE = re.compile(r"([.!?])\'(?=\w)")
RE_COMMA_SINGLE_QUOTE_SPACE = re.compile(r"([,:;])\'(?=\w)")
RE_EXCESS_SPACES = re.compile(r"\s{2,}")
RE_TRAILING_SENTENCE_PUNCT = re.compile(r'[.!?]["\']?\s*$')
RE_QUERY_TERMS = re.compile(r'"[^"]+"|[^\s+]+')
RE_PAGE_NUMBER_PATTERN = re.compile(
    r"^\s*(?:page\s+|p\.\s+)?\d+(?:\s+of\s+\d+)?\s*$", re.IGNORECASE
)
RE_NUMBERS_TO_TOKEN = re.compile(r"\d+")
RE_VISUAL_TOC_LINE = re.compile(r"^(.+?)(?:\s|\.){5,}(\d+)$")
RE_OCR_WORDS = re.compile(r"\b[a-z]+\b")
# --- Consolidated Fix Patterns ---
RE_PUNCT_QUOTE_CLEANUP = re.compile(r'([.!?])\s+([\'"\u201c\u201d\u2018\u2019])(?!\w)')
RE_URL_MANGLE_FIX_1 = re.compile(
    r"((?:https?://|www\.|HTTPS?://|WWW\.))\s+([a-z0-9])", re.IGNORECASE
)
RE_URL_MANGLE_FIX_2 = re.compile(
    r"((?:https?://|www\.|HTTPS?://|WWW\.)[^\s]*?[.,:;!?;])\s+([a-z0-9])",
    re.IGNORECASE,
)
SENT_BOUNDARY = re.compile(
    r"""
    (?:
        (?:
            (?<!\b(?:Mr|Ms|Dr|Jr|Sr|St|vs|cf|Mt|Lt|Fr))
            (?<!\b(?:Mrs|Gen|Col|Maj|Sgt|Adm|Rev|Hon|Gov|Sen|Rep))
            (?<!\b(?:Prof|Capt|Pres))
            (?<!\b(?:e\.g|i\.e))
            (?<!\bpp)
            (?<!\b[pA-Z])
            \.
          |
            [!?…]
          |
            \.{3}
        )
        \s*
        (?:['"](?=\s|$))?
        \s*
        (?=
            $
          |
            (?:['"]\s*)*
            [A-Z0-9(]
        )
      |
        \n{2,}
    )
    """,
    re.VERBOSE,
)
# Single-quote version removed: (?<!\w)'\s*(.*?)\s*' pairs opening quotes with
# contraction apostrophes on the same line (e.g. 'don't), corrupting text.
# The rest of the pipeline handles single-quote spacing adequately.
# --- Hyphenation ---
RE_HYPHEN_FIX = re.compile(r"\b([a-zA-Z]+)-\s+([a-zA-Z]+)")
# Prefixes that should retain a hyphen even if found at a line break or followed by a space.
HYPHEN_PREFIXES = {
    "self",
    "ex",
    "all",
    "well",
    "ill",
    "near",
    "non",
    "anti",
    "semi",
    "quasi",
    "pseudo",
    "vice",
    "co",
    "cross",
    "high",
    "low",
    "mid",
    "snap",
    "neuro",
    # Numbers
    "twenty",
    "thirty",
    "forty",
    "fifty",
    "sixty",
    "seventy",
    "eighty",
    "ninety",
    # Common Prefixes & Adjectives
    "pre",
    "post",
    "sub",
    "super",
    "inter",
    "intra",
    "extra",
    "over",
    "under",
    "multi",
    "bi",
    "tri",
    "quad",
    "uni",
    "poly",
    "macro",
    "micro",
    "infra",
    "ultra",
    "hyper",
    "arch",
    "auto",
    "out",
    "deep",
    "wide",
    "long",
    "short",
    "hard",
    "soft",
    "full",
    "half",
    "double",
    "single",
    "neo",
    "pan",
    "pro",
    "top",
    "bottom",
    "up",
    "down",
    "off",
    "on",
    "back",
    "front",
    "side",
    "far",
}


def _fix_hyphen(m):
    # Helper callback for fixing hyphenated words broken across lines.
    c1, c2 = m.group(1), m.group(2)
    # If second part is capitalized and first is not, it's likely a new word/sentence (e.g. "I- I")
    if c2[0].isupper() and not c1.isupper():
        return m.group(0)
    # If both parts are capitalized, it's likely a proper noun compound (e.g. "Jean-Luc")
    if c1[0].isupper() and c2[0].isupper():
        return m.group(0)
    # Heuristic: Certain prefixes are almost always hyphenated when used as compound modifiers.
    if c1.lower() in HYPHEN_PREFIXES:
        return f"{c1}-{c2}" if c1.isupper() else f"{c1}-{c2.lower()}"
    # Stutter detection: If the suffix starts with the prefix, it's likely a stutter (e.g. "d- don't", "Th- Then")
    # We restrict this to short prefixes (< 4 chars) to avoid false positives on compound words that might share roots.
    if c2.lower().startswith(c1.lower()) and len(c1) < 4:
        return m.group(0)
    # Otherwise, merge. Ensure second part matches case of first (for ALL CAPS support)
    return c1 + (c2 if c1.isupper() else c2.lower())


# --- Boilerplate Chapter Titles ---
# Used to identify and skip indexing of pages belonging to boilerplate chapters.
BOILERPLATE_CHAPTER_TITLES = [
    re.compile(r"^\s*(?:Table of )?Contents\s*$", re.IGNORECASE | re.DOTALL),
    re.compile(r"^\s*Introduction(?:\s+to.*)?\s*$", re.IGNORECASE | re.DOTALL),
    re.compile(r"^\s*Preface\s*$", re.IGNORECASE | re.DOTALL),
    re.compile(r"^\s*Foreword\s*$", re.IGNORECASE | re.DOTALL),
    re.compile(
        r"^\s*About the (?:Author|Publisher|Illustrator)s?\s*$",
        re.IGNORECASE | re.DOTALL,
    ),
    re.compile(r"^\s*Copyright\s*$", re.IGNORECASE | re.DOTALL),
    re.compile(r"^\s*Dedication\s*$", re.IGNORECASE | re.DOTALL),
    re.compile(r"^\s*Acknowledgements?\s*$", re.IGNORECASE | re.DOTALL),
    re.compile(r"^\s*Legal\s*$", re.IGNORECASE | re.DOTALL),
    re.compile(r"^\s*eBook license\s*$", re.IGNORECASE | re.DOTALL),
    re.compile(r"^\s*Afterword\s*$", re.IGNORECASE | re.DOTALL),
    re.compile(r"^\s*Further reading.*$", re.IGNORECASE | re.DOTALL),
    re.compile(r"^\s*Praise for.*$", re.IGNORECASE | re.DOTALL),
    re.compile(r"^\s*Backlist\s*$", re.IGNORECASE | re.DOTALL),
    re.compile(
        r"^\s*An?\s+(?:[\w-]+\s+){1,5}Publication\s*$", re.IGNORECASE | re.DOTALL
    ),
    re.compile(r"^\s*Maps\s*$", re.IGNORECASE | re.DOTALL),
    re.compile(r"^\s*Title Page\s*$", re.IGNORECASE | re.DOTALL),
    re.compile(r"^\s*Front Matter\s*$", re.IGNORECASE | re.DOTALL),
    re.compile(r"^\s*Other Titles\s*$", re.IGNORECASE | re.DOTALL),
    re.compile(r"^\s*Also by this Author\s*$", re.IGNORECASE | re.DOTALL),
    re.compile(r"^\s*Excerpt from.*$", re.IGNORECASE | re.DOTALL),
    re.compile(r"^\s*An Extract from.*$", re.IGNORECASE | re.DOTALL),
    re.compile(r"^\s*Star Wars Legends Novels Timeline", re.IGNORECASE | re.DOTALL),
    re.compile(r"^\s*Star Wars Novels Timeline", re.IGNORECASE | re.DOTALL),
    re.compile(r"^\s*Timeline", re.IGNORECASE | re.DOTALL),
]
# --- OCR Character Normalization ---
# Control character removal is folded into this table so that str.translate handles
# both OCR normalization and ctrl char deletion in a single C-level scan, eliminating
# the need for a separate regex pass.
OCR_NORMALIZATION_MAP = {
    "\u201c": '"',
    "\u201d": '"',
    "\u2018": "'",
    "\u2019": "'",
    "\u00b4": "'",
    "`": "'",  # Backtick -> '
    "|": "I",  # Pipe -> I (OCR correction)
    "\u017f": "s",  # Long s -> s (safe)
    "\u0283": "f",  # Esh -> f (OCR artifact correction)
    "\u0279": "fi",  # Turned r -> fi (OCR artifact correction)
    "\u027b": "fl",  # Turned r with hook -> fl (OCR artifact correction)
    "\u027d": "ffi",  # Retroflex flap -> ffi (OCR artifact correction)
    "\u0280": "ff",  # Latin Letter Small Capital R -> ff (OCR artifact correction)
    "\u14fc": "\u00f1",  # Canadian Syllabics Western Cree R -> n-tilde (OCR artifact correction)
    "\xad": "",  # Soft hyphen -> remove entirely
    "\ufb00": "ff",
    "\ufb01": "fi",
    "\ufb02": "fl",
    "\ufb03": "ffi",
    "\ufb04": "ffl",  # Ligatures
    "\u2013": "-",
    "\u2014": "-",  # Dashes
    "\u2026": "...",  # Ellipsis
    "\xe6": "ae",
    "\u0153": "oe",  # Diphthongs
    "\xc6": "AE",
    "\u0152": "OE",  # Uppercase Diphthongs
    "\ufffd": "",  # Unicode replacement character
}
# Control chars: 0x00-0x08, 0x0B, 0x0C, 0x0E-0x1F, 0x7F (excludes tab 0x09, LF 0x0A, CR 0x0D)
# Also include surrogates (0xD800-0xDFFF) which are invalid in UTF-8.
for _c in (
    *range(0x00, 0x09),
    0x0B,
    0x0C,
    *range(0x0E, 0x20),
    0x7F,
    *range(0xD800, 0xE000),
):
    OCR_NORMALIZATION_MAP[chr(_c)] = ""
TRANS_TABLE = str.maketrans(OCR_NORMALIZATION_MAP)
RE_DUAL_WHITESPACE = re.compile(r"[^\S\n]\s+")


def _normalize_quotes_smart(text):
    # Stateful double-quote normalizer that handles merged spacing and chooses
    # smart quotes (u201c, u201d) based on positional heuristics and parity.
    # Resets at each newline to properly handle fiction paragraph quotes.
    if '"' not in text:
        return text

    blocks = text.split("\n")
    processed_blocks = []

    for block in blocks:
        if not block.strip() or '"' not in block:
            processed_blocks.append(block)
            continue

        result = []
        is_open = False
        chars = list(block)
        i = 0
        while i < len(chars):
            c = chars[i]
            if c == '"':
                prev_c = chars[i - 1] if i > 0 else " "
                next_char_exists = i < len(chars) - 1
                next_c = chars[i + 1] if next_char_exists else " "

                # Refined Heuristics:
                # - OPENING if preceded by whitespace, opening bracket, or punctuation followed immediately by a word (OCR merge).
                is_opening_heuristic = (
                    prev_c.isspace()
                    or (prev_c in "([{")
                    or (prev_c in ".:!?;," and next_char_exists and next_c.isalnum())
                )
                # - CLOSING if followed by whitespace or closing punctuation.
                is_closing_heuristic = next_c.isspace() or (next_c in ".,!?;)]}")

                # Resolve parity with heuristics as hints
                if is_opening_heuristic and not is_closing_heuristic:
                    quote_char = "\u201c"
                    is_open = True
                elif is_closing_heuristic and not is_opening_heuristic:
                    quote_char = "\u201d"
                    is_open = False
                else:
                    # Ambiguous case: rely on current parity
                    if is_open:
                        quote_char = "\u201d"
                        is_open = False
                    else:
                        quote_char = "\u201c"
                        is_open = True

                # Spacing logic:
                # - If opening after punctuation, insert space before: . " -> . “
                if quote_char == "\u201c" and prev_c in ".:!?;," and i > 0:
                    result.append(" ")

                result.append(quote_char)

                # - If closing before word, insert space after: "Word -> ” Word
                if quote_char == "\u201d" and next_c.isalnum() and i < len(chars) - 1:
                    result.append(" ")
            else:
                result.append(c)
            i += 1
        processed_blocks.append("".join(result))

    return "\n".join(processed_blocks)


def apply_display_fixes(text):
    # Shared display-time text fixes for contractions, quote spacing, and possessives.
    # Used by both indexing (clean_and_normalize_text) and search (snippet generation)
    # to ensure consistent text presentation.
    #
    # --- Pipeline ordering reference ---
    # The substitutions below are grouped into 4 phases. Ordering within and
    # between phases matters -- see the constraints noted on each.
    #
    # Phase 1: Contraction & possessive repair (steps 1-4)
    #   Fixes apostrophe-related OCR artifacts (glued, split, or spaced).
    #   MUST run before Phase 2 because the single-quote spacer (step 8) would
    #   otherwise insert spaces into contractions (e.g. don't -> don' t).
    #
    # Phase 2: Quote spacing normalization (steps 5-8)
    #   Fixes stray spaces around opening/closing quotes.
    #   Step 5 (close-quote space removal) MUST run before step 6 (open-quote
    #   space removal) to avoid misidentifying comma-preceded closing quotes
    #   as openers.
    #   Step 7 (single-quote spacer) is intentionally broad and MUST run after
    #   Phase 1. Step 8 (O'Brien fix) immediately undoes step 7 for known
    #   mid-word apostrophe patterns.
    #
    # Phase 3: Quote/period swap (steps 9-10)
    #   Fixes American vs British quote placement (e.g. skirts". -> skirts. ")
    #   No strict ordering dependency with Phase 2, but logically runs after
    #   quote spacing is settled.
    #
    # Phase 4: Final punctuation spacing (steps 11-14)
    #   Step 11 (ellipsis spacing) MUST run before step 13 (RE_FIX_PUNCT_SPACING_2),
    #   which would corrupt "...Word" into ".. . Word" by matching the trailing dot.
    #   Steps 12-13 handle general punctuation spacing artifacts.
    #   MUST run last because earlier phases can produce new [.!?]"[A-Z] patterns.
    # -- Phase -1: Filter Invalid Characters --
    # Remove Unicode replacement character (U+FFFD)
    text = text.replace("\ufffd", "")
    # -- Phase 0: URL & Abbreviation Protection --
    # Protect URLs and common abbreviations from being split by later spacing rules.
    # We replace punctuation with tokens and restore them at the end.
    if RE_URL_PROTECT.search(text):
        text = RE_URL_PROTECT.sub(
            lambda m: (
                m.group(0)
                .replace(".", "<PRD>")
                .replace(":", "<COLON>")
                .replace(",", "<COMMA>")
            ),
            text,
        )
    if RE_ABBREV_PROTECT.search(text):
        text = RE_ABBREV_PROTECT.sub(lambda m: m.group(0).replace(".", "<PRD>"), text)
    if RE_ENTITY_PROTECT.search(text):
        text = RE_ENTITY_PROTECT.sub(
            lambda m: m.group(0).replace(";", "<SEMICOLON>"), text
        )
    # -- Phase 0: OCR Artifact Fixes --
    # Fixes specific frequent OCR failures before general spacing rules apply.
    text = RE_FNE_FIX.sub("fine", text)
    text = RE_STUS_FIX.sub("stuff", text)
    # -- Phase 1: Contraction & possessive repair --
    if "'" in text:
        # 1a. Fix glued contractions using the combined high-performance regex (e.g. "that'sa" -> "that's a")
        text = RE_GLUED_CONTRACTION.sub(r"\1'\2 \3", text)
        # 1b. Fix possessive "s" glued to next word (e.g. parents'The -> parents' The)
        text = RE_POSSESSIVE_S_FIX.sub(r"\1' \2", text)
        # 2 & 3. Join contractions/possessives split by space after apostrophe (e.g. Don' t -> Don't, Cortnay' s -> Cortnay's)
        # Note: RE_CONTRACTION_SPACE_FIX explicitly covers all possessive subsets implicitly.
        text = RE_CONTRACTION_SPACE_FIX.sub(r"\1'\2", text)
        # 4. Fix space BEFORE apostrophe (e.g. John 's -> John's, Don 't -> Don't)
        text = RE_SPACE_BEFORE_APOSTROPHE.sub(r"\1'\2", text)
        # 5. Remove stray space before closing single quote (e.g. Europa, ' said -> Europa,' said)
        text = RE_SPACE_BEFORE_CLOSE_QUOTE.sub(r"\1'", text)

    # -- Phase 2: Quote spacing normalization --
    if "'" in text or '"' in text:
        # 6. Remove space after opening quote (e.g. " What" -> "What", ' What -> 'What)
        text = RE_OPEN_QUOTE_SPACE_FIX.sub(r"\1", text)
        # 7. Remove space before closing quote (e.g. Word ' -> Word')
        text = RE_CLOSE_QUOTE_SPACE_FIX.sub(r"\1", text)
        # 8. Add space around single quotes at word/punctuation boundaries (broad rule)
        #    8a. Punct+'I -> Punct. 'I (space before quote for openers after punctuation)
        text = RE_SINGLE_QUOTE_SPACER_BEFORE.sub(r" '", text)
        #    8b. Smart Quote & Dialogue Normalization (State-based)
        text = _normalize_quotes_smart(text)
        # Handle remaining single quote spacing
        if "'" in text:
            text = RE_COMMA_SINGLE_QUOTE_SPACE.sub(r"\1' ", text)
            text = RE_PUNCT_SINGLE_QUOTE_SPACE.sub(r"\1' ", text)
            text = RE_CLOSE_SINGLE_QUOTE_SPACE.sub(r"\1 ", text)
        if '"' in text:
            text = RE_DOT_COMMA_QUOTE_LOWERCASE.sub(r'\1 "', text)
            text = RE_CLOSE_QUOTE_SPACE.sub(r"\1 ", text)
        text = RE_JOINED_DOUBLE_QUOTES.sub(r'\1" "', text)
        text = RE_JOINED_SINGLE_QUOTES.sub(r"\1' '", text)
    # -- Phase 3: Dialogue Cleanup --
    # 9. Fix quote-period swap (e.g. Word'. Next -> Word. 'Next)
    #    Restored and refined to only swap when followed by a capital letter (start of new sentence).
    text = RE_QUOTE_PERIOD_SWAP_ALL.sub(r"\1. \2\3", text)
    # -- Phase 4: Final punctuation spacing --
    # 10. Ensure space after [.!?]" + capital letter (e.g. end."He -> end." He)
    text = RE_PUNCT_QUOTE_CAP.sub(r'\1" \2', text)
    # 11. Insert space between closing ellipsis and next sentence start (e.g. "...Next" -> "... Next").
    #     MUST precede RE_FIX_PUNCT_SPACING_2 (step 13), which would corrupt
    #     "...Word" into ".. . Word" by matching the trailing dot as punctuation.
    text = RE_ELLIPSIS_SPACE.sub("... ", text)
    # 12. Fix spacing around punctuation (e.g. "word ," -> "word,")
    text = RE_FIX_PUNCT_SPACING_1.sub(r"\1", text)
    # 13. Ensure space after punctuation (e.g. "word,word" -> "word, word")
    #     This regex specifically targets punctuation followed by a letter, protecting numbers like 1,000.
    text = RE_FIX_PUNCT_SPACING_2.sub(r"\1 ", text)
    #    14. Final cleanup for trailing quoted punctuation (e.g. unlimited. " -> unlimited.")
    text = RE_PUNCT_QUOTE_CLEANUP.sub(r"\1\2", text)
    # Final: Restore abbreviations & URLs
    text = (
        text.replace("<PRD>", ".")
        .replace("<COLON>", ":")
        .replace("<COMMA>", ",")
        .replace("<SEMICOLON>", ";")
    )
    # -- Phase 5: URL Healer --
    # Fix existing mangled URLs (e.g. from previous bugs) by rejoining sequences
    # starting with http/www that were split by spaces. RECURSIVE (max 5).
    # Optimization: Only attempt if URL markers are present.
    if "http" in text or "www." in text or "HTTP" in text or "WWW." in text:
        for _ in range(5):
            orig = text
            # 1. Fix "www. google" -> "www.google" (mangled after the anchor itself)
            text = RE_URL_MANGLE_FIX_1.sub(r"\1\2", text)
            # 2. Fix "google. com" -> "google.com" (mangled in subsequent parts)
            text = RE_URL_MANGLE_FIX_2.sub(r"\1\2", text)
            if text == orig:
                break
    return text


def strip_surrogates(text):
    """
    Remove surrogate characters (U+D800 to U+DFFF) from a string.
    These are invalid in UTF-8 and cause errors when saving to SQLite.
    """
    if not isinstance(text, str) or not text:
        return text
    # Optimization: Only clean if surrogates are actually present.
    # The encode/decode cycle is expensive for large text blocks.
    if not RE_SURROGATES.search(text):
        return text.replace("\ufffd", "") if "\ufffd" in text else text

    # The 'ignore' error handler with utf-8 encoding/decoding is a standard
    # way to strip these invalid characters in Python.
    cleaned = text.encode("utf-8", "ignore").decode("utf-8")
    # Filter out Unicode replacement character (U+FFFD) from DB-bound strings.
    return cleaned.replace("\ufffd", "")


def clean_db_string(obj):
    """
    Recursively strips surrogates from strings, lists, or tuples.
    Used to sanitize database parameters before execution.
    """
    if isinstance(obj, str):
        return strip_surrogates(obj)
    elif isinstance(obj, list):
        return [clean_db_string(item) for item in obj]
    elif isinstance(obj, tuple):
        return tuple(clean_db_string(item) for item in obj)
    return obj


def truncate_text(text: str, limit: int) -> str:
    """Truncates text to limit and adds ellipsis if necessary."""
    return text if len(text) <= limit else text[: limit - 1] + "…"


def is_folder_allowed(folder: str, allowed: list) -> bool:
    """Checks if a folder path is within the allowed whitelist."""
    if not allowed:
        return True
    for a in allowed:
        if folder == a or folder.startswith(a + "/"):
            return True
    return False


def get_instance_lock(lock_name):
    """
    Ensures only one instance of the application runs at a time using a file lock.
    Returns the file handle if lock is acquired, or None if already locked.
    """
    import os
    import sys

    # Use the script's directory for the lock file
    script_file = sys.modules["__main__"].__file__ if "__main__" in sys.modules and getattr(sys.modules["__main__"], "__file__", None) else __file__
    script_dir = os.path.dirname(os.path.abspath(str(script_file)))
    lock_path = os.path.join(script_dir, f"{lock_name}.lock")
    f = None
    try:
        f = open(lock_path, "a+")
        if os.name == "nt":
            import msvcrt

            f.seek(0)
            msvcrt.locking(f.fileno(), msvcrt.LK_NBLCK, 1)
        else:
            import fcntl

            fcntl.flock(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
        return f
    except (IOError, BlockingIOError, PermissionError):
        if f is not None:
            f.close()
        return None
