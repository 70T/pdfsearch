import unittest
import logging
import sys
import os
import html

# Configure logging
logging.basicConfig(level=logging.DEBUG, format="%(message)s")

# Add parent directory to path so tests can import app modules
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from shared_utils import apply_display_fixes, TRANS_TABLE  # noqa: E402
from search_logic import _get_full_sentence_snippet  # noqa: E402


def _get_test_snippet(text, terms, min_match_pos=0, highlight_patterns=None):
    # Mimics the data processing in search_logic.py (_get_cleaned_text + _cached_snippet)
    cleaned = apply_display_fixes(text.translate(TRANS_TABLE))
    escaped = html.escape(cleaned, quote=False)
    return _get_full_sentence_snippet(escaped, terms, min_match_pos, highlight_patterns)


class TestSearchLogic(unittest.TestCase):
    def test_basic_highlighting(self):
        text = "The quick brown fox jumps over the lazy dog."
        terms = ['"fox"']
        snippet = _get_test_snippet(text, terms)
        self.assertIn("<b>fox</b>", snippet)
        self.assertIn("The quick brown <b>fox</b> jumps", snippet)

    def test_abbreviation_handling(self):
        # Ensure sentence boundary doesn't break on Mr. or e.g.
        text = "Mr. Fox is smart. He lives in a hole. See e.g. the diagram."
        terms = ['"Fox"']
        snippet = _get_test_snippet(text, terms)
        # Should include "Mr. Fox is smart." without cutting at "Mr."
        self.assertIn("Mr. <b>Fox</b> is smart.", snippet)

        terms_eg = ['"diagram"']
        snippet_eg = _get_test_snippet(text, terms_eg)
        self.assertIn("See e.g. the <b>diagram</b>.", snippet_eg)

    def test_initials_handling(self):
        # Ensure sentence boundary doesn't break on initials (J.K. Rowling)
        text = "Written by J.K. Rowling in the 90s."
        terms = ['"Rowling"']
        snippet = _get_test_snippet(text, terms)
        self.assertIn("Written by J.K. <b>Rowling</b> in the 90s.", snippet)

    def test_quote_expansion(self):
        # Test that snippet expands to cover the full quote if the match is inside it
        text = 'Then he said, "I will catch that fox eventually, I promise." The story ends.'
        terms = ['"fox"']
        snippet = _get_test_snippet(text, terms)
        self.assertIn("“I will catch that <b>fox</b> eventually, I promise.”", snippet)

    def test_quote_expansion_glued(self):
        # Test quote expansion when text is glued (e.g. end."Start)
        text = 'He said, "The fox is fast."But the dog is slow.'
        terms = ['"fox"']
        snippet = _get_test_snippet(text, terms)
        self.assertIn("“The <b>fox</b> is fast. “", snippet)

    def test_smart_quote_normalization(self):
        # Search term is "fox", text has smart quotes.
        # The snippet generator normalizes quotes to ASCII for display consistency.
        text = "The “fox” is cunning."
        terms = ['"fox"']
        snippet = _get_test_snippet(text, terms)
        self.assertIn("The “<b>fox</b>” is cunning.", snippet)

    def test_html_safety(self):
        # Ensure HTML tags in the source text are escaped so they don't render
        text = "Use the <script> tag for the fox."
        terms = ['"fox"']
        snippet = _get_test_snippet(text, terms)
        self.assertIn("&lt;script&gt;", snippet)
        self.assertIn("<b>fox</b>", snippet)

    def test_snippet_cleanup_heuristics(self):
        # Test the display-time cleanup regexes (e.g. missing spaces, contractions)
        # Input: "Hello"he said. "That'sa fox".
        text = 'He said "Hello"he said. "That\'sa fox".'
        terms = ['"Hello"']
        snippet = _get_test_snippet(text, terms)

        # Check missing space after quote: "Hello"he -> "Hello" he
        self.assertIn("“<b>Hello</b>” he said", snippet)

        # Check contraction fix: "That'sa" -> "That's a"
        self.assertIn("That's a fox", snippet)

    def test_dialogue_opening_quote_retention(self):
        # Regression test: Ensure opening quotes of dialogue are not stripped
        # when they follow a sentence boundary with a space.
        text = "Previous sentence. 'I just do. I have faith.' 'Faith?' 'Yes,' answered Aximand."
        terms = ['"faith"']
        snippet = _get_test_snippet(text, terms)
        self.assertIn("'I just do.", snippet)

    def test_glued_opening_quote_boundary(self):
        # Regression test: Ensure opening quote immediately following a period (glued)
        # is NOT consumed by the sentence boundary.
        text = "Previous.‘I just do. I have faith.’"
        terms = ['"faith"']
        snippet = _get_test_snippet(text, terms)
        self.assertIn("'I just do.", snippet)

    def test_dialogue_tag_fixes(self):
        # Test RE_FIX_QUOTE_AFTER_TAG_1: said Haldon." By -> said Haldon. "By
        text = 'He said Haldon." By the throne.'
        terms = ['"Haldon"']
        snippet = _get_test_snippet(text, terms)
        self.assertIn("said <b>Haldon</b>.” By", snippet)

        # Test RE_SINGLE_QUOTE_PERIOD_SWAP: skirts'. Your -> skirts. 'Your
        text = "The skirts'. Your turn."
        terms = ['"skirts"']
        snippet = _get_test_snippet(text, terms)
        self.assertIn("<b>skirts</b>. 'Your", snippet)

    def test_hyphen_highlighting_space_separated(self):
        # Term "kelbor-hal" should highlight "Kelbor Hal" (space instead of hyphen)
        text = "She is the newly arrived representative of Kelbor Hal, the Fabricator General."
        terms = ["kelbor-hal"]
        snippet = _get_test_snippet(text, terms)
        self.assertIn("<b>Kelbor Hal</b>", snippet)

    def test_hyphen_highlighting_with_hyphen(self):
        # Term "kelbor-hal" should highlight "Kelbor-Hal" (actual hyphen)
        text = "She is the representative of Kelbor-Hal, the Fabricator."
        terms = ["kelbor-hal"]
        snippet = _get_test_snippet(text, terms)
        self.assertIn("<b>Kelbor-Hal</b>", snippet)

    def test_hyphen_highlighting_merged(self):
        # Term "kelbor-hal" should highlight "KelborHal" (no separator)
        text = "The agent of KelborHal arrived at dawn."
        terms = ["kelbor-hal"]
        snippet = _get_test_snippet(text, terms)
        self.assertIn("<b>KelborHal</b>", snippet)


# def test_closing_single_quote_spacing(self):
#     # Ensure that no space is added before a closing single quote
#     text = "It was,' says Oll. He cried,'Hello there.'"
#     terms = ['"Oll"']
#     snippet = _get_test_snippet(text, terms)
#     self.assertIn("was,' says <b>Oll</b>.", snippet)
#
#     terms2 = ['"Hello"']
#     snippet2 = _get_test_snippet(text, terms2)
#     self.assertIn("cried, '<b>Hello</b> there.'", snippet2)

if __name__ == "__main__":
    unittest.main()
