import unittest
import sys
import random
import os

# Add parent directory to path so tests can import app modules
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

# Import your test classes
from test_heuristics import TestHeuristics
from test_search_logic import TestSearchLogic
from test_pdf_heuristics import TestPdfHeuristics
from test_shared_utils import TestSharedUtils
from test_database import TestInitDb, TestFileOperations, TestWipeDb
from test_fts_queries import TestBasicSearch, TestHyphenatedSearch, TestFTSErrorHandling


def create_master_suite():
    loader = unittest.TestLoader()

    # Heuristics Tests (Randomized)
    # We explicitly load and shuffle these to catch edge cases as requested previously
    suite_heuristics = loader.loadTestsFromTestCase(TestHeuristics)
    tests_heuristics = list(suite_heuristics)
    random.shuffle(tests_heuristics)

    # Search Logic Tests
    suite_search = loader.loadTestsFromTestCase(TestSearchLogic)

    # PDF Heuristics Tests
    suite_pdf = loader.loadTestsFromTestCase(TestPdfHeuristics)

    # Shared Utils Tests
    suite_shared = loader.loadTestsFromTestCase(TestSharedUtils)

    # Database Tests
    suite_db_init = loader.loadTestsFromTestCase(TestInitDb)
    suite_db_ops = loader.loadTestsFromTestCase(TestFileOperations)
    suite_db_wipe = loader.loadTestsFromTestCase(TestWipeDb)

    # FTS Queries Tests
    suite_fts_basic = loader.loadTestsFromTestCase(TestBasicSearch)
    suite_fts_hyphen = loader.loadTestsFromTestCase(TestHyphenatedSearch)
    suite_fts_error = loader.loadTestsFromTestCase(TestFTSErrorHandling)

    # Other Feature Tests (Load locally to avoid import errors if not imported at top)
    from test_routes import TestRoutes
    from test_sorting_feature import TestSortingFeature
    from test_ux_features import TestUXFeatures

    suite_routes = loader.loadTestsFromTestCase(TestRoutes)
    suite_sorting = loader.loadTestsFromTestCase(TestSortingFeature)
    suite_ux = loader.loadTestsFromTestCase(TestUXFeatures)

    # Combine into one master suite
    master_suite = unittest.TestSuite(tests_heuristics)
    master_suite.addTests(suite_search)
    master_suite.addTests(suite_pdf)
    master_suite.addTests(suite_shared)

    # Add newly discovered suites
    master_suite.addTests(suite_db_init)
    master_suite.addTests(suite_db_ops)
    master_suite.addTests(suite_db_wipe)
    master_suite.addTests(suite_fts_basic)
    master_suite.addTests(suite_fts_hyphen)
    master_suite.addTests(suite_fts_error)
    master_suite.addTests(suite_routes)
    master_suite.addTests(suite_sorting)
    master_suite.addTests(suite_ux)

    return master_suite


if __name__ == "__main__":
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(create_master_suite())

    if not result.wasSuccessful():
        print("\n" + "=" * 40)
        print("FAILURE SUMMARY:")
        print("=" * 40)

        if result.failures:
            print("\nFAILED TESTS:")
            for test, trace in result.failures:
                print(f" - {test.id()}")

        if result.errors:
            print("\nERRORS:")
            for test, trace in result.errors:
                print(f" - {test.id()}")
        print("=" * 40 + "\n")

    # Exit with code 1 if failed, 0 if success (for the batch file to catch)
    sys.exit(not result.wasSuccessful())
