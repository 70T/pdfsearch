@echo off
echo ==========================================
echo      RUNNING PDFSEARCH TEST SUITE
echo ==========================================
echo.

cd test
python run_all_tests.py
set EXIT_CODE=%errorlevel%
cd ..

if %EXIT_CODE% neq 0 (
    echo.
    echo [FAIL] Some tests failed! Check output above.
    pause
    exit /b 1
)

echo.
echo [SUCCESS] All tests passed.
pause