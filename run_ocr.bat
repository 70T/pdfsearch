@echo off

echo Processing OCR queue...
python process_ocr_queue.py %*

if errorlevel 1 (
    echo.
    echo OCR processing finished with errors. Review output above.
    pause
    exit /b 1
)

pause
