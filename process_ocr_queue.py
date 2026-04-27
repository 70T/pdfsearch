import json
import logging
import os
import shutil
import subprocess
import sys

# --- Configuration ---
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
LIST_MISSING = os.path.join(SCRIPT_DIR, "ocr_todo_list_missing.txt")
LIST_REDO = os.path.join(SCRIPT_DIR, "ocr_todo_list_redo.txt")
LIST_RETRIES = os.path.join(SCRIPT_DIR, "ocr_todo_list_retries.json")
LIST_POISON = os.path.join(SCRIPT_DIR, "ocr_failed_permanently.txt")

# Number of OCR worker threads: all cores minus one for system responsiveness.
CPU_COUNT = os.cpu_count() or 2
JOBS_COUNT = max(1, CPU_COUNT - 1)

# Maximum lines of stderr to print on failure (avoids log flooding).
STDERR_TAIL_LINES = 20

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Tool detection
# ---------------------------------------------------------------------------


def check_ocrmypdf_installed():
    if shutil.which("ocrmypdf") is None:
        logger.error("'ocrmypdf' not found in PATH.")
        logger.error(
            "  Install guide: https://ocrmypdf.readthedocs.io/en/latest/installation.html"
        )
        return False
    return True


_GS_BINARY = None


def get_ghostscript_binary():
    global _GS_BINARY
    if _GS_BINARY:
        return _GS_BINARY
    for binary in ("gswin64c", "gswin32c", "gs"):
        if shutil.which(binary):
            _GS_BINARY = binary
            return binary
    return None


# ---------------------------------------------------------------------------
# PDF sanitization via Ghostscript
# ---------------------------------------------------------------------------


def sanitize_pdf(input_path, output_path):
    """Attempt to repair a corrupt PDF with Ghostscript. Returns True on success."""
    gs = get_ghostscript_binary()
    if not gs:
        logger.warning("Ghostscript not found; cannot sanitize PDF.")
        return False

    cmd = [
        gs,
        "-o",
        output_path,
        "-sDEVICE=pdfwrite",
        "-dPDFSETTINGS=/default",
        "-dPassThroughJPEGImages=false",  # Force JPEG re-encoding to fix corruption
        "-dMaxPatternBitmap=500000",  # Allocate more memory for huge pattern tiles (bypasses slow clist caching)
        "-c",
        "30000000 setvmthreshold",
        "-f",  # Provide 30MB extra RAM for vector states, fonts, and large documents
        "-dNOPAUSE",
        "-dBATCH",
        "-dQUIET",
        input_path,
    ]
    try:
        subprocess.run(cmd, check=True, capture_output=True)
        return True
    except subprocess.CalledProcessError as e:
        logger.debug(
            "Ghostscript sanitization failed:\n%s",
            e.stderr.decode(errors="replace"),
        )
        return False


# ---------------------------------------------------------------------------
# Core processing
# ---------------------------------------------------------------------------


def _stderr_tail(text):
    lines = text.splitlines()
    tail = lines[-STDERR_TAIL_LINES:]
    return "\n".join(tail)


def _build_base_cmd(enhance):
    """Return the ocrmypdf command prefix shared by all invocations."""
    cmd = [
        "ocrmypdf",
        "--jobs",
        str(JOBS_COUNT),
        "--optimize",
        "0",  # Avoid "image file is truncated" errors
        "--output-type",
        "pdf",  # Plain PDF; PDF/A strict validation causes spurious failures
        "-l",
        "eng",
        "--fast-web-view",
        "999999",  # Disable linearisation; we don't need it here
        "--tesseract-timeout",
        "300",  # 5 min per page; prevents hangs on complex pages
        "--skip-big",
        "150",  # Skip pages over 150 megapixels to avoid OOM
    ]
    if enhance:
        cmd.append("--deskew")
        cmd.append("--rotate-pages")
        # Optimize unpaper: Enforce single-page layout to prevent it from wasting CPU heuristics
        # trying to aggressively slice large full-page RPG artworks in half.
        cmd.extend(["--unpaper-args", "--layout single"])
    return cmd


def _run_ocrmypdf(cmd):
    """Run a command, returning the CompletedProcess. Low priority on Windows."""
    if sys.platform == "win32":
        return subprocess.run(cmd, capture_output=True, text=True, creationflags=0x00004000)
    return subprocess.run(cmd, capture_output=True, text=True)


def process_file_list(list_file, ocr_args, enhance=False):
    """
    Read a todo list and OCR each file in place.

    On first failure, attempts Ghostscript sanitization and retries.
    The list file is rewritten at the end to contain only failures.
    Returns the count of successfully processed files.

    Parameters
    ----------
    list_file : str
        Path to the newline-delimited list of PDF paths.
    ocr_args : list[str]
        Extra arguments appended to the ocrmypdf command (e.g. --force-ocr).
    enhance : bool
        When True, adds --deskew and --rotate-pages for scanned material.
        Requires unpaper.
    """
    if not os.path.exists(list_file):
        logger.info("No '%s' found.", list_file)
        return 0

    with open(list_file, "r", encoding="utf-8") as f:
        files = [line.strip() for line in f if line.strip()]

    if not files:
        logger.info("List '%s' is empty.", list_file)
        return 0

    logger.info("Found %d file(s) in '%s'.", len(files), list_file)

    has_unpaper = shutil.which("unpaper") is not None
    if enhance and not has_unpaper:
        logger.warning(
            "--enhance requested but 'unpaper' is not installed; skipping deskew/rotate."
        )
        enhance = False

    base_cmd = _build_base_cmd(enhance)
    successful = []
    failed = []

    retries_dict = {}
    if os.path.exists(LIST_RETRIES):
        try:
            with open(LIST_RETRIES, "r", encoding="utf-8") as f:
                retries_dict = json.load(f)
        except Exception:
            pass

    for i, file_path in enumerate(files):
        logger.info("[%d/%d] Processing: %s", i + 1, len(files), file_path)

        if not os.path.exists(file_path):
            logger.warning("File not found, skipping: %s", file_path)
            failed.append(file_path)
            continue

        temp_output = file_path + ".ocr_temp.pdf"

        try:
            # --- First attempt ---
            cmd = base_cmd + ocr_args + [file_path, temp_output]
            result = _run_ocrmypdf(cmd)

            if result.returncode == 0:
                shutil.move(temp_output, file_path)
                logger.info("Success: %s", os.path.basename(file_path))
                successful.append(file_path)
                continue

            # --- Sanitize and retry ---
            logger.warning(
                "OCR failed (exit %d). Attempting Ghostscript sanitization.",
                result.returncode,
            )
            sanitized_input = file_path + ".sanitized.pdf"

            try:
                if sanitize_pdf(file_path, sanitized_input):
                    cmd_retry = base_cmd + ocr_args + [sanitized_input, temp_output]
                    result2 = _run_ocrmypdf(cmd_retry)

                    if result2.returncode == 0:
                        shutil.move(temp_output, file_path)
                        logger.info(
                            "Success after sanitization: %s",
                            os.path.basename(file_path),
                        )
                        successful.append(file_path)
                        continue

                    logger.error(
                        "Failed after sanitization (exit %d). Last %d lines of stderr:\n%s",
                        result2.returncode,
                        STDERR_TAIL_LINES,
                        _stderr_tail(result2.stderr),
                    )
                else:
                    logger.error(
                        "Sanitization failed. Last %d lines of stderr from first attempt:\n%s",
                        STDERR_TAIL_LINES,
                        _stderr_tail(result.stderr),
                    )
            finally:
                if os.path.exists(sanitized_input):
                    os.remove(sanitized_input)

            failed.append(file_path)

        except Exception as e:
            logger.error("Unhandled exception processing '%s': %s", file_path, e)
            failed.append(file_path)

        finally:
            if os.path.exists(temp_output):
                os.remove(temp_output)

    # --- Summary ---
    logger.info("=" * 40)
    logger.info(
        "Batch complete. Successful: %d  Failed: %d", len(successful), len(failed)
    )

    failed_retry = []
    for p in failed:
        count = retries_dict.get(p, 0) + 1
        retries_dict[p] = count
        if count >= 3:
            logger.error("File '%s' failed OCR 3 times. Moving to poison list.", p)
            try:
                with open(LIST_POISON, "a", encoding="utf-8") as pf:
                    pf.write(p + "\n")

                ignore_file = os.path.join(os.path.dirname(p), ".pdfsearchignore")
                with open(ignore_file, "a", encoding="utf-8") as ignf:
                    ignf.write(
                        "\n# Auto-added by OCR failure poison pill\n"
                        + os.path.basename(p)
                        + "\n"
                    )
            except Exception as e:
                logger.warning("Failed to add %s to poison pill config: %s", p, e)
        else:
            failed_retry.append(p)

    # Save retry state
    try:
        with open(LIST_RETRIES, "w", encoding="utf-8") as f:
            json.dump(retries_dict, f)
    except Exception as e:
        logger.warning("Failed to save retry dictionary: %s", e)

    if failed_retry:
        logger.warning("Files queued for retry (%d):", len(failed_retry))
        for p in failed_retry:
            logger.warning("  %s", p)
        with open(list_file, "w", encoding="utf-8") as f:
            for p in failed_retry:
                f.write(p + "\n")
        logger.info(
            "Rewrote '%s' with %d failed file(s).", list_file, len(failed_retry)
        )
    else:
        if os.path.exists(list_file):
            os.remove(list_file)
        logger.info("All files processed or poisoned. Removed '%s'.", list_file)

    return len(successful)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main():
    if not check_ocrmypdf_installed():
        sys.exit(1)

    # Single-file force mode
    if len(sys.argv) > 1:
        target_file = sys.argv[1]
        if not os.path.isfile(target_file):
            logger.error("Argument is not a file: %s", target_file)
            sys.exit(1)

        logger.info("--- Force-processing single file: %s ---", target_file)
        temp_list = os.path.join(SCRIPT_DIR, "temp_ocr_force.txt")
        try:
            with open(temp_list, "w", encoding="utf-8") as f:
                f.write(target_file + "\n")
            process_file_list(temp_list, ["--force-ocr"])
        finally:
            if os.path.exists(temp_list):
                os.remove(temp_list)
        return

    total_success = 0

    # --redo-ocr: removes any existing OCR layer and re-runs Tesseract,
    # but preserves pages that contain native (non-OCR) digital text.
    # Use this for PDFs that were scanned and OCR'd with a poor engine,
    # producing a bad OCR layer over a page that has no native text.
    # Note: this will NOT strip garbage text left by a prior OCR pass
    # if that pass embedded it as native text; use --force-ocr for that.
    logger.info("--- Processing un-OCR'd files (missing text layer) ---")
    total_success += process_file_list(LIST_MISSING, ["--redo-ocr"])

    # --force-ocr: discards all existing text and runs Tesseract unconditionally.
    logger.info("\n--- Processing garbage-text files (forced redo) ---")
    total_success += process_file_list(LIST_REDO, ["--force-ocr"])

    if total_success > 0:
        logger.info(
            "\nTip: re-run the Index Builder in the web app to index newly OCR'd files."
        )


if __name__ == "__main__":
    main()
