import argparse
from email.utils import formatdate
import logging
import os
import sqlite3
import subprocess
import sys
import secrets
import threading

from concurrent.futures import BrokenExecutor, ProcessPoolExecutor

from flask import (
    Flask,
    flash,
    g,
    jsonify,
    redirect,
    render_template,
    request,
    send_from_directory,
    session,
    url_for,
)
import database as db
import indexing_logic
import search_logic
from shared_utils import natural_sort_key, get_instance_lock

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# --- Environment & Flask App ---

logging.getLogger("waitress.queue").setLevel(logging.ERROR)

app = Flask(__name__)


# --- Security & Middlewares ---


def _load_or_create_secret_key():
    key_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".secret_key")
    if os.path.exists(key_path):
        with open(key_path, "r") as f:
            return f.read().strip()
    key = secrets.token_hex(32)
    with open(key_path, "w") as f:
        f.write(key)
    return key


app.secret_key = _load_or_create_secret_key()
app.teardown_appcontext(db.close_db)


def generate_csrf_token():
    if "csrf_token" not in session:
        session["csrf_token"] = secrets.token_hex(32)
    return session["csrf_token"]


@app.before_request
def check_csrf():
    if request.method == "POST":
        token = session.get("csrf_token")
        form_token = request.form.get("csrf_token")
        if not token or not form_token or not secrets.compare_digest(token, form_token):
            return "CSRF validation failed.", 403


@app.before_request
def set_csp_nonce():
    g.csp_nonce = secrets.token_urlsafe(16)


def get_csp_nonce():
    return getattr(g, "csp_nonce", "")


@app.after_request
def set_security_headers(response):
    nonce = getattr(g, "csp_nonce", "")
    response.headers["Content-Security-Policy"] = (
        f"default-src 'self'; "
        f"script-src 'nonce-{nonce}'; "
        f"style-src 'self' 'unsafe-inline'"
    )
    response.headers["X-Frame-Options"] = "DENY"
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["Referrer-Policy"] = "no-referrer"
    return response


SEARCH_RESULTS_LIMIT = 20


# --- Indexing Manager ---


SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

# Global cache for folders to reduce DB hits on search page load
FOLDER_CACHE = None
_folder_cache_lock = threading.Lock()


def get_cached_folders(db_name):
    global FOLDER_CACHE
    with _folder_cache_lock:
        if FOLDER_CACHE is None:
            FOLDER_CACHE = db.get_unique_folders(db_name)
            FOLDER_CACHE.sort(key=natural_sort_key)
        return FOLDER_CACHE


# --- Background Indexing State ---
_indexing_state = {
    "running": False,
    "done": False,
    "error": None,
    "total": 0,
    "indexed": 0,
    "skipped": 0,
    "failed": 0,
    "current_file": "",
    "indexed_files": [],
    "skipped_files": [],
    "failed_files": [],
}
_indexing_lock = threading.Lock()


# --- Jinja Helpers ---


# Helper for browser links
def generate_browser_link(relative_path, page_num=1):
    # Always returns a plain #page= anchor which works in all PDF viewers.
    if not app.config.get("FILES_DIRECTORY"):
        return None
    url = url_for("serve_files", filename=relative_path.replace("\\", "/"))
    return f"{url}#page={page_num}"


# Register helper for use in templates
app.jinja_env.globals.update(
    generate_browser_link=generate_browser_link,
    csrf_token=generate_csrf_token,
    csp_nonce=get_csp_nonce,
)


# --- Web Routes ---


@app.route("/favicon.ico")
def favicon():
    return "", 204


@app.route("/", methods=["GET"])
def search_form():
    # Handles the main search page. Uses GET so that search URLs are
    # bookmarkable and shareable. If a search_query param is present,
    # performs the search; otherwise renders the empty form.
    db_name = app.config["DATABASE"]
    folders = get_cached_folders(db_name)

    search_query = request.args.get("search_query", "").strip()

    if search_query:
        try:
            offset = int(request.args.get("offset", 0))
        except (ValueError, TypeError):
            offset = 0
        limit = SEARCH_RESULTS_LIMIT  # Number of books to display per page
        raw_folders = request.args.getlist("folders")
        selected_folders = []
        for f in raw_folders:
            selected_folders.extend([x.strip() for x in f.split(",") if x.strip()])
        sort_by = request.args.get("sort_by", "filename")
        try:
            max_matches = int(request.args.get("max_matches_per_book", 0))
        except (ValueError, TypeError):
            max_matches = 0

        if not db.check_db_has_content(db_name):
            flash(
                "Database is empty or not initialized. Please index a directory first.",
                "error",
            )
            return redirect(url_for("index_builder"))

        results_list, has_more, search_error, search_terms = (
            search_logic.perform_search(
                db_name,
                search_query,
                limit=limit,
                offset=offset,
                selected_folders=selected_folders,
                sort_by=sort_by,
                max_matches_per_book=max_matches if max_matches > 0 else None,
            )
        )

        if search_error:
            flash(search_error, "error")
            return render_template(
                "search_form.html", folders=folders, prefill_query=search_query
            )

        if results_list:
            # Extract metadata from the custom list object
            total_books = getattr(results_list, "total_books", 0)
            total_pages = getattr(results_list, "total_pages", 0)

            results = []
            for i, (filename, relative_path, matches, book_total_matches) in enumerate(
                results_list, start=offset + 1
            ):
                open_link = url_for(
                    "open_in_viewer", filename=relative_path.replace("\\", "/")
                )
                folder_name = os.path.dirname(relative_path)
                results.append(
                    (
                        i,
                        filename,
                        relative_path,
                        folder_name,
                        open_link,
                        matches,
                        book_total_matches,
                    )
                )
            return render_template(
                "results.html",
                search_query=search_query,
                results=results,
                total_books=total_books,
                total_pages=total_pages,
                current_offset=offset,
                has_more=has_more,
                next_offset=offset + limit,
                folders=folders,
                selected_folders=selected_folders,
                search_terms=search_terms,
                sort_by=sort_by,
            )
        else:
            return render_template(
                "no_results.html",
                search_query=search_query,
                folders=folders,
                selected_folders=selected_folders,
            )

    return render_template("search_form.html", folders=folders, prefill_query="")


@app.route("/api/snippets")
def api_snippets():
    """Generate a snippet for a specific file and page on demand."""
    db_name = app.config["DATABASE"]
    try:
        file_id = int(request.args.get("file_id"))
        page_num = int(request.args.get("page_num"))
        search_query = request.args.get("search_query", "")
    except (ValueError, TypeError):
        return jsonify({"error": "Invalid parameters"}), 400

    if not search_query:
        return jsonify({"error": "Missing search query"}), 400

    snippet = search_logic.get_snippet_for_page(
        db_name, file_id, page_num, search_query
    )
    # The snippet is safe HTML (contains <b> tags).
    return jsonify({"snippet": snippet})


@app.route("/api/search")
def api_search():
    """Main search API endpoint returning results as JSON."""
    db_name = app.config["DATABASE"]
    search_query = request.args.get("search_query", "").strip()
    if not search_query:
        return jsonify({"error": "search_query is required"}), 400

    try:
        offset = max(0, int(request.args.get("offset", 0)))
        limit = min(
            max(1, int(request.args.get("limit", SEARCH_RESULTS_LIMIT))),
            SEARCH_RESULTS_LIMIT,
        )
    except (ValueError, TypeError):
        offset, limit = 0, SEARCH_RESULTS_LIMIT

    raw_folders = request.args.getlist("folders")
    selected_folders = []
    for f in raw_folders:
        selected_folders.extend([x.strip() for x in f.split(",") if x.strip()])
    sort_by = request.args.get("sort_by", "filename")
    try:
        max_matches = int(request.args.get("max_matches_per_book", 0))
    except (ValueError, TypeError):
        max_matches = 0

    if not db.check_db_has_content(db_name):
        return jsonify({"error": "Index is empty."}), 503

    # Call with exactly relevant arguments
    results_list, has_more, search_error, _ = search_logic.perform_search(
        db_name,
        search_query,
        limit=limit,
        offset=offset,
        selected_folders=selected_folders,
        sort_by=sort_by,
        max_matches_per_book=max_matches if max_matches > 0 else None,
    )

    if search_error:
        return jsonify({"error": search_error}), 400

    out = []
    for filename, relative_path, matches, book_total_matches in results_list:
        encoded_path = relative_path.replace("\\", "/")
        match_out = [
            {
                "page": m["page"],
                "chapter": m.get("chapter"),
                "snippet": m.get("snippet"),
                "file_id": m.get("file_id"),
            }
            for m in matches
        ]
        out.append(
            {
                "filename": filename,
                "relative_path": encoded_path,
                "folder": os.path.dirname(encoded_path),
                "match_count": book_total_matches,
                "matches": match_out,
            }
        )

    return jsonify(
        {
            "query": search_query,
            "total_books": getattr(results_list, "total_books", len(out)),
            "total_pages": getattr(results_list, "total_pages", 0),
            "has_more": has_more,
            "offset": offset,
            "results": out,
        }
    )


@app.route("/api/folders")
def api_folders():
    """Return a list of all indexed folders as JSON."""
    db_name = app.config["DATABASE"]
    folders = get_cached_folders(db_name)
    return jsonify({"folders": folders})


@app.route("/search_results_partial", methods=["GET"])
def search_results_partial():
    """AJAX endpoint for loading more results."""
    db_name = app.config["DATABASE"]
    search_query = request.args.get("search_query", "").strip()
    try:
        offset = int(request.args.get("offset", 0))
    except (ValueError, TypeError):
        offset = 0
    limit = SEARCH_RESULTS_LIMIT
    selected_folders = request.args.getlist("folders")
    sort_by = request.args.get("sort_by", "filename")
    try:
        max_matches = int(request.args.get("max_matches_per_book", 0))
    except (ValueError, TypeError):
        max_matches = 0

    if not search_query:
        return "", 400

    if not db.check_db_has_content(db_name):
        return "", 503

    results_list, has_more, _, search_terms = search_logic.perform_search(
        db_name,
        search_query,
        limit=limit,
        offset=offset,
        selected_folders=selected_folders,
        sort_by=sort_by,
        max_matches_per_book=max_matches if max_matches > 0 else None,
    )

    results = []
    for i, (filename, relative_path, matches, book_total_matches) in enumerate(
        results_list, start=offset + 1
    ):
        open_link = url_for("open_in_viewer", filename=relative_path.replace("\\", "/"))
        folder_name = os.path.dirname(relative_path)
        results.append(
            (
                i,
                filename,
                relative_path,
                folder_name,
                open_link,
                matches,
                book_total_matches,
            )
        )

    return render_template(
        "result_items.html",
        results=results,
        has_more=has_more,
        next_offset=offset + limit,
        search_query=search_query,
        selected_folders=selected_folders,
    )


@app.route("/index", methods=["GET", "POST"])
def index_builder():
    # Handles the indexing page. On GET, it displays the list of currently
    # indexed files or the progress page if indexing is running.
    # On POST, it launches a background indexing thread.
    db_name = app.config["DATABASE"]
    if request.method == "POST":
        path_input = request.form.get("pdf_directory", "").strip()
        if path_input:
            path_input = os.path.abspath(path_input)

        # Restrict indexing to the configured FILES_DIRECTORY
        files_dir = app.config.get("FILES_DIRECTORY")
        if files_dir:
            allowed_root = os.path.abspath(files_dir)
            if not (
                path_input == allowed_root
                or path_input.startswith(allowed_root + os.sep)
            ):
                flash("Path must be within the configured files directory.", "error")
                return redirect(url_for("index_builder"))

        # Validate the path before acquiring the lock — no point locking for a bad path
        if not (
            os.path.isdir(path_input)
            or (os.path.isfile(path_input) and path_input.lower().endswith(".pdf"))
        ):
            logger.error(
                f"Invalid path: {path_input}. Not a valid directory or PDF file."
            )
            flash(
                "Invalid path. Please provide a valid directory or a direct path to a PDF file.",
                "error",
            )
            return redirect(url_for("index_builder"))

        # Check and update running state atomically — prevents TOCTOU race where two
        # simultaneous POSTs both pass the running check before either updates the state.
        with _indexing_lock:
            if _indexing_state["running"]:
                flash("Indexing is already running.", "error")
                return redirect(url_for("index_builder"))
            _indexing_state.update(
                {
                    "running": True,
                    "done": False,
                    "error": None,
                    "total": 0,
                    "indexed": 0,
                    "skipped": 0,
                    "failed": 0,
                    "current_file": "Scanning...",
                    "indexed_files": [],
                    "skipped_files": [],
                    "failed_files": [],
                }
            )

        files_dir = app.config.get("FILES_DIRECTORY")
        thread = threading.Thread(
            target=_run_indexing, args=(db_name, path_input, files_dir), daemon=True
        )
        thread.start()
        return redirect(url_for("index_builder"))

    # For GET request: if indexing is running, show progress page.
    with _indexing_lock:
        is_running = _indexing_state["running"]
        is_done = _indexing_state["done"]

    if is_running or is_done:
        # Show progress/results page
        currently_indexed_files = sorted(
            db.get_indexed_files(db_name), key=natural_sort_key
        )
        return render_template(
            "index_builder.html",
            currently_indexed_files=currently_indexed_files,
            indexing_active=False,
        )

    currently_indexed_files = sorted(
        db.get_indexed_files(db_name), key=natural_sort_key
    )
    return render_template(
        "index_builder.html", currently_indexed_files=currently_indexed_files
    )


def _run_indexing(db_name, path_input, files_dir):
    """Background indexing function. Runs in a separate thread."""
    try:
        logger.info(f"Starting index build from: {path_input}")

        tasks = []
        scanned_relative_paths = set()

        if os.path.isdir(path_input):
            logger.info("Scanning directory for PDF files...")

            visited_realpaths = set()
            for root, dirs, files in os.walk(path_input, followlinks=True):
                real_root = os.path.realpath(root)
                if real_root in visited_realpaths:
                    dirs[:] = []
                    continue
                visited_realpaths.add(real_root)
                dirs[:] = [d for d in dirs if not d.startswith(".")]

                for file in files:
                    if file.startswith(".") or file.startswith("~"):
                        continue
                    if file.lower().endswith(".pdf"):
                        file_path = os.path.join(root, file)

                        # Fix: Calculate relative path from the designated FILES_DIRECTORY if available.
                        # This ensures paths in the DB remain consistent regardless of what subfolder is indexed,
                        # and allows serve_files to always find them.
                        if files_dir and file_path.startswith(
                            os.path.abspath(files_dir)
                        ):
                            relative_path = os.path.relpath(
                                file_path, files_dir
                            ).replace("\\", "/")
                        else:
                            relative_path = os.path.relpath(
                                file_path, path_input
                            ).replace("\\", "/")

                        scanned_relative_paths.add(relative_path)
                        last_modified = os.path.getmtime(file_path)
                        # Use files_dir as the rename-detection base when available,
                        # since relative_path is always relative to files_dir in that case.
                        rename_base = (
                            os.path.abspath(files_dir) if files_dir else path_input
                        )
                        tasks.append(
                            (
                                db_name,
                                file_path,
                                file,
                                relative_path,
                                last_modified,
                                rename_base,
                            )
                        )
        elif os.path.isfile(path_input) and path_input.lower().endswith(".pdf"):
            logger.info("Processing single PDF file...")
            file_path = path_input
            file = os.path.basename(file_path)

            if files_dir and file_path.startswith(os.path.abspath(files_dir)):
                relative_path = os.path.relpath(file_path, files_dir).replace("\\", "/")
            else:
                relative_path = file.replace("\\", "/")

            last_modified = os.path.getmtime(file_path)
            rename_base = (
                os.path.abspath(files_dir) if files_dir else os.path.dirname(path_input)
            )
            tasks.append(
                (db_name, file_path, file, relative_path, last_modified, rename_base)
            )

        # Clear previous OCR todo lists
        ocr_missing_path = os.path.join(SCRIPT_DIR, "ocr_todo_list_missing.txt")
        ocr_redo_path = os.path.join(SCRIPT_DIR, "ocr_todo_list_redo.txt")
        for ocr_list in [ocr_missing_path, ocr_redo_path]:
            if os.path.exists(ocr_list):
                os.remove(ocr_list)

        with _indexing_lock:
            _indexing_state["total"] = len(tasks)
            _indexing_state["current_file"] = "Starting..."

        logger.info(f"Found {len(tasks)} PDF files to process")

        indexed_files, failed_files, skipped_files = [], [], []
        ocr_candidates_missing = []
        ocr_candidates_redo = []

        if tasks:
            try:
                # Use as_completed for better utilization (don't block on long-running first tasks)
                # and batch commits to reduce SQLite transaction overhead.
                max_workers = max(1, (os.cpu_count() or 1) - 1)
                executor = ProcessPoolExecutor(max_workers=max_workers)
                try:
                    from concurrent.futures import as_completed

                    futures = [
                        executor.submit(indexing_logic.process_file, task)
                        for task in tasks
                    ]

                    batch_data = []  # Buffer for batch committing

                    for future in as_completed(futures):
                        try:
                            res_data = future.result()
                        except Exception as e:
                            logger.error(f"Worker task failed with exception: {e}")
                            continue

                        # process_file always returns a WorkerResult dict now.
                        status = res_data.get("status")
                        filename = res_data.get("filename")
                        full_path = res_data.get("file_path")
                        data = res_data.get("data")

                        if status == "indexed" and data:
                            # Buffer the indexed data for a batch commit
                            batch_data.append(data)
                            indexed_files.append(filename)

                            # Commit in batches of 10 to balance performance and memory
                            if len(batch_data) >= 10:
                                try:
                                    db.commit_indexed_pdfs_batch(db_name, batch_data)
                                    batch_data = []
                                except Exception as b_e:
                                    logger.error(
                                        f"Failed to commit batch to database: {b_e}"
                                    )
                                    failed_files.append(f"Batch Error: {filename}")

                        # Standard status reporting
                        if status == "renamed":
                            indexed_files.append(f"{filename} (Renamed)")
                        elif status == "failed":
                            if filename not in failed_files:
                                failed_files.append(filename)
                        elif status == "needs_ocr_missing":
                            ocr_candidates_missing.append(full_path)
                            skipped_files.append(f"{filename} (Needs OCR - Empty)")
                        elif status == "needs_ocr_redo":
                            ocr_candidates_redo.append(full_path)
                            skipped_files.append(f"{filename} (Needs OCR - Redo)")
                        elif status == "skipped":
                            skipped_files.append(filename)

                        # Update progress
                        with _indexing_lock:
                            _indexing_state["indexed"] = len(indexed_files)
                            _indexing_state["skipped"] = len(skipped_files)
                            _indexing_state["failed"] = len(failed_files)
                            _indexing_state["current_file"] = (
                                filename or "Processing..."
                            )

                    # Final commit for any remaining files in the buffer
                    if batch_data:
                        try:
                            db.commit_indexed_pdfs_batch(db_name, batch_data)
                        except Exception as b_e_final:
                            logger.error(f"Failed to commit final batch: {b_e_final}")

                    executor.shutdown(wait=True)
                except KeyboardInterrupt:
                    logger.warning("Indexing interrupted. Shutting down workers...")
                    executor.shutdown(wait=False)
                    raise
                except Exception:
                    executor.shutdown(wait=True)
                    raise
            except BrokenExecutor:
                logger.error(
                    "A worker process terminated abruptly (likely SegFault or OOM). Indexing incomplete."
                )
                with _indexing_lock:
                    _indexing_state["error"] = (
                        "Worker process crashed (possibly a bad PDF). Some files may not have been indexed."
                    )
            except Exception as e:
                logger.error(f"Unexpected error during indexing: {e}")
                with _indexing_lock:
                    _indexing_state["error"] = str(e)

        # Cleanup: Remove files from index that are no longer on disk
        if os.path.isdir(path_input):
            logger.info("Checking for deleted files...")

            # Determine the base directory for resolving full paths.
            # If files_dir is set and path_input is inside it, the DB paths are relative to files_dir.
            base_dir = (
                files_dir
                if files_dir and path_input.startswith(os.path.abspath(files_dir))
                else path_input
            )

            current_db_files = db.get_indexed_files(db_name)
            for db_rel_path in current_db_files:
                expected_full_path = os.path.normpath(
                    os.path.join(base_dir, db_rel_path)
                )

                # Only clean up files that fall UNDER the directory we just scanned!
                # If we only scanned a subfolder, we shouldn't delete missing files from other unrelated folders.
                if expected_full_path.startswith(os.path.abspath(path_input)):
                    if db_rel_path not in scanned_relative_paths:
                        if not os.path.exists(expected_full_path):
                            try:
                                db.delete_file(db_name, db_rel_path)
                                logger.info(
                                    f"Deleted missing file from index: {db_rel_path}"
                                )
                                indexed_files.append(f"{db_rel_path} (Deleted)")
                            except Exception as e:
                                logger.error(f"Failed to delete {db_rel_path}: {e}")

        # Write OCR candidates
        if ocr_candidates_missing:
            with open(ocr_missing_path, "w", encoding="utf-8") as f:
                for path in ocr_candidates_missing:
                    f.write(f"{path}\n")
            logger.info(
                f"Wrote {len(ocr_candidates_missing)} files to ocr_todo_list_missing.txt"
            )

        if ocr_candidates_redo:
            with open(ocr_redo_path, "w", encoding="utf-8") as f:
                for path in ocr_candidates_redo:
                    f.write(f"{path}\n")
            logger.info(
                f"Wrote {len(ocr_candidates_redo)} files to ocr_todo_list_redo.txt"
            )

        indexed_files.sort(key=natural_sort_key)
        failed_files.sort(key=natural_sort_key)
        skipped_files.sort(key=natural_sort_key)

        logger.info(
            f"Index build complete: {len(indexed_files)} indexed, {len(skipped_files)} skipped, {len(failed_files)} failed"
        )

        if indexed_files:
            db.optimize_db(db_name)

        # Invalidate folder cache
        with _folder_cache_lock:
            global FOLDER_CACHE
            FOLDER_CACHE = None

        # Final state update
        with _indexing_lock:
            _indexing_state["running"] = False
            _indexing_state["done"] = True
            _indexing_state["indexed_files"] = indexed_files
            _indexing_state["skipped_files"] = skipped_files
            _indexing_state["failed_files"] = failed_files
            _indexing_state["indexed"] = len(indexed_files)
            _indexing_state["skipped"] = len(skipped_files)
            _indexing_state["failed"] = len(failed_files)
            _indexing_state["current_file"] = ""

    except Exception as e:
        logger.error(f"Background indexing failed: {e}", exc_info=True)
        with _indexing_lock:
            _indexing_state["running"] = False
            _indexing_state["done"] = True
            _indexing_state["error"] = str(e)


@app.route("/index/status")
def index_status():
    """JSON endpoint for polling indexing progress."""
    with _indexing_lock:
        return jsonify(
            {
                "running": _indexing_state["running"],
                "done": _indexing_state["done"],
                "error": _indexing_state["error"],
                "total": _indexing_state["total"],
                "indexed": _indexing_state["indexed"],
                "skipped": _indexing_state["skipped"],
                "failed": _indexing_state["failed"],
                "current_file": _indexing_state["current_file"],
                "indexed_files": _indexing_state.get("indexed_files", []),
                "skipped_files": _indexing_state.get("skipped_files", []),
                "failed_files": _indexing_state.get("failed_files", []),
            }
        )


@app.route("/index/dismiss", methods=["POST"])
def index_dismiss():
    """Dismiss indexing results so the form is shown again."""
    with _indexing_lock:
        _indexing_state["done"] = False
    return redirect(url_for("index_builder"))


@app.route("/force_ocr", methods=["POST"])
def force_ocr():
    # Manually adds a file to the OCR redo queue.
    filename = request.form.get("filename")
    if not filename:
        flash("No filename provided.", "error")
        return redirect(url_for("index_builder"))

    files_dir = app.config.get("FILES_DIRECTORY")
    if not files_dir:
        flash(
            "Cannot queue for OCR: Please restart the app with -f /path/to/pdfs to enable file operations.",
            "error",
        )
        return redirect(url_for("index_builder"))

    full_path = os.path.join(files_dir, filename)
    ocr_redo_path = os.path.join(SCRIPT_DIR, "ocr_todo_list_redo.txt")

    try:
        with open(ocr_redo_path, "a", encoding="utf-8") as f:
            f.write(f"{full_path}\n")
        flash(
            f"Added '{filename}' to OCR Redo Queue. Run process_ocr_queue.py to execute.",
            "success",
        )
    except Exception as e:
        logger.error(f"Failed to write to OCR queue: {e}")
        flash("Failed to add file to OCR queue.", "error")

    return redirect(url_for("index_builder"))


@app.route("/delete", methods=["POST"])
def delete_file_from_index():
    # Handles the deletion of a specific file from the search index.
    # Expects a POST request with the 'filename' to delete.
    path_to_delete = request.form.get(
        "filename"
    )  # Form field name is 'filename' but contains path
    db_name = app.config["DATABASE"]
    try:
        db.delete_file(db_name, path_to_delete)
        with _folder_cache_lock:
            global FOLDER_CACHE
            FOLDER_CACHE = None
        flash(f"Successfully deleted '{path_to_delete}' from the index.", "success")
    except sqlite3.Error as e:
        logger.error(f"Database error when deleting file: {e}")
        flash("Failed to delete file from index.", "error")
    return redirect(url_for("index_builder"))


@app.route("/wipe_index", methods=["POST"])
def wipe_index():
    # Handles the complete deletion of the entire search index.
    db_name = app.config["DATABASE"]
    try:
        db.wipe_db(db_name)
        with _folder_cache_lock:
            global FOLDER_CACHE
            FOLDER_CACHE = None
        flash("The entire search index has been successfully wiped.", "success")
    except sqlite3.Error as e:
        logger.error(f"Database error when wiping the index: {e}")
        flash("Failed to wipe index.", "error")
    return redirect(url_for("index_builder"))


@app.route("/open/<path:filename>")
def open_in_viewer(filename):
    # Launches the PDF in the configured viewer (Okular) or default system viewer.
    # Only available via local access.
    if request.host.split(":")[0] not in ("localhost", "127.0.0.1"):
        return "This endpoint is only available locally.", 403
    files_directory = app.config.get("FILES_DIRECTORY")
    if not files_directory or not os.path.isdir(files_directory):
        return (
            "Files directory not configured. Start with -f pointing to your PDF root.",
            400,
        )
    full_path = os.path.normpath(os.path.join(files_directory, filename))
    if not full_path.startswith(os.path.abspath(files_directory)):
        return "Access denied: File is outside the configured directory.", 403
    if not os.path.isfile(full_path):
        return "File not found", 404
    viewer = app.config.get("PDF_VIEWER")
    try:
        if viewer:
            subprocess.Popen([viewer, full_path])
        else:
            # Fallback to system default viewer
            if hasattr(os, "startfile"):
                os.startfile(full_path)  # Windows
            else:
                # Linux/macOS fallback
                opener = "open" if sys.platform == "darwin" else "xdg-open"
                subprocess.Popen([opener, full_path])
        # Return 204 No Content so the page doesn't reload or redirect
        return "", 204
    except Exception as e:
        logger.error(f"Failed to open viewer for {full_path}: {e}")
        return "Failed to open viewer.", 500


@app.route("/file/<path:filename>")
def serve_files(filename):
    # Serves PDF files directly so they can be opened from the search results.
    # Requires the --files command-line argument to be set to the PDF directory.
    if not filename.lower().endswith(".pdf"):
        return "Only PDF files can be served.", 403
    files_directory = app.config.get("FILES_DIRECTORY")
    if files_directory and os.path.isdir(files_directory):
        response = send_from_directory(
            files_directory, filename, mimetype="application/pdf"
        )
        safe_name = os.path.basename(filename).replace('"', "_")
        response.headers["Content-Disposition"] = f'inline; filename="{safe_name}"'
        # Allow the browser to cache PDFs locally for 24 hours.
        # 'private' prevents intermediate proxies (Cloudflare, Caddy) from
        # caching the file, which would be wrong for a personal library.
        # Last-Modified lets the browser validate with a cheap 304 instead of
        # re-downloading the whole file when the cache entry expires.
        response.headers["Cache-Control"] = "private, max-age=86400"
        try:
            full_path = os.path.join(files_directory, filename)
            mtime = os.path.getmtime(full_path)
            response.headers["Last-Modified"] = formatdate(mtime, usegmt=True)
        except OSError:
            pass
        return response

    return "Files directory not configured", 404


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Flask web interface to search PDF files by their content."
    )
    parser.add_argument(
        "-d", "--database", default="pdf_search.db", help="Path of the database"
    )
    parser.add_argument(
        "-f",
        "--files",
        default=None,
        help="Directory of PDF files to make them accessible via the web interface",
    )
    parser.add_argument(
        "--port", type=int, default=5001, help="Port to run the Flask app"
    )
    parser.add_argument(
        "--pdf-viewer",
        default=None,
        help="Path to a PDF viewer executable (e.g., Okular). If omitted, system default viewer is used.",
    )
    parser.add_argument(
        "--host", default="127.0.0.1", help="Host IP address to bind the server to."
    )
    parser.add_argument(
        "--public-url",
        default="http://localhost:5001",
        help="Public base URL for link generation (e.g. https://yourdomain.com)",
    )
    args = parser.parse_args()

    # --- Main Execution ---

    # --- Singleton Check ---
    # Ensure only one instance of the application runs concurrently.
    _instance_lock = get_instance_lock("pdfsearch")
    if not _instance_lock:
        print("\nError: Another instance of PDFSearch is already running.")
        logger.error(
            "Startup aborted: Application is already running (locked by pdfsearch.lock)."
        )
        sys.exit(1)

    # Load configuration into the Flask app config
    app.config["DATABASE"] = args.database
    app.config["FILES_DIRECTORY"] = args.files
    app.config["PDF_VIEWER"] = args.pdf_viewer
    app.config["PUBLIC_BASE_URL"] = args.public_url.rstrip("/")

    db.init_db(app.config["DATABASE"])
    db.startup_wal_checkpoint(app.config["DATABASE"])

    indexed_files = db.get_indexed_files(app.config["DATABASE"])
    if indexed_files:
        logger.info(f"Found {len(indexed_files)} files in the index.")
    else:
        logger.info("No files found in the index.")

    # The server initialization must be inside the __name__ == "__main__" block
    # for multiprocessing to work correctly.
    try:
        from waitress import serve

        # Calculate optimal thread count for concurrent SQLite/Flask processing
        # Waitress defaults to 4. We boost this to (CPU * 2) or at least 8 to
        # allow massive parallel searching, since SQLite WAL allows concurrent reads.
        optimal_threads = max(8, (os.cpu_count() or 1) * 2)

        logger.info(
            f"Starting Waitress server on {args.host}:{args.port} with {optimal_threads} threads"
        )

        # We explicitly trust Caddy/Cloudflared for proxying and disable Waitress's internal proxy checks
        # to save micro-seconds, but provide the massive thread pool.
        serve(app, host=args.host, port=args.port, threads=optimal_threads)
    except (OSError, SystemExit):
        # Handle "Address already in use" gracefully if it slips through or if lock file didn't catch it (e.g. different folders)
        logger.error("Server stopped or failed to start.")
        raise
    except KeyboardInterrupt:
        print("\nServer stopped by user.")
        sys.exit(0)
