import logging
import os
import sqlite3
import threading
from shared_utils import clean_db_string

logger = logging.getLogger(__name__)

# --- Thread-Local Storage ---

# Thread-local storage for non-Flask contexts (worker processes, CLI scripts).
# Each worker thread/process gets its own persistent connection, avoiding the
# overhead of opening and closing a new connection for every query.
_worker_local = threading.local()


def _get_conn(db_name):
    """Internal helper to configure a new SQLite connection with performance PRAGMAs."""
    conn = sqlite3.connect(db_name, timeout=60.0)
    conn.execute("PRAGMA synchronous = NORMAL")
    conn.execute("PRAGMA foreign_keys = ON")
    # Performance optimizations for read-heavy workloads
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute(
        "PRAGMA cache_size = -200000"
    )  # ~200MB cache (negative value is in KB)
    conn.execute(
        "PRAGMA mmap_size = 30000000000"
    )  # ~30GB mmap limit (OS handles actual allocation)
    conn.execute("PRAGMA temp_store = MEMORY")  # Store temp tables in RAM
    return conn


def get_db(db_name):
    """Return a per-request connection when running inside Flask,
    or a persistent thread-local connection otherwise (worker processes, CLI scripts).
    Using thread-local storage avoids creating a new SQLite connection for every
    query in worker processes, which was causing significant overhead when indexing
    large PDF libraries (multiple open/close cycles per file)."""

    try:
        from flask import g

        if "_db_conn" not in g:
            g._db_conn = _get_conn(db_name)
        return g._db_conn
    except (RuntimeError, ImportError):
        # Outside Flask request context (worker processes, CLI scripts).
        # Reuse the existing connection if db_name matches; otherwise close and reconnect.
        conn = getattr(_worker_local, "conn", None)
        stored_name = getattr(_worker_local, "db_name", None)
        if conn is None or stored_name != db_name:
            if conn is not None:
                try:
                    conn.close()
                except Exception:
                    pass
            _worker_local.conn = _get_conn(db_name)
            _worker_local.db_name = db_name
        return _worker_local.conn


def close_db(e=None):
    """Teardown hook: closes the per-request connection if one was opened."""
    try:
        from flask import g

        conn = g.pop("_db_conn", None)
        if conn is not None:
            conn.close()
    except (RuntimeError, ImportError):
        pass


def init_db(db_name):
    """Initialize the database schema, creating tables if they don't exist.
    Uses _get_conn (not get_db) because this runs at startup, outside any
    Flask application context, and the connection must be closed when done."""
    logger.info(f"Initializing database: {db_name}")
    conn = _get_conn(db_name)
    try:
        with conn:
            cursor = conn.cursor()
            # Create a table to store file metadata. This allows for fast lookups and deletions.
            cursor.execute("""CREATE TABLE IF NOT EXISTS files (
                                id INTEGER PRIMARY KEY,
                                filename TEXT NOT NULL,
                                relative_path TEXT UNIQUE NOT NULL,
                                last_modified REAL NOT NULL,
                                file_hash TEXT
                              )""")
            # Create indexes for faster lookups.
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_filename ON files (filename)"
            )
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_file_hash ON files (file_hash)"
            )

            # --- Schema versioning (PRAGMA user_version) ---
            cursor.execute("PRAGMA user_version")
            schema_version = cursor.fetchone()[0]

            # --- FTS5 table (page_num must be UNINDEXED) ---
            if schema_version < 1:
                # Migration: rebuild FTS table so page_num is UNINDEXED.
                # Without this, page numbers are tokenized and pollute search results.
                cursor.execute(
                    "SELECT count(*) FROM sqlite_master WHERE type='table' AND name='pdf_text_fts'"
                )
                if cursor.fetchone()[0] > 0:
                    cursor.execute("SELECT COUNT(*) FROM pdf_text_fts")
                    row_count = cursor.fetchone()[0]
                    if row_count > 0:
                        logger.info(
                            f"Migrating FTS table: marking page_num as UNINDEXED ({row_count:,} rows). This may take a while..."
                        )
                        cursor.execute(
                            "CREATE TABLE _fts_backup (file_id INTEGER, page_num INTEGER, text TEXT)"
                        )
                        cursor.execute(
                            "INSERT INTO _fts_backup SELECT file_id, page_num, text FROM pdf_text_fts"
                        )
                    cursor.execute("DROP TABLE pdf_text_fts")

            cursor.execute("""CREATE VIRTUAL TABLE IF NOT EXISTS pdf_text_fts USING fts5(
                                file_id UNINDEXED, 
                                page_num UNINDEXED,
                                text,
                                tokenize = 'unicode61 remove_diacritics 2'
                              )""")

            # Shadow table: fast (file_id, page_num) → rowid_fts mapping.
            # FTS5 UNINDEXED columns cannot be indexed; this table fills that gap.
            cursor.execute("""CREATE TABLE IF NOT EXISTS pages (
                                file_id  INTEGER NOT NULL,
                                page_num INTEGER NOT NULL,
                                rowid_fts INTEGER NOT NULL,
                                PRIMARY KEY (file_id, page_num)
                              ) WITHOUT ROWID""")
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_pages_rowid_fts ON pages (rowid_fts)"
            )


# --- Migrations ---


            if schema_version < 1:
                cursor.execute(
                    "SELECT count(*) FROM sqlite_master WHERE type='table' AND name='_fts_backup'"
                )
                if cursor.fetchone()[0] > 0:
                    cursor.execute(
                        "INSERT INTO pdf_text_fts(file_id, page_num, text) SELECT file_id, page_num, text FROM _fts_backup"
                    )
                    cursor.execute("DROP TABLE _fts_backup")
                    logger.info("FTS migration complete.")

            # Schema v2: populate pages shadow table from existing FTS data.
            if schema_version < 2:
                cursor.execute("SELECT COUNT(*) FROM pdf_text_fts")
                fts_row_count = cursor.fetchone()[0]
                if fts_row_count > 0:
                    logger.info(
                        f"Migrating to schema v2: populating pages table ({fts_row_count:,} rows). This may take a moment..."
                    )
                    cursor.execute(
                        "INSERT OR IGNORE INTO pages (file_id, page_num, rowid_fts) "
                        "SELECT file_id, page_num, rowid FROM pdf_text_fts"
                    )
                    logger.info("Schema v2 migration complete.")
            # Create a table to store PDF Table of Contents (Chapters) with hierarchy levels
            cursor.execute("""CREATE TABLE IF NOT EXISTS chapters (
                                file_id INTEGER,
                                page_num INTEGER,
                                title TEXT,
                                level INTEGER DEFAULT 1,
                                FOREIGN KEY(file_id) REFERENCES files(id))""")
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_chapters ON chapters (file_id, page_num)"
            )

            cursor.execute("PRAGMA table_info(files)")
            columns = [info[1] for info in cursor.fetchall()]
            if "file_hash" not in columns:
                logger.info(
                    "Migrating database: Adding file_hash column to files table."
                )
                cursor.execute("ALTER TABLE files ADD COLUMN file_hash TEXT")
                # Index is created unconditionally above; no need to repeat here.

            cursor.execute("PRAGMA table_info(chapters)")
            chapter_columns = [info[1] for info in cursor.fetchall()]
            if "level" not in chapter_columns:
                logger.info(
                    "Migrating database: Adding level column to chapters table."
                )
                cursor.execute(
                    "ALTER TABLE chapters ADD COLUMN level INTEGER DEFAULT 1"
                )

            # Migration: ensure relative_path uses forward slashes to support indexed folder queries.
            # Use a parameter for the backslash character to avoid Python/SQL escaping confusion.
            # (Previous version used '\\\\' which sent TWO backslashes to SQL, matching nothing.)
            bs = "\\"
            cursor.execute(
                "SELECT COUNT(*) FROM files WHERE relative_path LIKE ?", (f"%{bs}%",)
            )
            backslash_count = cursor.fetchone()[0]

            if backslash_count > 0:
                logger.info(
                    f"Migrating database: Normalizing {backslash_count} paths with backslashes."
                )

                # Cleanup duplicates: If a backslash version exists AND the forward-slash version
                # also already exists, delete the old backslash version to avoid UNIQUE constraint violation.
                cursor.execute(
                    "SELECT id FROM files WHERE relative_path LIKE ? "
                    "AND REPLACE(relative_path, ?, '/') IN (SELECT relative_path FROM files)",
                    (f"%{bs}%", bs),
                )
                duplicate_ids = [row[0] for row in cursor.fetchall()]
                if duplicate_ids:
                    logger.info(
                        f"  Removing {len(duplicate_ids)} duplicate backslash entries."
                    )
                    chunk_size = 900
                    for i in range(0, len(duplicate_ids), chunk_size):
                        chunk = duplicate_ids[i : i + chunk_size]
                        placeholders = ",".join("?" for _ in chunk)
                        cursor.execute(
                            f"DELETE FROM pdf_text_fts WHERE file_id IN ({placeholders})",
                            chunk,
                        )
                        cursor.execute(
                            f"DELETE FROM chapters WHERE file_id IN ({placeholders})",
                            chunk,
                        )
                        cursor.execute(
                            f"DELETE FROM files WHERE id IN ({placeholders})", chunk
                        )

                # Convert any remaining backslash-only paths (no forward-slash duplicate exists)
                cursor.execute(
                    "UPDATE files SET relative_path = REPLACE(relative_path, ?, '/') WHERE relative_path LIKE ?",
                    (bs, f"%{bs}%"),
                )

            # Finalize schema version
            if schema_version < 3:
                cursor.execute("PRAGMA user_version = 3")
    finally:
        conn.close()
    logger.info("Database initialized.")


def startup_wal_checkpoint(db_name, max_size_mb=100):
    """Truncate WAL file on startup if it exceeds the threshold to keep reads fast.
    Uses _get_conn (not get_db) -- runs before the Flask app context exists."""
    try:
        wal_path = f"{db_name}-wal"
        if os.path.exists(wal_path):
            size_mb = os.path.getsize(wal_path) / (1024 * 1024)
            if size_mb > max_size_mb:
                logger.info(
                    f"WAL file size ({size_mb:.1f}MB) exceeds {max_size_mb}MB threshold. Truncating..."
                )
                conn = _get_conn(db_name)
                try:
                    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
                finally:
                    conn.close()
                logger.info("WAL checkpoint complete.")
    except Exception as e:
        logger.error(f"Error checking WAL size: {e}")


def query_db(db_name, sql_query, params=()):
    """Query the database and fetch all results.
    Uses get_db() for connection reuse within Flask requests and worker processes."""
    conn = get_db(db_name)
    params = clean_db_string(params)
    try:
        cursor = conn.cursor()
        cursor.execute(sql_query, params)
        return cursor.fetchall()
    except sqlite3.OperationalError as e:
        error_str = str(e)
        # Only swallow schema-related errors (missing tables/columns) gracefully.
        # Re-raise all other errors (FTS5 syntax, etc.) so callers can handle them.
        if "no such table" in error_str or "no such column" in error_str:
            logger.error(f"Database query error: {e}. The table might not exist.")
            return []
        raise


def execute_db(db_name, sql_query, params=()):
    """Execute a write command (INSERT, DELETE, etc.) on the database.
    Uses get_db() for connection reuse within Flask requests and worker processes."""
    conn = get_db(db_name)
    params = clean_db_string(params)
    try:
        with conn:
            cursor = conn.cursor()
            cursor.execute(sql_query, params)
    except sqlite3.OperationalError as e:
        logger.error(f"Database execution error: {e}")
        raise


# --- Content Retrieval ---


def get_last_modified_from_db(db_name, relative_path):
    """Get the last modified timestamp for a file from the database."""
    sql = "SELECT last_modified FROM files WHERE relative_path = ?"
    result = query_db(db_name, sql, (relative_path,))
    return result[0][0] if result else None


def get_indexed_files(db_name):
    """Get a list of all unique relative paths currently in the index."""
    sql = "SELECT relative_path FROM files"
    results = query_db(db_name, sql)
    return [row[0] for row in results]


def get_unique_folders(db_name):
    """Get a sorted list of unique folder paths from the indexed files.
    Paths in the DB are always stored with forward slashes."""
    sql = "SELECT relative_path FROM files"
    rows = query_db(db_name, sql)
    folders = set()

    for row in rows:
        path = row[0]
        folder = path.rsplit("/", 1)[0] if "/" in path else ""
        if not folder:
            folders.add("(Root)")
            continue

        # Add the folder and all its parent folders to the list
        parts = folder.split("/")
        for i in range(1, len(parts) + 1):
            folders.add("/".join(parts[:i]))

    return sorted(list(folders))


def check_db_has_content(db_name):
    """Check if the FTS table has any rows.
    Uses get_db() to reuse connection within Flask requests."""
    conn = get_db(db_name)
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT 1 FROM pdf_text_fts LIMIT 1")
        return cursor.fetchone() is not None
    except sqlite3.OperationalError:
        # This happens if the table doesn't exist yet.
        return False
    # No explicit close() needed here; Flask teardown handles g._db_conn cleanup.


# --- Batch Insertion ---


def bulk_insert_pages(db_name, file_id, page_data_list):
    """Insert multiple pages of PDF data in a single transaction.

    Args:
        db_name: Path to the SQLite database file.
        file_id: ID of the file from the 'files' table.
        page_data_list: List of (page_num, text) tuples.
    """
    if not page_data_list:
        return

    # Add file_id to each page tuple for insertion.
    full_page_data = clean_db_string(
        [(file_id, page_num, text) for page_num, text in page_data_list]
    )

    conn = get_db(db_name)
    with conn:
        cursor = conn.cursor()
        cursor.executemany(
            "INSERT INTO pdf_text_fts (file_id, page_num, text) VALUES (?, ?, ?)",
            full_page_data,
        )
        # Keep shadow table in sync.
        cursor.execute(
            "INSERT OR REPLACE INTO pages (file_id, page_num, rowid_fts) "
            "SELECT file_id, page_num, rowid FROM pdf_text_fts WHERE file_id = ?",
            (file_id,),
        )


def insert_file_chapters(db_name, file_id, chapters):
    """Insert chapter metadata for a file.
    chapters is a list of (page_num, title, level) tuples."""
    if not chapters:
        return
    data = clean_db_string([(file_id, p, t, level) for p, t, level in chapters])
    sql = "INSERT INTO chapters (file_id, page_num, title, level) VALUES (?, ?, ?, ?)"
    conn = get_db(db_name)
    with conn:
        conn.executemany(sql, data)


def get_chapters_for_files(db_name, file_ids):
    """Batch-fetch all chapter data for a set of file IDs in one query.
    Returns {file_id: [(page_num, title, level), ...]} sorted by page_num descending
    so callers can find the nearest preceding chapter via simple iteration."""
    if not file_ids:
        return {}
    chunk_size = 900
    all_rows = []
    file_ids = list(file_ids)
    for i in range(0, len(file_ids), chunk_size):
        chunk = file_ids[i : i + chunk_size]
        placeholders = ",".join("?" for _ in chunk)
        sql = f"SELECT file_id, page_num, title, level FROM chapters WHERE file_id IN ({placeholders}) ORDER BY file_id, page_num DESC"
        all_rows.extend(query_db(db_name, sql, tuple(chunk)))
    result = {}
    for file_id, page_num, title, level in all_rows:
        result.setdefault(file_id, []).append((page_num, title, level))
    return result


def commit_indexed_pdf(
    db_name, filename, relative_path, last_modified, file_hash, pages, chapters
):
    """Atomically commits a fully indexed PDF into the database in a SINGLE transaction,
    wiping out any old data for the same file. Dramatically faster than discrete commits."""
    conn = get_db(db_name)
    with conn:  # Single transaction wrapping the entire file ingestion!
        cursor = conn.cursor()

        # 1. Add or Update File
        sql_file = """INSERT INTO files (filename, relative_path, last_modified, file_hash) VALUES (?, ?, ?, ?)
                 ON CONFLICT(relative_path) DO UPDATE SET last_modified=excluded.last_modified, file_hash=excluded.file_hash"""
        params = clean_db_string((filename, relative_path, last_modified, file_hash))
        cursor.execute(sql_file, params)

        cursor.execute(
            "SELECT id FROM files WHERE relative_path = ?",
            (clean_db_string(relative_path),),
        )
        file_id = cursor.fetchone()[0]

        # 2. Wipe old FTS/Chapter/pages data mapping to this file_id
        cursor.execute("DELETE FROM pdf_text_fts WHERE file_id = ?", (file_id,))
        cursor.execute("DELETE FROM chapters WHERE file_id = ?", (file_id,))
        cursor.execute("DELETE FROM pages WHERE file_id = ?", (file_id,))

        # 3. Insert Pages into FTS, then populate the shadow table from the new rowids.
        if pages:
            full_page_data = clean_db_string(
                [(file_id, page_num, text) for page_num, text in pages]
            )
            cursor.executemany(
                "INSERT INTO pdf_text_fts (file_id, page_num, text) VALUES (?, ?, ?)",
                full_page_data,
            )
            cursor.execute(
                "INSERT OR REPLACE INTO pages (file_id, page_num, rowid_fts) "
                "SELECT file_id, page_num, rowid FROM pdf_text_fts WHERE file_id = ?",
                (file_id,),
            )

        # 4. Insert Chapters
        if chapters:
            chapters_data = clean_db_string(
                [(file_id, p, t, level) for p, t, level in chapters]
            )
            cursor.executemany(
                "INSERT INTO chapters (file_id, page_num, title, level) VALUES (?, ?, ?, ?)",
                chapters_data,
            )

    return file_id


def commit_indexed_pdfs_batch(db_name, batch_data):
    """
    Commits a batch of indexed PDF results in a single database transaction.
    batch_data: List of dicts, each containing:
        - filename
        - relative_path
        - last_modified
        - file_hash
        - pages: List of (page_num, text)
        - chapters: List of (page_num, title, level)
    """
    if not batch_data:
        return

    conn = get_db(db_name)
    with conn:  # One transaction for the whole batch!
        cursor = conn.cursor()
        for data in batch_data:
            filename = data.get("filename")
            rel_path = data.get("relative_path")
            last_mod = data.get("last_modified")
            file_hash = data.get("file_hash")
            pages = data.get("pages", [])
            chapters = data.get("chapters", [])

            # 1. Add or Update File
            sql_file = """INSERT INTO files (filename, relative_path, last_modified, file_hash) VALUES (?, ?, ?, ?)
                     ON CONFLICT(relative_path) DO UPDATE SET last_modified=excluded.last_modified, file_hash=excluded.file_hash"""
            cursor.execute(
                sql_file, clean_db_string((filename, rel_path, last_mod, file_hash))
            )

            cursor.execute(
                "SELECT id FROM files WHERE relative_path = ?",
                (clean_db_string(rel_path),),
            )
            file_id = cursor.fetchone()[0]

            # 2. Wipe old data
            cursor.execute("DELETE FROM pdf_text_fts WHERE file_id = ?", (file_id,))
            cursor.execute("DELETE FROM chapters WHERE file_id = ?", (file_id,))
            cursor.execute("DELETE FROM pages WHERE file_id = ?", (file_id,))

            # 3. Insert Pages, then populate shadow table.
            if pages:
                full_page_data = clean_db_string(
                    [(file_id, p_num, text) for p_num, text in pages]
                )
                cursor.executemany(
                    "INSERT INTO pdf_text_fts (file_id, page_num, text) VALUES (?, ?, ?)",
                    full_page_data,
                )
                cursor.execute(
                    "INSERT OR REPLACE INTO pages (file_id, page_num, rowid_fts) "
                    "SELECT file_id, page_num, rowid FROM pdf_text_fts WHERE file_id = ?",
                    (file_id,),
                )

            # 4. Insert Chapters
            if chapters:
                chapters_data = clean_db_string(
                    [(file_id, p, t, level) for p, t, level in chapters]
                )
                cursor.executemany(
                    "INSERT INTO chapters (file_id, page_num, title, level) VALUES (?, ?, ?, ?)",
                    chapters_data,
                )


def add_or_update_file(db_name, filename, relative_path, last_modified, file_hash=None):
    """Add a file to the 'files' table or update its timestamp. Returns the file's ID."""
    sql = """INSERT INTO files (filename, relative_path, last_modified, file_hash) VALUES (?, ?, ?, ?)
             ON CONFLICT(relative_path) DO UPDATE SET last_modified=excluded.last_modified, file_hash=excluded.file_hash"""
    conn = get_db(db_name)
    with conn:
        cursor = conn.cursor()
        params = clean_db_string((filename, relative_path, last_modified, file_hash))
        cursor.execute(sql, params)
        # Return the correct id for both INSERT and UPDATE paths
        cursor.execute(
            "SELECT id FROM files WHERE relative_path = ?",
            (clean_db_string(relative_path),),
        )
        row = cursor.fetchone()
        return row[0] if row else None


def get_files_by_hash(db_name, file_hash):
    """Find files with a matching hash. Used for rename detection."""
    sql = "SELECT id, filename, relative_path FROM files WHERE file_hash = ?"
    return query_db(db_name, sql, (file_hash,))


def update_file_path(
    db_name, file_id, new_relative_path, new_filename, new_last_modified
):
    """Update the path and filename of an existing record (rename operation)."""
    sql = "UPDATE files SET relative_path = ?, filename = ?, last_modified = ? WHERE id = ?"
    execute_db(
        db_name, sql, (new_relative_path, new_filename, new_last_modified, file_id)
    )


# --- Deletion & Cleanup ---


def delete_file(db_name, relative_path):
    """Atomically delete a file and its associated FTS and chapter data."""
    conn = get_db(db_name)
    with conn:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT id FROM files WHERE relative_path = ?",
            (clean_db_string(relative_path),),
        )
        row = cursor.fetchone()
        if row:
            cursor.execute("DELETE FROM pdf_text_fts WHERE file_id = ?", (row[0],))
            cursor.execute("DELETE FROM chapters WHERE file_id = ?", (row[0],))
            cursor.execute("DELETE FROM pages WHERE file_id = ?", (row[0],))
            cursor.execute("DELETE FROM files WHERE id = ?", (row[0],))


def wipe_db(db_name):
    """Delete all records from the index tables and reinitialize."""
    logger.warning(f"Wiping all tables from database: {db_name}")
    conn = get_db(db_name)
    with conn:
        cursor = conn.cursor()
        # The most robust way to wipe is to drop the tables entirely.
        cursor.execute("DROP TABLE IF EXISTS pdf_text_fts")
        cursor.execute("DROP TABLE IF EXISTS pages")
        cursor.execute("DROP TABLE IF EXISTS chapters")
        cursor.execute("DROP TABLE IF EXISTS files")
        # Reset schema version so init_db recreates with current schema.
        cursor.execute("PRAGMA user_version = 0")
    # Re-initialize the tables to be ready for new data.
    init_db(db_name)
    logger.info("Database wipe and re-initialization complete.")


# --- Optimization ---


def optimize_db(db_name):
    """Optimize the FTS5 index structure for faster queries.
    Should be run after bulk updates."""
    logger.info("Optimizing database (FTS merge)...")
    conn = get_db(db_name)
    with conn:
        # Merges FTS segments into a single b-tree structure
        conn.execute("INSERT INTO pdf_text_fts(pdf_text_fts) VALUES('optimize')")
    # PRAGMA optimize and wal_checkpoint must run outside the transaction block:
    # SQLite silently ignores PRAGMA optimize inside a transaction, and the
    # checkpoint should run regardless of whether the FTS merge succeeded.
    conn.execute("PRAGMA optimize")
    # Checkpoint WAL back into the main database file.
    # Worker processes write via separate connections, bypassing auto-checkpoint,
    # so the WAL can grow very large. This folds it back for faster reads.
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    logger.info("Database optimization complete.")
