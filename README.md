# PDFSearch

A high-performance, private PDF search engine with a Discord bot frontend and a context-aware Web UI. Built specifically for managing and searching massive local PDF libraries (tens of thousands of pages) with sub-millisecond retrieval times.

## Key Features

- **High-Performance Retrieval**: Utilizes SQLite FTS5 with custom `unicode61` tokenization and a parallel shadow-table (`pages`) for indexed metadata lookups, eliminating unindexed group-by operations.
- **Smart Snippets**: Sophisticated context-aware result generation featuring:
  - **Sentence Boundary Detection**: Results start and end at logical sentence edges.
  - **Page Stitching**: Seamlessly merges matches that span across physical PDF pages.
  - **Dialogue Recovery**: Consolidates split quotes and repairs dialogue spacing.
- **Discord Bot Interface**: Full-featured slash command interface with:
  - **Guild/User Whitelisting**: Restricted access for specific Discord servers and users.
  - **Folder-Level Security**: Server-specific folder visibility and autocompletion.
  - **On-Demand Snippets**: "Load Snippets" functionality for fetching extended results without flooding chat.
- **Advanced Text Normalization**: A consolidated 20-phase pipeline in `shared_utils.py` that handles:
  - **OCR Repair**: Fixes common artifacts (e.g., "fne" -> "fine", "s o m e t e x t" normalization).
  - **Structure Recovery**: Repairs hyphenation broken by line wraps and consolidates visual Table of Contents.
  - **Safety**: Built-in HTML entity protection and surrogate-pair stripping for database integrity.
- **Multi-Process Indexing**: High-throughput library builds utilizing all available CPU cores, featuring hash-based rename detection to avoid redundant re-indexing.
- **Secure Remote Access**: Architected for private hosting behind Caddy and Cloudflare Zero Trust (Tunnels).

## Architecture & Logic

### Text Processing Pipeline
All text extracted via `PyMuPDF` (fitz) passes through a centralized normalization engine before indexing:
1. **Cleaning**: Removal of watermarks and boilerplate (Horus Heresy intros, etc.).
2. **Translation**: Character normalization (straightening quotes, ligatures).
3. **Display Fixes**: Space-aware repair of contractions, possessives, and dialogue tags.
4. **Hyphenation**: Context-aware merging of line-split words.

### Search Engine
The search logic balances speed with richness:
- **V6 Search Optimization**: Leverages indexed JOINs between FTS5 results and the `pages` shadow table.
- **Dynamic Snippet Windows**: Generates ~600-character context windows around query terms.

## Project Structure

- `app.py`: Flask application providing the Web UI and REST API.
- `bot.py`: Discord bot implementation using `discord.py` and slash commands.
- `search_logic.py`: The core FTS5 search and snippet reconstruction engine.
- `indexing_logic.py`: The multi-process pipeline for PDF parsing and heuristic cleaning.
- `shared_utils.py`: Centralized normalization rules, regex patterns, and security utilities.
- `database.py`: SQLite schema management, multi-threaded connection handling, and migrations.
- `benchmark.py`: Comprehensive performance testing suite for latency verification.

## Setup & Installation

### 1. Environment Preparation
Ensure Python 3.10+ is installed on your Windows machine.
```powershell
# Clone the repository (or extract files) to C:\pdfsearch
cd C:\pdfsearch
python -m venv venv
.\venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

### 2. Discord Bot Configuration
1.  Go to the [Discord Developer Portal](https://discord.com/developers/applications).
2.  Create a new Application and add a Bot.
3.  **Privileged Gateway Intents**: Enable `Server Members Intent` (required for guild whitelisting logic). Note: `Message Content Intent` is **NOT** required for slash commands.
4.  Copy the Bot Token into your `.env` file:
    ```env
    DISCORD_TOKEN=your_token_here
    ```

### 3. Application Configuration
Create/Edit your `.env` file in the project root:
- `FILES_DIRECTORY`: (Optional) Absolute path to your base PDF folder.
- `DATABASE`: (Optional) Path to your SQLite DB (defaults to `pdf_search.db`).

### 4. Indexing & OCR Workflow
The indexing process is split into two phases for optimal performance:
1.  **Metadata Extraction**: 
    - Run the Web UI: `python app.py`
    - Navigate to `/index` and enter your PDF directory.
    - This performs multi-process text extraction and identifies files needing OCR.
2.  **OCR Processing**:
    - If files are flagged for OCR, they are added to `ocr_todo_list_missing.txt` or `ocr_todo_list_redo.txt`.
    - Run the OCR worker: `python process_ocr_queue.py`
    - This uses `OCRmyPDF` (requires Tesseract) to process the queue and re-index the results.

### 5. Running the Bot
```powershell
python bot.py
```
Ensure the bot is added to your server with the `applications.commands` scope enabled. Use `/search` to verify.

## Remote Access (Caddy & Cloudflare)
This project is optimized for remote access via mobile. Example configuration:
1.  **Caddy**: Configured to reverse-proxy `localhost:5001`.
2.  **Cloudflare Tunnel**: `cloudflared` tunnel connects your local machine to your public domain.
3.  **Authentication**: Cloudflare Access (Zero Trust) configured with Email OTP to secure the domain.

## Maintenance & Testing
Before committing changes to the normalization logic:
- Run `python test\run_all_tests.py` to check for regressions in dialogue and quote handling.
- Run `python benchmark.py` to verify that search latency remains sub-500ms on production-scale databases.

## Implementation Details (Windows)
This project is configured for deployment on Windows using a local proxy setup:
- **Proxy**: Caddy and `cloudflared` for secure external access.
- **Security**: Cloudflare Zero Trust with Email OTP recommended.
- **Paths**: Project root defaults to `C:\pdfsearch`.

## Credits
This project is based on [pdf_search](https://github.com/FelixKohlhas/pdf_search) by Felix Kohlhas. It has been significantly modified and expanded for higher performance and Discord integration.

## License
MIT License. See [LICENSE](LICENSE) for details.
