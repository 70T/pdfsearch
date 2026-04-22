import sys
import os
import tempfile
import pytest

# Add parent directory to path so tests can import app modules
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import database as db


@pytest.fixture
def temp_db():
    """Creates a temporary SQLite database, initializes it, and cleans up after."""
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    db.init_db(path)
    yield path
    try:
        os.remove(path)
    except PermissionError:
        pass


@pytest.fixture
def app_client(temp_db):
    """Creates a Flask test client configured with a temporary database."""
    from app import app

    app.config["TESTING"] = True
    app.config["DATABASE"] = temp_db
    with app.test_client() as client:
        yield client
