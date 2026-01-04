# tests/database/test_detail_page_enrichment.py
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import pytest
from pydantic import HttpUrl
from sqlalchemy import text

from src.database import Database
from src.models import Event


@pytest.fixture
def temp_db():
    """Create a temporary database for testing."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test_events.sqlite"
        db_gz_path = Path(tmpdir) / "test_events.sqlite.gz"
        
        # Initialize the database schema
        with Database(compress_on_exit=False, db_path=db_path, db_gz_path=db_gz_path) as db:
            db.init_database()
            yield db


def test_store_enrichment_with_httpurl(temp_db):
    """Test that detail page enrichments can be stored when event has HttpUrl objects.
    
    This is a regression test for the bug where HttpUrl objects were passed directly
    to SQLite without being converted to strings first, causing:
    'HttpUrl' object has no attribute 'decode'
    """
    # Create a mock event with HttpUrl (simulating what comes from the database)
    test_event = Event(
        source="TEST_HTTPURL",
        source_id="test-event-httpurl-1",
        title="Test HttpUrl Event",
        start=datetime(2026, 1, 1, 10, 0, tzinfo=timezone.utc),
        end=datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc),
        url=HttpUrl("https://example.com/test-httpurl/1"),
    )
    
    # Store event first
    temp_db.upsert_source_events([test_event])
    
    # Now store detail page enrichment - passing HttpUrl directly
    # The function should convert it to string internally
    enrichment_data = {"test_field": "test_value"}
    
    # This is the critical call - passing HttpUrl object directly
    temp_db.store_detail_page_enrichment(
        source="TEST_HTTPURL",
        source_id="test-event-httpurl-1",
        detail_page_url=test_event.url,  # HttpUrl object
        enrichment_data=enrichment_data,
        status="success"
    )
    
    # Verify the enrichment was stored
    enrichments = temp_db.session.execute(
        text("SELECT * FROM detail_page_enrichments WHERE source = 'TEST_HTTPURL' AND source_id = 'test-event-httpurl-1'")
    ).fetchall()
    
    assert len(enrichments) == 1
    assert enrichments[0][2] == str(test_event.url)  # detail_page_url should be string
    assert enrichments[0][5] == "success"  # processing_status column


def test_store_enrichment_with_string(temp_db):
    """Test that detail page enrichments can be stored with string URLs."""
    
    test_event = Event(
    source="TEST_STRING",
    source_id="test-event-string-1",
        title="Test String Event",
        start=datetime(2026, 1, 1, 10, 0, tzinfo=timezone.utc),
        end=datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc),
        url=HttpUrl("https://example.com/test-string/1"),
    )
    
    temp_db.upsert_source_events([test_event])
    
    # Store with string URL
    temp_db.store_detail_page_enrichment(
        source="TEST_STRING",
        source_id="test-event-string-1",
        detail_page_url="https://example.com/test-string/1",  # String
        enrichment_data={"another_field": "another_value"},
        status="success"
    )
    
    enrichments = temp_db.session.execute(
        text("SELECT * FROM detail_page_enrichments WHERE source = 'TEST_STRING'")
    ).fetchall()
    
    assert len(enrichments) == 1
    assert enrichments[0][5] == "success"
