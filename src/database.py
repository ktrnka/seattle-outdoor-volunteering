"""Database module using SQLAlchemy for managing the events database."""

import gzip
import json
import shutil
import sqlite3
import uuid
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional

from sqlalchemy import DateTime, Float, Integer, PrimaryKeyConstraint, String, Text, create_engine, text
from sqlalchemy.dialects.sqlite import insert
from sqlalchemy.orm import Mapped, Session, declarative_base, mapped_column, object_session, sessionmaker
from sqlalchemy.sql import func

from .config import DB_GZ, DB_PATH
from .models import (
    CanonicalEvent as PydanticCanonicalEvent,
)
from .models import (
    ETLRun as PydanticETLRun,
)
from .models import (
    Event as PydanticEvent,
)
from .models import (
    EventGroupMembership as PydanticEventGroupMembership,
)
from .models import (
    LLMEventCategorization,
)

Base = declarative_base()


def read_utc(db_datetime_value: datetime) -> datetime:
    """Repair a tz-stripped datetime read from sqlite"""
    if db_datetime_value.tzinfo:
        return db_datetime_value

    return db_datetime_value.replace(tzinfo=timezone.utc)


class Event(Base):
    """SQLAlchemy model for events table."""

    __tablename__ = "events"

    source: Mapped[str] = mapped_column(String, nullable=False)
    source_id: Mapped[str] = mapped_column(String, nullable=False)
    title: Mapped[str] = mapped_column(String, nullable=False)
    start: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    end: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    venue: Mapped[str | None] = mapped_column(String, nullable=True)
    address: Mapped[str | None] = mapped_column(String, nullable=True)
    url: Mapped[str] = mapped_column(Text, nullable=False)
    cost: Mapped[str | None] = mapped_column(String, nullable=True)
    latitude: Mapped[float | None] = mapped_column(Float, nullable=True)
    longitude: Mapped[float | None] = mapped_column(Float, nullable=True)
    tags: Mapped[str | None] = mapped_column(Text, nullable=True)  # Stored as comma-separated string
    same_as: Mapped[str | None] = mapped_column(String, nullable=True)
    source_dict: Mapped[str | None] = mapped_column(Text, nullable=True)

    __table_args__ = (PrimaryKeyConstraint("source", "source_id"),)

    def to_pydantic(
        self, enrichment: Optional["EnrichedSourceEvent"] = None, detail_page_enrichment: Optional["DetailPageEnrichment"] = None
    ) -> PydanticEvent:
        """Convert SQLAlchemy model to Pydantic model, optionally with enrichment data."""
        tags = [tag.strip() for tag in self.tags.split(",")] if self.tags else []

        # Parse enrichment data if provided
        llm_categorization = None
        if enrichment:
            from .models import LLMEventCategorization

            llm_categorization_dict = json.loads(enrichment.llm_categorization)
            llm_categorization = LLMEventCategorization(**llm_categorization_dict)

        # Use same_as from Event, or fall back to website_url from detail page enrichment
        same_as = self.same_as
        if not same_as and detail_page_enrichment:
            detail_data = json.loads(detail_page_enrichment.enrichment_data)
            website_url = detail_data.get("website_url")
            if website_url:
                same_as = website_url

        return PydanticEvent(
            source=self.source,
            source_id=self.source_id,
            title=self.title,
            start=read_utc(self.start),
            end=read_utc(self.end),
            venue=self.venue,
            address=self.address,
            url=self.url,  # type: ignore
            cost=self.cost,
            latitude=self.latitude,
            longitude=self.longitude,
            tags=tags,
            same_as=same_as,  # type: ignore
            source_dict=self.source_dict,
            llm_categorization=llm_categorization,
        )


class CanonicalEvent(Base):
    """SQLAlchemy model for canonical events table."""

    __tablename__ = "canonical_events"

    canonical_id: Mapped[str] = mapped_column(String, primary_key=True)
    title: Mapped[str] = mapped_column(String, nullable=False)
    start: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    end: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    venue: Mapped[str | None] = mapped_column(String, nullable=True)
    address: Mapped[str | None] = mapped_column(String, nullable=True)
    url: Mapped[str] = mapped_column(Text, nullable=False)
    cost: Mapped[str | None] = mapped_column(String, nullable=True)
    latitude: Mapped[float | None] = mapped_column(Float, nullable=True)
    longitude: Mapped[float | None] = mapped_column(Float, nullable=True)
    tags: Mapped[str | None] = mapped_column(Text, nullable=True)  # Stored as comma-separated string

    def to_pydantic(self) -> PydanticCanonicalEvent:
        """Convert SQLAlchemy model to Pydantic model."""
        tags = [tag.strip() for tag in self.tags.split(",")] if self.tags else []
        # Get source events from the membership table
        session = object_session(self)
        source_events = []
        if session:
            memberships = session.query(EventGroupMembership).filter(EventGroupMembership.canonical_id == self.canonical_id).all()
            source_events = [f"{m.source}:{m.source_id}" for m in memberships]

        return PydanticCanonicalEvent(
            canonical_id=self.canonical_id,
            title=self.title,
            start=read_utc(self.start),
            end=read_utc(self.end),
            venue=self.venue,
            address=self.address,
            url=self.url,  # type: ignore
            cost=self.cost,
            latitude=self.latitude,
            longitude=self.longitude,
            tags=tags,
            source_events=source_events,
        )


class EventGroupMembership(Base):
    """SQLAlchemy model for tracking which source events belong to which canonical events."""

    __tablename__ = "event_group_memberships"

    canonical_id: Mapped[str] = mapped_column(String, nullable=False)
    source: Mapped[str] = mapped_column(String, nullable=False)
    source_id: Mapped[str] = mapped_column(String, nullable=False)

    __table_args__ = (PrimaryKeyConstraint("canonical_id", "source", "source_id"),)

    def to_pydantic(self) -> PydanticEventGroupMembership:
        """Convert SQLAlchemy model to Pydantic model."""
        return PydanticEventGroupMembership(canonical_id=self.canonical_id, source=self.source, source_id=self.source_id)


class ETLRun(Base):
    """SQLAlchemy model for tracking ETL runs for each data source."""

    __tablename__ = "etl_runs"
    id: Mapped[str] = mapped_column(String, primary_key=True)  # Auto-generated unique ID
    source: Mapped[str] = mapped_column(String, nullable=False)
    run_datetime: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    status: Mapped[str] = mapped_column(String, nullable=False)  # "success" or "failure"
    num_rows: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    def to_pydantic(self) -> PydanticETLRun:
        """Convert SQLAlchemy model to Pydantic model."""
        return PydanticETLRun(source=self.source, run_datetime=read_utc(self.run_datetime), status=self.status, num_rows=self.num_rows)


class EnrichedSourceEvent(Base):
    """SQLAlchemy model for storing LLM enrichment data for source events."""

    __tablename__ = "enriched_source_events"

    # Reference to source event (no formal FK for now)
    source: Mapped[str] = mapped_column(String, nullable=False)
    source_id: Mapped[str] = mapped_column(String, nullable=False)

    # LLM results and metadata stored as JSON
    llm_categorization: Mapped[str] = mapped_column(Text, nullable=False)  # JSON string
    llm_request_metadata: Mapped[str] = mapped_column(Text, nullable=False)  # JSON string

    # Processing metadata
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    processing_status: Mapped[str] = mapped_column(String, nullable=False)  # "success", "failed", "pending"
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)  # Only set if failed

    __table_args__ = (PrimaryKeyConstraint("source", "source_id"),)


class DetailPageEnrichment(Base):
    """SQLAlchemy model for storing detail page enrichment data for any source."""

    __tablename__ = "detail_page_enrichments"

    # Reference to source event (no formal FK for now)
    source: Mapped[str] = mapped_column(String, nullable=False)
    source_id: Mapped[str] = mapped_column(String, nullable=False)

    # Detail page crawl results stored as JSON
    detail_page_url: Mapped[str] = mapped_column(Text, nullable=False)  # URL that was crawled
    enrichment_data: Mapped[str] = mapped_column(Text, nullable=False)  # JSON string of extracted data

    # Processing metadata
    fetched_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    processing_status: Mapped[str] = mapped_column(String, nullable=False)  # "success", "failed", "pending"
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)  # Only set if failed

    __table_args__ = (PrimaryKeyConstraint("source", "source_id"),)


class NoSessionError(RuntimeError):
    def __init__(self, message: str = "Database session not available. Use within 'with' statement."):
        super().__init__(message)


def ensure_database_exists() -> None:
    """Ensure the uncompressed database exists by extracting from gzipped version if needed."""
    if not DB_PATH.exists() and DB_GZ.exists():
        print(f"Extracting {DB_GZ} to {DB_PATH}")
        with gzip.open(DB_GZ, "rb") as f_in:
            with open(DB_PATH, "wb") as f_out:
                shutil.copyfileobj(f_in, f_out)


class Database:
    """Context manager for database operations that handles compression/decompression."""

    def __init__(self, compress_on_exit: bool = True):
        self.session: Optional[Session] = None
        self.engine = None
        self.compress_on_exit = compress_on_exit
        self.initial_data_version = None

    def __enter__(self):
        """Enter the context manager: decompress DB, create engine and session."""
        # Ensure database exists (decompresses if needed)
        # TODO: Use a mkstemp file
        ensure_database_exists()

        # Create engine and session
        self.engine = create_engine(f"sqlite:///{DB_PATH}", echo=False)
        SessionLocal = sessionmaker(bind=self.engine)
        self.session = SessionLocal()

        # Get the initial user_version to detect changes later
        self.initial_data_version = self.get_data_version()
        self._save_needed = False

        return self

    def __exit__(self, exc_type, exc_val, _exc_tb):
        """Exit the context manager: close session and recompress DB if changed."""
        print("Database.__exit__")
        if self.session:
            if exc_type is not None:
                # If there was an exception, rollback the session
                print(f"Rolling back database session due to error: {exc_val}")
                self.session.rollback()
            else:
                # If no exception, commit any pending changes
                self.session.commit()

            db_changed = self.get_data_version() != self.initial_data_version
            print(f"Database changed? {db_changed}")
            print(f"self._save_needed: {self._save_needed}")

            self.session.close()

            # Only recompress if database changed and compression is enabled
            if self.compress_on_exit and self._save_needed:
                print(f"Recompressing database to {DB_GZ}")
                with open(DB_PATH, "rb") as src, gzip.open(DB_GZ, "wb") as dst:
                    shutil.copyfileobj(src, dst)

    def get_data_version(self) -> int:
        """Get the current data version from the database."""
        assert self.session
        version = self.session.execute(text("PRAGMA user_version")).scalar()
        assert version is not None and isinstance(version, int)
        return version

    def get_source_events(self) -> List[PydanticEvent]:
        """Retrieve all events from the database sorted by start date."""
        if not self.session:
            raise NoSessionError()

        # Join with detail page enrichments to get website URLs
        results = (
            self.session.query(Event, DetailPageEnrichment)
            .outerjoin(DetailPageEnrichment, (Event.source == DetailPageEnrichment.source) & (Event.source_id == DetailPageEnrichment.source_id))
            .order_by(Event.start)
            .all()
        )

        return [event.to_pydantic(detail_page_enrichment=detail_enrichment) for event, detail_enrichment in results]

    def get_source_event(self, source: str, source_id: str) -> Optional[PydanticEvent]:
        """Retrieve a single event by source and source_id, with optional enrichment data."""
        if not self.session:
            raise NoSessionError()

        # Try to get event with enrichment data first
        result = (
            self.session.query(Event, EnrichedSourceEvent)
            .outerjoin(EnrichedSourceEvent, (Event.source == EnrichedSourceEvent.source) & (Event.source_id == EnrichedSourceEvent.source_id))
            .filter(Event.source == source, Event.source_id == source_id)
            .first()
        )

        if not result:
            return None

        event, enrichment = result
        return event.to_pydantic(enrichment)

    def get_uncategorized_source_events(self, limit: int = 20) -> List[PydanticEvent]:
        """
        Retrieve source events that don't have enrichment data yet.

        Returns:
            List of Events without llm_categorization
        """
        if not self.session:
            raise NoSessionError()

        # Use left anti join to find events without enrichment
        results = (
            self.session.query(Event)
            .outerjoin(EnrichedSourceEvent, (Event.source == EnrichedSourceEvent.source) & (Event.source_id == EnrichedSourceEvent.source_id))
            .filter(EnrichedSourceEvent.source.is_(None))  # Anti join condition
            .order_by(Event.start)
            .limit(limit)
            .all()
        )

        return [event.to_pydantic() for event in results]

    def store_event_enrichment(self, source: str, source_id: str, llm_categorization: LLMEventCategorization) -> None:
        """Store LLM enrichment data for a source event."""
        if not self.session:
            raise NoSessionError()

        # Convert Pydantic model to JSON
        categorization_json = llm_categorization.model_dump_json()

        # For now, use empty metadata
        metadata_json = json.dumps({})

        # Create enrichment record
        enrichment_data = {
            "source": source,
            "source_id": source_id,
            "llm_categorization": categorization_json,
            "llm_request_metadata": metadata_json,
            "created_at": datetime.now(timezone.utc),
            "processing_status": "success",
            "error_message": None,
        }

        # Use upsert to handle potential duplicates
        stmt = insert(EnrichedSourceEvent).values(**enrichment_data)
        stmt = stmt.on_conflict_do_update(index_elements=["source", "source_id"], set_=enrichment_data)
        self.session.execute(stmt)

        self.commit()

    def get_unenriched_detail_page_events(self, source: str, limit: int = 20) -> List[PydanticEvent]:
        """
        Get upcoming source events that don't have detail page enrichment yet.

        Args:
            source: Source to filter by (e.g., "SPF")
            limit: Maximum number of events to return

        Returns:
            List of upcoming events without detail page enrichment, sorted by start date (soonest first)
        """
        if not self.session:
            raise NoSessionError()

        now = datetime.now(timezone.utc)

        # Left join with detail_page_enrichments and filter for nulls (anti-join)
        # Only include upcoming events, sorted by start date ascending
        results = (
            self.session.query(Event)
            .outerjoin(DetailPageEnrichment, (Event.source == DetailPageEnrichment.source) & (Event.source_id == DetailPageEnrichment.source_id))
            .filter(Event.source == source)
            .filter(Event.start >= now)  # Only upcoming events
            .filter(DetailPageEnrichment.source.is_(None))  # Anti join condition
            .order_by(Event.start.asc())  # Soonest events first
            .limit(limit)
            .all()
        )

        return [event.to_pydantic() for event in results]

    def store_detail_page_enrichment(
        self, source: str, source_id: str, detail_page_url: str, enrichment_data: dict, status: str = "success", error_message: Optional[str] = None
    ) -> None:
        """
        Store detail page enrichment data for a source event.

        Args:
            source: Event source (e.g., "SPF")
            source_id: Event source ID
            detail_page_url: URL that was crawled
            enrichment_data: Dictionary of extracted data
            status: Processing status ("success", "failed", "pending")
            error_message: Error details if failed
        """
        if not self.session:
            raise NoSessionError()

        # Convert enrichment data dict to JSON
        enrichment_json = json.dumps(enrichment_data)

        # Log what we're storing
        print(f"[store_detail_page_enrichment] Storing enrichment for {source}:{source_id}")
        print(f"  URL: {detail_page_url}")
        print(f"  Status: {status}")
        print(f"  Data keys: {list(enrichment_data.keys())}")
        if enrichment_data:
            print(f"  Data preview: {str(enrichment_data)[:200]}...")
        if error_message:
            print(f"  Error: {error_message}")

        # Create enrichment record
        enrichment_record = {
            "source": source,
            "source_id": source_id,
            "detail_page_url": detail_page_url,
            "enrichment_data": enrichment_json,
            "fetched_at": datetime.now(timezone.utc),
            "processing_status": status,
            "error_message": error_message,
        }

        # Use upsert to handle potential duplicates
        stmt = insert(DetailPageEnrichment).values(**enrichment_record)
        stmt = stmt.on_conflict_do_update(index_elements=["source", "source_id"], set_=enrichment_record)
        self.session.execute(stmt)

        self.commit()
        print(f"[store_detail_page_enrichment] Successfully stored enrichment for {source}:{source_id}")

    def init_database(self, reset: bool = False):
        """Initialize the database by creating all tables."""
        Base.metadata.create_all(bind=self.engine)

        if reset:
            # If reset is True, clear existing data
            with get_session() as session:
                session.query(Event).delete()
                session.query(CanonicalEvent).delete()
                session.query(EventGroupMembership).delete()
                session.query(ETLRun).delete()
                session.query(EnrichedSourceEvent).delete()
                session.query(DetailPageEnrichment).delete()
                session.commit()

    def upsert_source_events(self, events: List[PydanticEvent]):
        """Insert or update source events in the database using SQLAlchemy."""
        if not self.session:
            raise NoSessionError()

        for event in events:
            # Convert Pydantic model to dict for upsert
            event_data = {
                "source": event.source,
                "source_id": event.source_id,
                "title": event.title,
                "start": event.start.astimezone(timezone.utc),
                "end": event.end.astimezone(timezone.utc),
                "venue": event.venue,
                "address": event.address,
                "url": str(event.url),
                "cost": event.cost,
                "latitude": event.latitude,
                "longitude": event.longitude,
                "tags": ",".join(event.tags) if event.tags else "",
                "same_as": str(event.same_as) if event.same_as else None,
                "source_dict": event.source_dict,
            }

            # Use SQLite-specific upsert syntax
            stmt = insert(Event).values(**event_data)
            stmt = stmt.on_conflict_do_update(index_elements=["source", "source_id"], set_=event_data)
            self.session.execute(stmt)

        self.commit()

    def get_canonical_events(self) -> List[PydanticCanonicalEvent]:
        """Retrieve all canonical events from the database."""
        if not self.session:
            raise NoSessionError()

        events = self.session.query(CanonicalEvent).order_by(CanonicalEvent.start).all()
        return [event.to_pydantic() for event in events]

    def get_future_canonical_events(self) -> List[PydanticCanonicalEvent]:
        """Retrieve canonical events that haven't ended yet."""
        if not self.session:
            raise NoSessionError()

        # TODO: Merge with get_canonical_events
        now = datetime.now(timezone.utc)
        events = self.session.query(CanonicalEvent).filter(CanonicalEvent.end >= now).order_by(CanonicalEvent.start).all()
        return [event.to_pydantic() for event in events]

    def get_source_events_by_canonical_id(self, canonical_id: str) -> List[PydanticEvent]:
        """Get all source events that belong to a specific canonical event."""
        if not self.session:
            raise NoSessionError()

        source_events = (
            self.session.query(Event)
            .join(EventGroupMembership, (Event.source == EventGroupMembership.source) & (Event.source_id == EventGroupMembership.source_id))
            .filter(EventGroupMembership.canonical_id == canonical_id)
            .all()
        )
        return [event.to_pydantic() for event in source_events]

    def get_source_updated_stats(self) -> Dict[str, datetime]:
        """Get the most recent successful ETL run datetime for each source."""
        if not self.session:
            raise NoSessionError()

        # Get the most recent successful run for each source

        # Subquery to get the max datetime for each source where status = 'success'
        max_datetimes = (
            self.session.query(ETLRun.source, func.max(ETLRun.run_datetime).label("max_datetime"))
            .filter(ETLRun.status == "success")
            .group_by(ETLRun.source)
            .subquery()
        )

        # Get the actual ETL run records for those max datetimes
        runs = (
            self.session.query(ETLRun)
            .join(max_datetimes, (ETLRun.source == max_datetimes.c.source) & (ETLRun.run_datetime == max_datetimes.c.max_datetime))
            .all()
        )

        # Convert to dict of source -> datetime
        result = {}
        for run in runs:
            result[run.source] = read_utc(run.run_datetime)

        return result

    def get_etl_run_stats(self, days: int = 7) -> Dict[str, Dict[str, int]]:
        """Get ETL run statistics for each source over the last N days."""
        if not self.session:
            raise NoSessionError()

        # Calculate cutoff date
        cutoff_date = datetime.now(timezone.utc) - timedelta(days=days)

        # Get all runs in the time period
        runs = self.session.query(ETLRun).filter(ETLRun.run_datetime >= cutoff_date).all()

        # Group by source and count success/failure
        stats = {}
        for run in runs:
            if run.source not in stats:
                stats[run.source] = {"success": 0, "failure": 0, "total": 0}

            stats[run.source][run.status] += 1
            stats[run.source]["total"] += 1

        return stats

    def get_data_freshness_grid(self, days: int = 5) -> Dict[str, Dict[str, bool]]:
        """Get data freshness grid showing success/failure for each source by date.

        Returns:
            Dict mapping source -> Dict mapping date string (YYYY-MM-DD) -> bool (success status)
        """
        if not self.session:
            raise NoSessionError()

        # Calculate date range (last N days)
        end_date = datetime.now(timezone.utc).date()
        start_date = end_date - timedelta(days=days - 1)

        # Get all ETL runs in the date range
        start_datetime = datetime.combine(start_date, datetime.min.time(), timezone.utc)
        end_datetime = datetime.combine(end_date, datetime.max.time(), timezone.utc)

        runs = (
            self.session.query(ETLRun)
            .filter(ETLRun.run_datetime >= start_datetime)
            .filter(ETLRun.run_datetime <= end_datetime)
            .order_by(ETLRun.run_datetime.desc())
            .all()
        )

        # Initialize grid with all dates and sources
        grid = {}
        all_sources = set()

        # First pass: collect all sources
        for run in runs:
            all_sources.add(run.source)

        # Initialize grid with False (no successful run) for all source/date combinations
        for source in all_sources:
            grid[source] = {}
            current_date = start_date
            while current_date <= end_date:
                grid[source][current_date.strftime("%Y-%m-%d")] = False
                current_date += timedelta(days=1)

        # Second pass: mark successful runs
        for run in runs:
            if run.status == "success":
                run_date = read_utc(run.run_datetime).date()
                date_str = run_date.strftime("%Y-%m-%d")
                if run.source in grid and date_str in grid[run.source]:
                    grid[run.source][date_str] = True

        return grid

    def record_etl_run(self, source: str, status: str, num_rows: int) -> None:
        """Record an ETL run for a data source."""
        if not self.session:
            raise NoSessionError()

        etl_run = ETLRun(id=str(uuid.uuid4()), source=source, run_datetime=datetime.now(timezone.utc), status=status, num_rows=num_rows)
        self.session.add(etl_run)
        self.commit()

    def overwrite_canonical_events(self, canonical_events: List[PydanticCanonicalEvent]) -> None:
        """Overwrite all canonical events in the database."""
        if not self.session:
            raise NoSessionError()

        # Clear existing canonical events first
        self.session.query(CanonicalEvent).delete()

        for canonical_event in canonical_events:
            # Convert Pydantic model to dict for upsert
            event_data = {
                "canonical_id": canonical_event.canonical_id,
                "title": canonical_event.title,
                "start": canonical_event.start.astimezone(timezone.utc),
                "end": canonical_event.end.astimezone(timezone.utc),
                "venue": canonical_event.venue,
                "address": canonical_event.address,
                "url": str(canonical_event.url),
                "cost": canonical_event.cost,
                "latitude": canonical_event.latitude,
                "longitude": canonical_event.longitude,
                "tags": ",".join(canonical_event.tags) if canonical_event.tags else "",
            }

            # Use SQLite-specific upsert syntax
            stmt = insert(CanonicalEvent).values(**event_data)
            stmt = stmt.on_conflict_do_update(index_elements=["canonical_id"], set_=event_data)
            self.session.execute(stmt)

            # Make sure to link source events to this canonical event
            for source, source_id in canonical_event.iter_source_events():
                membership_data = {
                    "canonical_id": canonical_event.canonical_id,
                    "source": source,
                    "source_id": source_id,
                }
                stmt = insert(EventGroupMembership).values(**membership_data)
                stmt = stmt.on_conflict_do_update(index_elements=["canonical_id", "source", "source_id"], set_=membership_data)
                self.session.execute(stmt)

        self.commit()

    def commit(self):
        """Commit the current session and flag that we need to save changes."""
        if not self.session:
            raise NoSessionError()

        try:
            self.session.commit()
            self._save_needed = True
        except Exception:
            self.session.rollback()
            raise


#### SEE IF I CAN DELETE THESE!


def get_engine():
    """Create and return a SQLAlchemy engine for the SQLite database."""
    ensure_database_exists()
    return create_engine(f"sqlite:///{DB_PATH}", echo=False)


def get_session() -> Session:
    """Create and return a SQLAlchemy session."""
    engine = get_engine()
    SessionLocal = sessionmaker(bind=engine)
    return SessionLocal()


def get_connection():
    """Get a connection to the database."""
    engine = get_engine()
    return engine.connect()


def get_regular_connection():
    return sqlite3.connect(DB_PATH)
