"""Database module using SQLAlchemy for managing the events database."""

import gzip
import shutil
import sqlite3
import uuid
from datetime import datetime, timedelta, timezone
from sqlalchemy import create_engine, String, DateTime, Float, Text, Integer, PrimaryKeyConstraint, text
from sqlalchemy.dialects.sqlite import insert
from sqlalchemy.orm import Mapped, mapped_column, object_session, sessionmaker, Session, declarative_base
from sqlalchemy.sql import func
from typing import List, Dict, Optional, Tuple

from .config import DB_PATH, DB_GZ
from .models import (
    Event as PydanticEvent,
    CanonicalEvent as PydanticCanonicalEvent,
    EventGroupMembership as PydanticEventGroupMembership,
    ETLRun as PydanticETLRun,
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

    def to_pydantic(self) -> PydanticEvent:
        """Convert SQLAlchemy model to Pydantic model."""
        tags = [tag.strip() for tag in self.tags.split(",")] if self.tags else []

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
            same_as=self.same_as,  # type: ignore
            source_dict=self.source_dict,
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


class NoSessionError(RuntimeError):
    def __init__(self, message: str = "Database session not available. Use within 'with' statement."):
        super().__init__(message)


def ensure_database_exists() -> None:
    """Ensure the uncompressed database exists by extracting from gzipped version if needed."""
    if not DB_PATH.exists() and DB_GZ.exists():
        print(f"Extracting {DB_GZ} to {DB_PATH}")
        with gzip.open(DB_GZ, 'rb') as f_in:
            with open(DB_PATH, 'wb') as f_out:
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

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit the context manager: close session and recompress DB if changed."""
        if self.session:
            if exc_type is not None:
                # If there was an exception, rollback the session
                self.session.rollback()
            else:
                # If no exception, commit any pending changes
                self.session.commit()

            db_changed = self.get_data_version() != self.initial_data_version

            self.session.close()

            # Only recompress if database changed and compression is enabled
            if self.compress_on_exit and db_changed:
                with open(DB_PATH, "rb") as src, gzip.open(DB_GZ, "wb") as dst:
                    shutil.copyfileobj(src, dst)

    def get_data_version(self) -> int:
        """Get the current data version from the database."""
        assert self.session
        version = self.session.execute(text("PRAGMA user_version")).scalar()
        assert version is not None and isinstance(version, int)
        return version

    def get_source_events_count(self) -> int:
        """Get the total count of events in the database."""
        if not self.session:
            raise NoSessionError()
        return self.session.query(Event).count()

    def get_source_events(self) -> List[PydanticEvent]:
        """Retrieve all events from the database sorted by start date."""
        if not self.session:
            raise NoSessionError()
        events = self.session.query(Event).order_by(Event.start).all()
        return [event.to_pydantic() for event in events]

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
                session.commit()

    def upsert_source_events(self, events: List[PydanticEvent]):
        """Insert or update source events in the database using SQLAlchemy."""
        if not self.session:
            raise NoSessionError()

        try:
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

            self.session.commit()
        except Exception as e:
            self.session.rollback()
            raise e

    def get_upcoming_source_events(self, days_ahead: int = 30) -> List[PydanticEvent]:
        """Retrieve upcoming events within the specified number of days."""
        if not self.session:
            raise NoSessionError()

        # TODO: Consider merging this in with get_source_events
        now = datetime.now()
        future_limit = now + timedelta(days=days_ahead)

        events = self.session.query(Event).filter(Event.start >= now, Event.start <= future_limit).order_by(Event.start).all()
        return [event.to_pydantic() for event in events]

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

    def find_canonical_event_with_sources(self, title: str) -> Optional[Tuple[PydanticCanonicalEvent, List[PydanticEvent]]]:
        """Find a canonical event by title."""
        if not self.session:
            raise NoSessionError()

        event = self.session.query(CanonicalEvent).filter(CanonicalEvent.title.ilike(f"%{title}%")).first()
        if event:
            return event.to_pydantic(), self.get_source_events_by_canonical_id(event.canonical_id)
        return None
    
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

    def record_etl_run(self, source: str, status: str, num_rows: int) -> None:
        """Record an ETL run for a data source."""
        if not self.session:
            raise NoSessionError()

        try:
            etl_run = ETLRun(id=str(uuid.uuid4()), source=source, run_datetime=datetime.now(timezone.utc), status=status, num_rows=num_rows)
            self.session.add(etl_run)
            self.session.commit()
        except Exception:
            self.session.rollback()
            raise

    def overwrite_canonical_events(self, canonical_events: List[PydanticCanonicalEvent]) -> None:
        """Overwrite all canonical events in the database."""
        if not self.session:
            raise NoSessionError()

        try:
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

            self.session.commit()
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
