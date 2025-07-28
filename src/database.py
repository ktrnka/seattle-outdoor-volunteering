"""Database module using SQLAlchemy for managing the events database."""

from typing import List
from datetime import datetime, timedelta
from sqlalchemy import create_engine, Column, String, DateTime, Float, Text, PrimaryKeyConstraint
from sqlalchemy.orm import sessionmaker, Session, declarative_base
from sqlalchemy.dialects.sqlite import insert

from .config import DB_PATH, ensure_database_exists
from .models import Event as PydanticEvent

Base = declarative_base()


class Event(Base):
    """SQLAlchemy model for events table."""
    __tablename__ = 'events'

    source = Column(String, nullable=False)
    source_id = Column(String, nullable=False)
    title = Column(String, nullable=False)
    start = Column(DateTime, nullable=False)
    end = Column(DateTime, nullable=False)
    venue = Column(String, nullable=True)
    address = Column(String, nullable=True)
    url = Column(Text, nullable=False)
    cost = Column(String, nullable=True)
    latitude = Column(Float, nullable=True)
    longitude = Column(Float, nullable=True)
    tags = Column(Text, nullable=True)  # Stored as comma-separated string
    # URL of the canonical/primary version of this event
    same_as = Column(String, nullable=True)

    __table_args__ = (
        PrimaryKeyConstraint('source', 'source_id'),
    )

    def to_pydantic(self) -> PydanticEvent:
        """Convert SQLAlchemy model to Pydantic model."""
        tags = self.tags.split(',') if self.tags else []
        return PydanticEvent(
            source=self.source,
            source_id=self.source_id,
            title=self.title,
            start=self.start,
            end=self.end,
            venue=self.venue,
            address=self.address,
            url=self.url,
            cost=self.cost,
            latitude=self.latitude,
            longitude=self.longitude,
            tags=tags,
            same_as=self.same_as
        )


def get_engine():
    """Create and return a SQLAlchemy engine for the SQLite database."""
    ensure_database_exists()
    return create_engine(f'sqlite:///{DB_PATH}', echo=False)


def get_session() -> Session:
    """Create and return a SQLAlchemy session."""
    engine = get_engine()
    SessionLocal = sessionmaker(bind=engine)
    return SessionLocal()


def init_database() -> None:
    """Initialize the database by creating all tables."""
    engine = get_engine()
    Base.metadata.create_all(bind=engine)


def upsert_events(events: List[PydanticEvent]) -> None:
    """Insert or update events in the database using SQLAlchemy."""
    session = get_session()

    try:
        for event in events:
            # Convert Pydantic model to dict for upsert
            event_data = {
                'source': event.source,
                'source_id': event.source_id,
                'title': event.title,
                'start': event.start,
                'end': event.end,
                'venue': event.venue,
                'address': event.address,
                'url': str(event.url),
                'cost': event.cost,
                'latitude': event.latitude,
                'longitude': event.longitude,
                'tags': ','.join(event.tags) if event.tags else '',
                'same_as': str(event.same_as) if event.same_as else None
            }

            # Use SQLite-specific upsert syntax
            stmt = insert(Event).values(**event_data)
            stmt = stmt.on_conflict_do_update(
                index_elements=['source', 'source_id'],
                set_=event_data
            )
            session.execute(stmt)

        session.commit()
    except Exception as e:
        session.rollback()
        raise e
    finally:
        session.close()


def get_all_events_sorted() -> List[PydanticEvent]:
    """Retrieve all events from the database sorted by start date."""
    session = get_session()

    try:
        events = session.query(Event).order_by(Event.start).all()
        return [event.to_pydantic() for event in events]
    finally:
        session.close()


def get_events_count() -> int:
    """Get the total count of events in the database."""
    session = get_session()

    try:
        return session.query(Event).count()
    finally:
        session.close()


def get_upcoming_events(days_ahead: int = 30) -> List[PydanticEvent]:
    """Retrieve upcoming events within the specified number of days."""
    session = get_session()
    now = datetime.now()
    future_limit = now + timedelta(days=days_ahead)

    try:
        events = session.query(Event).filter(
            Event.start >= now,
            Event.start <= future_limit
        ).order_by(Event.start).all()
        return [event.to_pydantic() for event in events]
    finally:
        session.close()


def get_all_future_events() -> List[PydanticEvent]:
    """Retrieve all future events sorted by start date."""
    session = get_session()
    now = datetime.now()

    try:
        events = session.query(Event).filter(
            Event.start >= now
        ).order_by(Event.start).all()
        return [event.to_pydantic() for event in events]
    finally:
        session.close()


def get_all_past_events() -> List[PydanticEvent]:
    """Retrieve all past events sorted by start date (most recent first)."""
    session = get_session()
    now = datetime.now()

    try:
        events = session.query(Event).filter(
            Event.start < now
        ).order_by(Event.start.desc()).all()
        return [event.to_pydantic() for event in events]
    finally:
        session.close()


def get_non_duplicate_events() -> List[PydanticEvent]:
    """Retrieve all events that are not marked as duplicates."""
    session = get_session()

    try:
        events = session.query(Event).filter(
            Event.same_as.is_(None)
        ).order_by(Event.start).all()
        return [event.to_pydantic() for event in events]
    finally:
        session.close()


def get_duplicate_events() -> List[PydanticEvent]:
    """Retrieve all events that are marked as duplicates."""
    session = get_session()

    try:
        events = session.query(Event).filter(
            Event.same_as.isnot(None)
        ).order_by(Event.start).all()
        return [event.to_pydantic() for event in events]
    finally:
        session.close()


def get_events_by_canonical(canonical_url: str) -> List[PydanticEvent]:
    """Retrieve all events (canonical + duplicates) for a given canonical event URL."""
    session = get_session()

    try:
        # Get the canonical event
        canonical_event = session.query(Event).filter(
            Event.url == canonical_url
        ).first()

        if not canonical_event:
            return []

        # Get all duplicates of this event
        duplicate_events = session.query(Event).filter(
            Event.same_as == canonical_url
        ).all()

        all_events = [canonical_event] + duplicate_events
        return [event.to_pydantic() for event in all_events]
    finally:
        session.close()
