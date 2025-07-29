"""Database module using SQLAlchemy for managing the events database."""

from typing import List, Dict, Tuple
from datetime import datetime, timedelta, timezone
from sqlalchemy import create_engine, Column, String, DateTime, Float, Text, PrimaryKeyConstraint
from sqlalchemy.orm import sessionmaker, Session, declarative_base
from sqlalchemy.dialects.sqlite import insert

from .config import DB_PATH, ensure_database_exists
from .models import Event as PydanticEvent, CanonicalEvent as PydanticCanonicalEvent, EventGroupMembership as PydanticEventGroupMembership

Base = declarative_base()


def read_utc(db_datetime_value: datetime) -> datetime:
    """Repair a tz-stripped datetime read from sqlite"""
    if db_datetime_value.tzinfo:
        return db_datetime_value

    return db_datetime_value.replace(tzinfo=timezone.utc)


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
            start=read_utc(self.start),
            end=read_utc(self.end),
            venue=self.venue,
            address=self.address,
            url=self.url,
            cost=self.cost,
            latitude=self.latitude,
            longitude=self.longitude,
            tags=tags,
            same_as=self.same_as
        )


class CanonicalEvent(Base):
    """SQLAlchemy model for canonical events table."""
    __tablename__ = 'canonical_events'

    canonical_id = Column(String, primary_key=True)
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

    def to_pydantic(self) -> PydanticCanonicalEvent:
        """Convert SQLAlchemy model to Pydantic model."""
        tags = self.tags.split(',') if self.tags else []
        # Get source events from the membership table
        from sqlalchemy.orm import object_session
        session = object_session(self)
        source_events = []
        if session:
            memberships = session.query(EventGroupMembership).filter(
                EventGroupMembership.canonical_id == self.canonical_id
            ).all()
            source_events = [f"{m.source}:{m.source_id}" for m in memberships]

        return PydanticCanonicalEvent(
            canonical_id=self.canonical_id,
            title=self.title,
            start=read_utc(self.start),
            end=read_utc(self.end),
            venue=self.venue,
            address=self.address,
            url=self.url,
            cost=self.cost,
            latitude=self.latitude,
            longitude=self.longitude,
            tags=tags,
            source_events=source_events
        )


class EventGroupMembership(Base):
    """SQLAlchemy model for tracking which source events belong to which canonical events."""
    __tablename__ = 'event_group_memberships'

    canonical_id = Column(String, nullable=False)
    source = Column(String, nullable=False)
    source_id = Column(String, nullable=False)

    __table_args__ = (
        PrimaryKeyConstraint('canonical_id', 'source', 'source_id'),
    )

    def to_pydantic(self) -> PydanticEventGroupMembership:
        """Convert SQLAlchemy model to Pydantic model."""
        return PydanticEventGroupMembership(
            canonical_id=self.canonical_id,
            source=self.source,
            source_id=self.source_id
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


def init_database(reset: bool = False) -> None:
    """Initialize the database by creating all tables."""
    engine = get_engine()
    Base.metadata.create_all(bind=engine)

    if reset:
        # If reset is True, clear existing data
        with get_session() as session:
            session.query(Event).delete()
            session.query(CanonicalEvent).delete()
            session.query(EventGroupMembership).delete()
            session.commit()


def upsert_source_events(events: List[PydanticEvent]) -> None:
    """Insert or update source events in the database using SQLAlchemy."""
    session = get_session()

    try:
        for event in events:
            # Convert Pydantic model to dict for upsert
            event_data = {
                'source': event.source,
                'source_id': event.source_id,
                'title': event.title,
                'start': event.start.astimezone(timezone.utc),
                'end': event.end.astimezone(timezone.utc),
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


def get_source_events() -> List[PydanticEvent]:
    """Retrieve all events from the database sorted by start date."""
    session = get_session()

    try:
        events = session.query(Event).order_by(Event.start).all()
        return [event.to_pydantic() for event in events]
    finally:
        session.close()


def get_source_events_count() -> int:
    """Get the total count of events in the database."""
    session = get_session()

    try:
        return session.query(Event).count()
    finally:
        session.close()


def get_upcoming_source_events(days_ahead: int = 30) -> List[PydanticEvent]:
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


def get_future_source_events() -> List[PydanticEvent]:
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


def get_past_source_events() -> List[PydanticEvent]:
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


def overwrite_canonical_events(canonical_events: List[PydanticCanonicalEvent]) -> None:
    """Overwrite all canonical events in the database."""
    session = get_session()

    try:
        # Clear existing canonical events first
        session.query(CanonicalEvent).delete()

        for canonical_event in canonical_events:
            # Convert Pydantic model to dict for upsert
            event_data = {
                'canonical_id': canonical_event.canonical_id,
                'title': canonical_event.title,
                'start': canonical_event.start.astimezone(timezone.utc),
                'end': canonical_event.end.astimezone(timezone.utc),
                'venue': canonical_event.venue,
                'address': canonical_event.address,
                'url': str(canonical_event.url),
                'cost': canonical_event.cost,
                'latitude': canonical_event.latitude,
                'longitude': canonical_event.longitude,
                'tags': ','.join(canonical_event.tags) if canonical_event.tags else '',
            }

            # Use SQLite-specific upsert syntax
            stmt = insert(CanonicalEvent).values(**event_data)
            stmt = stmt.on_conflict_do_update(
                index_elements=['canonical_id'],
                set_=event_data
            )
            session.execute(stmt)

        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def overwrite_event_group_memberships(membership_map: Dict[Tuple[str, str], str]) -> None:
    """Insert or update event group membership records."""
    session = get_session()

    try:
        # Clear existing memberships first
        session.query(EventGroupMembership).delete()

        # Insert new memberships
        for (source, source_id), canonical_id in membership_map.items():
            membership_data = {
                'canonical_id': canonical_id,
                'source': source,
                'source_id': source_id,
            }

            # Use SQLite-specific upsert syntax
            stmt = insert(EventGroupMembership).values(**membership_data)
            stmt = stmt.on_conflict_do_update(
                index_elements=['canonical_id', 'source', 'source_id'],
                set_=membership_data
            )
            session.execute(stmt)

        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def get_canonical_events() -> List[PydanticCanonicalEvent]:
    """Retrieve all canonical events from the database."""
    session = get_session()

    try:
        events = session.query(CanonicalEvent).order_by(
            CanonicalEvent.start).all()
        return [event.to_pydantic() for event in events]
    finally:
        session.close()


def get_future_canonical_events() -> List[PydanticCanonicalEvent]:
    """Retrieve canonical events that haven't ended yet."""
    session = get_session()

    try:
        now = datetime.utcnow()
        events = session.query(CanonicalEvent).filter(
            CanonicalEvent.end >= now
        ).order_by(CanonicalEvent.start).all()
        return [event.to_pydantic() for event in events]
    finally:
        session.close()


def get_events_by_canonical_id(canonical_id: str) -> List[PydanticEvent]:
    """Get all source events that belong to a canonical event."""
    session = get_session()

    try:
        # Get membership records for this canonical event
        memberships = session.query(EventGroupMembership).filter(
            EventGroupMembership.canonical_id == canonical_id
        ).all()

        # Get the actual source events
        source_events = []
        for membership in memberships:
            event = session.query(Event).filter(
                Event.source == membership.source,
                Event.source_id == membership.source_id
            ).first()
            if event:
                source_events.append(event.to_pydantic())

        return source_events
    finally:
        session.close()
