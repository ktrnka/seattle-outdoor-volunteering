"""
Extractor for manually-defined recurring events.

This extractor reads from the manual_events.yaml file and generates
Event instances for upcoming occurrences of recurring events.
"""

from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import List, Optional
import yaml
from pydantic import BaseModel, ConfigDict, HttpUrl

from src.models import RecurringPattern


from ..models import Event, RecurringPattern, SEATTLE_TZ
from .base import BaseExtractor
from .url_utils import normalize_url


class ManualEventDefinition(BaseModel):
    """Definition of a recurring manual event."""
    model_config = ConfigDict(from_attributes=True)

    id: str  # Unique identifier for this recurring event definition
    title: str
    description: Optional[str] = None
    recurring_pattern: RecurringPattern
    venue: Optional[str] = None
    address: Optional[str] = None
    url: HttpUrl
    cost: Optional[str] = None
    tags: Optional[List[str]] = []


class ManualEventsConfig(BaseModel):
    """Configuration file structure for manual events."""
    model_config = ConfigDict(from_attributes=True)

    recurring_events: List[ManualEventDefinition] = []


class ManualExtractor(BaseExtractor):
    """Extract events from manual_events.yaml configuration."""

    source = "MAN"  # 3-letter code for manual events

    def __init__(self, raw_data: str):
        super().__init__(raw_data)
        self.config = ManualEventsConfig.model_validate(
            yaml.safe_load(raw_data))

    @classmethod
    def fetch(cls) -> 'ManualExtractor':
        """Load manual events from the YAML configuration file."""
        manual_events_path = Path(
            __file__).parent.parent.parent / "data" / "manual_events.yaml"

        if not manual_events_path.exists():
            raise FileNotFoundError(
                f"Manual events file not found: {manual_events_path}")

        with open(manual_events_path, 'r', encoding='utf-8') as f:
            raw_data = f.read()

        return cls(raw_data)

    def extract(self) -> List[Event]:
        """
        Extract upcoming occurrences of recurring events.

        Generates events for the next 6 months based on recurring patterns.
        """
        events = []

        # Generate events for the next 6 months
        today = datetime.now(SEATTLE_TZ).date()
        end_date = today + timedelta(days=180)  # 6 months ahead

        for event_def in self.config.recurring_events:
            recurring_events = self._generate_recurring_events(
                event_def, today, end_date)
            events.extend(recurring_events)

        return events

    def _generate_recurring_events(self, event_def, start_date, end_date) -> List[Event]:
        """Generate recurring event instances between start_date and end_date."""
        events = []

        # Start from the beginning of the current month
        current_date = start_date.replace(day=1)

        while current_date <= end_date:
            event_date = self._get_nth_weekday_of_month(
                current_date.year,
                current_date.month,
                event_def.recurring_pattern
            )

            # Only include future events
            if event_date and event_date >= start_date:
                event = self._create_event_instance(event_def, event_date)
                events.append(event)

            # Move to next month
            if current_date.month == 12:
                current_date = current_date.replace(
                    year=current_date.year + 1, month=1)
            else:
                current_date = current_date.replace(
                    month=current_date.month + 1)

        return events

    def _get_nth_weekday_of_month(self, year: int, month: int, pattern: RecurringPattern):
        """Get the specific weekday occurrence for a given month."""
        # Parse the pattern
        parts = pattern.value.split('_')
        nth_str = parts[0]  # first, second, third, fourth
        weekday_str = parts[1]  # saturday, sunday

        # Map ordinals
        nth_map = {
            'first': 1,
            'second': 2,
            'third': 3,
            'fourth': 4
        }

        # Map weekdays (Monday=0, Sunday=6)
        weekday_map = {
            'monday': 0,
            'tuesday': 1,
            'wednesday': 2,
            'thursday': 3,
            'friday': 4,
            'saturday': 5,
            'sunday': 6
        }

        nth = nth_map.get(nth_str)
        target_weekday = weekday_map.get(weekday_str)

        if nth is None or target_weekday is None:
            return None

        # Find the nth occurrence of the target weekday
        first_day = datetime(year, month, 1).date()
        first_weekday = first_day.weekday()

        # Calculate days until the first occurrence of target weekday
        days_to_first = (target_weekday - first_weekday) % 7
        first_occurrence = first_day + timedelta(days=days_to_first)

        # Calculate the nth occurrence
        target_date = first_occurrence + timedelta(weeks=nth - 1)

        # Make sure it's still in the same month
        if target_date.month != month:
            return None

        return target_date

    def _create_event_instance(self, event_def, event_date) -> Event:
        """Create an Event instance for a specific date."""
        # Create a unique source_id based on the stable event ID and date
        source_id = f"{event_def.id}_{event_date.strftime('%Y_%m_%d')}"

        # Convert date to UTC datetime (date-only event - same start/end time)
        # Create at midnight Seattle time, then convert to UTC for proper timezone handling
        start_seattle = datetime.combine(
            event_date, datetime.min.time()).replace(tzinfo=SEATTLE_TZ)
        start_utc = start_seattle.astimezone(timezone.utc)
        # Date-only events have same start/end (zero duration)
        end_utc = start_utc

        return Event(
            source=self.source,
            source_id=source_id,
            title=event_def.title,
            start=start_utc,
            end=end_utc,
            venue=event_def.venue,
            address=event_def.address,
            url=HttpUrl(normalize_url(str(event_def.url))),
            cost=event_def.cost,
            tags=event_def.tags or []
        )
