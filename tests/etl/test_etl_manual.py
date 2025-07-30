"""Tests for the manual events extractor."""

from datetime import timezone

from src.etl.manual import ManualExtractor
from src.models import Event, RecurringPattern


class TestManualExtractor:
    """Test the ManualExtractor with mocked data."""

    def test_manual_extractor_basic_functionality(self):
        """Test basic functionality with mocked YAML data."""
        # Create mock YAML data
        mock_yaml = """
recurring_events:
  - id: "test_cleanup"
    title: "Test Monthly Cleanup"
    description: "Test cleanup event"
    recurring_pattern: "first_saturday"
    venue: "Test Park"
    url: "https://example.com/test"
    tags: ["cleanup", "test"]
"""

        # Create extractor with mock data
        extractor = ManualExtractor(mock_yaml)

        # Verify configuration parsed correctly
        assert len(extractor.config.recurring_events) == 1
        event_def = extractor.config.recurring_events[0]
        assert event_def.id == "test_cleanup"
        assert event_def.title == "Test Monthly Cleanup"
        assert event_def.recurring_pattern == RecurringPattern.FIRST_SATURDAY
        assert event_def.venue == "Test Park"
        assert str(event_def.url) == "https://example.com/test"
        assert event_def.tags == ["cleanup", "test"]

    def test_recurring_pattern_calculation(self):
        """Test that recurring patterns calculate dates correctly."""
        mock_yaml = """
recurring_events:
  - id: "first_sat"
    title: "First Saturday Event"
    recurring_pattern: "first_saturday"
    url: "https://example.com/first-sat"
  - id: "second_sun"
    title: "Second Sunday Event"
    recurring_pattern: "second_sunday"
    url: "https://example.com/second-sun"
  - id: "third_sun"
    title: "Third Sunday Event"
    recurring_pattern: "third_sunday"
    url: "https://example.com/third-sun"
"""

        extractor = ManualExtractor(mock_yaml)

        # Test specific month calculations
        # August 2025: 1st is Friday, so first Saturday is Aug 2
        first_sat = extractor._get_nth_weekday_of_month(
            2025, 8, RecurringPattern.FIRST_SATURDAY)
        assert first_sat is not None
        assert first_sat.day == 2
        assert first_sat.weekday() == 5  # Saturday

        # August 2025: second Sunday is Aug 10
        second_sun = extractor._get_nth_weekday_of_month(
            2025, 8, RecurringPattern.SECOND_SUNDAY)
        assert second_sun is not None
        assert second_sun.day == 10
        assert second_sun.weekday() == 6  # Sunday

        # August 2025: third Sunday is Aug 17
        third_sun = extractor._get_nth_weekday_of_month(
            2025, 8, RecurringPattern.THIRD_SUNDAY)
        assert third_sun is not None
        assert third_sun.day == 17
        assert third_sun.weekday() == 6  # Sunday

    def test_event_generation(self):
        """Test that events are generated with correct properties."""
        mock_yaml = """
recurring_events:
  - id: "test_event"
    title: "Test Event"
    description: "Test description"
    recurring_pattern: "first_saturday"
    venue: "Test Venue"
    address: "123 Test St"
    url: "https://example.com/test"
    cost: "Free"
    tags: ["cleanup", "volunteer"]
"""

        extractor = ManualExtractor(mock_yaml)
        events = extractor.extract()

        # Should generate multiple events (next 6 months)
        assert len(events) > 0

        # Check first event properties
        first_event = events[0]
        assert isinstance(first_event, Event)
        assert first_event.source == "MAN"
        assert first_event.title == "Test Event"
        assert first_event.venue == "Test Venue"
        assert first_event.address == "123 Test St"
        assert str(first_event.url) == "https://example.com/test"
        assert first_event.cost == "Free"
        assert first_event.tags == ["cleanup", "volunteer"]

        # Check that it's a date-only event
        assert first_event.is_date_only()
        assert first_event.start == first_event.end

        # Note: Date-only events are now stored at midnight Seattle time (converted to UTC)
        # We verify it's date-only via the is_date_only() method which handles timezone conversion

        # Check that start time is in UTC
        assert first_event.start.tzinfo == timezone.utc

    def test_source_id_generation(self):
        """Test that source IDs are generated correctly and uniquely."""
        mock_yaml = """
recurring_events:
  - id: "test_monthly"
    title: "Test Monthly Event"
    recurring_pattern: "first_saturday"
    url: "https://example.com/test"
"""

        extractor = ManualExtractor(mock_yaml)
        events = extractor.extract()

        # Check that source IDs are unique and follow expected pattern
        source_ids = [event.source_id for event in events]
        assert len(source_ids) == len(set(source_ids))  # All unique

        # Check format: should include event ID and date
        first_source_id = source_ids[0]
        assert "test_monthly" in first_source_id
        assert "_2025_" in first_source_id or "_2026_" in first_source_id  # Should have year

    def test_multiple_recurring_events(self):
        """Test that multiple recurring events are all generated."""
        mock_yaml = """
recurring_events:
  - id: "saturday_event"
    title: "Saturday Event"
    recurring_pattern: "first_saturday"
    url: "https://example.com/sat"
  - id: "sunday_event"
    title: "Sunday Event"
    recurring_pattern: "second_sunday"
    url: "https://example.com/sun"
"""

        extractor = ManualExtractor(mock_yaml)
        events = extractor.extract()

        # Should have events from both definitions
        saturday_events = [e for e in events if e.title == "Saturday Event"]
        sunday_events = [e for e in events if e.title == "Sunday Event"]

        assert len(saturday_events) > 0
        assert len(sunday_events) > 0

        # Verify they have correct source IDs
        for event in saturday_events:
            assert event.source_id.startswith("saturday_event_")

        for event in sunday_events:
            assert event.source_id.startswith("sunday_event_")

    def test_empty_configuration(self):
        """Test handling of empty configuration."""
        mock_yaml = """
recurring_events: []
"""

        extractor = ManualExtractor(mock_yaml)
        events = extractor.extract()

        assert len(events) == 0

    def test_invalid_pattern_handling(self):
        """Test that the pattern parser handles edge cases."""
        mock_yaml = """
recurring_events: []
"""
        extractor = ManualExtractor(mock_yaml)

        # Test that we handle months where the nth occurrence doesn't exist
        # For example, there's no 5th Sunday in most months
        result = extractor._get_nth_weekday_of_month(
            2025, 2, RecurringPattern.FOURTH_SUNDAY)
        # February 2025 only has 4 Sundays, so 4th should exist
        assert result is not None

        # Test a case that should return None (method would handle gracefully)
        # This is more of an integration test of the date calculation logic
