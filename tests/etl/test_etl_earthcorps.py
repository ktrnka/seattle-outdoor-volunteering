from src.etl.earthcorps import EarthCorpsExtractor


class TestEarthCorpsExtractor:
    """Test cases for EarthCorps extractor using August 2025 fixture data."""

    def test_extract_events_from_august_2025(self):
        """Test extraction from August 2025 calendar HTML."""
        # Load fixture data
        with open('tests/etl/data/earthcorps_calendar_2025_08.html', 'r') as f:
            html_content = f.read()

        # Create extractor and extract events
        extractor = EarthCorpsExtractor(html_content)
        events = extractor.extract()

        # Should have found 4 events for August 2025
        assert len(events) == 4

        # Test first event (Aug 2) - Seattle: Kubota Garden
        first_event = events[0]
        assert first_event.source == "EC"
        assert first_event.source_id == "a0EUh000002w9hhMAA"
        assert first_event.title == "Seattle: Kubota Garden"
        assert first_event.venue == "Seattle"
        assert str(
            first_event.url) == "https://www.earthcorps.org/volunteer/event/a0EUh000002w9hhMAA"

        # Check datetime (should be 10am Pacific converted to UTC)
        assert first_event.start.year == 2025
        assert first_event.start.month == 8
        assert first_event.start.day == 2
        # 10am Pacific is 17:00 UTC (during PDT)
        assert first_event.start.hour == 17
        assert first_event.start.minute == 0

        # Should be 3 hour duration
        duration = first_event.end - first_event.start
        assert duration.total_seconds() == 3 * 3600  # 3 hours

        # Test second event (Aug 9) - Lake Washington Blvd
        second_event = events[1]
        assert second_event.source == "EC"
        assert second_event.source_id == "a0EUh000002vH2zMAE"
        assert second_event.title == "Lake Washington Blvd: South Seattle"
        assert second_event.venue == "Lake Washington Blvd"

        # Test third event (Aug 16) - Tukwila
        third_event = events[2]
        assert third_event.source == "EC"
        assert third_event.source_id == "a0EUh000002wmHdMAI"
        assert third_event.title == "Tukwila Community Center: Duwamish River"
        assert third_event.venue == "Tukwila Community Center"

        # Test fourth event (Aug 23) - Union Slough Everett
        fourth_event = events[3]
        assert fourth_event.source == "EC"
        assert fourth_event.source_id == "a0EUh000003uGjVMAU"
        assert fourth_event.title == "Union Slough Everett"
        assert fourth_event.venue == "Union Slough Everett"

    def test_extract_year_month_from_navigation(self):
        """Test extracting year and month from navigation links."""
        with open('tests/etl/data/earthcorps_calendar_2025_08.html', 'r') as f:
            html_content = f.read()

        from bs4 import BeautifulSoup
        soup = BeautifulSoup(html_content, 'html.parser')

        extractor = EarthCorpsExtractor(html_content)
        year, month = extractor._extract_year_month(soup)

        # Should detect August 2025
        assert year == 2025
        assert month == 8

    def test_extract_venue_logic(self):
        """Test venue extraction logic with different event title patterns."""
        extractor = EarthCorpsExtractor("")

        # Test title with colon separator
        event_data1 = {"Name": "Seattle: Kubota Garden"}
        venue1 = extractor._extract_venue(event_data1)
        assert venue1 == "Seattle"

        # Test title with location prefix
        event_data2 = {"Name": "Lake Washington Blvd: South Seattle"}
        venue2 = extractor._extract_venue(event_data2)
        assert venue2 == "Lake Washington Blvd"

        # Test title without colon, should use full title
        event_data3 = {"Name": "Union Slough Everett"}
        venue3 = extractor._extract_venue(event_data3)
        assert venue3 == "Union Slough Everett"

        # Test with region fallback - use empty title to trigger fallback
        event_data4 = {"Name": "", "Region": "North Sound",
                       "SubRegion": "Everett"}
        venue4 = extractor._extract_venue(event_data4)
        assert venue4 == "Everett, North Sound"

    def test_events_have_required_fields(self):
        """Test that all extracted events have required fields."""
        with open('tests/etl/data/earthcorps_calendar_2025_08.html', 'r') as f:
            html_content = f.read()

        extractor = EarthCorpsExtractor(html_content)
        events = extractor.extract()

        for event in events:
            # Required fields
            assert event.source == "EC"
            assert event.source_id
            assert event.title
            assert event.start
            assert event.end
            assert event.url
            assert event.venue

            # Time fields should be timezone-aware UTC
            assert event.start.tzinfo is not None
            assert event.end.tzinfo is not None

            # Start should be before end
            assert event.start < event.end

            # Should be free events
            assert event.cost is None

            # Should not have same_as initially
            assert event.same_as is None
