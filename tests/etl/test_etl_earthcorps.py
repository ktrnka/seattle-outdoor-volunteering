from src.etl.earthcorps import EarthCorpsCalendarExtractor


class MockResponse:
    """Mock response object for testing content validation."""

    def __init__(self, text: str, url: str = "https://example.com/test"):
        self.text = text
        self.url = url


class TestEarthCorpsExtractor:
    """Test cases for EarthCorps extractor using August 2025 fixture data."""

    def test_extract_events_from_august_2025(self):
        """Test extraction from August 2025 calendar HTML."""
        # Load fixture data
        with open('tests/etl/data/earthcorps_calendar_2025_08.html', 'r') as f:
            html_content = f.read()

        # Create extractor and extract events
        extractor = EarthCorpsCalendarExtractor(html_content)
        events = extractor.extract()

        # Should have found 4 events for August 2025
        assert len(events) == 4

        # Test first event (Aug 2) - Seattle: Kubota Garden
        first_event = events[0]
        assert first_event.source == "EC"
        assert first_event.source_id == "a0EUh000002w9hhMAA"
        assert first_event.title == "Seattle: Kubota Garden"
        assert first_event.venue == "South Seattle"
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
        assert second_event.venue == "South Seattle"

        # Test third event (Aug 16) - Tukwila
        third_event = events[2]
        assert third_event.source == "EC"
        assert third_event.source_id == "a0EUh000002wmHdMAI"
        assert third_event.title == "Tukwila Community Center: Duwamish River"
        assert third_event.venue == "Duwamish"

        # Test fourth event (Aug 23) - Union Slough Everett
        fourth_event = events[3]
        assert fourth_event.source == "EC"
        assert fourth_event.source_id == "a0EUh000003uGjVMAU"
        assert fourth_event.title == "Union Slough Everett"
        assert fourth_event.venue == "Everett"

    def test_extract_year_month_from_navigation(self):
        """Test extracting year and month from navigation links."""
        with open('tests/etl/data/earthcorps_calendar_2025_08.html', 'r') as f:
            html_content = f.read()

        from bs4 import BeautifulSoup
        soup = BeautifulSoup(html_content, 'html.parser')

        extractor = EarthCorpsCalendarExtractor(html_content)
        year, month = extractor._extract_year_month(soup)

        # Should detect August 2025
        assert year == 2025
        assert month == 8

    def test_extract_venue_logic(self):
        """Test venue extraction logic with different event title patterns."""
        extractor = EarthCorpsCalendarExtractor("")

        # Default
        event_data1 = {"Name": "Seattle: Kubota Garden"}
        venue1 = extractor._extract_venue(event_data1)
        assert venue1 == "Unknown"

        # Happy path
        event_data4 = {"Name": "", "Region": "North Sound",
                       "SubRegion": "Everett"}
        venue4 = extractor._extract_venue(event_data4)
        assert venue4 == "Everett"

    def test_events_have_required_fields(self):
        """Test that all extracted events have required fields."""
        with open('tests/etl/data/earthcorps_calendar_2025_08.html', 'r') as f:
            html_content = f.read()

        extractor = EarthCorpsCalendarExtractor(html_content)
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

    def test_content_validation_with_real_calendar_page(self):
        """Test content validation against real EarthCorps calendar HTML."""
        # Load the real calendar fixture
        with open('tests/etl/data/earthcorps_calendar_2025_08.html', 'r') as f:
            html_content = f.read()

        # Create mock response
        mock_response = MockResponse(
            html_content, "https://www.earthcorps.org/volunteer/calendar/2025/8/")

        # Should not raise any exceptions
        try:
            EarthCorpsCalendarExtractor.raise_for_missing_content(
                mock_response)
        except Exception as e:
            assert False, f"Content validation failed on real calendar page: {e}"

    def test_content_validation_detects_cloudflare(self):
        """Test that content validation detects CloudFlare protection."""
        cloudflare_html = """
        <html>
        <head><title>Just a moment...</title></head>
        <body>
        <h1>Please wait while we verify you're a human</h1>
        <p>This process is automatic. Your browser will redirect you to your requested content shortly.</p>
        <script>window.cloudflare = true;</script>
        </body>
        </html>
        """

        mock_response = MockResponse(
            cloudflare_html, "https://www.earthcorps.org/volunteer/calendar/2025/8/")

        try:
            EarthCorpsCalendarExtractor.raise_for_missing_content(
                mock_response)
            assert False, "Should have detected CloudFlare protection"
        except Exception as e:
            assert "Cloudflare protection detected" in str(e)

    def test_content_validation_detects_missing_content(self):
        """Test that content validation detects missing expected content."""
        invalid_html = """
        <html>
        <head><title>EarthCorps Calendar</title></head>
        <body>
        <h1>This is a calendar page</h1>
        <p>But it doesn't have the events_by_date JavaScript variable</p>
        </body>
        </html>
        """

        mock_response = MockResponse(
            invalid_html, "https://www.earthcorps.org/volunteer/calendar/2025/8/")

        try:
            EarthCorpsCalendarExtractor.raise_for_missing_content(
                mock_response)
            assert False, "Should have detected missing events data"
        except Exception as e:
            assert "missing events data" in str(e)
