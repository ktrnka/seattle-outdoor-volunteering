import re
import xml.etree.ElementTree as ET
import html
from datetime import datetime, timezone
from typing import List, Optional
import requests
from pydantic import BaseModel, ConfigDict, HttpUrl

from .base import BaseListExtractor
from .url_utils import normalize_url
from ..models import Event, SEATTLE_TZ
from ..llm.llm import get_client

RSS_URL = "https://fremontneighbor.com/feed/"


class FremontArticle(BaseModel):
    """Structured data extracted from Fremont Neighbor RSS article."""
    model_config = ConfigDict(from_attributes=True)

    title: str
    link: str
    pub_date: datetime
    categories: List[str]
    description: str
    content: str
    guid: str


class ExtractedEvent(BaseModel):
    """Event data extracted from article content via LLM."""
    model_config = ConfigDict(from_attributes=True)

    is_event: bool
    title: Optional[str] = None
    date_time: Optional[str] = None  # Raw date/time string from article
    venue: Optional[str] = None
    description: Optional[str] = None
    contact_info: Optional[str] = None


class ExtractedEventList(BaseModel):
    """List of extracted events from LLM response."""
    model_config = ConfigDict(from_attributes=True)

    events: List[ExtractedEvent]


class FremontNeighborExtractor(BaseListExtractor):
    """Fremont Neighbor blog extractor for RSS feed with LLM event classification."""
    source = "FRE"

    @classmethod
    def fetch(cls):
        """Fetch raw RSS content from Fremont Neighbor blog."""
        response = requests.get(RSS_URL, timeout=30)
        return cls(response.text)

    def extract(self) -> List[Event]:
        """Extract events from Fremont Neighbor RSS feed using LLM classification."""
        try:
            root = ET.fromstring(self.raw_data)
        except ET.ParseError:
            # Return empty list for malformed XML
            return []

        events = []

        # Find all item elements in the RSS feed
        for item in root.findall(".//item"):
            article = self._parse_rss_item(item)
            if article:
                # Use LLM to extract events from article content
                extracted_events = self._extract_events_with_llm(article)
                events.extend(extracted_events)

        return events

    def _normalize_text(self, text: Optional[str]) -> str:
        """Normalize text by unescaping HTML entities and stripping whitespace."""
        if text is None:
            return ""
        return html.unescape(text.strip())

    def _parse_rss_item(self, item: ET.Element) -> Optional[FremontArticle]:
        """Parse a single RSS item into a FremontArticle."""
        try:
            title_elem = item.find("title")
            link_elem = item.find("link")
            pub_date_elem = item.find("pubDate")
            description_elem = item.find("description")
            content_elem = item.find(
                "{http://purl.org/rss/1.0/modules/content/}encoded")
            guid_elem = item.find("guid")

            if (title_elem is None or link_elem is None or pub_date_elem is None or
                title_elem.text is None or link_elem.text is None or
                    pub_date_elem.text is None):
                return None

            # Parse categories
            categories = []
            for category_elem in item.findall("category"):
                if category_elem.text:
                    categories.append(category_elem.text.strip())

            # Parse publication date - safe to access text now
            pub_date_str = pub_date_elem.text
            # Format: "Fri, 08 Aug 2025 13:02:00 +0000"
            pub_date = datetime.strptime(
                pub_date_str, "%a, %d %b %Y %H:%M:%S %z")

            return FremontArticle(
                title=self._normalize_text(title_elem.text),
                link=normalize_url(link_elem.text.strip()),
                pub_date=pub_date,
                categories=categories,
                description=self._normalize_text(
                    description_elem.text if description_elem is not None else None),
                content=self._normalize_text(
                    content_elem.text if content_elem is not None else None),
                guid=guid_elem.text.strip() if guid_elem is not None and guid_elem.text else "",
            )
        except Exception:
            # Skip malformed items
            return None

    def _extract_events_with_llm(self, article: FremontArticle) -> List[Event]:
        """Use LLM to extract volunteer events from article content."""
        # Check if this article is likely to contain volunteer events
        volunteer_categories = ["volunteering", "parks", "community"]
        has_volunteer_category = any(
            cat.lower() in volunteer_categories for cat in article.categories)

        if not has_volunteer_category:
            return []

        # Prepare content for LLM analysis
        content_for_analysis = self._prepare_content_for_llm(article)

        try:
            client = get_client()

            prompt = self._build_extraction_prompt(content_for_analysis)

            response = client.chat.completions.parse(
                model="openai/gpt-4.1",
                messages=[
                    {"role": "system", "content": self._get_system_prompt()},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.1,
                max_tokens=1000,
                response_format=ExtractedEventList
            )

            # Parse LLM response
            response_content = response.choices[0].message.content
            if response_content is None:
                return []

            extracted_data = self._parse_llm_response(response_content)

            if extracted_data and extracted_data.is_event:
                # Convert to Event object
                event = self._create_event_from_extraction(
                    article, extracted_data)
                if event:
                    return [event]

        except Exception as e:
            # Log error but don't fail the entire extraction
            print(f"LLM extraction failed for article {article.title}: {e}")

        return []

    def _prepare_content_for_llm(self, article: FremontArticle) -> str:
        """Clean and prepare article content for LLM analysis."""
        # Remove HTML tags from content
        content = re.sub(r'<[^>]+>', ' ', article.content)
        # Clean up whitespace
        content = re.sub(r'\s+', ' ', content).strip()

        # Truncate if too long (keep first 2000 chars)
        if len(content) > 2000:
            content = content[:2000] + "..."

        return content

    def _get_system_prompt(self) -> str:
        """Get the system prompt for LLM event extraction."""
        return """You are an expert at extracting volunteer event information from blog articles. 

Your task is to analyze article content and extract any outdoor volunteer events (like park cleanups, habitat restoration, community gardening, etc.).

Return your response as a JSON object with these fields:
- is_event: boolean indicating if the article contains a volunteer event
- title: extracted event title (if is_event is true)
- date_time: extracted date/time information as a string (if available)
- venue: location/venue name (if available) 
- description: brief description of the volunteer activity (if available)
- contact_info: any contact information mentioned (if available)

Focus only on actual volunteer events with specific dates. Ignore general announcements or past events."""

    def _build_extraction_prompt(self, content: str) -> str:
        """Build the extraction prompt for the LLM."""
        return f"""Please analyze this blog article content and extract any outdoor volunteer events:

CONTENT:
{content}

Extract volunteer event information and return as JSON."""

    def _parse_llm_response(self, response_text: str) -> Optional[ExtractedEvent]:
        """Parse the LLM response into an ExtractedEvent object."""
        try:
            import json
            # Try to extract JSON from response
            json_match = re.search(r'\{.*\}', response_text, re.DOTALL)
            if json_match:
                json_str = json_match.group()
                data = json.loads(json_str)
                return ExtractedEvent(**data)
        except Exception:
            pass
        return None

    def _create_event_from_extraction(self, article: FremontArticle, extracted: ExtractedEvent) -> Optional[Event]:
        """Create an Event object from the article and extracted data."""
        if not extracted.title:
            return None

        try:
            # Use article publication date as fallback
            event_datetime = article.pub_date

            # Try to parse extracted date if available
            if extracted.date_time:
                # This is a simplified date parser - could be enhanced
                try:
                    # Try common date formats
                    for fmt in ["%B %d", "%B %d, %Y", "%m/%d/%Y", "%Y-%m-%d"]:
                        try:
                            parsed_date = datetime.strptime(
                                extracted.date_time, fmt)
                            # If no year, use current year
                            if parsed_date.year == 1900:
                                parsed_date = parsed_date.replace(
                                    year=datetime.now().year)
                            # Convert to Seattle timezone
                            event_datetime = parsed_date.replace(
                                tzinfo=SEATTLE_TZ)
                            break
                        except ValueError:
                            continue
                except Exception:
                    # Fallback to article date
                    pass

            # Convert to UTC for storage
            event_datetime_utc = event_datetime.astimezone(timezone.utc)

            # Generate source_id from article GUID or URL
            source_id = article.guid if article.guid else article.link
            # Clean source_id to make it more stable
            source_id = re.sub(r'[^\w\-]', '_', source_id)

            return Event(
                source=self.source,
                source_id=source_id,
                title=extracted.title.strip(),
                venue=extracted.venue.strip() if extracted.venue else "Fremont",
                start=event_datetime_utc,
                # Use same time for start/end since blog posts don't specify duration
                end=event_datetime_utc,
                url=HttpUrl(article.link),
            )
        except Exception:
            # Return None for events that can't be properly parsed
            return None
