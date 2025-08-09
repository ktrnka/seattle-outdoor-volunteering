import html
import xml.etree.ElementTree as ET
from datetime import date, datetime, timedelta, timezone
from typing import List, Optional

import requests
from pydantic import BaseModel, ConfigDict

from src.llm.blog_event_extractor import extract_articles

from ..models import Event
from .base import BaseListExtractor
from .url_utils import normalize_url
import re

RSS_URL = "https://fremontneighbor.com/feed/"


class FremontArticle(BaseModel):
    """Fremont Neighbor RSS article."""
    model_config = ConfigDict(from_attributes=True)

    title: str
    link: str
    pub_date: datetime
    categories: List[str]
    description: str
    content: str
    guid: str

def strip_html_tags(text: str) -> str:
    """Remove HTML tags from a string using regex."""
    if not text:
        return ""
    return re.sub(r"<[^>]+>", "", text)

class FremontNeighborExtractor(BaseListExtractor):
    """Fremont Neighbor blog extractor for RSS feed with LLM event classification."""
    source = "FRE"

    # Only run LLM extraction for articles within this window
    extraction_window = timedelta(days=2)

    @classmethod
    def fetch(cls):
        """Fetch raw RSS content from Fremont Neighbor blog."""
        headers = {
            'User-Agent': 'Mozilla/5.0 (compatible; SeattleVolunteerBot/1.0)',
            'Accept': 'application/rss+xml, application/xml, text/xml, */*'
        }
        response = requests.get(RSS_URL, timeout=30, headers=headers)
        response.raise_for_status()
        return cls(response.text)

    def extract(self) -> List[Event]:
        """Extract events from Fremont Neighbor RSS feed using LLM classification."""
        try:
            # Raise on failure
            root = ET.fromstring(self.raw_data)
        except ET.ParseError as e:
            print(f"XML Parse Error: {e}")
            print(f"First 200 chars: {self.raw_data[:200]}")
            print(f"Last 200 chars: {self.raw_data[-200:]}")
            raise

        events = []

        # Find all item elements in the RSS feed
        for item in root.findall(".//item"):
            article = self._parse_rss_item(item)
            if article and article.pub_date >= datetime.now(timezone.utc) - self.extraction_window:
                # Use LLM to extract events from article content
                extracted_events = extract_articles(
                    title=article.title,
                    publication_date=str(article.pub_date),
                    body=strip_html_tags(article.content)
                )
                for extracted in extracted_events:
                    events.append(
                        extracted.to_common_event(
                            source=self.source,
                            source_id=generate_source_id(
                                article.guid, extracted.event_date),
                            url=article.link
                        )
                    )

        return events

    @staticmethod
    def _normalize_text(text: Optional[str]) -> str:
        """Normalize text by unescaping HTML entities and stripping whitespace."""
        if text is None:
            return ""
        return html.unescape(text.strip())

    @staticmethod
    def _parse_rss_item(item: ET.Element) -> Optional[FremontArticle]:
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
                title=FremontNeighborExtractor._normalize_text(
                    title_elem.text),
                link=normalize_url(link_elem.text.strip()),
                pub_date=pub_date,
                categories=categories,
                description=FremontNeighborExtractor._normalize_text(
                    description_elem.text if description_elem is not None else None),
                content=FremontNeighborExtractor._normalize_text(
                    content_elem.text if content_elem is not None else None),
                guid=guid_elem.text.strip() if guid_elem is not None and guid_elem.text else "",
            )
        except Exception:
            # Skip malformed items
            return None


def generate_source_id(article_guid: str, event_date: date) -> str:
    """Generate a unique source_id for an event based on article GUID and event date.

    Args:
        article_guid: The GUID from the RSS article (e.g., "https://fremontneighbor.com/?p=683")
        event_date: The date of the event

    Returns:
        A unique source_id string combining post ID and event date (e.g., "683_2025-08-10")
    """
    # Extract the post ID from the GUID URL
    # Format: https://fremontneighbor.com/?p=683 -> 683
    if "?p=" in article_guid:
        post_id = article_guid.split("?p=")[-1]
    else:
        # Fallback: use the last part of the URL or the whole GUID if no ?p= found
        post_id = article_guid.split("/")[-1] or article_guid

    return f"{post_id}_{event_date.isoformat()}"

    # def _create_event_from_extraction(self, article: FremontArticle, extracted: ExtractedEvent) -> Optional[Event]:
    #     """Create an Event object from the article and extracted data."""
    #     if not extracted.title:
    #         return None

    #     try:
    #         # Use article publication date as fallback
    #         event_datetime = article.pub_date

    #         # Try to parse extracted date if available
    #         if extracted.date_time:
    #             # This is a simplified date parser - could be enhanced
    #             try:
    #                 # Try common date formats
    #                 for fmt in ["%B %d", "%B %d, %Y", "%m/%d/%Y", "%Y-%m-%d"]:
    #                     try:
    #                         parsed_date = datetime.strptime(
    #                             extracted.date_time, fmt)
    #                         # If no year, use current year
    #                         if parsed_date.year == 1900:
    #                             parsed_date = parsed_date.replace(
    #                                 year=datetime.now().year)
    #                         # Convert to Seattle timezone
    #                         event_datetime = parsed_date.replace(
    #                             tzinfo=SEATTLE_TZ)
    #                         break
    #                     except ValueError:
    #                         continue
    #             except Exception:
    #                 # Fallback to article date
    #                 pass

    #         # Convert to UTC for storage
    #         event_datetime_utc = event_datetime.astimezone(timezone.utc)

    #         # Generate source_id from article GUID or URL
    #         source_id = article.guid if article.guid else article.link
    #         # Clean source_id to make it more stable
    #         source_id = re.sub(r'[^\w\-]', '_', source_id)

    #         return Event(
    #             source=self.source,
    #             source_id=source_id,
    #             title=extracted.title.strip(),
    #             venue=extracted.venue.strip() if extracted.venue else "Fremont",
    #             start=event_datetime_utc,
    #             # Use same time for start/end since blog posts don't specify duration
    #             end=event_datetime_utc,
    #             url=HttpUrl(article.link),
    #         )
    #     except Exception:
    #         # Return None for events that can't be properly parsed
    #         return None
