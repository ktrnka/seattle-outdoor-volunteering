from datetime import date, datetime
from typing import List, Optional
from pydantic import BaseModel, ConfigDict

from .llm import get_client


_SYSTEM_PROMPT = """
You are an expert at extracting volunteer event information from blog articles. 

Your task is to analyze article content and extract any outdoor volunteer events (like park cleanups, habitat restoration, community gardening, etc.).

Return your response as a JSON object with these fields:
- events: a list of extracted events (possibly none!), each with the following fields:
    - title: extracted event title
    - event_date: extracted event date (YYYY-MM-DD format)
    - start_datetime: full start datetime in ISO 8601 format (YYYY-MM-DDTHH:MM:SS) - combine event_date with start time
    - end_datetime: full end datetime in ISO 8601 format (YYYY-MM-DDTHH:MM:SS) - combine event_date with end time
    - start_time: extracted event start time (if available, for reference)
    - end_time: extracted event end time (if available, for reference)
    - venue: location/venue name (if available) 
    - description: brief description of the volunteer activity (if available)
    - contact_info: any contact information mentioned (if available)

Focus only on actual volunteer events with specific dates. Ignore general announcements or past events.
For datetimes, assume Pacific Time zone if not specified. Use ISO 8601 format: YYYY-MM-DDTHH:MM:SS
""".strip()


class ExtractedEvent(BaseModel):
    """Event data extracted from article content via LLM."""
    model_config = ConfigDict(from_attributes=True)

    title: str
    event_date: date
    start_datetime: Optional[datetime] = None  # Full datetime in ISO format
    end_datetime: Optional[datetime] = None    # Full datetime in ISO format
    start_time: Optional[str] = None  # Raw start time string from article
    end_time: Optional[str] = None  # Raw end time string from article

    venue: Optional[str] = None
    description: Optional[str] = None
    contact_info: Optional[str] = None


class ExtractedEventList(BaseModel):
    """List of extracted events from LLM response."""
    model_config = ConfigDict(from_attributes=True)

    events: List[ExtractedEvent]


def build_user_context(title: str, publication_date: str, body: str) -> str:
    return f"Title: {title}\nPublication Date: {publication_date}\nBody: {body}\n"


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


def extract_articles(title: str, publication_date: str, body: str) -> List[ExtractedEvent]:
    """Extract events from article content using LLM."""

    client = get_client()
    user_context = build_user_context(title, publication_date, body)

    response = client.chat.completions.parse(
        messages=[
            {"role": "system", "content": _SYSTEM_PROMPT},
            {"role": "user", "content": user_context}
        ],
        temperature=0.2,
        model="openai/gpt-4.1",
        response_format=ExtractedEventList
    )

    event_list = response.choices[0].message.parsed
    return event_list.events if event_list else []
