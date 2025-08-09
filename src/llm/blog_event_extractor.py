from datetime import date
from typing import List, Optional
from pydantic import BaseModel, ConfigDict

from .llm import get_client


_SYSTEM_PROMPT = """
You are an expert at extracting volunteer event information from blog articles. 

Your task is to analyze article content and extract any outdoor volunteer events (like park cleanups, habitat restoration, community gardening, etc.).

Return your response as a JSON object with these fields:
- events: a list of extracted events (possibly none!), each with the following fields:
    - title: extracted event title
    - event_date: extracted event date
    - start_time: extracted event start time (if available)
    - end_time: extracted event end time (if available)
    - venue: location/venue name (if available) 
    - description: brief description of the volunteer activity (if available)
    - contact_info: any contact information mentioned (if available)

Focus only on actual volunteer events with specific dates. Ignore general announcements or past events.
""".strip()


class ExtractedEvent(BaseModel):
    """Event data extracted from article content via LLM."""
    model_config = ConfigDict(from_attributes=True)

    title: str
    event_date: date
    start_time: Optional[str] = None  # Raw start time string from article
    end_time: Optional[str] = None  # Raw end time string from article

    venue: Optional[str] = None
    description: Optional[str] = None
    contact_info: Optional[str] = None


class ExtractedEventList(BaseModel):
    """List of extracted events from LLM response."""
    model_config = ConfigDict(from_attributes=True)

    events: List[ExtractedEvent]


def build_user_context(title: str, body: str) -> str:
    return f"Title: {title}\nBody: {body}\n"


def extract_articles(title: str, body: str) -> List[ExtractedEvent]:
    """Extract events from article content using LLM."""

    client = get_client()
    user_context = build_user_context(title, body)

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
