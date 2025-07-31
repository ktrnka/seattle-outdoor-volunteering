from pprint import pprint
from typing import List, Literal

from pydantic import BaseModel

from .models import CanonicalEvent, Event

import os
from openai import OpenAI


def get_client() -> OpenAI:
    """Cascade for the token. Prefer the """
    token = os.environ["GITHUB_TOKEN"]
    endpoint = "https://models.github.ai/inference"

    client = OpenAI(
        base_url=endpoint,
        api_key=token,
    )
    return client


_SYSTEM = """
You're an expert at canonicalizing events from multiple data sources, with some expertise in user experience and copy editing for clarity.

You will be given an existing canonical event along with source events, and asked to create a new canonical event that's more readable and user-friendly.

Response format:
- Return a JSON object with the following fields:
    - analysis_trace: A short list of thoughts analysing the existing canonical event and source events
    - planning_trace: A short list of thoughts on your goals for the new canonical event
    - title: The canonical title of the event
    - venue: The canonical venue name (e.g., park, neighborhood)
    - category: The type of event, one of 'landscaping', 'litter', 'concert', or 'other'
    - description: A short description of the event (optional, but recommended)

Tips:
- The event should answer the basic questions:
    - What will we be doing? This helps people decide if it's a phyisical activity that they're capable of going.
    - Where is the event? The venue should be a very short, recognizable location name. We don't need the full address in venue because we have a separate venue field.
    - When is the event? The existing data structure can be used for this, no need to add new fields.
    - Why are we doing it? This helps people understand the purpose and motivation. This should be in the description if possible.
- The venue should be the name of the park, neighborhood, or general area.
- The title and venue should provide enough information for the user to decide if they want to attend. (E.g., making it clear what type of event it is, where it is, if possible the group)
- The title should be short and descriptive, free of catchy phrases or marketing language.
- The title should not include the location if the venue already has it
""".strip()


def fill_prompt_messages(event: CanonicalEvent, source_events: List[Event]) -> List[dict]:
    """Fill the prompt template with event data."""
    return [
        {
            "role": "system",
            "content": _SYSTEM,
        },
        {
            "role": "user",
            "content": f"Canonical Event:\n{event.title}\n\nSource Events:\n" + "\n".join(f"- {se.title}" for se in source_events),
        }
    ]


class RevisedCanonicalEvent(BaseModel):
    analysis_trace: list[str]
    planning_trace: list[str]

    title: str
    venue: str
    category: Literal['landscaping', 'litter', 'concert', 'other']
    description: str | None = None


def run_llm_canonicalization(event: CanonicalEvent, source_events: List[Event]):
    print("Running LLM-based canonicalization for event:")
    pprint(event)

    print("\nSource events:")
    for source_event in source_events:
        pprint(source_event)

    # Step 2: LLM prep work
    client = get_client()
    messages = fill_prompt_messages(event, source_events)

    # Step 3: LLM call
    response = client.chat.completions.parse(
        messages=messages,
        temperature=0.2,
        model="openai/gpt-4.1",
        response_format=RevisedCanonicalEvent,
    )

    # Step 4: Pretty print to the console

    # The full response
    print("\nLLM Response:")
    pprint(response)

    # The specific content from the response
    content = response.choices[0].message.parsed
    assert content, "LLM response content is empty"
    print("\nLLM Content:")

    # Nicely formatted
    print("Analysis Trace:")
    for line in content.analysis_trace:
        print(f"- {line}")
    print("\nPlanning Trace:")
    for line in content.planning_trace:
        print(f"- {line}")

    print("\nTitle:", content.title,
          f"(previously: {event.title if event.title != content.title else 'SAME'})")
    print("Venue:", content.venue,
          f"(previously: {event.venue if event.venue != content.venue else 'SAME'})")
    print("Category:", content.category)
    print("Description:", content.description)
