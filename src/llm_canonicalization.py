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

# Response format:
- Return a JSON object with the following fields:
    - analysis_trace: A short list of thoughts analysing the existing canonical event and source events
    - planning_trace: A short list of thoughts on your goals for the new canonical event
    - title: The canonical title of the event
    - venue: The canonical venue name (e.g., park, neighborhood)
    - category: The type of event, one of 'landscaping', 'litter', 'concert', or 'other'
    - description: A short description of the event (optional, but recommended)

# High level strategy:
- The event should answer the basic questions:
    - What will we be doing?
    - Who is organizing or sponsoring it?
    - Where is the event?
    - When is the event?
    - Why are we doing it?
- We want the fields to be short and distinct from one another.
- Keep the changes relatively small and be careful not to imagine new details that aren't in the source events.

# Tactical tips:
- The venue should be the name of the park or neighborhood. If it's short, include the location within the park or neighborhood.
- The title should be short and descriptive, free of catchy phrases or marketing language.
- If we have "why" information, include it in the description.

# Examples

1. Lizard Haven weeding and watering

Input info:
Title: Lizard Haven weeding and watering
Venue: Discovery Park
tags=['Green Seattle Partnership', 'Magnolia', 'Volunteer/Work Party']
other: Join us for a restoration work party at Discovery Park. Lots of effort has been put into the lizard haven site; watering it during the ... "neighborhoods": "Magnolia", "parks": null, "sponsoring_organization": "Green Seattle Partnership", "contact": "Rob Stevens", "contact_phone": null, "contact_email": "dlibfrom@yahoo.com", "audience": "All", "pre_register": "No", "cost": null

Analysis:
- Lizard Haven is a specific site within Discovery Park, but it may not be widely known
- Discovery Park is a large, well-known park so it doesn't need additional context
- It might be nice to include the contact info to answer "who"

Revised title: Weeding and Watering with Rob and Green Seattle Partnership
Revised venue: Lizard Haven, Discovery Park

2. Peppi's Watering and Weeding

Input info:
Title: Peppi's Watering and Weeding
Venue: Peppi's Playground
tags=['Green Seattle Partnership', 'Madrona/Leschi', 'Volunteer/Work Party']
other: Join us for a restoration work party at Peppi\\\\\'s Playground. We will begin preparing for the return of school and fall planting season by weeding "sponsoring_organization": "Green Seattle Partnership", "contact": "Jana Robbins", "contact_phone": null, "contact_email": "janambrobbins@gmail.com", "audience": "All", "pre_register": "No", "cost": null, "link"

Analysis:
- Peppi's Playground is a specific site that may not be widely known, consider including the neighborhood for context
- The title makes it sound like a person is hosting, but it's actually a park
- It might be nice to include the "why" information about preparing for school and fall planting in the description
- The contact info could answer "who"

Revised title: Weeding and Watering with Jana and Green Seattle Partnership
Revised venue: Peppi's Playground, Madrona/Leschi

3. Preparing for Fall Planting at Woodland Park
Input info:
Title: Preparing for Fall Planting
Venue: Woodland Park
tags=['Green Seattle Partnership', 'Greenwood/Phinney Ridge', 'Volunteer/Work Party']
other: Join us for a restoration work party at Woodland Park - See website for important details on what to bring and where to meet "neighborhoods": "Greenwood/Phinney Ridge", "parks": null, "sponsoring_organization": "Green Seattle Partnership", "contact": "Greg Netols", "contact_phone": "2243889145", "contact_email": "gregnetols@gmail.com", "audience": "All", "pre_register": "No", "cost": null, "link": "http://seattle.greencitypartnerships.org/event/42030/"

Analysis:
- Woodland Park is a well-known park, so no need to include the neighborhood. It'd be nice to include the specific site within the park but I don't see it
- I see contact info, that might answer "who"

Revised title: Fall Planting Preparation with Greg and Green Seattle Partnership
Revised venue: Woodland Park

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
