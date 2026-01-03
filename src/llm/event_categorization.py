"""LLM-based event categorization."""

from ..models import Event, LLMEventCategorization
from .llm import get_client


def build_categorization_context(event: Event) -> str:
    context_parts = [f"Title: {event.title}"]

    if event.venue:
        context_parts.append(f"Venue: {event.venue}")

    context_parts.append(f"URL: {event.url}")

    return "\n".join(context_parts)


def categorize_event(event: Event) -> LLMEventCategorization:
    client = get_client()

    context = build_categorization_context(event)

    system_prompt = """You are an expert at categorizing outdoor volunteer events and community activities in Seattle.

Your task is to categorize events into one of these categories:
- volunteer/parks: Volunteer activities related to parks, forests, trails, restoration, gardening, or environmental work
- volunteer/litter: Volunteer activities focused on litter cleanup, trash removal, or general cleanup
- social_event: Social gatherings, community meetings, educational events, or networking activities
- concert: Musical performances, concerts, or entertainment events
- other: Events that don't clearly fit the above categories

Return your response as JSON with this structure:
{
  "category": "volunteer/parks",
  "reasoning": "Brief explanation of why you chose this category"
}

Focus on the primary purpose of the event. If an event combines multiple activities, choose the category that best represents the main activity."""

    user_prompt = f"""Please categorize this event:

{context}"""

    response = client.chat.completions.parse(
        model="openai/gpt-4.1-mini",
        messages=[{"role": "system", "content": system_prompt}, {"role": "user", "content": user_prompt}],
        response_format=LLMEventCategorization,
        temperature=0.1,  # Low temperature for consistent categorization
    )

    response_content = response.choices[0].message.parsed
    if not response_content:
        raise ValueError("Empty response from LLM")

    return response_content
