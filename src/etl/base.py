import abc
from typing import List

from ..models import Event


class BaseListExtractor(abc.ABC):
    """Base extractor for event list pages, calendar pages, etc"""

    source: str  # Source code for the event extractor

    def __init__(self, raw_data: str):
        self.raw_data = raw_data

    @classmethod
    @abc.abstractmethod
    def fetch(cls) -> "BaseListExtractor":
        """Fetch raw data and return an instance of the extractor."""
        ...

    @abc.abstractmethod
    def extract(self) -> List[Event]:
        """Extract events from the raw data."""
        ...


class BaseDetailExtractor(abc.ABC):
    """Base extractor for event detail pages"""

    source: str  # Source code for the event detail extractor

    def __init__(self, url: str, raw_data: str):
        self.url = url
        self.raw_data = raw_data

    @classmethod
    @abc.abstractmethod
    def fetch(cls, url: str) -> "BaseDetailExtractor":
        """Fetch raw data from the detail page URL."""
        ...

    @abc.abstractmethod
    def extract(self) -> Event:
        """Extract a single event from the detail page."""
        ...
