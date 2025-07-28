import abc
from typing import List
from ..models import Event


class BaseExtractor(abc.ABC):
    def __init__(self, raw_data: str):
        self.raw_data = raw_data

    @classmethod
    @abc.abstractmethod
    def fetch(cls) -> 'BaseExtractor':
        """Fetch raw data and return an instance of the extractor."""
        ...

    @abc.abstractmethod
    def extract(self) -> List[Event]:
        """Extract events from the raw data."""
        ...
