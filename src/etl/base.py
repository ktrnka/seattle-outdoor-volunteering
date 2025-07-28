import abc
from typing import List
from . import utils  # helper with caching, db write, etc.
from ..models import Event


class BaseExtractor(abc.ABC):
    def __init__(self, session):
        self.session = session

    @abc.abstractmethod
    def fetch(self) -> List[Event]:
        ...

    def run(self):
        events = self.fetch()
        utils.upsert_events(events)
        return len(events)
