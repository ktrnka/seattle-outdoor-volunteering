from typing import List

from .base import BaseExtractor
from ..models import Event


class SPRExtractor(BaseExtractor):
    """Seattle Parks & Recreation extractor - placeholder implementation."""
    source = "SPR"

    def fetch(self) -> List[Event]:
        """Placeholder - returns empty list for now."""
        return []
