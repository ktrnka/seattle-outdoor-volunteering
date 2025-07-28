from typing import List

from .base import BaseExtractor
from ..models import Event


class SPFExtractor(BaseExtractor):
    """Seattle Parks Foundation extractor - placeholder implementation."""
    source = "SPF"

    def fetch(self) -> List[Event]:
        """Placeholder - returns empty list for now."""
        return []
