import re, requests
from bs4 import BeautifulSoup
from dateutil import parser
from .base import BaseExtractor
from ..models import Event

CAL_URL = "https://seattle.greencitypartnerships.org/event/calendar/"

class GSPExtractor(BaseExtractor):
    source = "GSP"

    def fetch(self):
        html = requests.get(CAL_URL, timeout=30).text
        soup = BeautifulSoup(html, "html.parser")
        rows  = soup.select("table tr")
        events = []
        for row in rows:
            cells = row.find_all("td")
            if len(cells) < 1:
                continue
            # first cell looks like “07/31/2025 Partner in Employment's … more”
            raw  = cells[0].get_text(" ", strip=True)
            m = re.match(r"(\d{2})/(\d{2})/(\d{4})\s+(.+?)\s+more", raw)
            if not m:
                continue
            month, day, year, title = m.groups()
            start = parser.parse(f"{year}-{month}-{day} 09:00:00")  # default 9 AM
            evt = Event(
                source=self.source,
                source_id=title.lower().replace(" ", "-")[:64],
                title=title,
                start=start,
                end=start.replace(hour=12),
                url=CAL_URL,
            )
            events.append(evt)
        return events
