# Formats from SPR
# Event format: Sunday, August 3, 2025, 8&amp;nbsp;&amp;ndash;&amp;nbsp;11am
# Publication date: 03 Aug 2025 15:45:00 GMT

# Formats from SPU (no year)
# Saturday, August 9
# 10 am &ndash; 12 pm

# Formats from GSP
# July 28, 9am-12:30pm (no year)

from datetime import date, datetime, timedelta, tzinfo
from typing import Optional, Tuple


def parse_time(time_str: str) -> datetime:
    """Parse a time like '9am' or '12:30pm' into a datetime object."""

    norm_time_str = time_str.replace(
        '&nbsp;', ' ').replace('&ndash;', '-').replace(" ", "").strip()

    for time_format in ("%I:%M%p", "%I%p", "%I:%M", "%I"):
        try:
            return datetime.strptime(norm_time_str, time_format)
        except ValueError:
            continue
    raise ValueError(f"Could not parse time: {time_str}")


def parse_date(date_str: str, after: Optional[datetime] = None) -> date:
    """Parse a date string like Sunday, August 3, 2025, 'July 28' or 'Saturday, August 9' or 'Saturday, Nov 22' into a date object."""

    for date_format in ("%B %d", "%A, %B %d", "%A, %B %d, %Y", "%B %d, %Y", "%A, %b %d"):
        try:
            dt = datetime.strptime(date_str, date_format).date()

            if '%Y' not in date_format:
                # If no year is provided, use the current year
                dt = dt.replace(year=datetime.now().year)

                if after and dt < after.date():
                    # Try the same year first, then next year
                    dt = dt.replace(year=after.year)
                    if dt < after.date():
                        dt = dt.replace(year=after.year + 1)

            return dt
        except ValueError:
            continue

    raise ValueError(f"Could not parse date: {date_str}")


def parse_range(date_str: str, time_range_str: str, tz: tzinfo, after: Optional[datetime] = None) -> Tuple[datetime, datetime]:
    """Parse a date and time range like 'July 28, 9am-12:30pm' into start and end datetime objects."""

    partial_date = parse_date(date_str, after)

    # Replace HTML en-dash and Unicode en-dash with regular dash
    time_range_str = time_range_str.replace(
        '&ndash;', '-').replace('\u2013', '-')
    start_str, end_str = time_range_str.split('-')

    partial_start_time = parse_time(start_str.strip())
    partial_end_time = parse_time(end_str.strip())

    # If the start time doesn't have an ampm marker, pick whatever leads to the shorter duration
    if not start_str.strip().lower().endswith(('am', 'pm')):
        if partial_start_time + timedelta(hours=12) < partial_end_time:
            partial_start_time += timedelta(hours=12)

    start_dt = datetime.combine(partial_date, partial_start_time.time())
    end_dt = datetime.combine(partial_date, partial_end_time.time())

    # Apply the timezone
    start_dt = start_dt.replace(tzinfo=tz)
    end_dt = end_dt.replace(tzinfo=tz)

    return start_dt, end_dt


def parse_range_single_string(event_datetime_str: str, tz: tzinfo, after: Optional[datetime] = None) -> Tuple[datetime, datetime]:
    date_str, time_range_str = event_datetime_str.rsplit(', ', 1)

    return parse_range(date_str, time_range_str, tz, after)


# def parse_gsp_range(event_datetime_str: str, after: Optional[datetime] = None) -> Tuple[datetime, datetime]:
#     """
#     Parse a date like July 28, 9am-12:30pm
#     """

#     date_str, time_range_str = event_datetime_str.split(', ')

#     start_str, end_str = time_range_str.split('-')

#     partial_date = datetime.strptime(
#         date_str + " " + str(date.today().year), "%B %d %Y").date()
#     partial_start_time = parse_time(start_str.strip())
#     partial_end_time = parse_time(end_str.strip())

#     if after and partial_date < after.date():
#         # If the date is before the 'after' date, adjust to next year
#         partial_date = partial_date.replace(year=after.year + 1)

#     start_dt = datetime.combine(partial_date, partial_start_time.time())
#     end_dt = datetime.combine(partial_date, partial_end_time.time())

#     return start_dt, end_dt
