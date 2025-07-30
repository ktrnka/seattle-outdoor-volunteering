# TODO

## Short-Term

### Data Quality
- Title and Venue: It looks like SPF adds the " at Venue" into the title which is messing with merging
- Add times to the manual data sources

## Long-Term

### New Data Sources
- King County stuff
  - https://parksvolunteer.kingcounty.gov/ or https://kingcounty.gov/en/dept/dnrp/nature-recreation/parks-recreation/king-county-parks/get-involved/volunteer
  - https://kcls.org/faq/volunteer/
- WTA Work Parties: https://www.wta.org/volunteer/schedule/
- **Neighborhood blogs**: RSS/blog parsing with LLM classification
  - Ballard, Fremont, Wallyhood blogs
  - Eastlake Community Council: They don't seem to have an RSS feed so it may be a little different
- **Facebook Groups**:
  - Sparkling Wallingford: https://www.facebook.com/groups/1192189949067573/events
  - We Heart Seattle cleanups: https://www.facebook.com/groups/weheartseattle/events
- **RSS/Blog support**: Once LLM integration is ready, add support for RSS feeds and event calendars
- **Incremental crawling of detail pages**: Fill in missing details for events, enriching the data, and unlocking a click-to-expand feature for more information
- Other
  - https://www.seattlegreenways.org/get-involved/upcoming-events/

### Data Enhancement
- **Standardize titles**: Use LLM to create more informative titles like "Park restoration at Woodland Park with Greg"
- **Event categorization**: Support both parks work and litter cleanup work with user filtering options
  - **SPF events**: There are some SPF events that are not volunteer-related, so we may want to filter these out or have a separate category
  - **Future differentiation**: Distinguish cleanup events from parks/habitat restoration events for user filtering
- **Enhanced details**: Standardize event information to cover who/what/why/how/when (explaining "why" invasive species removal matters will be challenging)
- **Advanced deduplication**: Splink for more nuanced entity merging

### User Experience
- **Geographic filtering**: Allow users to filter to nearby events (consider cookie storage vs a simple neighborhood selector)
- **Site freshness**: Improve the last-updated display to be more like SODA data quality testing
- **Mobile optimization**: Ensure responsive design works well on all devices

### Infrastructure & Quality
- **Crawling frequency optimization**: Different sources may need different update frequencies
  - **SPU**: Less frequent (weekly/monthly) - fewer events, more stable schedule
  - **GSP/SPR/SPF**: More frequent (daily) - more events, dynamic scheduling
- **UI testing**: Automated UI preview testing to ensure site looks good on all devices
- **LLM integration**: Foundation for classification, title standardization, and content enhancement

