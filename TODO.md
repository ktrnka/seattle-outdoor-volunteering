# TODO

## Short-Term

### Completed
- ✅ **SPF detail page enrichment**: Implemented incremental crawling of SPF detail pages to extract GSP URLs, enabling improved deduplication (2 pages enriched per day during ETL)
- ✅ **Pipeline health tracking**: Added ETL run tracking for detail pages and LLM categorization as pseudo-sources (SPF_DETAILS, LLM_CATEGORIZATION) for frontend observability

### Observability
- **Commit SHA tracking**: Record git commit SHA in ETL runs to correlate failures with code changes vs data source changes

### 8/8
- There are a number of dedupe issues remaining in cases where the duplicate events have different titles

### UI
- Remove scrollbar memory on the modal (I tried, but it led to a quick flash-change on opening)

### Code Quality
- Refactor most pipelines to look like extracting CustomSourceEvent (with mostly strings) then generate SourceEvent with the standard fields
- Refactor most of the date extraction code: GSP, SPR, SPU could share some code
- In general, catch errors higher up the call stack to simplify the code and log errors more effectively
- Trim down much of the data wrangling at the extraction layer. Simplify it!

### Data Quality
- Title and Venue: It looks like SPF adds the " at Venue" into the title which is messing with merging
- Add a data_url to tag where the data came from (important to distinguish between data coming from calendars vs data coming from detail pages)
- Add a description field into more of the events
- Make venue optional. Don't set defaults if it's not in the source
- Add the description field to the shared SourceEvent model

## Long-Term

- Generalize the blog support from Fremont to support other blogs

### New Data Sources
- King County
  - https://parksvolunteer.kingcounty.gov/ or https://kingcounty.gov/en/dept/dnrp/nature-recreation/parks-recreation/king-county-parks/get-involved/volunteer
  - https://kcls.org/faq/volunteer/
  - Note: This is using a platform called Golden, which looks pretty locked down (nice API but they inject an API key into the page which is passed along, probably a temp one)
- WTA Work Parties: https://www.wta.org/volunteer/schedule/
- **Neighborhood blogs**: RSS/blog parsing with LLM classification
  - Ballard, Fremont, Wallyhood blogs
  - Eastlake Community Council: They don't seem to have an RSS feed so it may be a little different
- **Facebook Groups**:
  - Sparkling Wallingford: https://www.facebook.com/groups/1192189949067573/events
  - We Heart Seattle cleanups: https://www.facebook.com/groups/weheartseattle/events
- **Incremental crawling of detail pages for other sources**: Expand detail page enrichment pattern to GSP, SPR, etc.
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

