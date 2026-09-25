"""
Cross-source event deduplication with Splink.

How the model decides (see DEDUPLICATION.md for the reasoning and how to re-evaluate after changes):
- Only events on the same start date are compared (blocking), and the same date is also counted as evidence
  (the start_date comparison). Blocking alone adds no evidence, which made identical titles score ~0.07.
- Titles are compared on their *distinctive* words: stopwords and words used in more than
  COMMON_TOKEN_SHARE of titles ("park", "restoration", "work party", ...) are dropped first. There is
  deliberately no weak-overlap level; loose title levels chained parallel events (e.g. every Green Seattle Day site)
  into giant clusters.
- Addresses are cleaned and only count as similar with the same street number.
- Start times 30-60 minutes apart get a hand-set, mild penalty, because sources round start times differently and
  EM sees too few such duplicates to learn it.
"""

import re
from datetime import timezone
from typing import Any, List, Optional

import pandas as pd
import splink.comparison_level_library as cll
import splink.comparison_library as cl
from splink import DuckDBAPI, Linker, SettingsCreator, block_on

from src.etl.deduplication import normalize_title

from ..database import get_regular_connection
from ..models import CanonicalEvent
from .url_utils import normalize_url

# Pairs at or above this match probability are linked; clusters are the connected components of those links.
MATCH_THRESHOLD = 0.5

# Title words used in more than this share of distinct titles carry little identity ("park", "restoration", ...).
COMMON_TOKEN_SHARE = 0.05
TITLE_STOPWORDS = {"at", "the", "in", "on", "a", "an", "of", "and", "with", "for", "to", "s"}

# Hand-set m probabilities (share of true duplicates) for start times that differ by up to 30 / 60 minutes.
# Not trained: EM sees too few such duplicates and otherwise learns a -7 bit penalty for any offset.
M_START_WITHIN_30_MIN = 0.08
M_START_WITHIN_60_MIN = 0.03


def create_url_list(*urls: Optional[str]) -> list[str]:
    """
    Create a list of URLs from the provided arguments.

    Args:
        *urls: Variable number of URL strings

    Returns:
        List of URLs
    """
    return [normalize_url(url) for url in urls if url]


def load_source_events() -> pd.DataFrame:
    """
    Load source events from the database into a DataFrame. It's awkward that we're loading
    from Sqlite into Pandas only to insert into DuckDB, but it looks like the easiest way to
    go. I tried running Splink from Sqlite directly, but it doesn't support some of the operations
    we need.

    Returns:
        DataFrame containing source events
    """
    sqlite_connection = get_regular_connection()
    # Join with enrichment tables to get LLM categorization and detail page data
    query = """
    SELECT e.*, 
           json_extract(ese.llm_categorization, '$.category') as llm_category,
           json_extract(dpe.enrichment_data, '$.website_url') as website_url
    FROM events e
    LEFT JOIN enriched_source_events ese ON e.source = ese.source AND e.source_id = ese.source_id
    LEFT JOIN detail_page_enrichments dpe ON e.source = dpe.source AND e.source_id = dpe.source_id
    """
    df = pd.read_sql_query(query, sqlite_connection, parse_dates=["start", "end"])
    df["normalized_title"] = df["title"].apply(normalize_title)
    df["start_date"] = df["start"].dt.date.astype(str)

    # Create a start time of day column
    df["start_time"] = df["start"].dt.time.astype(str)

    # Null out start_time if start and end are the same (time unknown); canonical events prefer members with a time
    # (Splink compares start_minute from add_matching_columns instead)
    df.loc[df["start"] == df["end"], "start_time"] = None

    # Create a URL list col of URL, same_as, and website_url from detail page enrichment
    df["urls"] = df.apply(lambda row: create_url_list(row["url"], row["same_as"], row["website_url"]), axis=1)

    # Special fields used by Splink
    df["source_dataset"] = df["source"]
    df["unique_id"] = df["source"] + ":" + df["source_id"]

    # Double-check that we're using null instead of empty strings
    assert df["address"].isnull().mean() > 0.01

    return add_matching_columns(df)


def clean_address(address: Optional[str]) -> Optional[str]:
    """
    Normalize an address for comparison: lowercase, drop the trailing city/state/zip (nearly everything is in
    Seattle, so it only makes unrelated addresses look alike), and strip punctuation. Blank -> None.
    """
    if address is None or pd.isna(address):
        return None
    cleaned = address.strip().lower()
    if "refer to" in cleaned:  # SPR placeholder: "Please refer to the below link for address information."
        return None
    cleaned = re.sub(r",?\s*seattle,?\s*wa\b.*$", "", cleaned)
    cleaned = re.sub(r"[^\w\s&]", " ", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned or None


def distinctive_title_tokens(normalized_titles: pd.Series, max_share: float = COMMON_TOKEN_SHARE) -> pd.Series:
    """
    Sorted, de-duplicated title words minus stopwords and words used in more than max_share of distinct titles.
    This is a word-level version of Splink's term-frequency adjustment (which only works on whole values).
    A title made only of common words keeps all of its words, so it still has something to compare.
    """
    tokens = normalized_titles.apply(lambda title: sorted({word for word in title.split() if word not in TITLE_STOPWORDS}))
    distinct = tokens[~normalized_titles.duplicated()]
    share = distinct.explode().value_counts() / len(distinct)
    common = set(share[share > max_share].index)
    return tokens.apply(lambda words: [word for word in words if word not in common] or words)


def add_matching_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Derived columns the Splink comparisons read. Display columns (title, address, ...) are left untouched."""
    df = df.copy()
    df["title_tokens"] = distinctive_title_tokens(df["normalized_title"])
    df["address_clean"] = df["address"].apply(clean_address)
    df["street_number"] = df["address_clean"].str.extract(r"^(\d+)\b", expand=False)
    # Minutes since midnight, null when start == end (time unknown). Time of day only: the date is its own comparison,
    # and comparing full timestamps would count the date twice.
    df["start_minute"] = (df["start"].dt.hour * 60 + df["start"].dt.minute).where(df["start"] != df["end"])
    return df


# SQL over the title_tokens arrays (DuckDB list functions); _l / _r are the two records in a pair.
_SHARED = "len(list_intersect(title_tokens_l, title_tokens_r))"
_SHORTER = "least(len(list_distinct(title_tokens_l)), len(list_distinct(title_tokens_r)))"
_JACCARD = f"({_SHARED} / len(list_distinct(list_concat(title_tokens_l, title_tokens_r))))"


def title_comparison() -> cl.CustomComparison:
    """Strongest first. Two titles that each have distinctive words the other lacks (two different parks) fall to Else."""
    return cl.CustomComparison(
        output_column_name="title",
        comparison_levels=[
            cll.NullLevel("normalized_title"),
            cll.ExactMatchLevel("normalized_title").configure(tf_adjustment_column="normalized_title"),
            cll.CustomLevel(f"{_JACCARD} >= 0.99", "Same distinctive words (any order)"),
            # Requiring 2+ words stops a one-word leftover like "community" from matching any title containing it
            cll.CustomLevel(f"{_SHARED} / {_SHORTER} >= 0.99 AND {_SHORTER} >= 2", "Shorter title's distinctive words all in the other"),
            cll.CustomLevel(f"{_JACCARD} >= 0.6", "Most distinctive words shared"),
            cll.ElseLevel(),
        ],
    )


def address_comparison() -> cl.CustomComparison:
    """
    One 'similar' level that includes exact matches. A separate exact level trained to m≈0 (a veto), because
    most identical addresses EM saw belonged to parallel events at big parks. The street number gate keeps two
    parks on one long street ('5900 lake washington blvd s' vs '1800 ...') from looking alike.
    """
    return cl.CustomComparison(
        output_column_name="address",
        comparison_levels=[
            cll.NullLevel("address_clean"),
            cll.And(cll.ExactMatchLevel("street_number"), cll.JaroWinklerLevel("address_clean", 0.92)),
            cll.ElseLevel(),
        ],
    )


def start_time_comparison() -> cl.CustomComparison:
    minutes_apart = "abs(start_minute_l - start_minute_r)"
    return cl.CustomComparison(
        output_column_name="start_time",
        comparison_levels=[
            cll.NullLevel("start_minute"),
            cll.CustomLevel(f"{minutes_apart} = 0", "Same start time"),
            cll.CustomLevel(f"{minutes_apart} <= 30", "Start within 30 min").configure(m_probability=M_START_WITHIN_30_MIN, fix_m_probability=True),
            cll.CustomLevel(f"{minutes_apart} <= 60", "Start within 60 min").configure(m_probability=M_START_WITHIN_60_MIN, fix_m_probability=True),
            cll.ElseLevel(),
        ],
    )


def splink_settings() -> SettingsCreator:
    return SettingsCreator(
        link_type="link_only",  # records are only compared across sources
        comparisons=[
            title_comparison(),
            address_comparison(),
            cl.ArrayIntersectAtSizes("urls", [1]),
            start_time_comparison(),
            cl.ExactMatch("start_date"),  # always equal after blocking; this is what makes "same day" count as evidence
        ],
        blocking_rules_to_generate_predictions=[
            block_on("start_date"),
        ],
        retain_intermediate_calculation_columns=True,  # keeps bf_* columns so waterfall charts work when debugging
    )


def train_linker(df: pd.DataFrame) -> Linker:
    """Build and train the Splink model. Also handy for debugging (waterfall charts); see DEDUPLICATION.md."""
    dfs = [group for _, group in df.groupby("source")]
    linker = Linker(dfs, splink_settings(), db_api=DuckDBAPI())  # type: ignore

    linker.training.estimate_probability_two_random_records_match(
        [block_on("start_date", "normalized_title")],
        recall=0.8,
    )
    linker.training.estimate_u_using_random_sampling(max_pairs=5e6, seed=42)
    linker.training.estimate_parameters_using_expectation_maximisation(block_on("normalized_title"))
    linker.training.estimate_parameters_using_expectation_maximisation(block_on("start_date"))
    return linker


def cluster_events(df: pd.DataFrame, threshold: float = MATCH_THRESHOLD) -> pd.DataFrame:
    """
    Cluster events using Splink deduplication.

    Args:
        df: DataFrame from load_source_events (must include the add_matching_columns columns)
        threshold: Match probability at or above which two events are linked

    Returns:
        DataFrame with clustered events
    """
    linker = train_linker(df)
    pairwise_predictions = linker.inference.predict(threshold_match_probability=threshold)
    clusters = linker.clustering.cluster_pairwise_predictions_at_threshold(pairwise_predictions, threshold_match_probability=threshold)
    return clusters.as_pandas_dataframe()


def mode(series: pd.Series) -> Optional[Any]:
    series = series.dropna()
    if series.empty:
        return None
    return series.mode().iloc[0]


def aggregate_llm_categories(event_group: pd.DataFrame) -> List[str]:
    """
    Aggregate LLM categories from a group of events using majority vote.

    Args:
        event_group: DataFrame containing events in the group

    Returns:
        List of tags to include in canonical event
    """
    # Get non-null LLM categories
    llm_categories = event_group["llm_category"].dropna()

    if llm_categories.empty:
        return []

    # Count categories and get the most common one
    category_counts = llm_categories.value_counts()
    most_common_category = category_counts.index[0]

    # Return as a list with the llm: prefix for clarity
    return [f"llm:{most_common_category}"]


def create_canonical_event_from_group(cluster_id, event_group: pd.DataFrame) -> CanonicalEvent:
    """
    Create a CanonicalEvent from a group of events.

    Args:
        cluster_id: ID of the cluster
        event_group: DataFrame containing events in the group

    Returns:
        CanonicalEvent object
    """

    source_preferences = {source: i for i, source in enumerate(["DNDA", "EC", "GSP", "SPR"])}

    sorted_group = event_group.sort_values(by="source", key=lambda x: x.map(lambda val: source_preferences.get(val, len(source_preferences))))

    events_with_time = sorted_group[sorted_group["start_time"].notnull()]
    if not events_with_time.empty:
        start = mode(events_with_time["start"])
        end = mode(events_with_time["end"])
    else:
        start = mode(sorted_group["start"])
        end = mode(sorted_group["end"])

    # Ensure start and end are not None
    if start is None or end is None:
        raise ValueError(f"Could not determine start/end times for cluster {cluster_id}")

    # Aggregate LLM categories into tags
    tags = aggregate_llm_categories(event_group)

    try:
        return CanonicalEvent(
            canonical_id=f"cluster_{cluster_id}",
            title=sorted_group["title"].iloc[0],
            start=start.tz_localize(timezone.utc),
            end=end.tz_localize(timezone.utc),
            venue=mode(event_group["venue"]),
            address=mode(event_group["address"]),
            url=sorted_group["url"].iloc[0],
            source_events=sorted_group["unique_id"].tolist(),
            tags=tags,
        )
    except:
        print(f"Error creating canonical event for cluster {cluster_id}")
        print(event_group)
        raise


def create_canonical_events(df_clusters: pd.DataFrame) -> List[CanonicalEvent]:
    """
    Create canonical events from clustered DataFrame.

    Args:
        df_clusters: DataFrame with clustered events

    Returns:
        List of CanonicalEvent objects
    """
    canonical_events = []
    grouped = df_clusters.groupby("cluster_id")

    for cluster_id, group in grouped:
        canonical_events.append(create_canonical_event_from_group(cluster_id, group))

    return canonical_events


def run_splink_deduplication(show_examples: bool = True):
    df = load_source_events()
    df_clusters = cluster_events(df)

    # Show some of the clusters
    if show_examples:
        grouped = df_clusters.groupby("cluster_id")

        # Sort clusters by min start
        cluster_min_start = grouped["start"].min()
        sorted_cluster_ids = cluster_min_start.sort_values().index[:60]

        for cluster_id in sorted_cluster_ids:
            cluster_df = grouped.get_group(cluster_id)
            if len(cluster_df) <= 1:
                continue

            cols = ["source", "unique_id", "title", "address", "start_date", "start_time"]
            print(f"\nCluster {cluster_id}:")
            print(cluster_df[cols])

            # Show the URLs in the cluster
            url_counts = cluster_df["urls"].explode().value_counts()
            for url, count in url_counts.items():
                print(f"  {url} ({count} occurrences)")

        print(f"Total events: {len(df)}")
        print(f"Total clusters: {len(df_clusters['cluster_id'].unique())}")

        # Show the group sizes
        cluster_sizes = grouped.size().value_counts()
        print("\nCluster sizes:")
        for size, count in cluster_sizes.items():
            print(f"Size {size}: {count} clusters")

    return create_canonical_events(df_clusters)
