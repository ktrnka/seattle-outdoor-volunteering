from datetime import timezone
from typing import Any, List, Optional
import pandas as pd
import splink.comparison_library as cl
from splink import DuckDBAPI, Linker, SettingsCreator, block_on

from src.etl.deduplication import normalize_title

from ..database import get_regular_connection
from .url_utils import normalize_url
from ..models import CanonicalEvent


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
    df = pd.read_sql_query("SELECT * FROM events", sqlite_connection, parse_dates=["start", "end"])
    df["normalized_title"] = df["title"].apply(normalize_title)
    df["start_date"] = df["start"].dt.date.astype(str)

    # Create a start time of day column
    df["start_time"] = df["start"].dt.time.astype(str)

    # Null out start_time if start and end are the same, so that it's ignored in exact matching in Splink
    df.loc[df["start"] == df["end"], "start_time"] = None

    # Create a URL list col of URL and same_as
    df["urls"] = df.apply(lambda row: create_url_list(row["url"], row["same_as"]), axis=1)

    # Special fields used by Splink
    df["source_dataset"] = df["source"]
    df["unique_id"] = df["source"] + ":" + df["source_id"]

    # Double-check that we're using null instead of empty strings
    assert df["address"].isnull().mean() > 0.01

    return df


def cluster_events(df: pd.DataFrame) -> pd.DataFrame:
    """
    Cluster events using Splink deduplication.

    Args:
        df: DataFrame containing source events

    Returns:
        DataFrame with clustered events
    """
    db_api = DuckDBAPI()

    settings = SettingsCreator(
        link_type="link_only",
        comparisons=[
            cl.JaroAtThresholds("normalized_title").configure(term_frequency_adjustments=True),
            cl.JaroAtThresholds("address", [0.75]),
            cl.ArrayIntersectAtSizes("urls", [1]),
            cl.ExactMatch("start_time"),
        ],
        blocking_rules_to_generate_predictions=[
            block_on("start_date"),
        ],
    )

    grouped = df.groupby("source")

    dfs = [group for _, group in grouped]

    linker = Linker(dfs, settings, db_api=db_api)  # type: ignore
    linker.training.estimate_probability_two_random_records_match(
        [block_on("start_date", "normalized_title")],
        recall=0.8,
    )

    linker.training.estimate_u_using_random_sampling(max_pairs=5e6)

    linker.training.estimate_parameters_using_expectation_maximisation(block_on("normalized_title"))

    linker.training.estimate_parameters_using_expectation_maximisation(block_on("start_date"))

    pairwise_predictions = linker.inference.predict(threshold_match_probability=0.1)
    clusters = linker.clustering.cluster_pairwise_predictions_at_threshold(pairwise_predictions)
    return clusters.as_pandas_dataframe()


def mode(series: pd.Series) -> Optional[Any]:
    series = series.dropna()
    if series.empty:
        return None
    return series.mode().iloc[0]


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
