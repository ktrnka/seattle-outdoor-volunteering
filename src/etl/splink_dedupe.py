from typing import Optional
import pandas as pd
import splink.comparison_library as cl
from splink import DuckDBAPI, Linker, SettingsCreator, block_on, splink_datasets

from ..database import get_regular_connection
from ..models import Event as MemorySourceEvent
from .url_utils import normalize_url


def create_urls(*urls: Optional[str]) -> list[str]:
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
    Load source events from the database into a DataFrame.

    Returns:
        DataFrame containing source events
    """
    sqlite_connection = get_regular_connection()
    df = pd.read_sql_query(
        "SELECT * FROM events", sqlite_connection, parse_dates=["start", "end"]
    )
    df["start_date"] = df["start"].dt.date.astype(str)

    # Create a start time of day column
    df["start_time"] = df["start"].dt.time.astype(str)

    # Null out start_time if start and end are the same, so that it's ignored in exact matching in Splink
    df.loc[df["start"] == df["end"], "start_time"] = None

    # Create a URL list col of URL and same_as
    df["urls"] = df.apply(lambda row: create_urls(
        row["url"], row["same_as"]), axis=1)

    # Special fields used by Splink
    df["source_dataset"] = df["source"]
    df["unique_id"] = df["source"] + ":" + df["source_id"]

    # Double-check that we're using null instead of empty strings
    assert df["address"].isnull().mean() > 0.01

    return df


def run_splink_deduplication(show_examples: bool = True):
    df = load_source_events()

    db_api = DuckDBAPI()

    settings = SettingsCreator(
        link_type="link_only",
        comparisons=[
            cl.JaroAtThresholds("title").configure(
                term_frequency_adjustments=True
            ),
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

    linker = Linker(dfs, settings, db_api=db_api)
    linker.training.estimate_probability_two_random_records_match(
        [block_on("start_date", "title")],
        recall=0.8,
    )

    linker.training.estimate_u_using_random_sampling(max_pairs=5e6)

    linker.training.estimate_parameters_using_expectation_maximisation(
        block_on("title")
    )

    linker.training.estimate_parameters_using_expectation_maximisation(
        block_on("start_date")
    )

    pairwise_predictions = linker.inference.predict(
        threshold_match_probability=0.1)
    clusters = linker.clustering.cluster_pairwise_predictions_at_threshold(
        pairwise_predictions
    )
    df_clusters = clusters.as_pandas_dataframe()

    # Show some of the clusters
    if show_examples:
        grouped = df_clusters.groupby("cluster_id")

        # Sort clusters by min start
        cluster_min_start = grouped["start"].min()
        sorted_cluster_ids = cluster_min_start.sort_values().index[:60]

        for cluster_id in sorted_cluster_ids:
            cluster_events = grouped.get_group(cluster_id)
            if len(cluster_events) <= 1:
                continue

            cols = ["source", "unique_id", "title",
                    "address", "start_date", "start_time"]
            print(f"\nCluster {cluster_id}:")
            print(cluster_events[cols])

            # Show the URLs in the cluster
            url_counts = cluster_events["urls"].explode().value_counts()
            for url, count in url_counts.items():
                print(f"  {url} ({count} occurrences)")

        print(f"Total events: {len(df)}")
        print(f"Total clusters: {len(df_clusters['cluster_id'].unique())}")

        # Show the group sizes
        cluster_sizes = grouped.size().value_counts()
        print("\nCluster sizes:")
        for size, count in cluster_sizes.items():
            print(f"Size {size}: {count} clusters")
