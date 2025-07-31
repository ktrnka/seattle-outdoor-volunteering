from collections import Counter
from .url_utils import normalize_url
from ..models import Event as MemorySourceEvent
import splink.comparison_library as cl
from splink import DuckDBAPI, Linker, SettingsCreator, block_on, splink_datasets

import pandas as pd
from ..database import get_regular_connection, Event as DBSourceEvent


def create_urls(*urls: str) -> list[str]:
    """
    Create a list of URLs from the provided arguments.

    Args:
        *urls: Variable number of URL strings

    Returns:
        List of URLs
    """
    return [normalize_url(url) for url in urls if url]  # Filter out empty strings


def run_splink_deduplication(source_events: list[MemorySourceEvent]):
    sqlite_connection = get_regular_connection()
    df = pd.read_sql_query(
        "SELECT * FROM events", sqlite_connection, parse_dates=["start", "end"]
    )
    df["start_date"] = df["start"].dt.date.astype(str)

    # Create a start time of day column
    df["start_time"] = df["start"].dt.time.astype(str)

    # Null out start_time if start and end are the same
    df.loc[df["start"] == df["end"], "start_time"] = None

    # Create a URL list col of URL and same_as
    df["urls"] = df.apply(lambda row: create_urls(
        row["url"], row["same_as"]), axis=1)
    df["num_urls"] = df["urls"].apply(len)

    # Special fields used by Splink
    df["source_dataset"] = df["source"]
    df["unique_id"] = df["source"] + ":" + df["source_id"]

    db_api = DuckDBAPI()

    assert df["address"].isnull().mean() > 0.01

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
    df_names = [str(name) for name, _ in grouped]

    linker = Linker(dfs, settings, db_api=db_api, input_table_aliases=df_names)
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

    # Group clusters by cluster_id, sort by min start, and print first 10 clusters
    # pd.options.display.max_colwidth = 100
    if "cluster_id" in df_clusters.columns:
        grouped = df_clusters.groupby("cluster_id")
        # Get min start for each cluster
        cluster_min_start = grouped["start"].min()
        # Sort clusters by min start
        sorted_cluster_ids = cluster_min_start.sort_values().index[:60]
        for cluster_id in sorted_cluster_ids:
            cluster_events = grouped.get_group(cluster_id)
            if len(cluster_events) <= 1:
                continue

            cols = ["source", "unique_id", "title",
                    "address", "start_date", "start_time", "num_urls"]
            print(
                f"\nCluster {cluster_id} (min start: {cluster_min_start[cluster_id]}):")
            print(cluster_events[cols])

            url_counts = cluster_events["urls"].explode().value_counts()
            for url, count in url_counts.items():
                print(f"  {url} ({count} occurrences)")

        print(f"Total events: {len(df)}")
        print(f"Total clusters: {len(df_clusters['cluster_id'].unique())}")

        # Count the group sizes
        cluster_sizes = grouped.size().value_counts()
        print("\nCluster sizes:")
        for size, count in cluster_sizes.items():
            print(f"Size {size}: {count} clusters")
    else:
        print("No cluster_id column found in df_clusters.")


def run_splink_deduplication_demo(source_events: list[MemorySourceEvent]):
    db_api = DuckDBAPI()

    df = splink_datasets.fake_1000

    settings = SettingsCreator(
        link_type="dedupe_only",
        comparisons=[
            cl.NameComparison("first_name"),
            cl.JaroAtThresholds("surname"),
            cl.DateOfBirthComparison(
                "dob",
                input_is_string=True,
            ),
            cl.ExactMatch("city").configure(term_frequency_adjustments=True),
            cl.EmailComparison("email"),
        ],
        blocking_rules_to_generate_predictions=[
            block_on("first_name", "dob"),
            block_on("surname"),
        ]
    )

    linker = Linker(df, settings, db_api)

    linker.training.estimate_probability_two_random_records_match(
        [block_on("first_name", "surname")],
        recall=0.7,
    )

    linker.training.estimate_u_using_random_sampling(max_pairs=1e6)

    linker.training.estimate_parameters_using_expectation_maximisation(
        block_on("first_name", "surname")
    )

    linker.training.estimate_parameters_using_expectation_maximisation(
        block_on("email"))

    pairwise_predictions = linker.inference.predict(threshold_match_weight=-5)

    clusters = linker.clustering.cluster_pairwise_predictions_at_threshold(
        pairwise_predictions, 0.95
    )

    df_clusters = clusters.as_pandas_dataframe(limit=5)
    print(df_clusters)
