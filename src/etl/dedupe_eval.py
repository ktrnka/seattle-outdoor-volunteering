"""
Score a deduplication clustering against hand-labeled event pairs (data/dedupe_labels.csv).

A labeled pair counts as merged when both events end up in the same cluster, directly or through other events.
Rows labeled "unsure" are skipped.
"""

from dataclasses import dataclass, field
from pathlib import Path

import pandas as pd

from ..config import DATA_DIR

LABELS_PATH = DATA_DIR / "dedupe_labels.csv"


@dataclass
class DedupeScore:
    true_merges: int
    false_merges: int
    missed_merges: int
    false_merge_rows: pd.DataFrame = field(repr=False)
    missed_merge_rows: pd.DataFrame = field(repr=False)

    @property
    def precision(self) -> float:
        merged = self.true_merges + self.false_merges
        return self.true_merges / merged if merged else 1.0

    @property
    def recall(self) -> float:
        duplicates = self.true_merges + self.missed_merges
        return self.true_merges / duplicates if duplicates else 1.0


def load_labels(path: Path = LABELS_PATH) -> pd.DataFrame:
    labels = pd.read_csv(path, keep_default_na=False)
    return labels[labels["is_duplicate"].isin(["yes", "no"])]


def score_clusters(clusters: pd.DataFrame, labels: pd.DataFrame) -> DedupeScore:
    """clusters: Splink output with unique_id and cluster_id. labels: from load_labels."""
    cluster_of = dict(zip(clusters["unique_id"], clusters["cluster_id"]))
    merged = [cluster_of.get(a) is not None and cluster_of.get(a) == cluster_of.get(b) for a, b in zip(labels["id_l"], labels["id_r"])]
    merged = pd.Series(merged, index=labels.index)
    is_dup = labels["is_duplicate"] == "yes"
    return DedupeScore(
        true_merges=int((merged & is_dup).sum()),
        false_merges=int((merged & ~is_dup).sum()),
        missed_merges=int((~merged & is_dup).sum()),
        false_merge_rows=labels[merged & ~is_dup],
        missed_merge_rows=labels[~merged & is_dup],
    )
