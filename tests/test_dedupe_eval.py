import pandas as pd

from src.etl.dedupe_eval import load_labels, score_clusters


def test_score_clusters_counts_transitive_merges_and_skips_unsure():
    clusters = pd.DataFrame({"unique_id": ["A:1", "B:1", "C:1", "D:1", "E:1"], "cluster_id": [1, 1, 1, 2, 3]})
    labels = pd.DataFrame(
        {
            "id_l": ["A:1", "A:1", "D:1", "D:1"],
            "id_r": ["C:1", "B:1", "E:1", "A:1"],
            "is_duplicate": ["yes", "no", "yes", "no"],
            "title_l": "",
            "title_r": "",
        }
    )
    score = score_clusters(clusters, labels)
    assert (score.true_merges, score.false_merges, score.missed_merges) == (1, 1, 1)  # A-C via B; A-B wrong; D-E missed
    assert score.precision == 0.5
    assert score.recall == 0.5


def test_checked_in_labels_load():
    labels = load_labels()
    assert set(labels["is_duplicate"]) == {"yes", "no"}
    assert labels["id_l"].str.contains(":").all()
