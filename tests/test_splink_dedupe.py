"""Tests for the Splink matching columns and comparison levels (see DEDUPLICATION.md for why each exists)."""

import pandas as pd
import pytest
from splink import DuckDBAPI, Linker

from src.etl.deduplication import normalize_title
from src.etl.splink_dedupe import add_matching_columns, clean_address, distinctive_title_tokens, splink_settings


@pytest.mark.parametrize(
    "raw, expected",
    [
        ("5200 35th Avenue SW Seattle, WA 98126-2804", "5200 35th avenue sw"),
        ("1901 SW Genesee St, Seattle, WA, 98122", "1901 sw genesee st"),
        ("29th Ave SW & SW Brandon St, Seattle, WA 98106", "29th ave sw & sw brandon st"),
        ("Please refer to the below link for address information.", None),
        ("   ", None),
        (None, None),
    ],
)
def test_clean_address(raw, expected):
    assert clean_address(raw) == expected


def test_distinctive_title_tokens_drops_common_words_and_stopwords():
    # "park" is in 3 of 4 distinct titles, so it's common at a 50% cutoff; "at" is a stopword
    titles = pd.Series(["pigeon point park", "camp long park", "lincoln park", "green lake"])
    tokens = distinctive_title_tokens(titles, max_share=0.5)
    assert tokens.tolist() == [["pigeon", "point"], ["camp", "long"], ["lincoln"], ["green", "lake"]]


def test_distinctive_title_tokens_keeps_all_words_when_all_are_common():
    titles = pd.Series(["forest restoration", "forest restoration at x", "forest restoration at y"])
    tokens = distinctive_title_tokens(titles, max_share=0.5)
    assert tokens.iloc[0] == ["forest", "restoration"]


def test_add_matching_columns():
    df = pd.DataFrame(
        {
            "normalized_title": ["a b", "c d"],
            "address": ["5900 Lake Washington Blvd S, Seattle, WA", None],
            "start": pd.to_datetime(["2026-06-06 17:30", "2026-06-06 00:00"]),
            "end": pd.to_datetime(["2026-06-06 20:00", "2026-06-06 00:00"]),
        }
    )
    out = add_matching_columns(df)
    assert out["address_clean"].tolist() == ["5900 lake washington blvd s", None]
    assert out["street_number"].iloc[0] == "5900"
    assert out["start_minute"].iloc[0] == 17 * 60 + 30
    assert pd.isna(out["start_minute"].iloc[1])  # start == end means the time is unknown
    assert out["address"].iloc[0] == "5900 Lake Washington Blvd S, Seattle, WA"  # display column untouched


# ---- comparison levels: which level does a pair land in? ----

# Filler titles make the generic words common (> 5% of distinct titles), like in real data, while words that only
# appear in one test pair stay distinctive (2 of ~100 titles)
FILLER = [f"forest restoration work party park event day seattle site{i}" for i in range(100)]


def _records(pairs):
    """Build one DataFrame holding both sides of every (left, right) record pair, plus filler titles."""
    rows = []
    for i, (left, right) in enumerate(pairs):
        for side, rec in (("L", left), ("R", right)):
            rows.append({"source": side, "source_id": str(i), **rec})
    for i, title in enumerate(FILLER):
        rows.append({"source": "F", "source_id": str(i), "title": title})
    df = pd.DataFrame(rows)
    df["title"] = df["title"].fillna("x")
    df["address"] = df.get("address", pd.Series(dtype=object))
    df["start"] = pd.to_datetime(df.get("start", pd.Series(dtype=object)).fillna("2026-01-01 17:00"))
    df["end"] = df["start"] + pd.Timedelta(hours=3)
    df["normalized_title"] = df["title"].apply(normalize_title)
    df["start_date"] = df["start"].dt.date.astype(str)
    df["urls"] = [[] for _ in range(len(df))]
    df["unique_id"] = df["source"] + ":" + df["source_id"]
    df["source_dataset"] = df["source"]
    df = add_matching_columns(df)
    # An all-null column would reach DuckDB as INTEGER; production data always has some addresses
    for col in ("address", "address_clean", "street_number"):
        df[col] = df[col].astype("string")
    return df


def _gamma(left, right, column):
    """
    Comparison vector value (gamma) for one pair; higher = stronger level, 0 = Else, -1 = null.
    Uses predict() on an untrained model (same code path as production; the gammas don't depend on training).
    """
    df = _records([(left, right)])
    linker = Linker([g for _, g in df.groupby("source")], splink_settings(), db_api=DuckDBAPI())
    preds = linker.inference.predict().as_pandas_dataframe()
    pair = preds[preds.unique_id_l.isin(["L:0", "R:0"]) & preds.unique_id_r.isin(["L:0", "R:0"])]
    return int(pair[f"gamma_{column}"].iloc[0])


# title levels: 4 exact, 3 same distinctive words, 2 containment, 1 mostly shared, 0 else
@pytest.mark.parametrize(
    "a, b, expected",
    [
        ("Sturtevant Ravine Work Party!", "Sturtevant Ravine Work Party!", 4),
        ("Forest Restoration at Pigeon Point Park", "Pigeon Point Park Restoration Event", 3),
        ("MLK day of service", "MLK Day of Service at Pigeon Point Park!", 2),
        # Different parks on the same day: each side has distinctive words the other lacks
        ("Green Seattle Day at Colman Park", "Green Seattle Day at Westcrest Park", 0),
        # A single leftover distinctive word isn't enough for containment
        ("Community Forest Restoration", "C-ID Community Monthly Clean-Up", 0),
    ],
)
def test_title_levels(a, b, expected):
    assert _gamma({"title": a}, {"title": b}, "title") == expected


# address levels: 1 same number and similar, 0 else, -1 null
@pytest.mark.parametrize(
    "a, b, expected",
    [
        ("2850 SW Roxbury St, Seattle, WA 98126", "2850 SW Roxbury Street, Seattle, WA", 1),
        ("5900 Lake Washington Blvd S, Seattle, WA", "1800 Lake Washington Blvd S, Seattle, WA 98144", 0),
        ("5900 Lake Washington Blvd S, Seattle, WA", None, -1),
    ],
)
def test_address_levels(a, b, expected):
    assert _gamma({"address": a}, {"address": b}, "address") == expected


# start time levels: 3 same, 2 within 30 min, 1 within 60 min, 0 else
@pytest.mark.parametrize(
    "a, b, expected",
    [
        ("2026-07-21 17:00", "2026-07-21 17:00", 3),
        ("2026-07-21 16:30", "2026-07-21 17:00", 2),
        ("2026-07-21 16:00", "2026-07-21 17:00", 1),
        ("2026-07-21 09:00", "2026-07-21 17:00", 0),
    ],
)
def test_start_time_levels(a, b, expected):
    assert _gamma({"start": a}, {"start": b}, "start_time") == expected
