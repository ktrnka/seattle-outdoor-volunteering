# Deduplication

The same event is often listed by several sources (GSP, SPR, SPF, DNDA, ...). `src/etl/splink_dedupe.py` links those listings into one canonical event with [Splink](https://moj-analytical-services.github.io/splink/) 4 (DuckDB backend).

## How it decides

Splink scores each pair of records as **prior + one piece of evidence per comparison**, in bits (log2 Bayes factors), then turns the total into a match probability. Pairs at or above `MATCH_THRESHOLD` (0.5) are linked, and clusters are the connected components of those links.

* **Which pairs are compared:** only records from *different* sources (`link_only`) that start on the *same date* (the blocking rule).
* **Comparisons:**

| Comparison | Levels (strongest first) | Why it's built this way |
|---|---|---|
| `title` | exact / same distinctive words in any order / shorter title's distinctive words all in the other (2+ words) / most words shared / else | Titles are compared on *distinctive* words: stopwords and words in more than 5% of titles ("park", "restoration", "work party") are dropped. That handles reordering ("Forest Restoration at Pigeon Point Park" vs "Pigeon Point Park Restoration Event"). There's no weak-overlap level on purpose: it chained all ~27 parallel Green Seattle Day sites into one cluster. |
| `address` | same street number *and* similar (Jaro-Winkler ≥ 0.92) / else | Cleaned first (lowercase, city/state/zip dropped). A separate "exact" level trained to a veto (m≈0), because most identical addresses belong to parallel events at big parks. The street-number gate stops "5900" and "1800 Lake Washington Blvd S" from matching. |
| `urls` | any shared URL / else | Many sources link the GSP event page. Only *shared* URLs count; different GSP IDs are **not** used against a match, because GSP itself sometimes lists an event twice. |
| `start_time` | same / within 30 min / within 60 min / else | Time of day only (the date is its own comparison). m for the 30 and 60-minute levels is **hand-set** (`M_START_WITHIN_*`): sources round start times differently, and EM sees too few such duplicates to learn it. |
| `start_date` | exact / else | Always equal after blocking. It's there so that "same day" counts as evidence; blocking alone adds none. |

* **Training:** the prior comes from a deterministic rule (same date and title, recall 0.8), u from random sampling (seeded), and m from two EM passes, blocked on title and on date.

## Checking a change

```bash
uv run seattle-volunteering dev dedupe-eval
```

This re-runs clustering on `data/events.sqlite` (read-only) and scores it against the hand-labeled pairs in `data/dedupe_labels.csv`. It prints precision/recall, cluster counts, and every wrongly merged or missed labeled pair. At the time of writing: precision 1.00, recall 0.93 (25/27), 2,034 clusters from 3,609 events, and a largest cluster of 7.

Also check the **largest clusters**, not just the labeled pairs. One loose edge can join two whole groups.

`uv run pytest tests/test_splink_dedupe.py` pins which comparison level representative pairs land in, so edits to the level SQL are caught quickly.

## Adding labels

Add rows to `data/dedupe_labels.csv`:
* `id_l` and `id_r` are `SOURCE:source_id` (the Splink `unique_id`).
* `is_duplicate` is `yes`, `no`, or `unsure` (skipped).
* `note` is free text.

Label pairs where the model is uncertain, such as new merges with different titles, rather than easy ones.

## Debugging one pair

A waterfall chart shows the bits each comparison contributed:

```python
from src.etl.splink_dedupe import load_source_events, train_linker

linker = train_linker(load_source_events())
preds = linker.inference.predict().as_pandas_dataframe()   # every same-day cross-source pair, with gamma_* and bf_* columns
pair = preds[(preds.unique_id_l == "DNDA:9109") & (preds.unique_id_r == "GSP:44130")]
linker.visualisations.waterfall_chart(pair.to_dict(orient="records")).save("waterfall.html")
linker.visualisations.match_weights_chart().save("weights.html")  # trained m/u per level
```

(Which side of a pair is `_l` depends on source order, so check both orders when filtering.)

Things to look for:
* A big negative bar on a harmless difference.
* A level with m ≈ 0 or 1 (EM learned from a biased handful of pairs).
* Two comparisons that encode the same fact. Splink assumes they're independent, so the fact gets counted twice.

## Known limits

* **Titles with no words in common** can't be matched by string comparison. For example, DNDA "Private Volunteer Event" vs SPR "EC Hughes Restoration Event", or abbreviations like "Brandon Street NA" vs "Brandon Street Natural Area".
* **Same-source reposts** (for example SPR listing an event twice) are only merged when another source's listing links them. `link_and_dedupe` was tried: it merged SPU's "All Hands" cleanups across neighborhoods, which share one URL.
* **The common-word list is recomputed on every run** from the current data, so the threshold between "common" and "distinctive" can drift slightly over time.

The full experiment history (29 variants, with reasons) is on Linear KT-284.
