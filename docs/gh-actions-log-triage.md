# Triaging ETL Errors from GitHub Actions

## List recent ETL runs

```bash
gh run list --limit 10 --workflow ETL
```

Output columns: STATUS, TITLE, WORKFLOW, BRANCH, EVENT, **ID**, ELAPSED, AGE

## View errors/warnings from a run

The ETL workflow has a single job named `run-etl`. To get its ID:

```bash
gh run view <RUN_ID> --json jobs | python3 -c "import json,sys; d=json.load(sys.stdin); print(d['jobs'][0]['databaseId'])"
```

Then fetch the full log and filter to just ETL-relevant lines:

```bash
gh run view <RUN_ID> --job=<JOB_ID> --log \
  | sed 's/^run-etl UNKNOWN STEP\s*//' \
  | sed 's/2[0-9-]*T[0-9:.Z]* //' \
  | grep -i "error\|warn\|fail\|skip\|SPF\|SPR\|GSP\|DNDA\|Earth\|Fremon" \
  | grep -v "tar\|hint:\|\[command\]\|pathspec\|git "
```

## One-liner: errors from the most recent run

```bash
run_id=$(gh run list --limit 1 --workflow ETL --json databaseId -q '.[0].databaseId'); \
job_id=$(gh run view $run_id --json jobs | python3 -c "import json,sys; d=json.load(sys.stdin); print(d['jobs'][0]['databaseId'])"); \
gh run view $run_id --job=$job_id --log \
  | sed 's/^run-etl UNKNOWN STEP\s*//' \
  | sed 's/2[0-9-]*T[0-9:.Z]* //' \
  | grep -i "error\|fail\|Failed to parse" \
  | grep -v "tar\|hint:\|\[command\]\|pathspec\|git "
```

## Surveying errors across multiple recent runs

```bash
for run_id in $(gh run list --limit 7 --workflow ETL --json databaseId -q '.[].databaseId'); do
  echo "=== Run $run_id ==="
  job_id=$(gh run view $run_id --json jobs | python3 -c "import json,sys; d=json.load(sys.stdin); print(d['jobs'][0]['databaseId'])")
  gh run view $run_id --job=$job_id --log \
    | sed 's/^run-etl UNKNOWN STEP\s*//' \
    | sed 's/2[0-9-]*T[0-9:.Z]* //' \
    | grep -i "error\|fail\|Failed to parse" \
    | grep -v "tar\|hint:\|\[command\]\|pathspec\|git "
done
```

## Known recurring error patterns

| Source | Error pattern | First seen | Behavior | Notes |
|--------|---------------|------------|----------|-------|
| SPR | `Error parsing date/time '...'` + `ValueError: Missing start or end date` | ~Mar 2026 | **Crashed whole SPR run** | Fixed 2026-04-19: now skips with WARNING instead of crashing |
| SPU | `Failed to parse date/time for event: Date, Time - None` | ~Mar 2026 | Skips event, continues | Placeholder text in source HTML |
| SPU | `Failed to parse date/time for event: ..., TBA - None` | ~Mar 2026 | Skips event, continues | Time not yet posted by organizer |
| SPU | `Failed to parse date/time for event: ..., TBA - TBA` | ~Apr 2026 | Skips event, continues | Same as above, format shifted slightly |
| SPU | `Failed to parse date/time for event: SundayAugust 9, 2026, ...` | ~Apr 2026 | Skips event, continues | Missing space between day-of-week and month in source HTML |
| SPU | `Failed to parse date/time for event: ...**RSVP required, ...` | ~Apr 2026 | Skips event, continues | `**RSVP required` appended to date string before the comma |

Note: The "Failed to parse" messages appear in the log *after* `SPFExtractor: N events` but are emitted by **SPU**, not SPF. The log ordering reflects the CLI execution sequence.
