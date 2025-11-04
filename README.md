MIT © 2025 Richard Naimy • [StrategicAILeader.com](https://www.strategicaileader.com/)
# StrategicAI Visibility Loop ETL (Public Demo)

Minimal, public ready ETL that merges three exports — Screaming Frog, Google Search Console, and GA4 — into one canonical CSV for analysis or downstream tooling.

This repo ships with synthetic demo data and a one command run that writes:
- `merged/merged_visibility.csv`

No secrets. No external services. Works offline.

---

## Quickstart

```bash
# 1) Clone
git clone https://github.com/RNaimy/strategicai-visibility-loop-etl.git
cd strategicai-visibility-loop-etl

# 2) Setup a virtualenv and deps
make setup

# 3) Run the demo merge
make run
```

Results will be written to:
```
merged/merged_visibility.csv
```

Open it in Excel, Sheets, or VS Code.

---

## Inputs

Place your CSV exports in `data_demo/` (demo files are already included):

```
data_demo/
├── screaming_frog_export.csv   # url, status_code, title, meta_description, inlinks, word_count
├── gsc_export.csv              # url, clicks, impressions, ctr, position
└── ga4_export.csv              # url, views, users, sessions, engaged_sessions, avg_engagement_time
```

Tips:
- Column names are auto mapped. If your headers differ, keep the meaning the same.
- If your exports contain relative paths like `/blog/roses`, set a base so joins work:
  ```bash
  export SITE_BASE="https://www.example-florist.com"
  ```
- To drop all query parameters during normalization:
  ```bash
  export STRIP_ALL_QUERY_PARAMS=true
  ```

---

## Output

`merged/merged_visibility.csv` includes a normalized per URL view with fields like:

| url | status_code | title | meta_description | word_count | inlinks | clicks | impressions | ctr | position | views | users | sessions | engaged_sessions | avg_engagement_time |
|-----|-------------|-------|------------------|-----------:|--------:|------:|-----------:|---:|---------:|-----:|-----:|--------:|-----------------:|--------------------:|

Note: Columns appear if present in your inputs. Missing inputs will yield empty columns.

---

## Configuration

Basic configuration is environment first. Common variables:

```bash
export SITE_BASE="https://www.example-florist.com"   # optional, for relative paths
export STRIP_ALL_QUERY_PARAMS=false                   # set true to drop all ?query=params
```

Paths are defined in `etl_config.yaml` and default to the demo files:

```yaml
inputs:
  screaming_frog_csv: "data_demo/screaming_frog_export.csv"
  gsc_csv: "data_demo/gsc_export.csv"
  ga4_csv: "data_demo/ga4_export.csv"
```

---

## Project structure

```
strategicai_visibility_loop_etl/
├── data_demo/                    # example inputs
├── merged/                       # outputs (generated)
├── etl_merge.py                  # main ETL
├── etl_config.yaml               # paths and options
├── Makefile                      # setup, run, clean
├── requirements.txt              # pinned deps for the demo
├── .gitignore
├── LICENSE
└── README.md
```

This public demo intentionally omits advanced transforms, reports, and private analysis artifacts. If you want scoring, schema gaps, and triage HTML, use the `strategicai_visibility_loop_etl_advance` repo.

---

## Troubleshooting

- **File not found**  
  Ensure your CSVs exist at the paths in `etl_config.yaml`.

- **Weird joins**  
  Set `SITE_BASE` and re run so relative paths normalize to the same host.

- **Wrong or missing columns**  
  Keep the semantic meaning even if the header names differ.

---

## Data governance

- All demo data is synthetic. Do not commit client data.
- The repo contains no secrets and requires none.

---

## License

MIT © 2025 Richard Naimy • [StrategicAILeader.com](https://www.strategicaileader.com/)