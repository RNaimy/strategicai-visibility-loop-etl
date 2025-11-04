MIT © 2025 Richard Naimy • [StrategicAILeader.com](https://www.strategicaileader.com/)
# StrategicAI Visibility Loop ETL (Public Demo)

**StrategicAI Visibility Loop ETL** is a lightweight, public-ready tool that merges **Screaming Frog**, **Google Search Console (GSC)**, and **Google Analytics 4 (GA4)** exports into one unified visibility dataset.

This project provides a simple end-to-end workflow for consolidating your SEO and analytics data into a single CSV.  
It includes demo files, configuration templates, and a one-command ETL pipeline.

---

## 🚀 Quickstart

```bash
# 1) Clone the repository
git clone https://github.com/RNaimy/strategicai-visibility-loop-etl.git
cd strategicai-visibility-loop-etl

# 2) Create virtual environment and install dependencies
make setup

# 3) Run the demo merge
make run
```

✅ Python 3.9+ recommended.  
💾 Output is automatically written to:

```
merged/merged_visibility.csv
```

You can open the resulting CSV in Excel, Google Sheets, or VS Code.

---

## 📂 Input Data

The tool expects **three standard CSV exports** — one from each platform — placed in the `data_demo/` directory.  
Pre-populated demo files are included for testing.

```
data_demo/
├── screaming_frog_export.csv   # url, status_code, title, meta_description, inlinks, word_count
├── gsc_export.csv              # url, clicks, impressions, ctr, position
└── ga4_export.csv              # url, users, sessions, engaged_sessions, avg_engagement_time
```

⚙️ You can modify file paths in `etl_config.yaml` to point to your own exports.

---

## 📊 Output Overview

The ETL merges all datasets into a unified CSV with normalized columns per URL.

**Output:** `merged/merged_visibility.csv`

Example of merged fields:

| url | status_code | title | meta_description | word_count | inlinks | clicks | impressions | ctr | position | users | sessions | engaged_sessions | avg_engagement_time |
|-----|-------------|-------|------------------|-----------:|--------:|-------:|------------:|----:|---------:|------:|---------:|-----------------:|--------------------:|

> Missing data from any input will appear blank in the merged output.

---

## ⚙️ Configuration

Environment-first configuration ensures flexibility for local or team setups.  
You can define variables inline or in a `.envrc` file.

```bash
export SITE_BASE="https://www.example-florist.com"   # Optional — normalize relative paths
export STRIP_ALL_QUERY_PARAMS=false                  # Set true to remove ?query=params
```

Default paths in `etl_config.yaml`:

```yaml
inputs:
  screaming_frog_csv: "data_demo/screaming_frog_export.csv"
  gsc_csv: "data_demo/gsc_export.csv"
  ga4_csv: "data_demo/ga4_export.csv"
```

You can override any of these via environment variables.

---

## 🧩 Project Structure

```
strategicai_visibility_loop_etl/
├── data_demo/              # Example input data
├── merged/                 # Generated output (merged_visibility.csv)
├── etl_merge.py            # Main ETL script
├── etl_config.yaml         # Path configuration
├── Makefile                # Setup, run, clean commands
├── requirements.txt        # Python dependencies
├── docs/                   # Documentation and Quickstart guides
└── README.md
```

---

## 🛠️ Troubleshooting

| Issue | Possible Cause | Solution |
|-------|----------------|-----------|
| **File not found** | CSV path incorrect | Check file paths in `etl_config.yaml` |
| **Mismatched URLs** | Relative vs absolute paths | Set `SITE_BASE` and rerun |
| **Missing columns** | Different header naming | Ensure columns represent same metrics |

---

## 🔒 Data Governance

All demo data in this repository is **synthetic** and **safe for public use**.  
Never commit or share client or proprietary datasets.

---

## 📄 License

MIT © 2025 Richard Naimy  
[StrategicAILeader.com](https://www.strategicaileader.com/)