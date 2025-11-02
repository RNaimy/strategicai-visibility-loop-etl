# StrategicAI Visibility Loop ETL

This project provides ready-made Python ETL templates to merge SEO data from **Screaming Frog**, **Google Search Console**, and **GA4** into a single, structured dataset for analysis and AI reasoning. It supports the StrategicAILeader.com framework for AI-driven SEO visibility systems.

---

## Key Features

- **Unified Dataset** – Combines crawl, performance, and analytics data into one CSV.
- **Schema-Driven Configuration** – Controlled entirely via `etl_config.yaml`.
- **Clean, Reusable Code** – Clear comments for non-developers and repeatable runs.
- **Optional Google Sheets Upload** – Push merged data to a live sheet for team sharing.
- **Makefile Support** – Run setup, execute merges, or clean outputs with simple commands.

---

## Public and Private Outputs

The ETL pipeline generates both internal diagnostic CSVs for detailed analysis and lightweight, sanitized CSVs designed for public sharing or educational presentations. This dual-output approach helps maintain data governance while enabling transparent communication with stakeholders or broader audiences.

**Note:** The `samples/triage_public_lite.csv` file is the only file intended for public sharing or educational use. Other files in the `samples/` directory are for internal use or reference only.

---

## Project Structure

```
strategicai_visibility_loop_etl/
│
├── data/                        # Input CSVs (demo or production)
│   ├── screaming_frog_export.csv
│   ├── gsc_export.csv
│   └── ga4_export.csv
│
├── merged/                      # Output data after each ETL run
│   ├── merged_visibility.csv
│   ├── anomaly_ctr_underperf.csv
│   ├── schema_gaps.csv
│   └── ctr_priority_opportunities.csv
│
├── samples/                     # Triage examples and demo datasets
│   ├── triage_public_lite.csv   # Lightweight, sanitized CSV safe for sharing
│   ├── triage_actions_reference.csv  # Minimal example of playbook mappings for demonstration
│   ├── triage_priority_TOP20.md       # Top 20 triage output for human preview
│   └── sample_run_outputs.md           # Example console outputs for orientation
│
├── transforms/                  # Data transformation and reporting modules
│   ├── triage_report.py         # Generates triage reports, public-share, and reference CSVs
│   ├── anomaly_scoring.py       # (Optional) Computes CTR/engagement anomalies
│   ├── entity_enrichment.py     # (Optional) Adds schema/entity intelligence fields
│   └── __init__.py              # Module initializer
│
├── docs/                        # Project documentation
│   └── data_dictionary.md
│
├── utils/                       # Placeholder for general helper scripts or utilities
│
├── etl_merge.py                 # Main ETL script
├── etl_config.yaml              # Config file for all input/output paths
├── Makefile                     # Setup and automation tasks
├── requirements.txt             # Python dependencies
├── LICENSE                      # MIT license
└── README.md                    # Project documentation and guide
```

**Note:**  
The `/samples/`, `/transforms/`, and `/utils/` directories are placeholders for future modules and helper utilities, such as enrichment transforms, reusable scripts, or additional demo datasets.

Only the `samples/triage_public_lite.csv` and `samples/triage_actions_reference.csv` files are intended for public sharing or educational use. The other files in the `samples/` directory are for internal use or reference only.

---

## Installation

Clone the repository:
```bash
git clone https://github.com/RNaimy/strategicai-visibility-loop-etl.git
cd strategicai-visibility-loop-etl
```

Run setup with the included Makefile:
```bash
make setup
```

This creates a `.venv` virtual environment, installs dependencies, and prepares your project for execution.

---

## Configuration

Edit `etl_config.yaml` to specify your CSV input paths and output settings:
```yaml
inputs:
  screaming_frog_csv: "data/screaming_frog_export.csv"
  gsc_csv: "data/gsc_export.csv"
  ga4_csv: "data/ga4_export.csv"

output:
  merged_csv: "merged/merged_visibility.csv"
  upload_to_sheets: false
```

---

## Usage

Run the ETL merge pipeline:
```bash
make run
```

Or manually execute:
```bash
python etl_merge.py
```

The merged dataset will be written to:
```
merged/merged_visibility.csv
```

To run the triage pipeline and generate prioritized insights, use:
```bash
make triage
```
This command generates multiple CSV and Markdown outputs in the `samples/` directory, including `triage_priority_demo.md` and `triage_public_lite.csv` — a sanitized, lightweight CSV designed for public sharing or educational use.

---

## Example Output Schema

| url | status_code | title | meta_description | click_depth | inlinks | gsc_clicks_90d | gsc_impressions_90d | ga4_sessions_90d |
|-----|--------------|-------|------------------|--------------|----------|----------------|---------------------|------------------|
| https://example.com/ | 200 | Home | Example site | 1 | 52 | 240 | 12,500 | 480 |

---

## Demo Data and NDA Compliance

All sample datasets in this repository mimic the production data structure but contain no sensitive or client-specific metrics. The demo data is fully synthetic, with randomized yet realistic SEO and GA4 values to demonstrate the ETL process without exposing confidential information.

Included demo files:
- `data/screaming_frog_export.csv`
- `data/gsc_export.csv`
- `data/ga4_export.csv`

Additionally, the `samples/triage_public_lite.csv` file is a sanitized, lightweight CSV specifically designed for public sharing, educational purposes, or presentations, ensuring compliance with NDA and data governance requirements.

You can regenerate synthetic datasets using built-in demo scripts or by enabling the `demo_mode: true` flag in `etl_config.yaml`. This facilitates testing of joins, schema mappings, and output structures in a safe environment.

---

## License

This project is licensed under the [MIT License](LICENSE) © 2025 Richard Naimy, [StrategicAILeader.com](https://www.strategicaileader.com/).

---

## Attribution

Created and maintained by **Richard Naimy** as part of the StrategicAILeader operational systems library for AI-driven SEO visibility and growth analytics.

---

## How to Use This System

### 1. Run the full pipeline
```bash
make setup
make run
```
This reads demo CSVs in `data/`, merges them, and writes outputs to `merged/`.

### 2. Files created after each run
- `merged/merged_visibility.csv` — unified crawl, GSC, and GA4 dataset
- `merged/anomaly_ctr_underperf.csv` — ranked CTR underperformance slice
- `merged/schema_gaps.csv` — pages missing schema or entity definitions
- `merged/ctr_priority_opportunities.csv` — stakeholder-ready triage list

### 3. Recommended setup
Install csvkit for command-line analysis:
```bash
.venv/bin/pip install csvkit
```

---

## Makefile Shortcuts

Add or use these make targets for fast insight pulls:

```make
# Top 20 CTR opportunities
make triage

# Schema coverage
make gaps

# Merged data quick view
make preview

# Column index view
make columns
```

**Shortcut explanations**
- **triage**: writes `ctr_priority_opportunities.csv` with ranked missed_clicks and empty analyst note fields  
- **gaps**: lists pages with missing or undefined schema_types  
- **preview**: prints first 15 merged rows  
- **columns**: prints numbered header list from merged dataset  

---

## Common Analysis Recipes

### A. Find high-rank, low-CTR pages
**Purpose:** fix weak titles, intent drift, or missing proof signals  
**Command:**  
```bash
make triage
```  
**Interpretation:** review the top 10 URLs by `missed_clicks` and log changes in the Prompt Log.

### B. Check structured data coverage
**Purpose:** ensure pages are eligible for AI Overviews and rich results  
**Command:**  
```bash
make gaps
```  
**Interpretation:** review `schema_types`, add missing Article, Product, or Organization schema.

### C. Review internal linking opportunities
**Purpose:** strengthen authority paths for deeper pages  
**Command:**  
```bash
python - << 'PY'
import pandas as pd
m = pd.read_csv('merged/merged_visibility.csv')
print(m[(m['impressions']>20000) & (m['inlinks']<20) & (m['click_depth']>2)][['url','impressions','inlinks','click_depth']].head(20))
PY
```
**Interpretation:** add two contextual internal links to these pages.

---

## Prompt Log Template

Store this in Notion, Airtable, or Google Sheets. Tool choice matters less than consistency.

| Date | Run ID | Question | Data Scope | Insight | Hypothesis | Action | Owner | Review Date | Result |
|------|--------|-----------|-------------|----------|-------------|---------|--------|--------------|---------|
| 2025‑11‑01 | 0fc1f680 | Why did CTR drop for top‑5 pages? | anomaly_ctr_underperf.csv | Titles lack pricing keywords | Add price bracket to top 10 | Update titles | SEO | 2025‑11‑15 | +0.4 pts CTR |

---

## KPI Tracking

| Metric | Description | Target |
|--------|--------------|--------|
| Time to insight | Minutes from data export to triage list | < 60 |
| False positive rate | % of triaged pages needing no action | < 20% |
| Action completion | % of recommended updates shipped | > 80% |
| CTR improvement | Average CTR lift after update | +0.3–1.0 pts |

---

## Governance and Notes

- All demo data are synthetic; no client or NDA data is included.  
- Each `run_id` is timestamped for reproducibility.  
- Analysts should validate outputs before sharing externally.  
- Use Git for versioning prompt logs and triage files where possible.  
- Older files in the `samples/` directory may be trimmed or anonymized for release; contributors should verify filenames before publication.

---

## Changelog

**2024-06-01**  
- Added public-share CSV pipeline with `triage_public_lite.csv` for safe educational and presentation use.  
- Improved governance notes and clarified demo data compliance.