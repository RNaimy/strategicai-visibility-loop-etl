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
│   ├── anomaly_scored.csv
│   ├── schema_gaps.csv
│   └── triage_priority.csv
│
├── samples/                     # Triage examples and demo datasets
│   ├── triage_public_lite.csv        # Lightweight, sanitized CSV safe for sharing
│   ├── triage_actions_seed.csv       # Seed checklist of actions/playbooks
│   ├── triage_priority_TOP20.md      # Top 20 triage output for human preview
│   ├── triage_priority_TOP20.csv     # Top 20 triage output in CSV
│   └── triage_priority_pretty.csv    # Pretty-formatted triage preview table
│
├── transforms/                  # Data transformation and reporting modules
# (Note: public-share CSVs are generated directly by triage_report.py)
│   ├── triage_report.py         # Generates triage reports, public-share, and reference CSVs
│   ├── anomaly_scoring.py       # (Optional) Computes CTR/engagement anomalies
│   ├── entity_enrichment.py     # (Optional) Adds schema/entity intelligence fields
│   └── __init__.py              # Module initializer
│
├── docs/                        # Project documentation
│   └── data_dictionary.md
│
├── scripts/                     # Helper shell scripts for setup and ETL runs
│   ├── dev_setup.sh             # Developer setup script
│   └── run_full_etl.sh          # One-click full ETL execution helper
│
├── logs/                        # Run and ETL diagnostic logs
│   ├── etl_autodetect.csv       # Auto-detection log of input and config state
│   └── runs.csv                 # Logged pipeline run metadata and timestamps
│
├── utils/                       # Placeholder for general helper scripts or utilities
│
├── etl_merge.py                 # Main ETL script
├── etl_config.yaml              # Config file for all input/output paths
├── Makefile                     # Setup and automation tasks
├── requirements.txt             # Python dependencies
├── LICENSE                      # MIT license
├── SECURITY.md                  # Security policy and disclosure guidelines
├── requirements-lock.txt         # Frozen dependency versions for reproducibility
└── README.md                    # Project documentation and guide
```

The `transforms/` directory includes the main triage and anomaly modules. Public-share CSVs (like `samples/triage_public_lite.csv`) are now generated automatically by `triage_report.py` — no separate `triage_public_lite.py` file is needed.

**Note:**  
The `/samples/`, `/transforms/`, and `/utils/` directories are placeholders for future modules and helper utilities, such as enrichment transforms, reusable scripts, or additional demo datasets.

Only samples/triage_public_lite.csv is intended for public sharing. The other items in samples/ are internal previews or seeds for analyst use.

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
  upload_to_sheets: false
```

**Note:** Merges are dynamically produced from `data/` inputs and no longer rely on a single `merged_visibility.csv` file.

---

## Developer Setup

To get the development environment ready and run the full ETL pipeline, follow these steps:

- Run the setup script:
  ```bash
  scripts/dev_setup.sh
  ```
  This script will:
  - Create a Python virtual environment (`.venv`)
  - Install all required dependencies from `requirements.txt`
  - Create necessary folders such as `data/`, `merged/`, and `samples/`
  - Prepare configuration files if needed

- To run the full ETL pipeline with helper scripts, use:
  ```bash
  scripts/run_full_etl.sh
  ```
  This script automates the entire ETL process, including merging, transformation, and output generation, to streamline development and testing.

---

## Output Files

| File Name                      | Purpose                                                  |
|-------------------------------|----------------------------------------------------------|
| `merged/merged_visibility.csv`       | Canonical merged spine used by all transforms           |
| `merged/anomaly_ctr_underperf.csv`   | Ranked pages with CTR underperformance anomalies        |
| `merged/anomaly_scored.csv`          | CTR anomaly rows with standardized z-scores             |
| `merged/schema_gaps.csv`             | Pages missing schema or undefined `schema_types`        |
| `merged/triage_priority.csv`         | Primary triage export with playbook, why, effort hint   |
| `samples/triage_public_lite.csv`     | Lightweight, sanitized CSV safe for public sharing      |
| `samples/triage_actions_seed.csv`    | Seed checklist of actions/playbooks for analysts        |
| `samples/triage_priority_TOP20.md`   | Top 20 triage output for human preview                  |
| `samples/triage_priority_TOP20.csv`  | Top 20 triage output in CSV                             |
| `samples/triage_priority_pretty.csv` | Pretty-formatted triage preview table                   |

---

## Usage

### Required Input Files

To ensure accurate merges, export these reports from your analytics tools and place them in the `data/` folder.

**Google Analytics 4 (GA4)**  
Export the **Pages and Screens** report with:
- Page path and screen class (as `url`)
- Views  
- Users  
- Sessions  
- Engaged sessions  
- Engagement rate  
- Average engagement time  

**Google Search Console (GSC)**  
Export the **Search Results → Pages tab** with columns:
- Page (as `url`)  
- Clicks  
- Impressions  
- CTR  
- Position  

**Optional: Screaming Frog SEO Spider**  
Include crawl-level context (status_code, title, inlinks, schema_types, etc.) if available.

Example structure:
data/
- ga4_export.csv
- gsc_export.csv
- screaming_frog_export.csv

For full schema and mapping details, see [docs/data_dictionary.md](docs/data_dictionary.md).

Run the ETL merge pipeline:
```bash
make run
```

To run the triage pipeline and generate prioritized insights, use:
```bash
make triage
```
This command generates multiple CSV and Markdown outputs in the samples/ directory, including triage_priority_TOP20.md, triage_priority_TOP20.csv, triage_priority_pretty.csv, and triage_public_lite.csv — a sanitized CSV designed for public sharing.

**Reference:** For detailed column definitions and schema information, please check `docs/data_dictionary.md`.

---

## Example Output Schema

| url | status_code | title | meta_description | word_count | click_depth | inlinks | schema_types | clicks | impressions | ctr | position |
|-----|-------------|-------|------------------|-----------:|------------:|--------:|--------------|-------:|------------:|----:|---------:|
| https://example.com/blogs/news/example | 200 | Example Title | Example meta description | 950 | 3 | 18 | Article, BreadcrumbList, Organization | 120 | 24,500 | 0.0049 | 11.2 |

---

## Troubleshooting

If you encounter issues running the ETL pipeline or setup scripts, here are some common problems and solutions:

- **Error: `direnv: command not found`**  
  *Solution:* Install `direnv` on your system and enable it in your shell. For example, on macOS with Homebrew:  
  ```bash
  brew install direnv
  ```
  Then add `eval "$(direnv hook bash)"` (or your shell) to your shell profile.

- **csvkit not found when using aliases**  
  *Solution:* Either install csvkit inside the venv or call the tools with absolute paths:  
  `.venv/bin/pip install csvkit` or use `.venv/bin/csvlook` / `.venv/bin/csvcut`.

- **Virtual environment not activating or missing dependencies**  
  *Solution:* Ensure you run the setup script `scripts/dev_setup.sh` to create and populate the `.venv` environment. Activate it manually with:  
  ```bash
  source .venv/bin/activate
  ```

- **Missing input files or wrong paths**  
  *Solution:* Verify your `etl_config.yaml` points to the correct CSV files in the `data/` directory. Make sure files are named correctly and accessible.

- **Permission denied when running scripts**  
  *Solution:* Make sure the scripts have execution permissions:  
  ```bash
  chmod +x scripts/*.sh
  ```

If problems persist, consult the project documentation or open an issue on the repository.

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
- `merged/merged_visibility.csv` — canonical merged spine
- `merged/anomaly_ctr_underperf.csv` — ranked CTR underperformance slice
- `merged/schema_gaps.csv` — pages with missing or undefined schema_types
- `merged/triage_priority.csv` — prioritized opportunities with playbook + why + effort
- `samples/triage_priority_TOP20.md` / `.csv` — quick-share top 20 preview
- `samples/triage_priority_pretty.csv` — readable triage table for reviews
- `samples/triage_public_lite.csv` — sanitized CSV suitable for public sharing

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
- **triage**: writes `merged/triage_priority.csv` plus sample previews in `samples/`
- **gaps**: lists pages with missing or undefined `schema_types`
- **preview**: prints first 15 rows from `merged/merged_visibility.csv`
- **columns**: prints numbered header list from the merged dataset

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

---

## Repository Status

This repository is now production-ready for public and educational sharing, with governance and documentation finalized to ensure transparency, compliance, and ease of use.