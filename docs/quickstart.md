

# Quickstart Guide

## 1. Overview
This tool merges SEO visibility data from Screaming Frog, Google Search Console (GSC), and Google Analytics 4 (GA4) into one unified dataset:  
**Output:** `merged/merged_visibility.csv`

## 2. Requirements
- Python 3.9+
- `make`
- Internet access (optional, required for GA4/GSC API integration)
- CSV exports from each platform

## 3. Installation
Clone the repository and set up dependencies:
```bash
git clone https://github.com/RNaimy/strategicai-visibility-loop-etl.git
cd strategicai-visibility-loop-etl
make setup
```

## 4. Configure Your Environment
You can either:
- **Export your data manually** into the `/data_demo/` folder (using the expected CSV format), **OR**
- **Configure paths** by creating a `.envrc` file (or copying from `.envrc.example`) with:
  ```bash
  export FROG_CSV_PATH="data_demo/screaming_frog_export.csv"
  export GSC_CSV_PATH="data_demo/gsc_export.csv"
  export GA4_CSV_PATH="data_demo/ga4_export.csv"
  ```

## 5. Run the ETL
Start the merging process:
```bash
make run
```
**Result:**  
A cleaned, combined SEO dataset at `merged/merged_visibility.csv`.

## 6. Optional: View Results
Preview the merged data in your terminal:
```bash
csvlook merged/merged_visibility.csv | head -n 20
```

## 7. Learn More
For detailed guides and methodology, visit [StrategicAILeader.com/resources](https://www.strategicaileader.com/resources).

> This open-source ETL is part of the StrategicAI Visibility Loop project — helping SEOs unify crawl, search, and analytics data into one actionable layer.