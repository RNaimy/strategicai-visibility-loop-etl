# Data Dictionary — StrategicAI Visibility Loop ETL

This reference explains every column produced in `merged/merged_visibility.csv`. Use it when validating inputs, creating dashboards, and reviewing anomalies.

## URL and Crawl Fields
- **url**  
  Type: string  
  Source: Screaming Frog (Address), normalized  
  Notes: Lowercase host, tracking params stripped, trailing slash removed except root. Primary join key across sources.

- **status_code**  
  Type: integer  
  Source: Screaming Frog (Status Code)  
  Usage: Filter 200 for analysis. Investigate 3xx, 4xx, 5xx.

- **title**  
  Type: string  
  Source: Screaming Frog (Title 1)  
  Usage: Check intent match and CTR outliers.

- **meta_description**  
  Type: string  
  Source: Screaming Frog (Meta Description 1)  
  Usage: Compare against query intent when CTR underperforms.

- **word_count**  
  Type: integer  
  Source: Screaming Frog (Word Count)  
  Usage: Thin-content checks. Use with impressions and CTR.

- **click_depth**  
  Type: integer  
  Source: Screaming Frog (Crawl Depth)  
  Usage: Find deep pages with strong demand that need internal links.

- **inlinks**  
  Type: integer  
  Source: Screaming Frog (Inlinks)  
  Usage: Internal link equity. Low inlinks on high-demand pages signal link work.

- **schema_types**  
  Type: string  
  Source: Screaming Frog (Structured Data)  
  Usage: Coverage checks for entity clarity and eligibility in AI results.

## Search Performance (GSC)
- **clicks**  
  Type: number  
  Source: GSC (Clicks)  
  Usage: Demand proxy. Use with click_share.

- **impressions**  
  Type: number  
  Source: GSC (Impressions)  
  Usage: Visibility baseline. Use for impression-drop slices.

- **ctr**  
  Type: number, 0 to 1, rounded to 4 decimals  
  Source: GSC (CTR)  
  Usage: Diagnose title and snippet issues.

- **ctr_pct**  
  Type: percentage, rounded to 2 decimals  
  Calc: `ctr * 100`  
  Usage: Reporting friendly CTR.

- **position**  
  Type: number, rounded to 1 decimal  
  Source: GSC (Position)  
  Usage: Rank context for CTR analysis.

## Analytics (GA4)
- **users**  
  Type: number  
  Source: GA4 (Users or totalUsers)  
  Usage: Audience size context.

- **sessions**  
  Type: number  
  Source: GA4 (Sessions)  
  Usage: Demand quality proxy.

- **engaged_sessions**  
  Type: number  
  Source: GA4 (Engaged sessions)  
  Usage: Engagement quality.

- **engagement_rate**  
  Type: number, 0 to 1, rounded to 4 decimals  
  Calc: `engaged_sessions / sessions`  
  Usage: Intent fit. Low rate with high CTR signals mismatch.

- **engagement_rate_pct**  
  Type: percentage, rounded to 1 decimal  
  Calc: `engagement_rate * 100`  
  Usage: Reporting friendly engagement rate.

- **avg_engagement_time**  
  Type: integer seconds  
  Source: GA4 (Average engagement time or averageSessionDuration)  
  Usage: Depth of engagement.

## Derived and Metadata
- **click_share**  
  Type: number, rounded to 6 decimals  
  Calc: `clicks / sum(clicks)` within the merged dataset  
  Usage: Relative demand across pages in the run.

- **run_id**  
  Type: string  
  Source: ETL generated  
  Usage: Correlate outputs and logs from the same run.

- **run_timestamp**  
  Type: unix epoch seconds  
  Source: ETL generated  
  Usage: Audit trail and comparisons between runs.

- **source_frog, source_gsc, source_ga4**  
  Type: string  
  Source: Input filenames  
  Usage: Provenance and reproducibility.

## Anomaly Slices (separate CSV outputs)
- **merged/anomaly_ctr_underperf.csv**  
  Definition: URLs with `position <= 5` and `ctr < median(ctr)` for the run  
  Goal: Find pages with strong rank but weak CTR.

- **merged/schema_gaps.csv**  
  Definition: URLs with missing or blank `schema_types`  
  Goal: Fix structured data coverage.

### Additional Anomaly Columns
- **missed_clicks**  
  Type: number, rounded to 3 decimals  
  Calc: `(median(ctr) - ctr) * impressions`, clipped at zero  
  Usage: Quantifies lost clicks compared to median CTR among top-5-ranked pages.

- **priority_rank**  
  Type: integer  
  Calc: Rank of missed_clicks descending, 1 = highest opportunity  
  Usage: Triage sequence for analyst review.

- **intent_note**  
  Type: string  
  Usage: Analyst annotation field for qualitative triage notes (intent, snippet, competitor angle).

### Triage Export
- **merged/ctr_priority_opportunities.csv**  
  Definition: Top opportunity pages ranked by missed_clicks, containing only triage columns  
  Goal: Share concise opportunity list with cross-functional teams without exposing full crawl or analytics data.

## Notes
- All demo data is synthetic for NDA compliance and reproducibility.  
- Store the Prompt Log in Notion, Airtable, or Google Sheets. Tool choice matters less than consistency.