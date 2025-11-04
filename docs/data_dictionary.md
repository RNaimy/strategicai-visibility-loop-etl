# Input Data Sources (GA4 & GSC)

This section details the specific reports to download from Google Analytics 4 (GA4) and Google Search Console (GSC) to serve as inputs for the ETL merge process, which produces a unified file: `merged/merged_visibility.csv`.

## 1. Google Analytics 4 (GA4) Input

**Report Path:**  
`Reports > Engagement > Pages and Screens`

**Required Columns:**  

| Column Name          | Description                                  |
|----------------------|----------------------------------------------|
| `url`                | Page URL, normalized to lowercase and stripped of tracking parameters. |
| `users`              | Number of unique users visiting the page.   |
| `sessions`           | Total sessions initiated on the page.       |
| `engaged_sessions`   | Sessions with meaningful engagement.        |
| `engagement_rate`    | Ratio of engaged sessions to total sessions (0 to 1). |
| `avg_engagement_time`| Average engagement time in seconds.          |

**Optional Advanced Columns:**  

| Column Name  | Description                              |
|--------------|------------------------------------------|
| `conversions`| Number of goal completions or conversions attributed to the page. |
| `revenue`    | Revenue generated from the page (if e-commerce tracking is enabled). |

## 2. Google Search Console (GSC) Input

**Report Path:**  
`Performance > Search Results > Pages tab`  
Export the full table as CSV.

**Required Columns:**  

| Column Name | Description                                |
|-------------|--------------------------------------------|
| `url`       | Page URL, normalized and cleaned.          |
| `clicks`    | Number of clicks from search results.      |
| `impressions`| Number of times the page appeared in search results. |
| `ctr`       | Click-through rate (clicks / impressions), decimal between 0 and 1. |
| `position`  | Average search ranking position.            |

**Optional Columns:**  

| Column Name | Description                               |
|-------------|-------------------------------------------|
| `query`     | Search query leading to the page.         |
| `device`    | Device category (desktop, mobile, tablet).|
| `country`   | Country from which the search was made.   |

## 3. Recommended Folder Setup

Organize your input CSV files in a `data/` folder as follows:

```
data/
├── ga4_export.csv            # GA4 Pages and Screens report export
├── gsc_export.csv            # GSC Search Results Pages export
└── screaming_frog_export.csv # Optional Screaming Frog crawl export
```

## 4. Summary Table

| Data Source       | Key Fields                             | Purpose                                    |
|-------------------|--------------------------------------|--------------------------------------------|
| Google Analytics 4 | `url`, `users`, `sessions`, `engaged_sessions`, `engagement_rate`, `avg_engagement_time` | Audience size, engagement quality, and behavior analysis |
| Google Search Console | `url`, `clicks`, `impressions`, `ctr`, `position` | Search demand, visibility, and ranking insights |
| Screaming Frog    | `url`, `status_code`, `title`, `meta_description`, `word_count`, `click_depth`, `inlinks`, `schema_types` | Crawl metadata for SEO health and content quality |

**Output:**  
The ETL produces a single merged dataset — `merged/merged_visibility.csv` — combining analytics, search, and crawl data into one unified visibility view.

## Column Legend

| Field Type | Description                                                   |
|------------|---------------------------------------------------------------|
| Required   | Essential fields that must be present for the ETL to run.     |
| Optional   | Fields that improve analysis but are not mandatory.           |
| Derived    | Fields calculated or generated during the ETL process.        |

## Minimum Viable Inputs

- **Required Fields:**  
  - `url`  
  - `clicks`  
  - `impressions`  
  - `ctr`  
  - `position`  
  - `run_id`  
  - `run_timestamp`

- **Optional but Recommended Fields:**  
  - `status_code`  
  - `title`  
  - `meta_description`  
  - `word_count`  
  - `click_depth`  
  - `inlinks`  
  - `schema_types`  
  - `users`  
  - `sessions`  
  - `engaged_sessions`  
  - `avg_engagement_time`  
  - Source fields like `source_frog`, `source_gsc`, `source_ga4`

## Lite Mode

When crawl data from Screaming Frog is missing or unavailable, the ETL can run in Lite Mode. In this mode, crawl fields such as `status_code`, `title`, `meta_description`, `word_count`, `click_depth`, `inlinks`, and `schema_types` are treated as optional. The ETL attempts to infer some metadata from URL patterns and available analytics and search performance data to maintain baseline analysis capabilities.

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

## Field Example Values

| Field              | Example Value                          |
|--------------------|-------------------------------------|
| url                | `https://example.com/products/widget` |
| status_code        | 200                                 |
| title              | "Example Widget - Buy Now"           |
| meta_description   | "Discover the best example widget." |
| word_count         | 450                                 |
| click_depth        | 3                                   |
| inlinks            | 12                                  |
| schema_types       | "Product,Review"                    |
| clicks             | 120                                 |
| impressions       | 1500                                |
| ctr                | 0.08                                |
| ctr_pct            | 8.00                                |
| position           | 2.3                                 |
| users              | 100                                 |
| sessions           | 150                                 |
| engaged_sessions   | 75                                  |
| engagement_rate    | 0.5                                 |
| engagement_rate_pct| 50.0                                |
| avg_engagement_time| 180                                 |
| click_share        | 0.002                               |
| run_id             | "20240601_run_001"                   |
| run_timestamp      | 1711929600                          |
| source_frog        | "crawl_20240601.csv"                 |
| source_gsc         | "gsc_20240601.csv"                   |
| source_ga4         | "ga4_20240601.csv"                   |

## Future Extensions

Potential additional fields for future ETL enhancements include:  
- Core Web Vitals (e.g., LCP, FID, CLS)  
- Backlinks and referring domains  
- Conversion metrics (goals, transactions)  
- Indexability and crawl status flags  
- Last modified dates from sitemaps or HTTP headers

## Notes

- All demo data is synthetic for NDA compliance and reproducibility.  
- Store the Prompt Log in Notion, Airtable, or Google Sheets. Tool choice matters less than consistency.  
- Future versions may include JSON schema validation for input files (`docs/data_schema.json`) to ensure data consistency and integrity.  
- In the public release, only the merged visibility dataset is generated. Diagnostic and triage files are available in the advanced version.