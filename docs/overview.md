# StrategicAI Visibility Loop ETL

**StrategicAI Visibility Loop ETL** is a minimal, open-source pipeline that merges **Screaming Frog**, **Google Search Console (GSC)**, and **Google Analytics 4 (GA4)** CSV exports into a single, unified visibility dataset.

### Purpose
This project helps SEOs, analysts, and operators standardize their data into one clear source of truth for visibility diagnostics and content optimization.

### Highlights
- ✅ Works completely offline — no API keys required  
- 🧠 Reproducible ETL logic for local or shared workflows  
- 📊 Outputs a single file: `merged/merged_visibility.csv`  
- 🧩 Normalizes metrics across crawler, search, and analytics data  

### Output
The merged CSV includes unified metrics per URL such as:
- CTR and impression data from GSC
- Engagement and session data from GA4
- Crawl depth, word count, and metadata from Screaming Frog

---

**Author:** StrategicAILeader.com  
**License:** MIT  
**Repository:** [GitHub.com/RNaimy/strategicai-visibility-loop-etl](https://github.com/RNaimy/strategicai-visibility-loop-etl)