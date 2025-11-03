"""
StrategicAI Visibility Loop ETL (config-driven)
Reads Screaming Frog, GSC, and GA4 CSV exports, merges on URL, normalizes fields, computes derived metrics,
and writes a clean dataset plus simple anomaly slices.
Configuration is loaded from etl_config.yaml.
"""

import os
import sys
import uuid
import time
import pandas as pd
import yaml
import numpy as np  # for weighted means if needed
from urllib.parse import urlparse, urlunparse, parse_qsl, urlencode
from datetime import datetime, timezone

# -------------------------------------------------
# Input path resolution: accept csv or xlsx via config or extension swap
# -------------------------------------------------

def _swap_ext(fp: str) -> str:
    if not isinstance(fp, str):
        return fp
    base, ext = os.path.splitext(fp)
    if ext.lower() == ".csv":
        return base + ".xlsx"
    if ext.lower() in {".xlsx", ".xls"}:
        return base + ".csv"
    # default: prefer csv alternative
    return fp + ".csv"


def _resolve_input_from_config(cfg: dict, key_csv: str, default_csv: str) -> str:
    """
    Resolve an input file path from config with flexibility:
    - Accepts either explicit `*_csv` or sibling `*_xlsx` keys
    - If the configured path doesn't exist, try swapping .csv/.xlsx
    - Finally, try falling back to the provided default path and its swapped extension
    Returns a normalized, existing path string. If none exist, returns the last attempted path.
    """
    inputs = (cfg.get("inputs", {}) or {})
    # 1) primary key (usually *_csv)
    cand = inputs.get(key_csv, default_csv)
    cand = _resolve_any_path(cand)
    tried = [cand]

    # 2) sibling explicit xlsx key if present (e.g., gsc_xlsx)
    key_xlsx = key_csv.replace("_csv", "_xlsx")
    if key_xlsx in inputs:
        cand_xlsx = _resolve_any_path(inputs.get(key_xlsx))
        tried.append(cand_xlsx)
        if os.path.exists(cand_xlsx):
            return cand_xlsx

    # 3) try swapping extension on primary
    alt = _swap_ext(cand)
    if alt != cand:
        tried.append(alt)
        if os.path.exists(alt):
            return alt

    # 4) try default and its swap explicitly
    defp = _resolve_any_path(default_csv)
    if defp not in tried:
        tried.append(defp)
        if os.path.exists(defp):
            return defp
    defp_swap = _swap_ext(defp)
    if defp_swap not in tried:
        tried.append(defp_swap)
        if os.path.exists(defp_swap):
            return defp_swap

    # Return the first candidate (may not exist) so caller can error out consistently
    return cand

# -------------------------------------------------
# Generic loader: supports .csv and .xlsx for all inputs
# -------------------------------------------------
def _resolve_any_path(p: str) -> str:
    """Expand user and env vars; return normalized string path."""
    if not isinstance(p, str):
        return p
    p = os.path.expanduser(os.path.expandvars(p))
    return p

def load_table_any(path: str) -> pd.DataFrame:
    """
    Load a table from CSV or Excel. Returns a DataFrame of strings.
    - .xlsx/.xls -> read_excel
    - otherwise  -> robust CSV reader with fallbacks
    """
    path = _resolve_any_path(path)
    ext = os.path.splitext(path)[1].lower()
    if ext in {".xlsx", ".xls"}:
        # Excel often has mixed dtypes; force strings to keep downstream normalization predictable
        return pd.read_excel(path, dtype=str)
    # CSV: try fast path then python engine with dialect sniffing
    try:
        return pd.read_csv(path, dtype=str, low_memory=False)
    except Exception:
        # engine="python" with sep=None lets pandas sniff delimiters; also ignore comment lines
        try:
            return pd.read_csv(path, dtype=str, engine="python", sep=None, comment="#")
        except Exception:
            # Last resort: read with default settings to at least surface the error downstream
            return pd.read_csv(path, dtype=str, low_memory=False, engine="python")

# -------------------------------------------------
# Robust column resolution and normalization helpers
# -------------------------------------------------

# Canonical fields we try to produce in the merged dataset
CANON_FIELDS = {
    "url": None,
    "status_code": None,
    "title": "",
    "meta_description": "",
    "click_depth": None,
    "inlinks": None,
    "word_count": None,
    "schema_types": "",
    "clicks": None,
    "impressions": None,
    "ctr": None,
    "position": None,
    "users": None,
    "sessions": None,
    "engaged_sessions": None,
    "avg_engagement_time": None,
}

# Synonyms across tools (lowercased, stripped of spaces/underscores/punctuation)
SYNONYMS = {
    # Core URL variants
    "url": {"url", "address", "page", "pageurl", "landingpage", "pagelocation", "pagepath", "pagepathquerystring", "pagepath+querystring"},

    # Screaming Frog field variants
    "status_code": {"statuscode", "status", "httpstatus"},
    "title": {"title", "title1", "pagetitle"},
    "meta_description": {"metadescription", "metadescription1", "description"},
    "click_depth": {"crawldepth", "depth", "clickdepth"},
    "inlinks": {"inlinks", "inboundlinks"},
    "word_count": {"wordcount", "words"},
    "schema_types": {"structureddata", "schematypes", "schema", "structuredcontent"},

    # GSC
    "clicks": {"clicks", "gscclicks", "totalclicks"},
    "impressions": {"impressions", "gscimpressions", "totalimpressions"},
    "ctr": {"ctr", "gscctr"},
    "position": {"position", "avgposition", "gscposition", "avgpos"},

    # GA4
    "users": {"users", "totalusers", "activeusers"},
    "sessions": {"sessions"},
    "engaged_sessions": {"engagedsessions"},
    "avg_engagement_time": {"avgengagementtime", "averagesessionduration", "averageengagementtime"},
}

# -------------------------------------------------
# URL autodetection helpers
# -------------------------------------------------

def looks_url_like(val: str) -> bool:
    try:
        s = str(val or "").strip().lower()
    except Exception:
        return False
    if not s:
        return False
    return s.startswith("http://") or s.startswith("https://") or s.startswith("/")


def autodetect_url_column(df: pd.DataFrame) -> str | None:
    """
    Try to find a column that contains URL-looking values when we don't have a clear 'url' header.
    Heuristics:
      1) Prefer columns whose slug matches known URL synonyms and whose values look like URLs.
      2) Otherwise, scan all columns and choose the first with many URL-looking values.
    """
    # 1) Prefer known synonyms by header name
    url_syns = list(SYNONYMS.get("url", set())) + ["url"]
    # Build slug map, but skip columns that slug to empty or look like comment/separator headers
    slug_to_col = {}
    for c in df.columns:
        s = _slug(c)
        if not s:
            continue
        if str(c).strip().startswith("#"):
            continue
        slug_to_col[s] = c
    for syn in url_syns:
        slug = _slug(syn)
        if slug in slug_to_col:
            c = slug_to_col[slug]
            # Verify values look like URLs for at least a few rows
            sample = df[c].head(50)
            if sample.map(looks_url_like).sum() >= max(3, int(len(sample) * 0.1)):
                return c

    # 2) Fallback: scan all columns to find the best candidate
    best_col = None
    best_hits = 0
    for c in df.columns:
        # Skip noisy headers and empty slugs
        if not _slug(c) or str(c).strip().startswith("#"):
            continue
        try:
            sample = df[c].head(50)
            hits = sample.map(looks_url_like).sum()
            if hits > best_hits:
                best_hits = hits
                best_col = c
        except Exception:
            continue
    if best_col and best_hits >= 3:
        return best_col
    return None


def write_autodetect_log(source_name: str, mapping: dict, autodetected: str | None, df_rows: int):
    """
    Optionally append a CSV line to an autodetect log.
    Controlled by:
      - ETL_AUTODETECT_LOG: bool, default True
      - ETL_AUTODETECT_LOG_PATH: path, default logs/etl_autodetect.csv
    """
    if not get_env_bool("ETL_AUTODETECT_LOG", True):
        return
    log_path = os.getenv("ETL_AUTODETECT_LOG_PATH", "logs/etl_autodetect.csv")
    ensure_parent_dir(log_path)
    import csv
    present = [k for k, v in mapping.items() if v is not None]
    missing = [k for k, v in mapping.items() if v is None]
    with open(log_path, "a", newline="") as fh:
        writer = csv.writer(fh)
        if fh.tell() == 0:
            writer.writerow(["timestamp_utc", "source", "rows", "autodetected_url_col", "mapped_fields", "missing_fields"])
        writer.writerow([
            datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            source_name,
            df_rows,
            autodetected or "",
            ";".join(present),
            ";".join(missing),
        ])

def _slug(name: str) -> str:
    """Lowercase and strip non-alphanumerics for fuzzy header matching."""
    name = str(name or "").lower()
    return "".join(ch for ch in name if ch.isalnum())

def resolve_columns(df: pd.DataFrame, wanted: list[str]) -> dict:
    """
    Return mapping canonical_name -> existing_column_name (or None if missing),
    using case/space-insensitive matching plus synonyms.
    """
    existing = { _slug(c): c for c in df.columns }
    out = {}
    for canon in wanted:
        found = None
        # exact
        if _slug(canon) in existing:
            found = existing[_slug(canon)]
        else:
            for syn in SYNONYMS.get(canon, set()):
                key = _slug(syn)
                if key in existing:
                    found = existing[key]
                    break
        out[canon] = found
    return out

def coalesce(df: pd.DataFrame, mapping: dict) -> pd.DataFrame:
    """Build a new DataFrame with canonical columns, filling defaults for missing ones."""
    out = pd.DataFrame(index=df.index)
    for canon, src in mapping.items():
        if src is not None:
            out[canon] = df[src]
        else:
            out[canon] = CANON_FIELDS.get(canon, None)
    return out

def to_float(series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")

def to_int(series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce").astype("Int64")

def to_float_nocomma(series) -> pd.Series:
    """Parse floats while tolerating thousand separators like '1,234.5'."""
    return pd.to_numeric(series.astype(str).str.replace(",", "", regex=False).str.strip(), errors="coerce")

def normalize_ctr(series) -> pd.Series:
    """
    Normalize GSC CTR to fraction:
    - Accepts values like '5.2%', '5,2 %', '0.052', or '5.2'
    - If a percent sign is present OR majority of values are between 1 and 100, divide by 100
    """
    s = series.astype(str)
    saw_percent = s.str.contains("%", na=False).any()
    cleaned = (
        s.str.replace("%", "", regex=False)
         .str.replace(",", "", regex=False)
         .str.strip()
    )
    vals = pd.to_numeric(cleaned, errors="coerce")
    # Heuristic: if formats look like percentages, scale to fraction
    if saw_percent or (((vals > 1.0) & (vals <= 100.0)).mean(skipna=True) > 0.5):
        vals = vals / 100.0
    return vals

def log_field_summary(name: str, mapping: dict):
    present = [k for k,v in mapping.items() if v is not None]
    missing = [k for k,v in mapping.items() if v is None]
    print(f"[merge] {name}: mapped={present} missing={missing}")

# -------------------------------------------------
# Config
# -------------------------------------------------

def read_config(path: str = "etl_config.yaml") -> dict:
    if not os.path.exists(path):
        print(f"[ERROR] Config file not found: {path}")
        sys.exit(1)
    with open(path, "r") as f:
        return yaml.safe_load(f)

# -------------------------------------------------
# Config-driven scoring utilities
# -------------------------------------------------

# Helper: Normalize expected CTR value according to config units and uplift
def _normalize_expected_ctr_value(val, cfg: dict) -> float:
    """
    Normalizes an expected CTR value from config according to units and optional uplift.
    - scoring.expected_ctr_units: 'fraction' (default) or 'percent'
    - scoring.expected_ctr_uplift: float, e.g. 0.15 to increase baselines by 15%
    """
    try:
        v = float(val)
    except Exception:
        return 0.0
    scoring = (cfg.get("scoring", {}) or {})
    units = str(scoring.get("expected_ctr_units", "fraction")).lower().strip()
    if units == "percent":
        v = v / 100.0
    # Optional uplift to make demos more sensitive or reflect business targets
    uplift = float(scoring.get("expected_ctr_uplift", 0.0) or 0.0)
    if uplift:
        v = v * (1.0 + uplift)
    return v

def _parse_pos_bucket_key(k: str):
    """
    Accepts bucket keys like '1-3', '3.1-5', or single values like '1', returns (lo, hi).
    """
    k = str(k).strip()
    if "-" in k:
        lo_s, hi_s = k.split("-", 1)
        try:
            return float(lo_s), float(hi_s)
        except Exception:
            return None
    try:
        v = float(k)
        return v, v
    except Exception:
        return None

def expected_ctr_for_position(position: float, cfg: dict, fallback: float) -> float:
    """
    Looks up an expected CTR for a given position based on cfg['scoring']['expected_ctr_by_position'].
    If no bucket matches, returns fallback.
    """
    try:
        buckets = (cfg.get("scoring", {}) or {}).get("expected_ctr_by_position", {}) or {}
        best = None
        for k, val in buckets.items():
            rng = _parse_pos_bucket_key(k)
            if not rng:
                continue
            lo, hi = rng
            if position is not None and lo <= position <= hi:
                best = _normalize_expected_ctr_value(val, cfg)
                break
        return float(best) if best is not None else _normalize_expected_ctr_value(fallback, cfg)
    except Exception:
        return _normalize_expected_ctr_value(fallback, cfg)

def expected_ctr_with_bucket(position: float, cfg: dict, fallback: float) -> tuple[float, str]:
    """
    Looks up an expected CTR and returns a tuple (expected_ctr_value, bucket_key_string).
    If no bucket matches, returns (fallback, 'fallback_median').
    On error, returns (fallback, 'fallback_error').
    """
    try:
        buckets = (cfg.get("scoring", {}) or {}).get("expected_ctr_by_position", {}) or {}
        for k, val in buckets.items():
            rng = _parse_pos_bucket_key(k)
            if not rng:
                continue
            lo, hi = rng
            if position is not None and lo <= position <= hi:
                return _normalize_expected_ctr_value(val, cfg), str(k)
        return _normalize_expected_ctr_value(fallback, cfg), "fallback_median"
    except Exception:
        return _normalize_expected_ctr_value(fallback, cfg), "fallback_error"

def intent_multiplier_for_row(row: pd.Series, cfg: dict) -> float:
    """
    Returns a multiplier based on simple intent hints.
    Checks cfg['scoring']['intent_multipliers'] and cfg['mappings']['url_intent_hints'].
    """
    default_mult = 1.0
    scoring = cfg.get("scoring", {}) or {}
    multipliers = scoring.get("intent_multipliers", {}) or {}
    mappings = cfg.get("mappings", {}) or {}
    url_hints = mappings.get("url_intent_hints", {}) or {}

    # URL hint match
    url = str(row.get("url", "") or "")
    for hint, intent in url_hints.items():
        if hint and hint in url:
            return float(multipliers.get(intent, default_mult))

    # Fallback by schema_types
    schema = str(row.get("schema_types", "") or "").lower()
    if "product" in schema:
        return float(multipliers.get("transactional", default_mult))
    if "article" in schema or "blogposting" in schema:
        return float(multipliers.get("informational", default_mult))
    return default_mult

# -------------------------------------------------
# Utilities
# -------------------------------------------------

def ensure_parent_dir(path: str):
    parent = os.path.dirname(path)
    if parent and not os.path.exists(parent):
        os.makedirs(parent, exist_ok=True)


# Helper: Read boolean-like env vars robustly
def get_env_bool(name: str, default: bool = False) -> bool:
    """
    Read a boolean-like environment variable.
    Accepts 1/true/yes/y/on (case-insensitive) as True, 0/false/no/n/off as False.
    If unset, returns default.
    """
    val = os.getenv(name)
    if val is None:
        return default
    s = str(val).strip().lower()
    if s in {"1", "true", "yes", "y", "on"}:
        return True
    if s in {"0", "false", "no", "n", "off"}:
        return False
    return default


def to_snake(s: str) -> str:
    return (
        s.strip()
        .lower()
        .replace("+", "_")
        .replace(" ", "_")
        .replace("-", "_")
    )


def url_normalize(u: str) -> str:
    """
    Normalize URLs:
    - Lower-case host
    - Drop utm_* params
    - Optionally strip all query params if STRIP_ALL_QUERY_PARAMS is truthy
    - Trim trailing slash (except root)
    - Always drop URL fragments
    - If a value starts with '/' treat it as a path and prefix SITE_BASE if provided
    """
    if not isinstance(u, str) or not u:
        return u
    u = u.strip()
    # Path-only -> prefix
    if u.startswith("/"):
        site_base = os.getenv("SITE_BASE", "").strip()
        if site_base:
            u = site_base.rstrip("/") + u
    try:
        p = urlparse(u)
        netloc = p.netloc.lower()

        # Query handling
        strip_all = str(os.getenv("STRIP_ALL_QUERY_PARAMS", "")).lower().strip() in {"1", "true", "yes"}
        if strip_all:
            query = ""
        else:
            q = [(k, v) for k, v in parse_qsl(p.query, keep_blank_values=True) if not k.lower().startswith("utm_")]
            query = urlencode(q)

        path = p.path
        if path != "/" and path.endswith("/"):
            path = path[:-1]

        # Always drop fragments for join consistency
        fragment = ""

        norm = urlunparse((p.scheme, netloc, path, p.params, query, fragment))
        return norm
    except Exception:
        return u


def coerce_numeric(df: pd.DataFrame, cols: list) -> pd.DataFrame:
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df

# -------------------------------------------------
# Loaders
# -------------------------------------------------

def load_and_normalize_frog(path: str) -> pd.DataFrame:
    print(f"Loading Screaming Frog data from {path} ...")
    df = load_table_any(path)
    mapping = resolve_columns(df, ["url","status_code","title","meta_description","click_depth","inlinks","word_count","schema_types"])
    # Autodetect URL column if missing
    autodet = None
    if mapping.get("url") is None:
        autodet = autodetect_url_column(df)
        if autodet:
            mapping["url"] = autodet
            print(f"[merge] ScreamingFrog: autodetected url column '{autodet}'")
    log_field_summary("ScreamingFrog", mapping)
    write_autodetect_log("ScreamingFrog", mapping, autodet, len(df))
    out = coalesce(df, mapping)
    out["url"] = out["url"].astype(str).apply(url_normalize)
    for c in ["status_code","click_depth","inlinks","word_count"]:
        out[c] = to_int(out[c])
    return out


def load_and_normalize_gsc(path: str) -> pd.DataFrame:
    print(f"Loading GSC data from {path} ...")
    df = load_table_any(path)
    df = df.rename(columns=lambda c: str(c).strip())
    mapping = resolve_columns(df, ["url","clicks","impressions","ctr","position"])
    autodet = None
    if mapping.get("url") is None:
        autodet = autodetect_url_column(df)
        if autodet:
            mapping["url"] = autodet
            print(f"[merge] GSC: autodetected url column '{autodet}'")
    log_field_summary("GSC", mapping)
    write_autodetect_log("GSC", mapping, autodet, len(df))
    out = coalesce(df, mapping)
    # Drop rows lacking a URL after coalescing to avoid polluting merges
    out["url"] = out["url"].astype(str)
    out = out[out["url"].apply(looks_url_like)].copy()
    out["url"] = out["url"].apply(url_normalize)
    # Clean numeric fields with robust parsing
    if "clicks" in out.columns:
        out["clicks"] = to_float_nocomma(out["clicks"])
    if "impressions" in out.columns:
        out["impressions"] = to_float_nocomma(out["impressions"])
    if "position" in out.columns:
        out["position"] = to_float_nocomma(out["position"])
    if "ctr" in out.columns:
        out["ctr"] = normalize_ctr(out["ctr"])
    return out



# Robust GA4 CSV reader to handle common export quirks
def _read_ga4_csv_robust(path: str) -> pd.DataFrame:
    """
    GA4 exports are notoriously inconsistent. Try a few parsers/dialects and strip comment lines.
    """
    # First attempt: vanilla pandas
    try:
        df = pd.read_csv(path, dtype=str, low_memory=False)
    except Exception:
        df = pd.read_csv(path, dtype=str, low_memory=False, engine="python", sep=None)

    # If we got only a single garbage column (often header like '# ------'), retry with python engine and comment filtering
    if len(df.columns) == 1:
        try:
            df = pd.read_csv(path, dtype=str, engine="python", sep=None, comment="#")
        except Exception:
            pass

    # Normalize header whitespace
    try:
        df = df.rename(columns=lambda c: str(c).strip())
    except Exception:
        pass
    return df

def load_and_normalize_ga4(path: str) -> pd.DataFrame:
    print(f"Loading GA4 data from {path} ...")
    df = load_table_any(path)
    # If CSV parsing produced a single garbage column, retry with the GA4-robust CSV parser
    try:
        if isinstance(df, pd.DataFrame) and len(df.columns) == 1:
            df = _read_ga4_csv_robust(path)
    except Exception:
        pass

    # Expand common GA4 URL header variants
    rename_candidates = {
        "pageLocation": "url",
        "Page": "url",
        "Page path": "url",
        "Page path + query string": "url",
        "Page path and query string": "url",
        "Page path and screen class": "url",
        "Landing page": "url",
        "Landing page + query string": "url",
        "Landing page and query string": "url",
    }
    for k, v in rename_candidates.items():
        if k in df.columns and "url" not in df.columns:
            df = df.rename(columns={k: v})
            break

    mapping = resolve_columns(df, ["url","users","sessions","engaged_sessions","avg_engagement_time"])
    autodet = None
    if mapping.get("url") is None:
        autodet = autodetect_url_column(df)
        if autodet:
            mapping["url"] = autodet
            print(f"[merge] GA4: autodetected url column '{autodet}'")

    log_field_summary("GA4", mapping)
    write_autodetect_log("GA4", mapping, autodet, len(df))

    out = coalesce(df, mapping)

    # Keep only rows with URL-looking values and drop GA4 junk markers
    out["url"] = out["url"].astype(str).str.strip()
    junk_markers = {"(not set)", "(other)", "other", "not set"}
    out = out[out["url"].apply(looks_url_like) & ~out["url"].str.lower().isin(junk_markers)].copy()
    out["url"] = out["url"].apply(url_normalize)

    for c in ["users","sessions","engaged_sessions","avg_engagement_time"]:
        if c in out.columns:
            out[c] = to_float(out[c])

    return out

# -------------------------------------------------
# Aggregation helpers to collapse sources to one row per URL
# -------------------------------------------------
def _agg_gsc(gdf: pd.DataFrame) -> pd.DataFrame:
    """
    Collapse GSC rows to a single row per URL.
    - Sum clicks and impressions
    - Impressions-weighted average for position
    - Recompute ctr = clicks / impressions
    """
    if gdf is None or gdf.empty:
        return gdf
    dfc = gdf.copy()
    for col in ("clicks", "impressions", "position", "ctr"):
        if col in dfc.columns:
            dfc[col] = pd.to_numeric(dfc[col], errors="coerce")
    # Base sums
    base = dfc.groupby("url", as_index=False).agg(
        clicks=("clicks", "sum"),
        impressions=("impressions", "sum"),
    )
    # Weighted position (use impressions as weights; fall back to equal weights if all zeros)
    w = dfc.copy()
    # ensure numeric for weights and position
    w["impressions"] = pd.to_numeric(w["impressions"], errors="coerce")
    w["position"] = pd.to_numeric(w["position"], errors="coerce")

    # weights: if impressions is 0 or NaN, use 1.0 as a neutral weight
    w["w"] = np.where((w["impressions"].fillna(0) > 0), w["impressions"], 1.0)

    # precompute weighted position and aggregate without using groupby.apply (avoids deprecation)
    w["pos_x_w"] = w["position"] * w["w"]
    w_agg = (
        w.groupby("url", as_index=False)
         .agg(pos_x_w_sum=("pos_x_w", "sum"), w_sum=("w", "sum"))
    )
    # safe division for weighted average
    w_agg["position"] = np.where(w_agg["w_sum"] > 0, w_agg["pos_x_w_sum"] / w_agg["w_sum"], np.nan)
    wpos = w_agg[["url", "position"]]
    out = base.merge(wpos, on="url", how="left")
    out["ctr"] = out["clicks"] / out["impressions"].replace({0: np.nan})
    return out

def _agg_ga4(adf: pd.DataFrame) -> pd.DataFrame:
    """
    Collapse GA4 rows to a single row per URL by summing numeric metrics.
    If later you add rate/avg fields, compute them with the appropriate weights upstream.
    """
    if adf is None or adf.empty:
        return adf
    dfc = adf.copy()
    num_cols = [c for c in dfc.columns if c != "url"]
    for c in num_cols:
        dfc[c] = pd.to_numeric(dfc[c], errors="coerce")
    agg = dfc.groupby("url", as_index=False).agg({c: "sum" for c in num_cols})
    return agg

# -------------------------------------------------
# Main
# -------------------------------------------------

def main():
    cfg = read_config("etl_config.yaml")
    # Normalize any user/env paths up-front
    os.environ["SITE_BASE"] = os.getenv("SITE_BASE", "").strip()
    strip_all = str(os.getenv("STRIP_ALL_QUERY_PARAMS", "")).lower().strip() in {"1", "true", "yes"}
    print(f"URL normalize: SITE_BASE='{os.getenv('SITE_BASE','')}', strip_all_query_params={strip_all}")

    # Inputs (accept either CSV or XLSX for all three)
    frog_csv = _resolve_input_from_config(cfg, "screaming_frog_csv", "data/screaming_frog_export.csv")
    gsc_csv  = _resolve_input_from_config(cfg, "gsc_csv", "data/gsc_export.csv")
    ga4_csv  = _resolve_input_from_config(cfg, "ga4_csv", "data/ga4_export.csv")

    # Friendly note on what we resolved
    print(f"Resolved inputs → Frog: {frog_csv} | GSC: {gsc_csv} | GA4: {ga4_csv}")

    # Output
    out_csv = cfg.get("output", {}).get("merged_csv", "merged/merged_visibility.csv")

    # Validate files exist
    for tag, fp in [("Screaming Frog", frog_csv), ("GSC", gsc_csv), ("GA4", ga4_csv)]:
        if not os.path.exists(fp):
            print(f"[ERROR] File not found for {tag}: {fp}")
            sys.exit(1)

    # Load
    frog = load_and_normalize_frog(frog_csv)
    frog = frog[frog["url"].astype(str).str.len() > 0].copy()
    # Ensure unique spine by URL
    frog = frog.drop_duplicates(subset=["url"]).copy()
    print(f"Screaming Frog spine (unique URLs): {len(frog)}")
    gsc = load_and_normalize_gsc(gsc_csv)
    gsc  = gsc[gsc["url"].astype(str).str.len() > 0].copy()
    # Aggregate GSC to one row per URL
    gsc = _agg_gsc(gsc)
    print(f"GSC aggregated rows: {len(gsc)}")
    ga4 = load_and_normalize_ga4(ga4_csv)
    ga4  = ga4[ga4["url"].astype(str).str.len() > 0].copy()
    # Aggregate GA4 to one row per URL
    ga4 = _agg_ga4(ga4)
    print(f"GA4 aggregated rows: {len(ga4)}")

    # Merge (left spine = Screaming Frog)
    print("Merging datasets ...")
    merged = frog.merge(gsc, on="url", how="left").merge(ga4, on="url", how="left")
    print(f"Merged rows: {len(merged)} (frog spine={len(frog)}, joined gsc on url={merged['clicks'].notna().sum() if 'clicks' in merged.columns else 0}, joined ga4 on url={merged['users'].notna().sum() if 'users' in merged.columns else 0})")

    # Derived metrics
    if "ctr" in merged.columns:
        merged["ctr_pct"] = merged["ctr"] * 100
    if "sessions" in merged.columns and "engaged_sessions" in merged.columns:
        merged["engagement_rate"] = merged["engaged_sessions"] / merged["sessions"]
        merged["engagement_rate_pct"] = merged["engagement_rate"] * 100
    if "clicks" in merged.columns:
        total_clicks = merged["clicks"].sum(skipna=True)
        merged["click_share"] = merged["clicks"] / total_clicks if total_clicks else 0

    # -------------------------------------------------
    # Scoring features on ALL rows (not just candidates)
    # -------------------------------------------------
    # Compute global fallback baseline (median CTR) for expected CTR lookup
    try:
        ctr_median_all = merged["ctr"].median(skipna=True)
    except Exception:
        ctr_median_all = 0.0

    # Pull config knobs used by scoring
    thresholds = cfg.get("thresholds", {}) or {}
    scoring    = cfg.get("scoring", {}) or {}
    margin     = float(thresholds.get("ctr_underperf_margin", 0.0) or 0.0)

    # Row-wise expected CTR and bucket for every row that has a position
    def _exp_tuple(pos):
        try:
            return expected_ctr_with_bucket(float(pos), cfg, ctr_median_all)
        except Exception:
            return (_normalize_expected_ctr_value(0.0, cfg), "fallback_error")

    pos_series = merged["position"] if "position" in merged.columns else pd.Series([], dtype=float)
    tmp = pos_series.apply(_exp_tuple) if len(merged) else []

    if len(merged):
        merged["expected_ctr"]         = pd.Series([t[0] for t in tmp], index=merged.index)
        merged["expected_ctr_bucket"]  = pd.Series([t[1] for t in tmp], index=merged.index)
        merged["expected_ctr_units"]   = str(scoring.get("expected_ctr_units", "fraction"))
        merged["expected_ctr_uplift"]  = float(scoring.get("expected_ctr_uplift", 0.0) or 0.0)

        # ctr_deficit = max(expected_ctr - ctr - margin, 0)
        merged["ctr_deficit"] = (
            (merged.get("expected_ctr") - merged.get("ctr") - margin)
            .astype(float)
            .clip(lower=0)
        )

        # Intent multiplier on all rows
        try:
            merged["intent_multiplier"] = merged.apply(lambda r: intent_multiplier_for_row(r, cfg), axis=1)
        except Exception:
            merged["intent_multiplier"] = 1.0

        # Missed clicks estimate for all rows
        if "impressions" in merged.columns:
            merged["missed_clicks"] = (
                merged["ctr_deficit"].astype(float)
                * pd.to_numeric(merged["impressions"], errors="coerce").fillna(0)
                * pd.to_numeric(merged["intent_multiplier"], errors="coerce").fillna(1.0)
            )
        else:
            merged["missed_clicks"] = 0.0

        # Convenience alias used by public_lite
        merged["missed_clicks_eff"] = merged["missed_clicks"]

    # Run metadata
    run_ts = int(time.time())  # epoch seconds (int)
    # Prefer stable, human-readable run_id; allow override via RUN_ID env
    run_id = os.getenv("RUN_ID") or datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    # Use timezone-aware datetime for ISO formatting (avoid pandas timedelta/string surprises)
    run_ts_iso = datetime.fromtimestamp(run_ts, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    # Ensure types that won't be mis-parsed downstream
    merged["run_id"] = str(run_id)
    merged["run_timestamp"] = run_ts  # epoch seconds (int)
    merged["run_timestamp_iso"] = run_ts_iso  # human-readable ISO (UTC)
    merged["source_frog"] = os.path.basename(frog_csv)
    merged["source_gsc"] = os.path.basename(gsc_csv)
    merged["source_ga4"] = os.path.basename(ga4_csv)

    # Format and rounding
    # Round floats sensibly and coerce integer-like fields
    if "ctr" in merged.columns:
        merged["ctr"] = merged["ctr"].round(4)
    if "ctr_pct" in merged.columns:
        merged["ctr_pct"] = merged["ctr_pct"].round(2)
    if "position" in merged.columns:
        merged["position"] = merged["position"].round(1)
    if "engagement_rate" in merged.columns:
        merged["engagement_rate"] = merged["engagement_rate"].round(4)
    if "engagement_rate_pct" in merged.columns:
        merged["engagement_rate_pct"] = merged["engagement_rate_pct"].round(1)
    if "click_share" in merged.columns:
        merged["click_share"] = merged["click_share"].round(6)
    if "avg_engagement_time" in merged.columns:
        merged["avg_engagement_time"] = pd.to_numeric(merged["avg_engagement_time"], errors="coerce").round(0).astype("Int64")
    if "expected_ctr" in merged.columns:
        merged["expected_ctr"] = pd.to_numeric(merged["expected_ctr"], errors="coerce").round(4)
    if "ctr_deficit" in merged.columns:
        merged["ctr_deficit"] = pd.to_numeric(merged["ctr_deficit"], errors="coerce").round(4)
    if "intent_multiplier" in merged.columns:
        merged["intent_multiplier"] = pd.to_numeric(merged["intent_multiplier"], errors="coerce").round(2)
    if "missed_clicks" in merged.columns:
        merged["missed_clicks"] = pd.to_numeric(merged["missed_clicks"], errors="coerce").round(3)
    if "missed_clicks_eff" in merged.columns:
        merged["missed_clicks_eff"] = pd.to_numeric(merged["missed_clicks_eff"], errors="coerce").round(3)

    # Column order preference
    preferred = [
        "url","status_code","title","meta_description","word_count","click_depth","inlinks","schema_types",
        "clicks","impressions","ctr","ctr_pct","position",
        "expected_ctr","expected_ctr_bucket","expected_ctr_units","expected_ctr_uplift","ctr_deficit","intent_multiplier","missed_clicks","missed_clicks_eff",
        "users","sessions","engaged_sessions","engagement_rate","engagement_rate_pct","avg_engagement_time","click_share",
        "run_id","run_timestamp","run_timestamp_iso","source_frog","source_gsc","source_ga4",
    ]
    cols = [c for c in preferred if c in merged.columns] + [c for c in merged.columns if c not in preferred]

    merged = merged[cols]

    # Output merged with a consistent float format (no scientific notation)
    ensure_parent_dir(out_csv)
    merged.to_csv(out_csv, index=False, float_format="%.6f")
    print(f"Merged data exported to {out_csv}")

    # Anomaly slices
    ensure_parent_dir("merged/anomaly_ctr_underperf.csv")
    if all(c in merged.columns for c in ["position", "ctr", "impressions"]):
        try:
            # Thresholds and scoring settings
            thresholds = cfg.get("thresholds", {}) or {}
            scoring = cfg.get("scoring", {}) or {}
            max_pos = float(scoring.get("max_position_for_ctr_eval", 5))
            min_clicks = float(scoring.get("min_clicks_for_ctr_eval", 0))
            margin = float(thresholds.get("ctr_underperf_margin", 0.0))

            # Compute a global fallback baseline (median)
            ctr_median = merged["ctr"].median(skipna=True)

            # Candidate rows: good positions, with data
            mask = (merged["position"].notna()) & (merged["ctr"].notna()) & (merged["impressions"].notna())
            mask &= (merged["position"] <= max_pos)
            if min_clicks > 0 and "clicks" in merged.columns:
                mask &= (merged["clicks"].fillna(0) >= min_clicks)

            ctr_under = merged.loc[mask].copy()

            # Row-wise expected CTR from config buckets with median fallback
            tmp = ctr_under["position"].apply(lambda p: expected_ctr_with_bucket(p, cfg, ctr_median))
            ctr_under["expected_ctr"] = tmp.apply(lambda t: t[0])
            ctr_under["expected_ctr_bucket"] = tmp.apply(lambda t: t[1])
            # Debug: capture config interpretation
            scoring_cfg = (cfg.get("scoring", {}) or {})
            ctr_under["expected_ctr_units"] = str(scoring_cfg.get("expected_ctr_units", "fraction"))
            ctr_under["expected_ctr_uplift"] = float(scoring_cfg.get("expected_ctr_uplift", 0.0) or 0.0)

            # Deficit vs baseline minus margin
            ctr_under["ctr_deficit"] = (ctr_under["expected_ctr"] - ctr_under["ctr"] - margin).clip(lower=0)

            # Intent multiplier
            ctr_under["intent_multiplier"] = ctr_under.apply(lambda r: intent_multiplier_for_row(r, cfg), axis=1)

            # Missed clicks
            ctr_under["missed_clicks"] = ctr_under["ctr_deficit"] * ctr_under["impressions"] * ctr_under["intent_multiplier"]
            ctr_under["missed_clicks"] = ctr_under["missed_clicks"].fillna(0)

            # Debug export for all candidate rows (including non-underperforming)
            ctr_debug = ctr_under.copy()
            debug_cols_first = [
                "url","position","ctr","impressions","expected_ctr","expected_ctr_bucket","expected_ctr_units","expected_ctr_uplift","ctr_deficit","intent_multiplier","missed_clicks"
            ]
            ctr_debug = ctr_debug[[c for c in debug_cols_first if c in ctr_debug.columns] + [c for c in ctr_debug.columns if c not in debug_cols_first]]
            ctr_debug.to_csv("merged/ctr_debug.csv", index=False, float_format="%.6f")
            print(f"Wrote merged/ctr_debug.csv with {len(ctr_debug)} candidate rows")

            # Keep only actual underperformance
            ctr_under = ctr_under[ctr_under["missed_clicks"] > 0].copy()

            try:
                candidates_n = len(ctr_debug)
                underperf_n = len(ctr_under)
                top_bucket = ctr_debug["expected_ctr_bucket"].value_counts().idxmax() if "expected_ctr_bucket" in ctr_debug.columns and len(ctr_debug) else "n/a"
                print(f"CTR underperf, candidates={candidates_n}, underperforming={underperf_n}, top_bucket={top_bucket}")
            except Exception as _:
                pass

            # Sort and rank by opportunity size
            ctr_under = ctr_under.sort_values("missed_clicks", ascending=False)
            ctr_under["priority_rank"] = range(1, len(ctr_under) + 1)

            # Analyst note column for triage
            ctr_under["intent_note"] = ""

            # Reorder to surface key triage fields first if present
            triage_first = [
                "url", "position", "ctr", "expected_ctr", "expected_ctr_bucket", "ctr_deficit",
                "impressions", "missed_clicks", "intent_multiplier", "priority_rank", "intent_note"
            ]
            triage_cols = [c for c in triage_first if c in ctr_under.columns]
            other_cols = [c for c in ctr_under.columns if c not in triage_cols]
            ctr_under = ctr_under[triage_cols + other_cols]

            ctr_under.to_csv("merged/anomaly_ctr_underperf.csv", index=False, float_format="%.6f")
            print(f"Wrote merged/anomaly_ctr_underperf.csv with {len(ctr_under)} rows (ranked by missed_clicks, config-driven)")
        except Exception as e:
            print(f"[WARN] Could not write CTR underperformance slice: {e}")

    # Governance: append a run log entry (optional)
    try:
        if get_env_bool("ETL_RUN_LOG", True):
            run_log_path = os.getenv("ETL_RUN_LOG_PATH", "logs/runs.csv")
            ensure_parent_dir(run_log_path)
            import csv
            with open(run_log_path, "a", newline="") as fh:
                writer = csv.writer(fh)
                if fh.tell() == 0:
                    writer.writerow(["run_id","run_timestamp","rows_merged","frog_csv","gsc_csv","ga4_csv","merged_csv"])
                writer.writerow([
                    run_id, run_ts, len(merged),
                    os.path.basename(frog_csv), os.path.basename(gsc_csv), os.path.basename(ga4_csv),
                    out_csv
                ])
        else:
            print("[governance] ETL_RUN_LOG disabled; skipping run log append")
    except Exception as e:
        print(f"[WARN] Could not append run log: {e}")

    if "schema_types" in merged.columns:
        try:
            gaps = merged[merged["schema_types"].isna() | (merged["schema_types"].astype(str).str.strip() == "")]
            gaps.to_csv("merged/schema_gaps.csv", index=False)
            print(f"Wrote merged/schema_gaps.csv with {len(gaps)} rows")
        except Exception as e:
            print(f"[WARN] Could not write schema gaps slice: {e}")


if __name__ == "__main__":
    main()