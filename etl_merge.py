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
from urllib.parse import urlparse, urlunparse, parse_qsl, urlencode
from datetime import datetime, timezone

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


def to_snake(s: str) -> str:
    return (
        s.strip()
        .lower()
        .replace("+", "_")
        .replace(" ", "_")
        .replace("-", "_")
    )


def url_normalize(u: str) -> str:
    if not isinstance(u, str) or not u:
        return u
    u = u.strip()
    try:
        p = urlparse(u)
        # lower-case hostname
        netloc = p.netloc.lower()
        # strip utm_* params
        q = [(k, v) for k, v in parse_qsl(p.query, keep_blank_values=True) if not k.lower().startswith("utm_")]
        query = urlencode(q)
        # strip trailing slash except root
        path = p.path
        if path != "/" and path.endswith("/"):
            path = path[:-1]
        norm = urlunparse((p.scheme, netloc, path, p.params, query, p.fragment))
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
    df = pd.read_csv(path, dtype=str)
    rename_map = {
        "Address": "url",
        "Status Code": "status_code",
        "Title 1": "title",
        "Meta Description 1": "meta_description",
        "Word Count": "word_count",
        "Crawl Depth": "click_depth",
        "Inlinks": "inlinks",
        "Structured Data": "schema_types",
    }
    df = df.rename(columns=rename_map)
    keep = [c for c in [
        "url","status_code","title","meta_description","word_count","click_depth","inlinks","schema_types"
    ] if c in df.columns]
    df = df[keep].copy()
    df["url"] = df["url"].astype(str).apply(url_normalize)
    df = coerce_numeric(df, ["status_code","word_count","click_depth","inlinks"])    
    return df


def load_and_normalize_gsc(path: str) -> pd.DataFrame:
    print(f"Loading GSC data from {path} ...")
    df = pd.read_csv(path, dtype=str)
    rename_map = {
        "Page": "url",
        "Clicks": "clicks",
        "Impressions": "impressions",
        "CTR": "ctr",
        "Position": "position",
    }
    df = df.rename(columns=rename_map)
    keep = [c for c in ["url","clicks","impressions","ctr","position"] if c in df.columns]
    df = df[keep].copy()
    df["url"] = df["url"].astype(str).apply(url_normalize)
    df = coerce_numeric(df, ["clicks","impressions","ctr","position"])    
    return df


def load_and_normalize_ga4(path: str) -> pd.DataFrame:
    print(f"Loading GA4 data from {path} ...")
    df = pd.read_csv(path, dtype=str)
    # Flexible URL column detection
    url_col = None
    for cand in ["pageLocation","Page","Page path","Page path + query string","Landing page"]:
        if cand in df.columns:
            url_col = cand
            break
    if not url_col:
        print("[ERROR] Could not find a GA4 URL column. Use pageLocation or Page path + query string.")
        sys.exit(1)
    rename_map = {
        url_col: "url",
        "totalUsers": "users",
        "Users": "users",
        "sessions": "sessions",
        "Sessions": "sessions",
        "Engaged sessions": "engaged_sessions",
        "Average engagement time": "avg_engagement_time",
        "averageSessionDuration": "avg_engagement_time",
    }
    df = df.rename(columns=rename_map)
    keep = [c for c in ["url","users","sessions","engaged_sessions","avg_engagement_time"] if c in df.columns]
    df = df[keep].copy()
    df["url"] = df["url"].astype(str).apply(url_normalize)
    df = coerce_numeric(df, ["users","sessions","engaged_sessions","avg_engagement_time"])    
    return df

# -------------------------------------------------
# Main
# -------------------------------------------------

def main():
    cfg = read_config("etl_config.yaml")

    # Inputs
    frog_csv = cfg.get("inputs", {}).get("screaming_frog_csv", "data/screaming_frog_export.csv")
    gsc_csv = cfg.get("inputs", {}).get("gsc_csv", "data/gsc_export.csv")
    ga4_csv = cfg.get("inputs", {}).get("ga4_csv", "data/ga4_export.csv")

    # Output
    out_csv = cfg.get("output", {}).get("merged_csv", "merged/merged_visibility.csv")

    # Validate files exist
    for tag, fp in [("Screaming Frog", frog_csv), ("GSC", gsc_csv), ("GA4", ga4_csv)]:
        if not os.path.exists(fp):
            print(f"[ERROR] File not found for {tag}: {fp}")
            sys.exit(1)

    # Load
    frog = load_and_normalize_frog(frog_csv)
    print(f"Screaming Frog data: {len(frog)} rows loaded.")
    gsc = load_and_normalize_gsc(gsc_csv)
    print(f"GSC data: {len(gsc)} rows loaded.")
    ga4 = load_and_normalize_ga4(ga4_csv)
    print(f"GA4 data: {len(ga4)} rows loaded.")

    # Merge (left spine = Screaming Frog)
    print("Merging datasets ...")
    merged = frog.merge(gsc, on="url", how="left").merge(ga4, on="url", how="left")

    # Derived metrics
    if "ctr" in merged.columns:
        merged["ctr_pct"] = merged["ctr"] * 100
    if "sessions" in merged.columns and "engaged_sessions" in merged.columns:
        merged["engagement_rate"] = merged["engaged_sessions"] / merged["sessions"]
        merged["engagement_rate_pct"] = merged["engagement_rate"] * 100
    if "clicks" in merged.columns:
        total_clicks = merged["clicks"].sum(skipna=True)
        merged["click_share"] = merged["clicks"] / total_clicks if total_clicks else 0

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

    # Column order preference
    preferred = [
        "url","status_code","title","meta_description","word_count","click_depth","inlinks","schema_types",
        "clicks","impressions","ctr","ctr_pct","position",
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

    # Governance: append a run log entry
    try:
        ensure_parent_dir("logs/runs.csv")
        import csv
        with open("logs/runs.csv", "a", newline="") as fh:
            writer = csv.writer(fh)
            if fh.tell() == 0:
                writer.writerow(["run_id","run_timestamp","rows_merged","frog_csv","gsc_csv","ga4_csv","merged_csv"])
            writer.writerow([run_id, run_ts, len(merged), os.path.basename(frog_csv), os.path.basename(gsc_csv), os.path.basename(ga4_csv), out_csv])
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