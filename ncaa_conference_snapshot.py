#!/usr/bin/env python3
"""
Automated Weekly Conference Snapshot (stats.ncaa.org)
Scrapes the "Ranking Summary" table for a given sport/division/conference and
saves a normalized long-format CSV.
Example:
  python ncaa_conference_snapshot.py \
    --sport_code WSB \
    --division 1.0 \
    --academic_year 2026.0 \
    --conf_id 827 \
    --out_dir data/ncaa_snapshots
"""
from __future__ import annotations
import argparse
import csv
import io
import os
import re
import sys
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Optional
import pandas as pd
import requests
from bs4 import BeautifulSoup
BASE_URL = "https://stats.ncaa.org"
RANKING_SUMMARY_PATH = "/rankings/ranking_summary"
DEFAULT_TIMEOUT = 30
DEFAULT_SLEEP_SEC = 1.0  # be polite
@dataclass
class PeriodOption:
    value: str        # e.g., "14.0"
    label: str        # e.g., "02/17/2026"
def _session() -> requests.Session:
    s = requests.Session()
    # A realistic UA helps avoid dumb blocks. Replace with yours if you want.
    s.headers.update({
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                      "(KHTML, like Gecko) Chrome/120.0 Safari/537.36 "
                      " ncaa-conference-snapshot/1.0",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Connection": "keep-alive",
    })
    return s
def fetch_ranking_summary_html(
    sess: requests.Session,
    academic_year: str,
    division: str,
    sport_code: str,
    conf_id: str,
    ranking_period: Optional[str] = None,
) -> str:
    params = {
        "academic_year": academic_year,
        "division": division,
        "sport_code": sport_code,
        "conf_id": conf_id,
        # Keep the rest consistent with what the UI uses.
        "team_individual": "T",
        "game_high": "N",
        "org_id": "-1",
        "region_id": "-1",
        "ncaa_custom_rank_summary_id": "-1",
        "user_custom_rank_summary_id": "-1",
    }
    if ranking_period is not None:
        params["ranking_period"] = ranking_period
    url = f"{BASE_URL}{RANKING_SUMMARY_PATH}"
    resp = sess.get(url, params=params, timeout=DEFAULT_TIMEOUT)
    resp.raise_for_status()
    return resp.text
def parse_ranking_period_options(html: str) -> List[PeriodOption]:
    """
    The ranking period selector is the 4th <select> on the page in the current UI,
    but we won't rely on position; we look for options whose labels look like dates.
    """
    soup = BeautifulSoup(html, "html.parser")
    opts: List[PeriodOption] = []
    for option in soup.select("select option"):
        label = (option.get_text() or "").strip()
        value = (option.get("value") or "").strip()
        # Match date like 02/17/2026 or 02/09/2026 - 02/15/2026
        if re.fullmatch(r"\d{2}/\d{2}/\d{4}", label) or re.fullmatch(
            r"\d{2}/\d{2}/\d{4}\s*-\s*\d{2}/\d{2}/\d{4}", label
        ):
            if value:
                opts.append(PeriodOption(value=value, label=label))
    # de-dupe
    unique = {(o.value, o.label): o for o in opts}
    return list(unique.values())
def pick_latest_period(periods: List[PeriodOption]) -> PeriodOption:
    """
    Prefer the latest single-day date. If the site gives a range, we use its end date.
    """
    def parse_label_to_date(label: str) -> datetime:
        if " - " in label:
            end = label.split(" - ")[-1].strip()
            return datetime.strptime(end, "%m/%d/%Y")
        return datetime.strptime(label, "%m/%d/%Y")
    if not periods:
        raise ValueError("No ranking_period options found. Page structure may have changed.")
    return max(periods, key=lambda p: parse_label_to_date(p.label))
def extract_summary_table(html: str) -> pd.DataFrame:
    """
    The page contains a main summary table with headers:
      Statistic | Team | National Rank | Value | National Leader | Value
    We'll grab the first table that has a 'Statistic' header.
    """
    tables = pd.read_html(io.StringIO(html))
    if not tables:
        raise ValueError("No HTML tables found on the page.")
    for df in tables:
        cols = [str(c).strip().lower() for c in df.columns]
        if "statistic" in cols and "team" in cols and "national" in " ".join(cols):
            return df
    # fallback: first table
    return tables[0]
def normalize_summary_table(df: pd.DataFrame) -> pd.DataFrame:
    """
    The 'Team', 'National Rank', and 'Value' columns often contain multiple lines
    (one per team). We'll split them and return one row per team-stat.
    """
    # Normalize column names
    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]
    # Identify the two "Value" columns (team value and national leader value)
    value_cols = [c for c in df.columns if c.lower() == "value"]
    if len(value_cols) < 2:
        # Some pandas versions label duplicates as Value and Value.1
        value_cols = [c for c in df.columns if c.lower().startswith("value")]
    if len(value_cols) < 2:
        raise ValueError(f"Could not find two Value columns. Columns: {list(df.columns)}")
    team_value_col = value_cols[0]
    leader_value_col = value_cols[1]
    required = ["Statistic", "Team", "National Rank", team_value_col, "National Leader", leader_value_col]
    for c in required:
        if c not in df.columns:
            raise ValueError(f"Missing expected column '{c}'. Columns: {list(df.columns)}")
    records: List[Dict[str, str]] = []
    for _, row in df.iterrows():
        stat = str(row["Statistic"]).strip()
        teams_blob = str(row["Team"]).strip()
        ranks_blob = str(row["National Rank"]).strip()
        vals_blob = str(row[team_value_col]).strip()
        leader_team = str(row["National Leader"]).strip()
        leader_val = str(row[leader_value_col]).strip()
        # Split multi-line cells
        teams = [t.strip() for t in re.split(r"\s*\n\s*", teams_blob) if t.strip()]
        ranks = [r.strip() for r in re.split(r"\s*\n\s*", ranks_blob) if r.strip()]
        vals = [v.strip() for v in re.split(r"\s*\n\s*", vals_blob) if v.strip()]
        # Some rows may be imperfect; align to min length
        n = min(len(teams), len(ranks), len(vals))
        for i in range(n):
            records.append({
                "statistic": stat,
                "team": teams[i],
                "national_rank": ranks[i],
                "team_value": vals[i],
                "national_leader_team": leader_team,
                "national_leader_value": leader_val,
            })
    return pd.DataFrame.from_records(records)
def safe_filename(s: str) -> str:
    return re.sub(r"[^A-Za-z0-9._-]+", "_", s).strip("_")
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sport_code", required=True, help="e.g., WSB for softball")
    ap.add_argument("--division", default="1.0", help="1.0, 2.0, 3.0")
    ap.add_argument("--academic_year", required=True, help="e.g., 2026.0 for 2025-26")
    ap.add_argument("--conf_id", required=True, help="e.g., 827 for Big Ten")
    ap.add_argument("--ranking_period", default=None, help="Optional: force a specific ranking_period value (e.g., 14.0)")
    ap.add_argument("--out_dir", default="data/ncaa_snapshots")
    ap.add_argument("--sleep_sec", type=float, default=DEFAULT_SLEEP_SEC)
    args = ap.parse_args()
    os.makedirs(args.out_dir, exist_ok=True)
    sess = _session()
    # Step 1: get page (either forced period or just to discover latest period)
    html = fetch_ranking_summary_html(
        sess=sess,
        academic_year=args.academic_year,
        division=args.division,
        sport_code=args.sport_code,
        conf_id=args.conf_id,
        ranking_period=args.ranking_period,
    )
    time.sleep(args.sleep_sec)
    # Step 2: determine latest period if not provided
    if args.ranking_period is None:
        periods = parse_ranking_period_options(html)
        latest = pick_latest_period(periods)
        ranking_period = latest.value
        ranking_label = latest.label
        # Re-fetch for latest (so the table matches)
        html = fetch_ranking_summary_html(
            sess=sess,
            academic_year=args.academic_year,
            division=args.division,
            sport_code=args.sport_code,
            conf_id=args.conf_id,
            ranking_period=ranking_period,
        )
        time.sleep(args.sleep_sec)
    else:
        ranking_period = args.ranking_period
        ranking_label = "custom"
    # Step 3: scrape + normalize
    summary_df = extract_summary_table(html)
    long_df = normalize_summary_table(summary_df)
    # Add metadata columns
    long_df.insert(0, "sport_code", args.sport_code)
    long_df.insert(1, "division", args.division)
    long_df.insert(2, "academic_year", args.academic_year)
    long_df.insert(3, "conf_id", args.conf_id)
    long_df.insert(4, "ranking_period", ranking_period)
    long_df.insert(5, "ranking_period_label", ranking_label)
    long_df.insert(6, "scraped_at_utc", datetime.utcnow().isoformat(timespec="seconds") + "Z")
    # Step 4: save
    stamp = datetime.utcnow().strftime("%Y%m%d")
    out_name = f"ncaa_snapshot_{args.sport_code}_div{args.division}_conf{args.conf_id}_period{ranking_period}_{stamp}.csv"
    out_path = os.path.join(args.out_dir, safe_filename(out_name))
    long_df.to_csv(out_path, index=False)
    print(f"Saved: {out_path}")
    print(f"Rows: {len(long_df):,}")
if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"[ERROR] {e}", file=sys.stderr)
        sys.exit(1)
