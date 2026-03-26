"""
Scrape D1Softball Team Leaderboards + Team Stats pages (batting/pitching),
then compute: wOBA, wRC+, ISO, BABIP, WAR (proxy) for Alabama and Top 50 teams.

Data source pages:
- Team Leaderboards: https://d1softball.com/team-leaderboards/
- Team Stats: https://d1softball.com/team/<team-slug>/stats/?split=overall

Notes:
- D1Softball may require auth/subscription for some endpoints. If you get 403/401,
  you'll need to pass cookies (see AUTH section).
- "WAR" here is a proxy because true WAR needs run environment, replacement level,
  positional adjustments, fielding, etc. (not reliably available on the page).
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
from pathlib import Path
import re
import sys
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

import pandas as pd
import requests
from bs4 import BeautifulSoup
from scraper_utils import (
    fetch_html,
    first_table_by_caption_or_heading,
    get_session,
    html_table_to_df,
    parse_top_teams_from_leaderboards,
    save_raw_html,
)
# NOTE: `get_session` remains centralized in scraper_utils to avoid duplicate
# implementations across scraper entrypoints during merges.

# ----------------------------
# Config
# ----------------------------

BASE = "https://d1softball.com"
LEADERBOARDS_URL = f"{BASE}/team-leaderboards/"
TEAM_STATS_URL_TMPL = f"{BASE}/team/{{slug}}/stats/?split=overall"
STATISTICS_URL = f"{BASE}/statistics/"

# wOBA weights are *league/season specific*.
# Softball weights are not universal; treat these as tunable defaults.
# If you have D1Softball-provided weights or a preferred set, plug them in here.
DEFAULT_WOBA_WEIGHTS = {
    "BB": 0.69,   # unintentional BB ideally; we only have BB
    "HBP": 0.72,
    "1B": 0.87,
    "2B": 1.24,
    "3B": 1.56,
    "HR": 1.95,
}

# wRC+ needs a wOBA scale + league runs/PA.
# We estimate wOBA_scale from data if not provided, but you can override.
DEFAULT_WOBA_SCALE = 1.15  # typical-ish; adjust if you have softball-specific scale

RUNS_PER_WIN = 10.0  # proxy; adjust to your run environment


# ----------------------------
# Helpers
# ----------------------------

def _to_int(x: str) -> int:
    if x is None:
        return 0
    x = re.sub(r"[,\s]", "", str(x))
    if x in ("", "-", "—", "–"):
        return 0
    try:
        return int(float(x))
    except ValueError:
        return 0

def _to_float(x: str) -> float:
    if x is None:
        return 0.0
    x = re.sub(r"[,\s]", "", str(x))
    if x in ("", "-", "—", "–"):
        return 0.0
    try:
        return float(x)
    except ValueError:
        return 0.0

def find_totals_row(df: pd.DataFrame) -> pd.Series:
    """
    Try to locate the "Totals" row in a batting/pitching table.
    If not present, we sum numeric columns across all players.
    """
    # Identify a likely name column
    name_col = None
    for c in df.columns:
        if str(c).strip().lower() in ("player", "name", "team", "pos", "pitcher"):
            name_col = c
            break

    if name_col:
        mask = df[name_col].astype(str).str.lower().str.contains("total")
        if mask.any():
            return df.loc[mask].iloc[0]

    # Otherwise: aggregate
    # Keep only columns that look numeric
    agg = {}
    for c in df.columns:
        # heuristically numeric
        vals = df[c].astype(str).str.replace(",", "", regex=False)
        numeric_rate = (vals.str.match(r"^-?\d+(\.\d+)?$").mean())
        if numeric_rate > 0.6:
            agg[c] = pd.to_numeric(vals, errors="coerce").fillna(0).sum()

    if not agg:
        raise RuntimeError("Could not find totals row and no numeric columns to sum.")
    return pd.Series(agg)

def slugify_team_name(name: str) -> str:
    # D1Softball team slugs typically match simple kebab-case, but not always.
    # This is a fallback.
    s = name.strip().lower()
    s = re.sub(r"&", "and", s)
    s = re.sub(r"[^a-z0-9]+", "-", s)
    s = re.sub(r"-{2,}", "-", s).strip("-")
    return s


# ----------------------------
# Advanced metrics
# ----------------------------

@dataclass
class BattingTotals:
    team: str
    PA: int
    AB: int
    H: int
    _2B: int
    _3B: int
    HR: int
    BB: int
    HBP: int
    SO: int
    SF: int

@dataclass
class PitchingTotals:
    team: str
    IP: float
    H: int
    HR: int
    BB: int
    HBP: int
    SO: int
    ER: int
    R: int

def compute_batting_metrics(
    bt: BattingTotals,
    woba_weights: Dict[str, float],
    lg_woba: float,
    lg_woba_scale: float,
    lg_r_per_pa: float,
) -> Dict[str, float]:
    singles = max(bt.H - bt._2B - bt._3B - bt.HR, 0)
    denom_woba = max(bt.AB + bt.BB + bt.HBP + bt.SF, 1)

    woba = (
        woba_weights["BB"] * bt.BB
        + woba_weights["HBP"] * bt.HBP
        + woba_weights["1B"] * singles
        + woba_weights["2B"] * bt._2B
        + woba_weights["3B"] * bt._3B
        + woba_weights["HR"] * bt.HR
    ) / denom_woba

    # ISO = SLG - AVG = (2B + 2*3B + 3*HR)/AB
    iso = 0.0
    if bt.AB > 0:
        iso = (bt._2B + 2 * bt._3B + 3 * bt.HR) / bt.AB

    # BABIP = (H - HR)/(AB - SO - HR + SF)
    babip_denom = bt.AB - bt.SO - bt.HR + bt.SF
    babip = ((bt.H - bt.HR) / babip_denom) if babip_denom > 0 else 0.0

    # wRAA = (wOBA - lg_wOBA) / wOBA_scale * PA
    pa_for_wraa = bt.PA if bt.PA > 0 else denom_woba
    wraa = (woba - lg_woba) / max(lg_woba_scale, 1e-9) * pa_for_wraa

    # wRC = wRAA + (lg R/PA * PA)
    wrc = wraa + (lg_r_per_pa * pa_for_wraa)

    # wRC+ ~ 100 * ( (wRAA/PA + lg R/PA) / (lg R/PA) )
    # This is a simplified scaling; park factors etc. omitted.
    wrc_per_pa = wrc / max(pa_for_wraa, 1)
    wrc_plus = 100.0 * (wrc_per_pa / max(lg_r_per_pa, 1e-9))

    return {
        "wOBA": float(woba),
        "ISO": float(iso),
        "BABIP": float(babip),
        "wRAA": float(wraa),
        "wRC": float(wrc),
        "wRC+": float(wrc_plus),
    }

def proxy_war_from_runs(batting_runs: float, pitching_runs: float) -> float:
    # Extremely simplified: WAR ≈ (batting_runs + pitching_runs) / runs_per_win
    return (batting_runs + pitching_runs) / RUNS_PER_WIN


# ----------------------------
# Parsers (D1Softball HTML)
# ----------------------------

def parse_team_stats_page(team_slug: str, html: str, team_name_fallback: Optional[str] = None) -> Tuple[BattingTotals, PitchingTotals]:
    soup = BeautifulSoup(html, "html.parser")

    # Identify team name from title/h1 if possible
    team_name = team_name_fallback or team_slug.replace("-", " ").title()
    h1 = soup.find("h1")
    if h1 and h1.get_text(strip=True):
        team_name = h1.get_text(" ", strip=True)

    # The team stats page typically has sections like "Batting" and "Pitching"
    bat_df = first_table_by_caption_or_heading(soup, ["batting", "hitting"])
    pit_df = first_table_by_caption_or_heading(soup, ["pitching"])

    if bat_df is None:
        raise RuntimeError(f"Could not find batting table for {team_slug}")
    if pit_df is None:
        raise RuntimeError(f"Could not find pitching table for {team_slug}")

    bat_tot = find_totals_row(bat_df)
    pit_tot = find_totals_row(pit_df)

    # Map columns (best-effort). If D1Softball changes labels, update here.
    # Common batting columns on D1Softball player tables: PA, AB, H, 2B, 3B, HR, HBP, BB, K, SF
    colmap_bat = {c.lower(): c for c in bat_df.columns}
    def pick_bat(*names):
        for n in names:
            if n in colmap_bat:
                return colmap_bat[n]
        return None

    PA_c  = pick_bat("pa")
    AB_c  = pick_bat("ab")
    H_c   = pick_bat("h")
    _2B_c = pick_bat("2b")
    _3B_c = pick_bat("3b")
    HR_c  = pick_bat("hr")
    BB_c  = pick_bat("bb")
    HBP_c = pick_bat("hbp")
    SO_c  = pick_bat("k", "so")
    SF_c  = pick_bat("sf")

    missing = [("PA", PA_c), ("AB", AB_c), ("H", H_c), ("2B", _2B_c), ("3B", _3B_c), ("HR", HR_c), ("BB", BB_c)]
    missing = [n for n, c in missing if c is None]
    if missing:
        raise RuntimeError(f"Missing expected batting columns for {team_slug}: {missing}. Columns={list(bat_df.columns)}")

    bt = BattingTotals(
        team=team_name,
        PA=_to_int(bat_tot.get(PA_c, 0)) if PA_c else 0,
        AB=_to_int(bat_tot.get(AB_c, 0)),
        H=_to_int(bat_tot.get(H_c, 0)),
        _2B=_to_int(bat_tot.get(_2B_c, 0)),
        _3B=_to_int(bat_tot.get(_3B_c, 0)),
        HR=_to_int(bat_tot.get(HR_c, 0)),
        BB=_to_int(bat_tot.get(BB_c, 0)),
        HBP=_to_int(bat_tot.get(HBP_c, 0)) if HBP_c else 0,
        SO=_to_int(bat_tot.get(SO_c, 0)) if SO_c else 0,
        SF=_to_int(bat_tot.get(SF_c, 0)) if SF_c else 0,
    )

    # Pitching: typical columns include IP, H, R, ER, BB, HBP, SO, HR
    colmap_pit = {c.lower(): c for c in pit_df.columns}
    def pick_pit(*names):
        for n in names:
            if n in colmap_pit:
                return colmap_pit[n]
        return None

    IP_c  = pick_pit("ip")
    H2_c  = pick_pit("h")
    R_c   = pick_pit("r")
    ER_c  = pick_pit("er")
    BB2_c = pick_pit("bb")
    HBP2_c= pick_pit("hbp")
    SO2_c = pick_pit("k", "so")
    HR2_c = pick_pit("hr")

    missing_p = [("IP", IP_c), ("H", H2_c), ("ER", ER_c), ("BB", BB2_c), ("SO", SO2_c), ("HR", HR2_c)]
    missing_p = [n for n, c in missing_p if c is None]
    if missing_p:
        # Not all pages show all pitching columns; we can still proceed if we have IP, ER, H, BB, SO at minimum.
        needed = ["ip", "er", "h", "bb"]
        have = [c.lower() for c in pit_df.columns]
        if not all(n in have for n in needed):
            raise RuntimeError(f"Missing expected pitching columns for {team_slug}: {missing_p}. Columns={list(pit_df.columns)}")

    pt = PitchingTotals(
        team=team_name,
        IP=_to_float(pit_tot.get(IP_c, 0)) if IP_c else 0.0,
        H=_to_int(pit_tot.get(H2_c, 0)) if H2_c else 0,
        HR=_to_int(pit_tot.get(HR2_c, 0)) if HR2_c else 0,
        BB=_to_int(pit_tot.get(BB2_c, 0)) if BB2_c else 0,
        HBP=_to_int(pit_tot.get(HBP2_c, 0)) if HBP2_c else 0,
        SO=_to_int(pit_tot.get(SO2_c, 0)) if SO2_c else 0,
        ER=_to_int(pit_tot.get(ER_c, 0)) if ER_c else 0,
        R=_to_int(pit_tot.get(R_c, 0)) if R_c else _to_int(pit_tot.get(ER_c, 0)),
    )

    return bt, pt


def scrape_statistics_tables_to_raw_csv(
    session: requests.Session,
    raw_dir: Path,
    run_stamp: str,
) -> Path:
    """
    Scrape all tables from https://d1softball.com/statistics/ and save them
    in a single raw CSV file.
    """
    statistics_html = fetch_html(session, STATISTICS_URL)
    save_raw_html(raw_dir, f"statistics_{run_stamp}.html", statistics_html)

    soup = BeautifulSoup(statistics_html, "html.parser")
    tables = soup.find_all("table")
    if not tables:
        raise RuntimeError("Could not find any tables on the D1Softball statistics page.")

    frames = []
    for idx, table in enumerate(tables, start=1):
        table_df = html_table_to_df(table)
        if table_df.empty:
            continue
        table_df.insert(0, "table_index", idx)
        frames.append(table_df)

    if not frames:
        raise RuntimeError("Parsed tables from the statistics page were empty.")

    raw_dir.mkdir(parents=True, exist_ok=True)
    stats_csv_path = raw_dir / f"statistics_tables_{run_stamp}.csv"
    pd.concat(frames, ignore_index=True).to_csv(stats_csv_path, index=False)
    return stats_csv_path

# ----------------------------
# Main pipeline
# ----------------------------

def build_team_metrics_frame(
    session: requests.Session,
    team_slugs: List[Tuple[str, str]],
    woba_weights: Dict[str, float],
    woba_scale: float,
    raw_dir: Optional[Path] = None,
    run_stamp: Optional[str] = None,
) -> pd.DataFrame:
    batting_rows = []
    pitching_rows = []

    # First pass: scrape totals
    for team_name, slug in team_slugs:
        url = TEAM_STATS_URL_TMPL.format(slug=slug)
        html = fetch_html(session, url)
        if raw_dir and run_stamp:
            save_raw_html(raw_dir, f"team_stats_{slug}_{run_stamp}.html", html)
        bt, pt = parse_team_stats_page(slug, html, team_name_fallback=team_name)

        batting_rows.append(bt.__dict__)
        pitching_rows.append(pt.__dict__)

    bat_df = pd.DataFrame(batting_rows)
    pit_df = pd.DataFrame(pitching_rows)

    # League context (within the sample of teams you scraped)
    # Better: use all D1 teams as league; but you asked Top 50, so we do "Top 50 context".
    # wOBA league:
    def team_woba_row(r) -> float:
        singles = max(r["H"] - r["_2B"] - r["_3B"] - r["HR"], 0)
        denom = max(r["AB"] + r["BB"] + r["HBP"] + r["SF"], 1)
        return (
            woba_weights["BB"] * r["BB"]
            + woba_weights["HBP"] * r["HBP"]
            + woba_weights["1B"] * singles
            + woba_weights["2B"] * r["_2B"]
            + woba_weights["3B"] * r["_3B"]
            + woba_weights["HR"] * r["HR"]
        ) / denom

    bat_df["wOBA_raw"] = bat_df.apply(team_woba_row, axis=1)
    lg_woba = float(bat_df["wOBA_raw"].mean())

    # League runs per PA: needs team runs; if not present in batting table totals we can't compute.
    # D1Softball batting tables often include R; if present, use it; otherwise fallback to proxy.
    lg_r_per_pa = 0.12  # fallback proxy (tunable)
    if "R" in bat_df.columns:
        bat_df["R"] = bat_df["R"].astype(float)
        lg_r_per_pa = float((bat_df["R"].sum()) / max(bat_df["PA"].sum(), 1))

    # Compute batting advanced metrics
    adv = []
    for _, r in bat_df.iterrows():
        bt = BattingTotals(
            team=r["team"], PA=int(r["PA"]), AB=int(r["AB"]), H=int(r["H"]),
            _2B=int(r["_2B"]), _3B=int(r["_3B"]), HR=int(r["HR"]), BB=int(r["BB"]),
            HBP=int(r["HBP"]), SO=int(r["SO"]), SF=int(r["SF"])
        )
        adv.append(compute_batting_metrics(
            bt=bt,
            woba_weights=woba_weights,
            lg_woba=lg_woba,
            lg_woba_scale=woba_scale,
            lg_r_per_pa=lg_r_per_pa,
        ))
    adv_df = pd.DataFrame(adv)
    bat_out = pd.concat([bat_df.drop(columns=["wOBA_raw"]), adv_df], axis=1)

    # Pitching: we'll create a *very* rough pitching runs estimate from ER (or R).
    # You can improve this with opponent quality, park, defense, etc.
    pit_out = pit_df.copy()
    pit_out["PitchRuns"] = -pit_out["ER"].astype(float)  # lower ER => higher value (proxy)

    # Batting runs proxy: use wRAA
    bat_out["BatRuns"] = bat_out["wRAA"].astype(float)

    # Merge + proxy WAR
    out = bat_out.merge(pit_out[["team", "IP", "ER", "R", "PitchRuns"]], on="team", how="left")
    out["WAR_proxy"] = out.apply(lambda x: proxy_war_from_runs(x["BatRuns"], x.get("PitchRuns", 0.0)), axis=1)

    # Clean
    numeric_cols = ["wOBA", "wRC+", "ISO", "BABIP", "wRAA", "wRC", "WAR_proxy"]
    for c in numeric_cols:
        if c in out.columns:
            out[c] = pd.to_numeric(out[c], errors="coerce")

    return out.sort_values("WAR_proxy", ascending=False).reset_index(drop=True)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--team", type=str, default=None, help="Run a single team by name (e.g., Alabama).")
    ap.add_argument("--top_n", type=int, default=50, help="How many teams to pull from leaderboards.")
    ap.add_argument("--cookie", type=str, default=None, help="Optional Cookie header value for authenticated access.")
    ap.add_argument("--raw_dir", type=str, default="data/raw", help="Directory for raw source HTML.")
    ap.add_argument("--clean_dir", type=str, default="data/cleaned", help="Directory for processed output files.")
    ap.add_argument("--out", type=str, default=None, help="Output CSV path. Overrides --clean_dir if provided.")
    ap.add_argument("--woba_scale", type=float, default=DEFAULT_WOBA_SCALE, help="wOBA scale (tunable).")
    args = ap.parse_args()

    session = get_session(cookie=args.cookie)
    run_stamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H%M%S")
    run_stamp = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    raw_dir = Path(args.raw_dir)
    clean_dir = Path(args.clean_dir)
    clean_dir.mkdir(parents=True, exist_ok=True)
    out_path = Path(args.out) if args.out else clean_dir / f"team_advanced_metrics_{run_stamp}.csv"
    statistics_csv_path = scrape_statistics_tables_to_raw_csv(
        session=session,
        raw_dir=raw_dir,
        run_stamp=run_stamp,
    )

    if args.team:
        slug = slugify_team_name(args.team)
        team_slugs = [(args.team, slug)]
        # NOTE: if the slug doesn't match D1Softball's slug, replace it with the real one.
        # Example: "Florida State" might be "florida-state" etc.
    else:
        lb_html = fetch_html(session, LEADERBOARDS_URL)
        save_raw_html(raw_dir, f"team_leaderboards_{run_stamp}.html", lb_html)
        team_slugs = parse_top_teams_from_leaderboards(lb_html, top_n=args.top_n)

    df = build_team_metrics_frame(
        session=session,
        team_slugs=team_slugs,
        woba_weights=DEFAULT_WOBA_WEIGHTS,
        woba_scale=args.woba_scale,
        raw_dir=raw_dir,
        run_stamp=run_stamp,
    )

    df.to_csv(out_path, index=False)
    print(f"Saved cleaned file: {out_path}")
    print(f"Saved statistics tables CSV: {statistics_csv_path}")
    print(f"Saved raw files to: {raw_dir.resolve()}")
    print(df.head(10).to_string(index=False))


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"[ERROR] {e}", file=sys.stderr)
        sys.exit(1)
