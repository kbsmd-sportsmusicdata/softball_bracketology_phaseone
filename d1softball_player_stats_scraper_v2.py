"""
Scrape D1Softball player stats for Top N teams, persist raw HTML, and output
processed player-level advanced metrics.

Workflow (team-by-team):
1) Parse Top N team slugs from team leaderboards page.
2) Pull each team's yearly player stats page.
3) Save raw HTML pages to data/raw.
4) Save processed player metrics CSV to data/cleaned.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
from pathlib import Path
import re
import sys
from typing import Dict, List, Optional, Tuple

import pandas as pd
import requests
from bs4 import BeautifulSoup

BASE = "https://d1softball.com"
LEADERBOARDS_URL = f"{BASE}/team-leaderboards/"
TEAM_STATS_YEAR_URL_TMPL = f"{BASE}/team/{{slug}}/{{season}}/stats/"

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
}

DEFAULT_WOBA_WEIGHTS = {
    "BB": 0.69,
    "HBP": 0.72,
    "1B": 0.87,
    "2B": 1.24,
    "3B": 1.56,
    "HR": 1.95,
}


def _to_int(x: object) -> int:
    if x is None:
        return 0
    sx = re.sub(r"[,\s]", "", str(x))
    if sx in ("", "-", "—", "–"):
        return 0
    try:
        return int(float(sx))
    except ValueError:
        return 0


def _to_float(x: object) -> float:
    if x is None:
        return 0.0
    sx = re.sub(r"[,\s]", "", str(x))
    if sx in ("", "-", "—", "–"):
        return 0.0
    try:
        return float(sx)
    except ValueError:
        return 0.0


def get_session(cookie: Optional[str] = None) -> requests.Session:
    s = requests.Session()
    s.headers.update(DEFAULT_HEADERS)
    if cookie:
        s.headers.update({"Cookie": cookie})
    return s


def fetch_html(session: requests.Session, url: str, timeout: int = 30) -> str:
    r = session.get(url, timeout=timeout)
    if r.status_code >= 400:
        raise RuntimeError(f"HTTP {r.status_code} for {url}")
    return r.text


def save_raw_html(raw_dir: Path, filename: str, html: str) -> Path:
    raw_dir.mkdir(parents=True, exist_ok=True)
    out = raw_dir / filename
    out.write_text(html, encoding="utf-8")
    return out


def html_table_to_df(tbl) -> pd.DataFrame:
    headers: List[str] = []
    thead = tbl.find("thead")
    if thead:
        headers = [th.get_text(" ", strip=True) for th in thead.find_all("th")]
    else:
        first_row = tbl.find("tr")
        if first_row:
            headers = [c.get_text(" ", strip=True) for c in first_row.find_all(["th", "td"])]

    rows: List[List[str]] = []
    tbody = tbl.find("tbody")
    tr_list = tbody.find_all("tr") if tbody else tbl.find_all("tr")[1:]
    for tr in tr_list:
        vals = [c.get_text(" ", strip=True) for c in tr.find_all(["td", "th"])]
        if vals:
            rows.append(vals)

    df = pd.DataFrame(rows)
    if headers and len(headers) == df.shape[1]:
        df.columns = headers
    return df


def first_table_by_caption_or_heading(soup: BeautifulSoup, keywords: List[str]) -> Optional[pd.DataFrame]:
    headings = soup.find_all(["h1", "h2", "h3", "h4"])
    for h in headings:
        text = (h.get_text(" ", strip=True) or "").lower()
        if any(k.lower() in text for k in keywords):
            tbl = h.find_next("table")
            if tbl is not None:
                return html_table_to_df(tbl)

    tbl = soup.find("table")
    if tbl is not None:
        return html_table_to_df(tbl)

    return None


def parse_top_teams_from_leaderboards(html: str, top_n: int = 50) -> List[Tuple[str, str]]:
    soup = BeautifulSoup(html, "html.parser")
    teams: List[Tuple[str, str]] = []
    seen = set()

    for a in soup.find_all("a", href=True):
        m = re.search(r"/team/([^/]+)/", a["href"])
        if not m:
            continue
        slug = m.group(1).strip()
        name = a.get_text(" ", strip=True) or slug.replace("-", " ").title()
        key = (name.lower(), slug.lower())
        if key in seen:
            continue
        seen.add(key)
        teams.append((name, slug))

    if not teams:
        raise RuntimeError("Could not extract teams from leaderboards page.")

    return teams[:top_n]


def normalize_player_batting_df(team: str, team_slug: str, df: pd.DataFrame, woba_weights: Dict[str, float]) -> pd.DataFrame:
    colmap = {str(c).strip().lower(): c for c in df.columns}

    def pick(*names: str) -> Optional[str]:
        for n in names:
            if n in colmap:
                return colmap[n]
        return None

    player_c = pick("player", "name")
    ab_c = pick("ab")
    h_c = pick("h")
    b2_c = pick("2b")
    b3_c = pick("3b")
    hr_c = pick("hr")
    bb_c = pick("bb")
    hbp_c = pick("hbp")
    sf_c = pick("sf")
    so_c = pick("k", "so")

    required = [
        ("player", player_c),
        ("ab", ab_c),
        ("h", h_c),
        ("2b", b2_c),
        ("3b", b3_c),
        ("hr", hr_c),
        ("bb", bb_c),
    ]
    missing = [name for name, col in required if col is None]
    if missing:
        raise RuntimeError(f"Missing expected batting columns: {missing}. Columns={list(df.columns)}")

    out = pd.DataFrame()
    out["team"] = team
    out["team_slug"] = team_slug
    out["player"] = df[player_c].astype(str)
    out["AB"] = df[ab_c].map(_to_int)
    out["H"] = df[h_c].map(_to_int)
    out["2B"] = df[b2_c].map(_to_int)
    out["3B"] = df[b3_c].map(_to_int)
    out["HR"] = df[hr_c].map(_to_int)
    out["BB"] = df[bb_c].map(_to_int)
    out["HBP"] = df[hbp_c].map(_to_int) if hbp_c else 0
    out["SF"] = df[sf_c].map(_to_int) if sf_c else 0
    out["SO"] = df[so_c].map(_to_int) if so_c else 0

    out = out[~out["player"].str.lower().str.contains("total", na=False)].copy()

    out["1B"] = (out["H"] - out["2B"] - out["3B"] - out["HR"]).clip(lower=0)
    out["PA_est"] = out["AB"] + out["BB"] + out["HBP"] + out["SF"]

    pa_nonzero = out["PA_est"].clip(lower=1)
    out["wOBA"] = (
        woba_weights["BB"] * out["BB"]
        + woba_weights["HBP"] * out["HBP"]
        + woba_weights["1B"] * out["1B"]
        + woba_weights["2B"] * out["2B"]
        + woba_weights["3B"] * out["3B"]
        + woba_weights["HR"] * out["HR"]
    ) / pa_nonzero

    out["AVG"] = out["H"] / out["AB"].clip(lower=1)
    out["OBP_est"] = (out["H"] + out["BB"] + out["HBP"]) / pa_nonzero
    out["SLG"] = (out["1B"] + 2 * out["2B"] + 3 * out["3B"] + 4 * out["HR"]) / out["AB"].clip(lower=1)
    out["OPS_est"] = out["OBP_est"] + out["SLG"]
    out["ISO"] = out["SLG"] - out["AVG"]
    out["BABIP"] = (out["H"] - out["HR"]) / (out["AB"] - out["SO"] - out["HR"] + out["SF"]).clip(lower=1)

    return out


def scrape_player_metrics(
    session: requests.Session,
    team_slugs: List[Tuple[str, str]],
    season: int,
    raw_dir: Path,
    run_stamp: str,
) -> pd.DataFrame:
    frames: List[pd.DataFrame] = []

    for team_name, slug in team_slugs:
        url = TEAM_STATS_YEAR_URL_TMPL.format(slug=slug, season=season)
        html = fetch_html(session, url)
        save_raw_html(raw_dir, f"player_stats_{season}_{slug}_{run_stamp}.html", html)

        soup = BeautifulSoup(html, "html.parser")
        bat_df = first_table_by_caption_or_heading(soup, ["batting", "hitting"])
        if bat_df is None:
            raise RuntimeError(f"Could not find batting table for {slug}")

        normalized = normalize_player_batting_df(team_name, slug, bat_df, DEFAULT_WOBA_WEIGHTS)
        frames.append(normalized)

    if not frames:
        return pd.DataFrame()

    out = pd.concat(frames, ignore_index=True)
    return out.sort_values(["wOBA", "OPS_est"], ascending=False).reset_index(drop=True)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--season", type=int, default=datetime.now(timezone.utc).year)
    ap.add_argument("--top_n", type=int, default=50)
    ap.add_argument("--cookie", type=str, default=None)
    ap.add_argument("--raw_dir", type=str, default="data/raw")
    ap.add_argument("--clean_dir", type=str, default="data/cleaned")
    ap.add_argument("--out", type=str, default=None, help="Output CSV path. Overrides --clean_dir if provided.")
    args = ap.parse_args()

    run_stamp = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    raw_dir = Path(args.raw_dir)
    clean_dir = Path(args.clean_dir)
    clean_dir.mkdir(parents=True, exist_ok=True)
    out_path = Path(args.out) if args.out else clean_dir / f"player_advanced_metrics_{args.season}_{run_stamp}.csv"

    session = get_session(cookie=args.cookie)

    lb_html = fetch_html(session, LEADERBOARDS_URL)
    save_raw_html(raw_dir, f"team_leaderboards_{run_stamp}.html", lb_html)
    teams = parse_top_teams_from_leaderboards(lb_html, top_n=args.top_n)

    df = scrape_player_metrics(session=session, team_slugs=teams, season=args.season, raw_dir=raw_dir)
    df.to_csv(out_path, index=False)

    print(f"Saved cleaned file: {out_path}")
    print(f"Saved raw files to: {raw_dir.resolve()}")
    print(df.head(15).to_string(index=False))


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"[ERROR] {exc}", file=sys.stderr)
        sys.exit(1)
