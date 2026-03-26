"""Shared utilities for D1Softball scraping scripts."""

from __future__ import annotations

from pathlib import Path
import re
from typing import List, Optional, Tuple

import pandas as pd
import requests
from bs4 import BeautifulSoup

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
}


def get_session(cookie: Optional[str] = None) -> requests.Session:
    """Create a requests session with default scraping headers."""
    session = requests.Session()
    session.headers.update(DEFAULT_HEADERS)
    if cookie:
        # You can paste a "Cookie:" header value from your browser devtools if needed.
        session.headers.update({"Cookie": cookie})
    return session


def fetch_html(session: requests.Session, url: str, timeout: int = 30) -> str:
    """Fetch a URL and return the response body, raising on HTTP errors."""
    try:
        response = session.get(url, timeout=timeout)
        response.raise_for_status()
        return response.text
    except requests.exceptions.RequestException as e:
        raise RuntimeError(f"Failed to fetch {url}: {e}") from e


def save_raw_html(raw_dir: Path, filename: str, html: str) -> Path:
    """Persist raw HTML to disk and return the output path."""
    raw_dir.mkdir(parents=True, exist_ok=True)
    output_path = raw_dir / filename
    output_path.write_text(html, encoding="utf-8")
    return output_path


def html_table_to_df(tbl) -> pd.DataFrame:
    """Convert a BeautifulSoup table tag to a DataFrame."""
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
        cells = tr.find_all(["td", "th"])
        row = [c.get_text(" ", strip=True) for c in cells]
        if row:
            rows.append(row)

    df = pd.DataFrame(rows)
    if headers and len(headers) == df.shape[1]:
        df.columns = headers
    return df


def first_table_by_caption_or_heading(
    soup: BeautifulSoup,
    keywords: List[str],
) -> Optional[pd.DataFrame]:
    """Find the first table after a heading that matches keywords, with fallback to first table."""
    headings = soup.find_all(["h1", "h2", "h3", "h4"])
    for heading in headings:
        text = (heading.get_text(" ", strip=True) or "").lower()
        if any(keyword.lower() in text for keyword in keywords):
            table = heading.find_next("table")
            if table is not None:
                return html_table_to_df(table)

    table = soup.find("table")
    if table is not None:
        return html_table_to_df(table)

    return None


def parse_top_teams_from_leaderboards(html: str, top_n: int = 50) -> List[Tuple[str, str]]:
    """Extract (team_name, team_slug) pairs from team links on the leaderboard page."""
    soup = BeautifulSoup(html, "html.parser")
    teams: List[Tuple[str, str]] = []
    seen = set()

    for anchor in soup.find_all("a", href=True):
        match = re.search(r"/team/([^/]+)/", anchor["href"])
        if not match:
            continue

        slug = match.group(1).strip()
        name = anchor.get_text(" ", strip=True) or slug.replace("-", " ").title()
        key = (name.lower(), slug.lower())

        if key in seen:
            continue

        seen.add(key)
        teams.append((name, slug))

    if not teams:
        raise RuntimeError("Could not extract teams from leaderboards page; page structure may have changed.")

    return teams[:top_n]
