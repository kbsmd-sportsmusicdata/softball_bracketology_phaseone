"""
Called by the weekly-scraper workflow to print a markdown summary table
of the scraped output CSV to stdout (which the workflow appends to $GITHUB_STEP_SUMMARY).

Usage:
    python scripts/summarize_scrape.py <path_to_csv>
"""
import sys
import pandas as pd

def main():
    if len(sys.argv) < 2:
        print("Usage: summarize_scrape.py <csv_path>", file=sys.stderr)
        sys.exit(1)

    csv_path = sys.argv[1]
    df = pd.read_csv(csv_path)

    DISPLAY_COLS = ["team", "wOBA", "wRC+", "ISO", "BABIP", "wRAA", "WAR_proxy"]
    cols = [c for c in DISPLAY_COLS if c in df.columns]

    print(df[cols].head(10).round(4).to_markdown(index=False))

if __name__ == "__main__":
    main()
