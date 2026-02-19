"""
Called by the ncaa-conference-snapshot workflow to print a markdown sample table
to stdout (which the workflow appends to $GITHUB_STEP_SUMMARY).

Usage:
    python scripts/summarize_ncaa_snapshot.py <path_to_csv>
"""
import argparse
import sys
import pandas as pd


def main():
    parser = argparse.ArgumentParser(description="Print a markdown sample of an NCAA snapshot CSV.")
    parser.add_argument("csv_path", help="Path to the input CSV file.")
    args = parser.parse_args()

    df = pd.read_csv(args.csv_path)

    DISPLAY_COLS = [
        "statistic", "team", "national_rank",
        "team_value", "national_leader_team", "national_leader_value",
    ]
    cols = [c for c in DISPLAY_COLS if c in df.columns]
    print(df[cols].head(10).to_markdown(index=False))


if __name__ == "__main__":
    main()
