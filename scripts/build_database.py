#!/usr/bin/env python3
"""One-time builder: compute season results and save to SQLite.

Usage:
    python scripts/build_database.py

This script calls `load_season_results(2023)` exactly once and writes the
resulting DataFrame to `data/cleaned/f1.db` in table `season_results`.
Prints minimal progress messages.
"""
import os
import sqlite3
import sys

# Ensure we can import src package
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from src.data_cleaning import load_season_results


def main():
    year = 2023
    out_dir = os.path.join(os.path.dirname(__file__), '..', 'data', 'cleaned')
    os.makedirs(out_dir, exist_ok=True)
    db_path = os.path.join(out_dir, 'f1.db')

    print(f"Start: building season {year}")

    # Compute once
    df = load_season_results(year)

    # Persist to SQLite
    conn = sqlite3.connect(db_path)
    try:
        df.to_sql('season_results', conn, index=False, if_exists='replace')
    finally:
        conn.close()

    print(f"Success: saved {len(df)} rows to {db_path}:season_results")


if __name__ == '__main__':
    main()
