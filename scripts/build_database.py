# #!/usr/bin/env python3
# """One-time builder: compute season results and save to SQLite.

# Usage:
#     python scripts/build_database.py

# This script computes multiple seasons (2023, 2024, 2025) by calling
# `load_season_results(year)` for each year and writes the resulting
# DataFrames to `data/cleaned/f1.db` in table `season_results`.
# Prints minimal progress messages.
# """
# # import os
# # import sqlite3
# # import sys

# # # Ensure we can import src package
# # sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

# # from src.data_cleaning import load_season_results


# # def main():
# #     year = 2023
# #     out_dir = os.path.join(os.path.dirname(__file__), '..', 'data', 'cleaned')
# #     os.makedirs(out_dir, exist_ok=True)
# #     db_path = os.path.join(out_dir, 'f1.db')

# #     print(f"Start: building season {year}")

# #     # Compute once
# #     df = load_season_results(year)

# #     # Persist to SQLite
# #     conn = sqlite3.connect(db_path)
# #     try:
# #         df.to_sql('season_results', conn, index=False, if_exists='replace')
# #     finally:
# #         conn.close()

# #     print(f"Success: saved {len(df)} rows to {db_path}:season_results")


# # if __name__ == '__main__':
# #     main()








# def main():
#     years = [2023, 2024, 2025]

#     out_dir = os.path.join(os.path.dirname(__file__), '..', 'data', 'cleaned')
#     os.makedirs(out_dir, exist_ok=True)
#     db_path = os.path.join(out_dir, 'f1.db')

#     print(f"Start: building seasons {years}")

#     conn = sqlite3.connect(db_path)
#     try:
#         # Determine which seasons are already present (if table exists)
#         try:
#             existing = (
#                 conn.execute("SELECT DISTINCT season FROM season_results").fetchall()
#             )
#             existing_years = [row[0] for row in existing]
#         except sqlite3.OperationalError:
#             existing_years = []

#         total_added = 0
#         for year in years:
#             if year in existing_years:
#                 print(f"Season {year} already exists. Skipping.")
#                 continue

#             print(f"Processing season {year}...")
#             df = load_season_results(year)

#             if df is None or df.empty:
#                 print(f"  Skipping {year} (no data)")
#                 continue

#             if "season" not in df.columns:
#                 df["season"] = year

#             df.to_sql("season_results", conn, index=False, if_exists="append")
#             total_added += len(df)
#             print(f"  Saved {len(df)} rows for season {year}")

#     finally:
#         conn.close()

#     print(f"Success: added {total_added} rows to {db_path}:season_results")



# # !/usr/bin/env python3
# # """
# # One-time builder: compute multi-season F1 results and save to SQLite.

# # Usage:
# #     python scripts/build_database.py
# # """

# # import os
# # import sqlite3
# # import sys

# # # Ensure src is importable
# # sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

# # from src.data_cleaning import load_season_results


# # def main():
# #     years = [2022, 2023, 2024, 2025]  # Only completed seasons

# #     out_dir = os.path.join(os.path.dirname(__file__), '..', 'data', 'cleaned')
# #     os.makedirs(out_dir, exist_ok=True)

# #     db_path = os.path.join(out_dir, 'f1.db')
# #     print(f"Start: building seasons {years}")

# #     conn = sqlite3.connect(db_path)
# #     try:
# #         # Reset table
# #         conn.execute("DROP TABLE IF EXISTS season_results")

# #         total_rows = 0
# #         for year in years:
# #             print(f"Processing season {year}...")
# #             df = load_season_results(year)

# #             if df is None or df.empty:
# #                 print(f"  Skipping {year} (no data)")
# #                 continue

# #             # Ensure season column exists
# #             if "season" not in df.columns:
# #                 df["season"] = year

# #             df.to_sql("season_results", conn, index=False, if_exists="append")
# #             total_rows += len(df)

# #     finally:
# #         conn.close()

# #     print(f"Success: saved {total_rows} rows to {db_path}:season_results")


# # if __name__ == "__main__":
# #     main()



#!/usr/bin/env python3
"""
Incremental F1 database builder.

Usage:
    python scripts/build_database.py 2022
    python scripts/build_database.py 2023
    python scripts/build_database.py 2024
    python scripts/build_database.py 2025
"""

import os
import sqlite3
import sys
import pandas as pd

# Ensure src is importable
sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

from src.data_cleaning import load_season_results


def main():
    if len(sys.argv) != 2:
        print("Usage: python scripts/build_database.py <year>")
        sys.exit(1)

    year = int(sys.argv[1])

    out_dir = os.path.join(os.path.dirname(__file__), "..", "data", "cleaned")
    os.makedirs(out_dir, exist_ok=True)

    db_path = os.path.join(out_dir, "f1.db")
    print(f"▶ Building season {year}")

    conn = sqlite3.connect(db_path)

    try:
        # Check which seasons already exist
        try:
            existing_years = pd.read_sql(
                "SELECT DISTINCT season FROM season_results",
                conn
            )["season"].tolist()
        except Exception:
            existing_years = []

        if year in existing_years:
            print(f"✔ Season {year} already exists — skipping")
            return

        print(f"⏳ Loading FastF1 data for {year} (this may take time)...")
        df = load_season_results(year)

        if df is None or df.empty:
            print(f"⚠ No data found for {year}")
            return

        # Ensure season column exists
        df["season"] = year

        df.to_sql(
            "season_results",
            conn,
            index=False,
            if_exists="append"
        )

        print(f"✅ Saved {len(df)} rows for season {year}")

    finally:
        conn.close()


if __name__ == "__main__":
    main()
