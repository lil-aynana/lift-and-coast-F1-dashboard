import fastf1
import pandas as pd

# Enable cache once for the whole module
# fastf1.Cache.enable_cache('../data/cache')
import os
import fastf1

# Build cache path relative to THIS file
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CACHE_DIR = os.path.join(BASE_DIR, '..', 'data', 'cache')

# Ensure folder exists
os.makedirs(CACHE_DIR, exist_ok=True)

# Enable cache
fastf1.Cache.enable_cache(CACHE_DIR)



# -----------------------------------------------------------
# 1. Load results for ONE race
# -----------------------------------------------------------
def load_race_results(year, round):
    """Load results for a single race."""
    session = fastf1.get_session(year, round, 'R')
    session.load()

    results = session.results

    df = pd.DataFrame({
        'year': year,
        'round': round,
        'raceName': session.event['EventName'],
        'driver': results['Abbreviation'],
        'driverNumber': results['DriverNumber'],
        'team': results['TeamName'],
        'grid': results['GridPosition'],
        'position': results['Position'],
        'points': results['Points'],
        'status': results['Status']
    })
    return df


# -----------------------------------------------------------
# 2. Load results for a FULL SEASON
# -----------------------------------------------------------
def load_season_results(year):
    """Load results for all races in a given season."""
    season_results = []

    # Event schedule for the year
    schedule = fastf1.get_event_schedule(year)
    rounds = schedule.index  # round numbers

    for rnd in rounds:
        try:
            df_race = load_race_results(year, rnd)
            season_results.append(df_race)
        except Exception as e:
            print(f"Skipping round {rnd} due to error: {e}")

    full_season = pd.concat(season_results, ignore_index=True)
    return full_season


# -----------------------------------------------------------
# 3. Get a DRIVER’S full season results
# -----------------------------------------------------------
def get_driver_season_details(year, driver_name):
    df = load_season_results(year)

    driver_df = df[df['driver'].str.contains(driver_name, case=False, na=False)]

    if driver_df.empty:
        print(f"No results found for driver: {driver_name} in {year}.")
        return None, None, None

    # Sort results
    driver_df = driver_df.sort_values(by="round").reset_index(drop=True)

    # Calculate metrics
    avg_finish = driver_df["position"].mean()
    total_points = driver_df["points"].sum()

    # ------------------------------
    # AUTO-PRINT RESULTS (your request)
    # ------------------------------
    # print(f"\nSeason Summary for {driver_name} ({year}):")
    # print("----------------------------------------")
    # print(driver_df[["round", "raceName", "position", "points"]])
    print(f"\nAverage Finishing Position: {avg_finish:.2f}")
    print(f"Total Points Scored: {total_points}\n")

    return driver_df



# -----------------------------------------------------------
# 4. Get ALL DRIVERS for a specific race
# -----------------------------------------------------------
def get_race_details(year, round_number):
    """
    Loads the full season for the given year, then returns all drivers' details
    for the specified race round.
    """
    df = load_season_results(year)

    race_df = df[df['round'] == round_number]

    if race_df.empty:
        print(f"No race data found for round {round_number} in {year}.")
        return None

    return race_df.sort_values(by="position").reset_index(drop=True)
