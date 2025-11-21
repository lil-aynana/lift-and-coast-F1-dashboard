import fastf1
import pandas as pd

# Enable cache once for the whole module
fastf1.Cache.enable_cache('../data/cache')


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
    """
    Loads the full season for the given year, then returns all race details
    for the specified driver.
    """
    df = load_season_results(year)

    driver_df = df[df['driver'].str.contains(driver_name, case=False, na=False)]

    if driver_df.empty:
        print(f"No results found for driver: {driver_name} in {year}.")
        return None

    return driver_df.sort_values(by="round").reset_index(drop=True)


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
