import fastf1
import pandas as pd

fastf1.Cache.enable_cache('../data/cache')


def load_race_results(year, round):
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
