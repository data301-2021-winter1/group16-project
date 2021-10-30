''' Functions for the preparation of my data '''

import numpy as np
import pandas as pd

def count_overtakes(raceId, df):
    '''Function to count the number of overtakes in a race, given the raceid and the laptime dataframe'''
    records = df[df['raceId']==raceId][['driverId', 'lap', 'position']]
    records_lat = records.rename(columns={'position':'position_after_round'})
    records_lat['lap'] = records_lat['lap'] + 1
    records = records.merge(records_lat, how='left',on=['driverId','lap'])
    records['overtakes'] = records['position'] - records['position_after_round']     
    records['overtakes'] = records['overtakes'].apply(lambda x: -x if x<0 else 0)
    return records['overtakes'].sum()


def count_prev_races(data, df):
    '''Count the number races that have been taken place on the race before this one'''
    year = int(data.split(',')[0])
    circuitId = int(data.split(',')[1])
    return len(df[(df['year']<year)&(df['circuitId']==circuitId)])


def lap_record_to_date(data, df):
    '''find the lap record at the moment the race took place'''
    year = int(data.split(',')[0])
    circuitId = int(data.split(',')[1])
    df = df[(df['year']<year)&(df['circuitId']==circuitId)]
    return df['fastestLapTime'].min()


def load_and_process(path_to_files, save=False):
    '''Load and Process all the data needed for my analysis, taking the path to the data and returning the races dataframe
       Set save to True to save the dataframe into processed data folder'''

    # filenames
    circuits_filename = 'circuits.csv'
    constructor_res_filename = 'constructor_results.csv'
    constructor_stand_filename = 'constructor_standings.csv'
    constructors_filename = 'constructors.csv'
    driver_stand_filename = 'driver_standings.csv'
    drivers_filename = 'drivers.csv'
    pit_stops_filename = 'pit_stops.csv'
    qualifying_filename = 'qualifying.csv'
    races_filename = 'races.csv'
    results_filename = 'results.csv'
    lap_times_filename = 'lap_times.csv'

    # Data needed to match Circuit Name with Circuit ID
    circuits = (
        pd.read_csv(path_to_files + circuits_filename)
        .replace('\\N',np.nan)
        .rename(columns={'name':'Circuit Name', 'location':'City'})
        .drop(columns=['circuitRef', 'country', 'lat', 'lng', 'alt', 'url'])
    )

    # Data needed to match Team Name with Team ID
    constructors = (
        pd.read_csv(path_to_files + constructors_filename)
        .replace('\\N',np.nan)
        .drop(columns=[	'constructorRef', 'nationality', 'url'])
        .rename(columns={'name': 'Team Name'})
    )

    # Data needed to state which constructor won the grand prix
    constructor_res = (
        pd.read_csv(path_to_files + constructor_res_filename)
        .replace('\\N',np.nan)
        .groupby(['raceId'])
        .max()
        .reset_index()
        .drop(columns=['constructorId', 'constructorResultsId'])
        .merge(pd.read_csv(path_to_files + constructor_res_filename)[['raceId', 'points', 'constructorId']], how='left', on=['raceId', 'points'])
        .merge(constructors, how='left', on='constructorId')
        .drop(columns=['constructorId', 'points'])
        .rename(columns={'Team Name':'Constructors Winner'})
    )

    # Data needed to state which team is leading in the championship
    constructor_stand = (
        pd.read_csv(path_to_files + constructor_stand_filename)
        .replace('\\N',np.nan)
        .merge(constructors, how='left', on='constructorId')
        .drop(columns=['constructorStandingsId', 'positionText', 'wins', 'constructorId'])
    )

    # subset of constructor_stand needed to give the championship leader at each race
    const_stand_leader = (
        constructor_stand[constructor_stand['position']==1]
        .rename(columns={'points':'points1'})
        .drop(columns=['position', 'Team Name'])
    )
    
    # subset of constructor_stand needed to give the 2nd place in the championship at each race 
    const_stand_second = (
        constructor_stand[constructor_stand['position']==2]
        .rename(columns={'points':'points2'})
        .drop(columns=['position', 'Team Name'])
    )

    # Calculated difference between leader and second place in constructor championship to see whether title race is still open
    const_stand_difference = const_stand_leader.merge(const_stand_second, how='left', on='raceId')
    const_stand_difference['Constructors Championship Point Difference'] = const_stand_difference['points1']-const_stand_difference['points2']

    # Data to Match the driver id with the drivers name
    drivers = (
        pd.read_csv(path_to_files + drivers_filename)
        .replace('\\N',np.nan)
        .assign(Driver_Name =lambda x: x['forename'] + ' ' +  x['surname'])
        .drop(columns=['driverRef', 'number', 'code', 'forename', 'surname', 'dob', 'nationality', 'url'])
        .rename(columns={'Driver_Name': 'Driver Name'})
    )

    # Data needed to state which driver is leading in the driver championship
    driver_stand = (
        pd.read_csv(path_to_files + driver_stand_filename)
        .replace('\\N',np.nan)
        .merge(drivers, how='left', on='driverId')
        .drop(columns=['driverId', 'driverStandingsId', 'positionText', 'wins'])
    )

    # subset of driver_stand needed to give the 1st place in the championship at each race 
    driver_stand_leader = (
        driver_stand[driver_stand['position']==1]
        .rename(columns={'points':'points1'})
        .drop(columns=['position', 'Driver Name'])
    )

    # subset of driver_stand needed to give the 2nd place in the championship at each race 
    driver_stand_second = (
        driver_stand[driver_stand['position']==2]
        .rename(columns={'points':'points2'})
        .drop(columns=['position', 'Driver Name'])
    )
    
    # Calculated difference between leader and second place in driver championship to see whether title race is still open
    driver_stand_difference = driver_stand_leader.merge(driver_stand_second, how='left', on='raceId')
    driver_stand_difference['Driver Championship Point Difference'] = driver_stand_difference['points1']-driver_stand_difference['points2']

    # Information on who one the qualifying (not sure yet if I will include it)
    qualifying = (
        pd.read_csv(path_to_files + qualifying_filename)
        .replace('\\N',np.nan)
        .merge(drivers, how='left', on='driverId')
        .rename(columns={'Driver Name': 'Pole Sitter'})
        .query('position == 1')
        .drop(columns=['driverId', 'qualifyId', 'constructorId', 'number', 'q1', 'q2', 'q3', 'position'])
    )

    # transform format of pitstop duratino and prepare for merging, needed to state average duration of pitstop for each race
    pit_stops = (
        pd.read_csv(path_to_files + pit_stops_filename)
        .assign(duration_s = lambda y: y.duration.apply(lambda x: float(x.split(':')[1]) if ':' in x else float(x)))
        .replace('\\N',np.nan)
        .drop(columns=['driverId', 'stop', 'lap', 'time', 'milliseconds'])
        .rename(columns={'duration_s':'avg. Pitstop Duration (in s)'})
        .groupby(['raceId'])
        .mean()
        .reset_index()
    )

    # File needed to extract the number of overtakes that took place each round
    lap_times = (
        pd.read_csv(path_to_files + lap_times_filename)
        .replace('\\N',np.nan)
    )

    # initial reading of races because it contains the year which helps for further matching
    races = (
        pd.read_csv(path_to_files + races_filename)
        .replace('\\N',np.nan)
    )

    # Contains information regarding fastest lap time and lap speed, needed for track records and highspeeds as well as time differences
    results = (
        pd.read_csv(path_to_files + results_filename)
        .replace('\\N',np.nan)
        .assign(fastestLapSpeed = lambda x: pd.to_numeric(x.fastestLapSpeed))
        .assign(fastestLapTime = lambda y: y.fastestLapTime.apply(lambda x: float(x.split(':')[0])*60+float(x.split(':')[1]) if type(x)==str else x))
        .assign(time = lambda y: y.time.apply(lambda x: x.strip(' sec') if type(x)==str else x))
        .assign(time = lambda y: y.time.apply(lambda x: float(x[1:].split(':')[0])*60+float(x[1:].strip('s').split(':')[1]) if (type(x)==str and x.count(':')==1) else(float(x[1:].strip('s').split(':')[0]) if (type(x)==str and x.count('.')==1 and x.count(':')==0) else 0)))
        .merge(drivers, how='left', on='driverId')
        .merge(constructors, how='left', on='constructorId')
        .merge(races[['raceId', 'year', 'circuitId']], on='raceId', how='left')
    )

    # subset of results, with time difference between first and second to state whether there was a close battle for the win
    diff_1_2 = (
        results[results['positionOrder']==2][['raceId', 'time']]
        .rename(columns={'time':'Difference 1st to 2nd (in s)'})
    )

    # subset of results, with time difference between first and fifth to give avg. difference between top driver, state whether there was close racing till last lap
    diff_1_5 = (
        results[results['positionOrder']==5][['raceId', 'time']]
        .assign(time = lambda x: x.time/4)
        .rename(columns={'time':'avg. Difference Top 5 (in s)'})
    )

    # sub set of results to give top speed for each race
    highspeeds = (
        results[['raceId', 'fastestLapSpeed']]
        .groupby(['raceId'])
        .max()
        .reset_index()
    )

    # sub set of results to give fastest lap for each race
    fastest_lap = (
        results[['raceId', 'fastestLapTime']]
        .groupby(['raceId'])
        .min()
        .reset_index()
    )

    # Merging all the information from above into races, so we can analyze all info stated above in the next step
    races = (
        races
        .assign(date = lambda x: pd.to_datetime(x.date, infer_datetime_format=True))
        .drop(columns='time')
        .merge(circuits, how='left', on='circuitId')
        .merge(results[results['positionOrder']==1][['raceId', 'Driver Name', 'Team Name', 'positionOrder']], how='left', on='raceId')
        .rename(columns={'Driver Name': '1st Driver Name','Team Name': '1st Team Name'})
        .drop(columns='positionOrder')
        .merge(results[results['positionOrder']==2][['raceId', 'Driver Name', 'Team Name', 'positionOrder']], how='left', on='raceId')
        .rename(columns={'Driver Name': '2nd Driver Name','Team Name': '2nd Team Name'})
        .drop(columns='positionOrder')
        .merge(results[results['positionOrder']==3][['raceId', 'Driver Name', 'Team Name', 'positionOrder']], how='left', on='raceId')
        .rename(columns={'Driver Name': '3rd Driver Name','Team Name': '3rd Team Name'})
        .drop(columns='positionOrder')
        .merge(constructor_res, how='left', on='raceId')
        .merge(qualifying, how='left', on='raceId')
        .merge(driver_stand[driver_stand['position']==1][['raceId', 'Driver Name']], how='left', on='raceId')
        .rename(columns={'Driver Name':'Championship Leader (after Race)'})
        .merge(constructor_stand[constructor_stand['position']==1][['raceId', 'Team Name']], how='left', on='raceId')
        .rename(columns={'Team Name':'Constructors Leader (after Race)'})
        .merge(pit_stops, how='left', on='raceId')
        .merge(diff_1_2[['raceId', 'Difference 1st to 2nd (in s)']], how='left', on='raceId')
        .merge(diff_1_5[['raceId', 'avg. Difference Top 5 (in s)']], how='left', on='raceId')
        .merge(highspeeds, how='left', on='raceId')
        .rename(columns={'fastestLapSpeed':'Top Speed of the Race'})
        .assign(Overtakes = lambda x: x.raceId)
        .assign(Overtakes = lambda y: y.Overtakes.apply(lambda x: count_overtakes(x, lap_times)))
        .merge(fastest_lap, how='left', on='raceId')
        .rename(columns={'fastestLapTime': 'Fastest Lap of the Race'})
        .merge(driver_stand_difference[['raceId', 'Driver Championship Point Difference']], how='left', on='raceId')
        .merge(const_stand_difference[['raceId', 'Constructors Championship Point Difference']], how='left', on='raceId')
        .assign(lap_rec = lambda x: x['year'].astype(str) + ',' + x['circuitId'].astype(str))
        .assign(lap_rec = lambda y: y.lap_rec.apply(lambda x: lap_record_to_date(x,results)))
        .rename(columns={'lap_rec':'Lap record to Date'})
        .assign(prev_round = lambda x: x['round'] - 1)
    )

    # subset of races needed to state whether the leader in the championship changed at this race
    prevLeaders = (
        races[['year', 'round', 'Championship Leader (after Race)', 'Constructors Leader (after Race)']]
        .rename(columns={'Championship Leader (after Race)':'Championship Leader (before Race)', 'Constructors Leader (after Race)':'Constructors Leader (before Race)'})
    )

    # merging the above subset back into races, using shift of round/counter of race in season
    races = (
        races[(races['year']<2021) & (races['year']>1999)]
        .assign(prev_races = lambda x: x['year'].astype(str) + ',' + x['circuitId'].astype(str))
        .assign(prev_races = lambda y: y.prev_races.apply(lambda x: count_prev_races(x,races)))
        .rename(columns={'prev_races':'Number of prev. F1 Races'})
        .merge(prevLeaders, how='left', left_on=['year', 'prev_round'], right_on=['year', 'round'])
        .drop(columns={'prev_round', 'round_y'})
        .rename(columns={'round_x':'round'})
        .drop_duplicates(subset=['year', 'raceId'])
    )

    # if save set to true save the dataframe into the processed folder
    if save:
        races.to_csv('../data/processed/Niklas_Processed/races.csv')

    return races
