
#importing libraries 
import pandas as pd 
from os import listdir

def pipeline(raw_path,processed_path): 

    try: 
        for file in listdir(raw_path): 
            if file[-4:]== ".csv":
                path  = raw_path + file 
                data = pd.read_csv(path, na_values=['\\N'])
                data.to_csv(processed_path+file)
    except: 
        print("Could not locate the files")
        
    #merging the results, races and drivers df 
    try: 
        results = pd.read_csv(raw_path+"results.csv", na_values=['\\N'])
        races = pd.read_csv(raw_path+"races.csv", na_values=['\\N'])
        drivers = pd.read_csv(raw_path+"drivers.csv", na_values=['\\N'])
        results_races = pd.merge(results,races,on="raceId",how="left")
        results_races = pd.merge(results_races,drivers[["driverId","driverRef"]], on = "driverId", how = "left")
        results_races = results_races.drop(["url","time_y","fastestLapTime","rank","statusId","round","name","date"],axis=1)
        
        #The point system has changed, so we are normalizing the points for each season to the standart of a maximum of 25 points given in the 2021 season
        points = ( 
            results_races[["year","points"]]
            .groupby("year")
            .max()
            .reset_index()
            .rename(columns={"points":"max_points"})
            )
        results_races = ( pd.merge(results_races,points,on="year",how="inner") )
        results_races["points"] = ( results_races["points"] * (25/(results_races["max_points"])) )
        results_races= results_races.drop(columns=["max_points"])
        results_races.to_csv(processed_path+"results_races.csv")

    except: 
        print("Could not locate the results.csv and races.csv")

    #making a list of all driver who has been in more than 40 races     
    try: 
        drivers2 = (results_races[["driverId"]]
            .copy()
            .groupby("driverId")
            .size()
            .reset_index(name="size")
           )
        drivers2 = drivers2.drop(drivers2[drivers2["size"] < 40 ].index)
        drivers2.to_csv(processed_path+"drivers2.csv")
    except: 
        print("Could not make the filtered drivers list")
        
    #making the scoreboard for the individual drivers:     
    try: 
        overall_driver = drivers[["driverId","driverRef"]].copy() 
        overall_driver["points"] =0 
        overall_driver.to_csv(processed_path+"overall_driver.csv")
        
    except: 
        print("Could not create driver scoreboard")
 