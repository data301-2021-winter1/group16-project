#%%

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
        
    #merging the results and races df 
    try: 
        results = pd.read_csv(raw_path+"results.csv", na_values=['\\N'])
        races = pd.read_csv(raw_path+"races.csv", na_values=['\\N'])
        results_races = pd.merge(results,races,on="raceId",how="left")
        results_races = results_races.drop(["url","time_y"],axis=1)
        results_races.to_csv(processed_path+"results.csv")
    except: 
        print("Could not locate the results.csv and races.csv")

# %%
