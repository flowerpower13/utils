

#imports
import pandas as pd


#functions
from _pd_utils import _folder_to_filestems


#concatenate csv files with word counts
#folders=["_txts_to_counts", "_concat"]
#items=["_concat"]
#full_db="_advev"
def _concat(folders, items, full_db):
    resources=folders[0]
    results=folders[1]

    result=items[0]

    #colname
    colname_filestems="file_stem"

    #resources
    files, filestems_resources=_folder_to_filestems(resources)

    #full db
    files, filestems_fulldb=_folder_to_filestems(full_db)

    #n obs
    n_obs=len(filestems_fulldb)
    tot=n_obs-1

    ##ordered frames and cols
    frames=[None]*n_obs
    converteds=[None]*n_obs
    
    #iteration
    for i, file_stem in enumerate(filestems_fulldb):

        #file is present
        if file_stem in filestems_resources:

            #open df
            filepath=f"{resources}/{file_stem}.csv"
            df=pd.read_csv(filepath, dtype="string")

            #converted
            converted=True

            #print
            print(f"{i}/{tot} - {file_stem} - present")
            
        #file is NOT present
        elif not file_stem in filestems_resources:

            #converted
            converted=False

            #add file stem
            df=pd.DataFrame()
            df[colname_filestems]=[file_stem]

            #print
            print(f"{i}/{tot} - {file_stem} - missing")

        #ordered cols
        frames[i]=df
        converteds[i]=converted

    #concat
    df=pd.concat(frames)
    df.insert(1, f"converted{full_db}", converteds)  
    df=df.sort_values(by=colname_filestems)

    #save
    filepath=f"{results}/{result}.csv"
    df.to_csv(filepath, index=False)
    