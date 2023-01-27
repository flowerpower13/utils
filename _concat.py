
import pandas as pd
from pathlib import Path


#from folder to file stems' list
def _folder_to_filestems(folder):

    #global path
    p=Path(folder).glob('**/*')

    #file paths
    files={
        x for x in p 
        if x.is_file() and not x.name==f"{folder}.csv"
        }

    #file stems
    file_stems={x.stem for x in files}

    #sort
    files=sorted(files)
    file_stems=sorted(file_stems)

    return files, file_stems


#concatenate csv files with word counts
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
            file_path=f"{resources}/{file_stem}.csv"
            df=pd.read_csv(file_path, dtype="string")

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
    file_path=f"{results}/{result}.csv"
    df.to_csv(file_path, index=False)