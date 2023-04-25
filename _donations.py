

#imports
import numpy as np
import pandas as pd
from math import isnan


#variables
from _string_utils import encoding
sep="|"
codecol_sep="__"
filename="PolOrgsFileLayout"
columns_replace=["Pipe_Delimiter", "Pipe Delimiter"]


#code and columns
def _codecolumns(resources):

    #read
    file_path=f"{resources}/{filename}.csv"
    df=pd.read_csv(file_path, dtype="string")

    #replace pipe delimiter with nan
    df=df.replace(columns_replace, np.nan)

    #df to dict
    dict_codecolumns=df.to_dict(orient="list")

    #for key value
    for i, (code, columns) in enumerate(dict_codecolumns.items()):

        #remove na
        new_columns=[
            x for x in columns 
            if not pd.isna(x)
            ]
        
        #for value
        for j, new_col in enumerate(new_columns):

            #lowercase and replace whitespace
            new_col=new_col.lower().replace(" ", "_")
            new_col=f"{code}{codecol_sep}{new_col}"

            #update values
            new_columns[j]=new_col

        #update key
        dict_codecolumns[code]=new_columns
    
    #list columns
    list_columns=list(dict_codecolumns.values())

    return dict_codecolumns, list_columns



#open large file
def _open_large(file_path):

    #open()
    with open(
        file=file_path,
        mode="rb",
        encoding=encoding,
        errors="strict",
        ) as file_object:
    
        #split or readlines
        text=file_object.read()
        #lines=file_object.readlines()

    #mmap
    #'''
    import mmap
    with open(file_path, 'r') as f:
        with mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ) as data:
            text = data.read().decode(encoding)
    #'''

    #pandas
    #'''
    data = pd.read_csv(file_path, sep='\n', header=None, engine='c')
    text = data[0].str.cat(sep='\n')
    #'''

    #lines
    lines=text.split('\n')

    #try
    n=100
    lines=lines[0:n]
    
    return lines


folders=["donations", "_irstxt_to_df"]
items=["FullDataFile", "_irstxt_to_df"]
def _irstxt_to_df(folders, items):
    resources=folders[0]
    results=folders[1]

    resource=items[0]
    result=items[1]

    #read
    file_path=f"{resources}/{resource}.txt"

    #open
    lines=_open_large(file_path)

    #code and columns
    dict_codecolumns, list_columns=_codecolumns(resources)
    
    #n obs
    n_obs=len(lines)
    tot=n_obs-1

    #empty
    df=pd.DataFrame(columns=list_columns)

    #for line
    for i, line in enumerate(lines):

        #remove sep at end
        line=line[:-1] if line.endswith(sep) else line

        #values
        values=line.split(sep)

        #len values
        len_values=len(values)

        #first value
        val0=values[0]

        #for code
        for j, (code, columns) in enumerate(dict_codecolumns):

            #len columns
            len_columns=len(columns)

            #if code
            if val0==code:

                #check
                if len_values==len_columns:

                    #for column
                    for k, column in enumerate(columns):   

                        #update
                        df.at[i, column]=values[k]
                
                #break
                else:
                    #print
                    print(f"{i}/{tot} - error")

                    #break
                    break


        #print
        print(f"{i}/{tot} - done")

    #save
    file_path=f"{results}/{result}.csv"
    df.to_csv(file_path, index=False)
    #'''


folders=["donations", "_irstxt_to_df"]
items=["FullDataFile", "_irstxt_to_df"]
_irstxt_to_df(folders, items)


print("done")