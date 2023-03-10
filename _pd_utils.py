
import numpy as np
import pandas as pd


#clean file stem
def _clean_stem(file_stem):
    for r in ['\\', '/', ':', '*', '"', '<', '>', '|']:
        file_stem=file_stem.replace(r, '')
    return file_stem


#from df's column to list of values in column (drop nas and dups)
def _dfcol_to_listcol(df, col_name):

    #drop nas and dups
    df=df.dropna(subset=[col_name])
    df=df.drop_duplicates(subset=[col_name])

    #convert  to list
    col_values=df[col_name].to_list()

    return col_values


#for each column, create a separate csv
def _df_to_csvcols(df, results, result):
    
    #iterate over df columns
    for i, col_name in enumerate(df):

        col_values=_dfcol_to_listcol(df, col_name)

        df_i=pd.DataFrame()
        df_i[col_name]=col_values

        #clean col name
        col_name=_clean_stem(col_name)

        #save
        file_path=f"{results}/{result}_{col_name}.csv"
        df_i.to_csv(file_path, index=False)

        file_path=f"{results}/{result}_{col_name}.txt"
        df_i.to_csv(file_path, index=False)


#from dictionary to values and keys
def _dict_to_valscols(dict_data):
        
        #values and keys list
        values=[[x] for x in dict_data.values()]
        keys=[x for x in dict_data.keys()]

        return values, keys


#create pd.DataFrame from values and columns as list
'''values=[
        vals0, 
        vals1, 
        ]
columns=[
        "col0", 
        "col1", 
        ]'''
def _pd_DataFrame(values, columns):

    data=np.transpose(np.array(values))
    df=pd.DataFrame(data=data, columns=columns)

    return df


#e.g.,
#by=["var0"]
#dict_agg_colfunctions={"var1": [sum], "var2": [" ".join]}
def _groupby(df, by, dict_agg_colfunctions):
    df_cols=df.columns.values
    not_by=[x for x in df_cols if x not in by]

    df=df.groupby(by=by)[not_by].agg(dict_agg_colfunctions)

    #drop levels
    df.columns=df.columns.droplevel(1)

    #set index
    df=df.reset_index()

    return df


#rename col names
def _colfunctions_to_colnames(col, functions):

    #empty dictionary
    dict_cols={}

    #iterate over functions
    for i, funct in enumerate(functions):

        #function and col name
        funct_name=funct.__name__
        col_functname=f"{col}_{funct_name}"

        #update dictionary
        dict_cols[funct_name]=col_functname

    return dict_cols


#from columns and function to df
def _colfunctions_to_df(df, cols, functions):

    #n obs
    n_obs=len(cols)
    agg_frames=[None]*n_obs

    for i, col in enumerate(cols):
        #aggs
        dict_agg_colfunctions={col: functions}
        df_agg=df.agg(dict_agg_colfunctions)

        #drop levels
        df_agg.columns=df_agg.columns.droplevel(0)

        #rename cols
        dict_rename_cols=_colfunctions_to_colnames(col, functions)
        df_agg=df_agg.rename(columns=dict_rename_cols)

        #add to frames
        agg_frames[i]=df_agg

    #frames
    frames=[df]+agg_frames
    df_concat=pd.concat(frames, axis=1)

    return df_concat

