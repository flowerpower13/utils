
import pandas as pd


import numpy as np
import pandas as pd


def _dict_to_valscols(dict_data):
        count_values=dict_data.values()
        count_keys=dict_data.keys()
        
        values=[[x] for x in count_values]
        keys=[x for x in count_keys]

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

    dict_cols={}
    for i, funct in enumerate(functions):
        funct_name=funct.__name__
        col_functname=f"{col}_{funct_name}"

        dict_cols[funct_name]=col_functname

    return dict_cols


def _colfunctions_to_df(df, cols, functions):

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

    frames=[df]+agg_frames
    df_concat=pd.concat(frames, axis=1)

    return df_concat

