
import numpy as np
import pandas as pd
from rapidfuzz import process, utils as fuzz_utils


#read csv lowercase
def _readcsv_lowercase(df_path, df_ons):

    #read
    filepath=f"{df_path}.csv"
    df=pd.read_csv(
        filepath,
        dtype="string",
        #nrows=100000,
        )
    
    #lowercase cols
    df.columns=df.columns.str.lower()

    #lowercase col values
    for i, col in enumerate(df.columns):
        df[col]=df[col].str.lower()

    #return
    return df


#from dfpath to dfon
def _dfpath_to_dfon(df_path, df_ons):

    #read
    filepath=f"{df_path}.csv"
    df=pd.read_csv(
        filepath,
        dtype="string",
        #nrows=100000,
        )
    
    #lowercase cols
    df.columns=df.columns.str.lower()

    #lowercase col values
    for i, col in enumerate(df.columns):
        df[col]=df[col].str.lower()

    #dropna
    df=df.dropna(subset=df_ons)

    #return
    return df

#merge
folders=["zhao/_epa"]
items=["CASE_FACILITIES_screen_CASE_ENFORCEMENT_CONCLUSIONS_screen"]
left_path="zhao/_epa/CASE_FACILITIES_screen"
left_ons=["activity_id", "case_number"]
right_path="zhao/_epa/CASE_ENFORCEMENT_CONCLUSIONS_screen"
right_ons=["activity_id", "case_number"]
how="left"
validate="m:1"
def _pd_merge(folders, items, left_path, left_ons, right_path, right_ons, how, validate):

    #https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.merge.html

    #folders items
    results=folders[0]
    result=items[0]

    #df and df_on
    left=_dfpath_to_dfon(left_path, left_ons)
    right=_dfpath_to_dfon(right_path, right_ons)

    #args
    indicator=f"_merge_{result}"
    suffixes=('_left', '_right')

    #merge
    df=pd.merge(
        left=left,
        right=right,
        how=how,
        left_on=left_ons,
        right_on=right_ons,
        suffixes=suffixes,
        indicator=indicator,
        validate=validate,
        )

    #save
    filepath=f"{results}/{result}.csv"
    df.to_csv(filepath, index=False)


#from df on to numeric df
def _dfon_to_numericdf(df, df_on):

    #dropna
    df=df.dropna(subset=df_on)
    
    #astype
    df[df_on]=df[df_on].astype("int64")

    #sortvalues
    df=df.sort_values(by=df_on)
    
    #return
    return df


#merge as of
folders=["zhao/_epa"]
items=["CASE_FACILITIES_screen_CASE_ENFORCEMENT_CONCLUSIONS_screen_TRI_screen"]
left_path="zhao/_epa/CASE_FACILITIES_screen_CASE_ENFORCEMENT_CONCLUSIONS_screen"
left_bys=["registry_id"]
left_on="echo_initiation_year"
right_path="zhao/_epa/TRI_screen"
right_bys=["epa_registry_id"]
right_on="reporting_year"
def _pd_merge_asof(folders, items, left_path, left_bys, left_on, right_path, right_bys, right_on):

    #https://pandas.pydata.org/docs/reference/api/pandas.merge_asof.html

    #folders items
    results=folders[0]
    result=items[0]

    #df and df_on
    left=_dfpath_to_dfon(left_path, left_bys)
    right= _dfpath_to_dfon(right_path, right_bys)

    #to numeric
    left=_dfon_to_numericdf(left, left_on)
    right=_dfon_to_numericdf(right, right_on)

    #args
    suffixes=('_left', '_right')
    direction="nearest"

    #merge
    df=pd.merge_asof(
        left=left,
        right=right,
        left_on=left_on,
        right_on=right_on,
        left_by=left_bys,
        right_by=right_bys,
        suffixes=suffixes,
        direction=direction,
        )

    #save
    filepath=f"{results}/{result}.csv"
    df.to_csv(filepath, index=False)


#concat with same rows/cols
'''folders=["_finaldb"]
items=["_finaldb"]
left="_finaldb/_finaldb_isin"
right="_finaldb/_finaldb_cusip"
axis="index"
join="outer"
sort_id=["file_stem"]
#'''
def _pd_concat(folders, items, left_path, right_path, axis, join, sort_id):

    #https://pandas.pydata.org/docs/reference/api/pandas.concat.html

    #folders items
    results=folders[0]
    result=items[0]

    #read csvs
    left=_readcsv_lowercase(left_path)
    right=_readcsv_lowercase(right_path)

    #frames
    frames=[left, right]

    #https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.merge.html
    df=pd.concat(
        objs=frames,
        axis=axis,
        join=join,
        )

    #drop duplicates
    df=df.drop_duplicates(subset=sort_id,)
    
    #sortvalues
    df=df.sort_values(by=sort_id)

    #save
    filepath=f"{results}/{result}.csv"
    df.to_csv(filepath, index=False)


#extract similscore
def _fuzzy_extractscore(row):
    score=row[0][1]

    return score


#fuzzy merge
def fuzzy_merge(left, right, left_on, right_on, threshold, limit, how):

    #https://stackoverflow.com/questions/64360880/rapidfuzz-match-merge

    #s mapping
    s_mapping={x: fuzz_utils.default_process(x) for x in right[right_on]}

    #match col
    _function0=lambda x: process.extract(fuzz_utils.default_process(x), s_mapping, limit=limit, score_cutoff=threshold, processor=None)
    left['Match']=left[left_on].apply(_function0)

    #new col
    left_on_new=f"{left_on}_new"
    _function1=lambda x: ', '.join(i[2] for i in x)
    left[left_on_new]=left['Match'].apply(_function1)
    left[left_on_new]=left[left_on_new].replace("", np.nan)

    #merge
    df=pd.merge(
        left=left,
        right=right,
        how=how,
        left_on=left_on_new,
        right_on=right_on,
        suffixes=("_left", "_right"),
        indicator=True,
        validate=None,
        )  

    score_col="similscore"
    df[score_col]=df["Match"].apply(_fuzzy_extractscore)

    #change cols order
    new_cols=[left_on, right_on, score_col, "_merge"]
    df=df[new_cols]

    return df


#read with colnames and colvalues as lowercase
def _fuzzy_readcsv(df_stem, df_on):
    filepath=f"{df_stem}.csv"
    df=pd.read_csv(
        filepath,
        dtype="string",
        #nrows=100000,
        )
    
    #lowercase
    df.columns=df.columns.str.lower()

    #lowercase col values
    cols=df.columns.values
    for i, col in enumerate(cols):
        df[col]=df[col].str.lower()

    #select col
    df=df[[df_on]]

    #drop dups
    df=df.drop_duplicates(subset=df_on)

    return df


#fuzzy match
'''folders=["_fuzzymatch"]
items=["_fuzzymatch"]
left_stem="left"
right_stem="right"
left_on="coname"
right_on="name"
threshold=90 
limit=1
how="inner"
#'''
def _fuzzymatch(folders, items, left_stem, left_on, right_stem, right_on, threshold, limit, how):

    #folders items
    results=folders[0]
    result=items[0]

    #load dfs
    left=_fuzzy_readcsv(left_stem, left_on)
    right=_fuzzy_readcsv(right_stem, right_on)
    
    #match
    df=fuzzy_merge(left, right, left_on, right_on, threshold, limit, how)

    #save
    filepath=f"{results}/{result}.csv"
    df.to_csv(filepath, index=False)


#keep best match
'''folders=["_fuzzymatch", "_keepfirst"]
items=["_keepfirst"]
left_on="coname"
right_on="name"
similscore="similscore"
#'''
def _keepfirst(folders, items, left_on, right_on, similscore):
    resources=folders[0]
    results=folders[1]

    resource=items[0]
    result=items[1]

    filepath=f"{resources}/{resource}.csv"
    df=pd.read_csv(filepath, dtype="string")

    #matches with highest score at the top
    by=[right_on, similscore]
    df=df.sort_values(
        by=by, 
        ascending=False,
        )

    #drop duplicates
    df=df.drop_duplicates(subset=right_on)

    #sort by left_on
    df=df.sort_values(by=left_on) 

    #save
    filepath=f"{results}/{result}.csv"
    df.to_csv(filepath, index=False)

