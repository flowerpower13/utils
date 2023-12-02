

#imports
import numpy as np
import pandas as pd
from pathlib import Path


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
        filepath=f"{results}/{result}_{col_name}.csv"
        df_i.to_csv(filepath, index=False)

        filepath=f"{results}/{result}_{col_name}.txt"
        df_i.to_csv(filepath, index=False)


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


#by=["var0"]
#dict_agg_colfunctions={"var1": [sum], "var2": [" ".join]}
def _groupby(df, by, dict_agg_colfunctions):

    #df columns
    df_cols=df.columns.values

    #not by
    not_by=[x for x in df_cols if x not in by]

    #groupby
    grouped=df.groupby(by=by)[not_by]
    df=grouped.agg(dict_agg_colfunctions)

    #drop levels
    df.columns=df.columns.droplevel(1)

    #reset index
    df=df.reset_index()

    #return
    return df


#by=["var0"]
#dict_agg_colfunctions={"var1": sum, "var2": " ".join}
def _groupby_noagg(df, by, dict_agg_colfunctions):

    #dropna
    for i, col in enumerate(by):
        df=df.dropna(subset=col)

    #to string    
    for i, col in enumerate(by):
        df[col]=df[col].astype("string")

    #gen id
    by_var="_".join(by)
    df[by_var]=df[by].agg("_".join, axis="columns")

    #to numeric  
    #df[old_var]=pd.to_numeric(df[old_var])

    #groupby  
    for i, (key, value) in enumerate(dict_agg_colfunctions.items()):

        #group var
        grouped=df.groupby(by=by_var)[key].apply(value)

        #gen new var
        new_var=f"{key}_{value.__name__}"
        df[new_var]=df[by_var].map(grouped)

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


#from csv to dict_df (df as dictionary)
def _csv_to_dictdf(filepath, index_col):

    #read_csv
    df=pd.read_csv(
        filepath, 
        #dtype="string",
        )

    #set index
    df=df.set_index(index_col)

    #convert to dict
    df_to_dict=df.to_dict(orient="index")

    return df_to_dict


#from csv to set values ()
def _csv_to_setvalues(filepath, bad_keywords):

    #read csv
    df=pd.read_csv(
        f"{filepath}.csv", 
        dtype="string",
        )

    #col values to list
    col_values=df[filepath].str.lower().to_list()

    #list to set minus bad keywords
    set_values=set(col_values)-bad_keywords

    return set_values


#from folder to file stems' list
#files, file_stems = _folder_to_filestems(resources)
def _folder_to_filestems(folder):

    #global path
    p=Path(folder).glob('**/*')

    #file paths
    files=[
        x for x in p 
        if x.is_file() and not x.name==f"{folder}.csv"
        ]
    
    #sort
    files.sort()

    #file stems
    filestems=[x.stem for x in files]

    return files, filestems
    

#from df to column with unique values
'''folders=["zhao/_data", "zhao/_data"]
items=["crspcompustat_2000_2023", "crspcompustat_2000_2023_uniquecusip"]
colnames={
    "name": "conm",
    "identifier": "cusip",
    }'''
def _df_to_uniquecol(folders, items, colnames):

    #folders
    resources=folders[0]
    results=folders[0]

    #items
    resource=items[0]
    result=items[1]

    filepath=f"{resources}/{resource}.csv"
    df=pd.read_csv(
        filepath,
        dtype=object,
        )
    
    #unpack
    name=colnames["name"]
    identifier=colnames["identifier"]
    cols=[name, identifier]

    #keep vars
    df=df[[name, identifier]]

    #drop na
    df=df.dropna()

    #lowercase
    for i, col in enumerate(cols):
        df[col]=df[col].str.lower()

    #drop duplicates
    df=df.drop_duplicates(subset=cols)

    #sort
    df=df.sort_values(by=[name])

    #save
    filepath=f"{results}/{result}.xlsx"
    df.to_excel(filepath, index=False)


#from df to column with unmatched values
'''folders=["zhao/_search", "zhao/_search"]
items=["A_search", "A_search_unmatched"]
colnames={
    "name": "A__company_involved",
    "identifier": "CUSIP",
    }
#'''
def _df_to_unmatchedcol(folders, items, colnames):

    #folders
    resources=folders[0]
    results=folders[0]

    #items
    resource=items[0]
    result=items[1]

    filepath=f"{resources}/{resource}.csv"
    df=pd.read_csv(
        filepath,
        dtype=object,
        )
    
    #unpack
    name=colnames["name"]
    identifier=colnames["identifier"]
    cols=[name, identifier]

    #keep vars
    df=df[[name, identifier]]

    #drop not-na for identifier
    df=df[df[identifier].isna()]

    #lowercase
    for i, col in enumerate(cols):
        df[col]=df[col].str.lower()

    #drop duplicates
    df=df.drop_duplicates(subset=[name])

    #sort
    df=df.sort_values(by=[name])

    #save
    filepath=f"{results}/{result}.xlsx"
    df.to_excel(filepath, index=False)


#first value
def _first_value(df):

    #row
    row=df.iloc[0]

    #return
    return row


#first value (if same) or join (if different)
def _firstvalue_join(series, sep="||"):
    
    #unique
    unique_values = series.unique()
    
    #if
    if len(unique_values)==1:

        #value
        value=series.iloc[0]
    
    #else
    else:

        #value
        value=sep.join(map(str, series))
    
    #return
    return value


#lowercase col names and values
def _lowercase_colnames_values(df):

    #lowercase col names
    #df.columns=df.columns.str.lower()

    #lowercase col values
    for i, col in enumerate(df.columns):
        df[col]=df[col].str.lower()

    #return
    return df


#to date cols to df
def _todatecols_to_df(df, todate_cols, errors, format, new_format="%Y-%m-%d"):

    #for
    for i, col in enumerate(todate_cols):

        #to date
        df[col]=pd.to_datetime(
            df[col],
            errors=errors,
            format=format,
            )
    
    #for
    for i, col in enumerate(todate_cols):

        #to date
        df[col]=df[col].dt.strftime(new_format)

    #return
    return df


#to numeric cols to df
def _tonumericcols_to_df(df, tonumeric_cols, errors="raise"):
    
    #for
    for i, col in enumerate(tonumeric_cols):
        
        #to date
        df[col]=pd.to_numeric(
            df[col],
            errors=errors,
            )

    #return
    return df


#to fillna cols to df
def _fillnacols_to_df(df, fillna_cols, value=0):

    tonumeric_cols=fillna_cols
    errors="raise"
    df=_tonumericcols_to_df(df, tonumeric_cols, errors)

    #for
    for i, col in enumerate(fillna_cols):

        #to date
        df[col]=df[col].fillna(value)

    #return
    return df


#full panel
'''
cols_id=[
    violtrack_parent_id,
    violtrack_initiation_year,
    ]
years=[
    2000,
    2023,
    ]
fillna_cols=[
    violtrack_penalty_amount,
    ]
#'''
def _df_to_fullpanel(df, cols_id, years, fillna_cols):

    #ids
    company_id=cols_id[0]
    fiscal_year=cols_id[1]

    #years
    start_year=years[0]
    stop_year=years[1]

    #unique company id
    span_company_ids=df[company_id].unique()

    #sort
    span_company_ids=np.sort(span_company_ids)

    #span years
    span_years=[y for y in range(start_year, (stop_year+1))]

    #data list
    data_list=[
        {
            company_id: x,
            fiscal_year: y,
            }
            for x in span_company_ids
            for y in span_years
            ]

    #empty
    df_empty=pd.DataFrame(data=data_list)

    #args
    indicator=f"_merge_fullpanel"
    suffixes=('_left', '_right')

    #to numeric
    tonumeric_cols=[fiscal_year]
    errors="raise"
    df=_tonumericcols_to_df(df, tonumeric_cols, errors)

    #merge
    df=pd.merge(
        left=df_empty,
        right=df,
        how="left",
        on=cols_id,
        suffixes=suffixes,
        indicator=indicator,
        validate="1:1",
        )

    #if
    if fillna_cols:

        #fillna
        df=_fillnacols_to_df(df, fillna_cols)

    #else
    else:
        pass

    #return
    return df


#usecols, lowercase, dropna, to numeric, to date, fillna, sortvalues, fullpanel, ordered


#gen dummies
def _gen_dummies(df, col, prefix, drop_first):

    #get dummies
    dummies=pd.get_dummies(
        df[col],
        prefix=prefix,
        prefix_sep="_",
        drop_first=drop_first,
        )

    #colnames
    dummies_cols=list(dummies.columns)

    #concat
    df=pd.concat([df, dummies], axis=1)

    #return
    return df, dummies_cols


#gen aggregate pastn
def _aggregate_pastn(df, unit_var, oldvar, agg_funct, n_shifts):

    #to numeric
    tonumeric_cols=[
        oldvar,
        ]
    df=_tonumericcols_to_df(df, tonumeric_cols)

    #init cols
    var_shifted_cols=[None]*n_shifts

    #for
    for i in range(n_shifts):

        #periods
        periods=i+1

        #col name
        var_shifted=f"{oldvar}_shift{periods}"

        #gen new col
        df[var_shifted]=df.groupby(unit_var)[oldvar].shift(periods=periods, fill_value=0)

        #update cols
        var_shifted_cols[i]=var_shifted

    #y
    y=agg_funct(df[var_shifted_cols], axis=1)

    #newvar
    newvar=f"{oldvar}_past{n_shifts}"

    #gen
    df[newvar]=y

    #return
    return df

