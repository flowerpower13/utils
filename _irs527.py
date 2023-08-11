

#imports
import io
import numpy as np
import pandas as pd


#functions
from _pd_utils import _groupby


#vars
from _string_utils import ENCODING
SEP="|"  
LINETERMINATOR="\n"
NEW_SEP=","
QUOTECHAR='"'
tot=13428119
buffer_size=8192


#vars in df
#org
organization_name="a__org_name"
organization_id="a__ein"
#contributors
contributor_name_irs="a__contributor_name"
contributor_name_rdp="dtsubjectname"
contributor_isin="issueisin"
contributor_cusip="cusip"
contributor_ric="ric"
contributor_oapermid="oapermid"
contributor_address_state="a__contributor_address_state"
contributor_address_zipcode="a__contributor_address_zip_code"
contributor_employer="a__contributor_employer"
contributor_employer_new="a__contributor_employer_new"
company_involved="a__company_involved"
#contribution
contribution_amount_ytd="a__agg_contribution_ytd"
contribution_date="a__contribution_date"
contribution_year="a__contribution_year"


#code and columns
def _codecolumns(resources):

    #variables
    filename="PolOrgsFileLayout"
    columns_replace=["Pipe_Delimiter", "Pipe Delimiter"]
    codecol_sep="__"

    #read
    filepath=f"{resources}/{filename}.csv"
    df=pd.read_csv(
        filepath,
        dtype="string",
        )

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

            #update dict
            new_columns[j]=new_col

        #update key
        dict_codecolumns[code]=new_columns
    
    #return
    return dict_codecolumns


#create empty txts
def _new_txts(dict_codecolumns, results):

    #for code
    for j, (code, columns) in enumerate(dict_codecolumns.items()):

        #write
        filepath=f"{results}/{code}.csv"
        with open(
            file=filepath,
            mode="w",
            encoding=ENCODING,
            errors="strict",
            ) as file_object:

            #text
            quoted_text=[f'{QUOTECHAR}{x}{QUOTECHAR}' for x in columns]
            text=NEW_SEP.join(quoted_text) + LINETERMINATOR

            #remove carriage 
            text=text.replace("\r" ,"")

            #write
            file_object.write(text)


#irstxt to dfs
#folders=["_donations", "_irstxt_to_dfs"]
#items=["FullDataFile"]
def _irstxt_to_dfs(folders, items):

    #Download form data file (entire database of Forms 8871 and Forms 8872)
    #https://forms.irs.gov/app/pod/dataDownload/dataDownload
    #https://forms.irs.gov/app/pod/dataDownload/fullData 
    #downloaded on 2023/8/9
    #read PolOrgsFileLayout.doc 

    #folders
    resources=folders[0]
    results=folders[1]

    #items
    resource=items[0]
    result=items[1]

    #variables
    len_sep=len(SEP)

    #dict code and columnns
    dict_codecolumns=_codecolumns(resources)

    #create empty txts
    _new_txts(dict_codecolumns, results)

    #read
    filepath=f"{resources}/{resource}.txt"
    with open(
        file=filepath,
        mode="r",
        encoding=ENCODING,
        errors="strict",
        ) as file_object:

        #for
        for i, line in enumerate(file_object):

            #remove characters
            line=line.replace(LINETERMINATOR, "")\
                .replace("\r", "")\
                .replace(QUOTECHAR, "")
            
            #if line ends with sep
            if line.endswith(SEP):

                #remove sep
                line=line[:-len_sep]

            #values
            values=line.split(SEP)
            len_values=len(values)
            val0=values[0]

            #new line for csv
            quoted_values=[f'{QUOTECHAR}{x}{QUOTECHAR}' for x in values]
            new_line = NEW_SEP.join(quoted_values) + LINETERMINATOR 

            #for code
            for j, (code, columns) in enumerate(dict_codecolumns.items()):

                #len columns
                len_columns=len(columns)

                #if code and check length
                if (val0==code) and (len_values==len_columns):

                    #write
                    filepath=f"{results}/{code}.csv"
                    with open(
                        file=filepath,
                        mode="ab",
                        ) as file_object:

                        #buffer
                        with io.BufferedWriter(
                            raw=file_object,
                            buffer_size=buffer_size,
                            ) as buffered_file:

                            #encode
                            text=new_line.encode(ENCODING)

                            #remove carriage
                            text=text.replace("\r" ,"")

                            #write
                            buffered_file.write(text)

            #print
            print(f"{i}/{tot} - done")

            #try
            #if i==100000: break


#take df first value
def _first_value(df):

    #return
    return df.iloc[0]


#keep most recent obs
def _groupby_mostrecent(df, by_groupby, datevar):

    #by sortvalues
    by_sortvalues = by_groupby + [datevar]

    #dropna
    for i, col in enumerate(by_sortvalues):
        df=df.dropna(subset=col)

    #drop duplicates
    df=df.drop_duplicates(
        subset=by_sortvalues,
        )

    #ascending
    ascending = [True]*len(by_groupby) + [False]

    #sort
    df=df.sort_values(
        by=by_sortvalues,
        ascending=ascending,
        )

    #groupby most recent
    df=df.groupby(by_groupby, group_keys=False).apply(_first_value)

    #return
    return df


#contributors screen by ein
folders=["zhao/_irstxt_to_dfs", "zhao/_contributors_screen"]
items=["A", "A_screen"]
def _contributors_screen(folders, items):

    #folders
    resources=folders[0]
    results=folders[1]

    #items
    resource=items[0]
    result=items[1]

    #read
    filepath=f"{resources}/{resource}.csv"
    df=pd.read_csv(
        filepath, 
        sep=NEW_SEP,
        dtype="string",
        #nrows=35000,
        lineterminator=LINETERMINATOR,
        quotechar=QUOTECHAR,
        on_bad_lines='skip',
        )
    
    #lowercase
    df.columns=df.columns.str.lower()

    #remove carriage
    df.columns=[col.replace("\r", '') for col in df.columns]

    #lowercase
    for i, col in enumerate(df.columns):
        df[col]=df[col].str.lower()

        #remove carriage
        df[col]=df[col].replace("\r", "")

    #keep only attorneys general associations (resp. Dem and Rep)
    df=df.dropna(subset=organization_id)
    s=pd.to_numeric(df[organization_id])
    df=df[
        (s==134220019) | (s==464501717)
        ]

    #to datetime
    df[contribution_date]=pd.to_datetime(df[contribution_date], format="%Y%m%d")
    df[contribution_year]=pd.DatetimeIndex(df[contribution_date]).year

    #keep most recent organization-contributor-year obs
    by_groupby=[
        contributor_name_irs,
        organization_id,
        contribution_year,
        ]
    datevar=contribution_date
    df=_groupby_mostrecent(df, by_groupby, datevar)

    #substitute employer with nan
    dict_replace={
        "n/a": np.nan,
        "na": np.nan,
        "not available": np.nan,
        "not applicable": np.nan,
        "not employed": np.nan,
        "not a contribution": np.nan,
        "in-kind": np.nan,
        "self": np.nan,
        "self-employed": np.nan,
        "self-employed-employed": np.nan,
        "self employed": np.nan,
        "retired": np.nan,
        "attorney": np.nan,
        "vice president": np.nan,
        "teacher": np.nan,
        "founder": np.nan,
        }
    df[contributor_employer_new]=df[contributor_employer].replace(dict_replace)

    #init series
    x0=df[contributor_name_irs]
    x1=df[contributor_employer_new]

    condlist=[
        (x1.isna()),
        (~x1.isna()),
        ]   
    choicelist=[
        x0, 
        x1,
        ]
    y=np.select(condlist, choicelist, default="error")

    #outcome series
    df[company_involved]=y

    #save
    filepath=f"{results}/{result}.csv"
    df.to_csv(filepath, index=False)


#from df to full panel
def _df_to_fullpanel(df, col_id, col_year, start_year, stop_year):

    #unique ids
    unique_ids=df[col_id].unique()

    #years
    years=[str(year) for year in range(start_year, stop_year)]

    #data list
    data_list=[
        {
            col_id: identifier,
            col_year: year,
            }
            for identifier in unique_ids
            for year in years
            ]

    #empty
    df_empty=pd.DataFrame(data=data_list)

     #args
    indicator=f"_merge_fullpanel"
    suffixes=('_left', '_right')
            
    #merge
    df_fullpanel=pd.merge(
        left=df_empty,
        right=df,
        how="left",
        on=[col_id, col_year],
        suffixes=suffixes,
        indicator=indicator,
        validate="1:1",
        )

    #fillna
    df_fullpanel=df_fullpanel.fillna(0)

    #return
    return df_fullpanel


#contributors aggregate by contributor-year
folders=["zhao/_finaldb", "zhao/_aggregate"]
items=["donations_ids", "donations_ids_aggregate"]
def _contributors_aggregate(folders, items):

    #folders
    resources=folders[0]
    results=folders[1]

    #items
    resource=items[0]
    result=items[1]

    #read
    filepath=f"{resources}/{resource}.csv"
    df=pd.read_csv(
        filepath, 
        dtype="string",
        #nrows=1000,
        )
    
    #lowercase col values
    for i, col in enumerate(df.columns):
        df[col]=df[col].str.lower()

    #drop na
    dropna_cols=[
        organization_id,
        contributor_isin,
        contribution_year,
        contribution_amount_ytd,
        ]
    for i, col in enumerate(dropna_cols):
        df=df.dropna(subset=col)

    #to numeric
    df[contribution_amount_ytd]=pd.to_numeric(df[contribution_amount_ytd])

    #aggregate over organization-contributor-year obs
    by=[
        organization_id,
        contributor_isin,
        contribution_year,
        ]
    dict_agg_colfunctions={
        contribution_amount_ytd: [sum],
        contributor_name_irs: [_first_value],
        contributor_name_rdp: [_first_value],
        contributor_cusip: [_first_value],
        contributor_ric: [_first_value],
        contributor_address_state: [_first_value],
        contributor_address_zipcode: [_first_value],
        company_involved: [_first_value],
        }
    df=_groupby(df, by, dict_agg_colfunctions)

    #pivot
    index=[
        contributor_isin,
        contribution_year,
        ]
    pivot_df=pd.pivot_table(
        data=df,
        values=contribution_amount_ytd,
        index=index,
        columns=organization_id,  
        aggfunc='sum',
        fill_value=0,
        )
      
    #reset index
    pivot_df=pivot_df.reset_index()
    
    #rename
    columns={
        "134220019": "democratic_ag",
        "464501717": "republican_ag",
        }
    pivot_df=pivot_df.rename(columns=columns)

    #full panel
    col_id=contributor_isin
    col_year=contribution_year
    start_year=2000
    stop_year=2023
    df_fullpanel=_df_to_fullpanel(pivot_df, col_id, col_year, start_year, stop_year)

    #save
    filepath=f"{results}/{result}.csv"
    df_fullpanel.to_csv(filepath, index=False)

    #df without dups
    df_withoutdups=df.drop_duplicates(subset=contributor_isin)
    df_withoutdups=df_withoutdups.drop([contribution_year], axis=1)

    #args
    indicator=f"_merge_dups"
    suffixes=('_left', '_right')

    df=pd.merge(
        left=df_fullpanel,
        right=df_withoutdups,
        how="left",
        on=contributor_isin,
        suffixes=suffixes,
        indicator=indicator,
        validate="m:1",
        )

    #reorder
    ordered_cols=[
        contributor_isin,
        contribution_year,
        "democratic_ag",
        "republican_ag",
        company_involved,
        contributor_name_irs,
        contributor_name_rdp,
        contributor_cusip,
        contributor_ric,
        contributor_address_state,
        contributor_address_zipcode,
        ]
    df=df[ordered_cols]

    #save
    filepath=f"{results}/{result}.csv"
    df.to_csv(filepath, index=False)
    #'''




#violations aggregate by company-year
folders=["zhao/_data/violation_tracker", "zhao/_aggregate"]
items=["ViolationTracker_basic_28jul23", "violations_ids_aggregate"]
def _violations_aggregate(folders, items):

    #folders
    resources=folders[0]
    results=folders[1]

    #items
    resource=items[0]
    result=items[1]

    #read
    filepath=f"{resources}/{resource}.csv"
    df=pd.read_csv(
        filepath, 
        dtype="string",
        #nrows=1000,
        )

    #lowercase col values
    for i, col in enumerate(df.columns):
        df[col]=df[col].str.lower()

    #drop na
    dropna_cols=[
        "current_parent_ISIN",
        "pen_year",
        "penalty",
        "sub_penalty",
        "penalty_adjusted",
        ]
    for i, col in enumerate(dropna_cols):
        df=df.dropna(subset=col)

    #to numeric
    df["penalty"]=pd.to_numeric(df["penalty"])

    #first value dict
    list_firstvalue=[x for x in df.columns if x not in dropna_cols]
    dict_firstvalue={
        x: _first_value for x in list_firstvalue}

    print(dict_firstvalue)

    #aggregate over company-year obs
    by=[
        "current_parent_ISIN",
        "pen_year",
        ]
    dict_agg_colfunctions={
        "penalty": [sum],
        "sub_penalty": [sum],
        "penalty_adjusted": [sum],
        }
    dict_agg_colfunctions=dict_agg_colfunctions|dict_firstvalue
    df=_groupby(df, by, dict_agg_colfunctions)

    #save
    filepath=f"{results}/{result}.csv"
    df.to_csv(filepath, index=False)
    #'''


