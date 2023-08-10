

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
contributor_cusip="cusip"
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


#contributors aggregate by organization-contributor-year
#folders=["zhao/_contributors_screen", "zhao/_search"]
#items=["A_screen", "A_search"]
#colname="A__company_involved"
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
        #nrows=10**7,
        #on_bad_lines='skip',
        )
    
    #lowercase col values
    for i, col in enumerate(df.columns):
        df[col]=df[col].str.lower()

    #drop na
    dropna_cols=[
        organization_id,
        contributor_cusip,
        contribution_year,
        contribution_amount_ytd,
        ]
    for i, col in enumerate(dropna_cols):
        df=df.dropna(subset=col)

    #aggregate over organization-contributor-year obs
    by=[
        organization_id,
        contributor_cusip,
        contribution_year,
        ]
    dict_agg_colfunctions={
        contribution_amount_ytd: [sum],
        organization_name: [_first_value],
        contributor_name_irs: [_first_value],
        contributor_name_rdp: [_first_value],
        contributor_address_state: [_first_value],
        contributor_address_zipcode: [_first_value],
        company_involved: [_first_value],
        }
    
    df=_groupby(df, by, dict_agg_colfunctions)

    #save
    filepath=f"{results}/{result}.csv"
    df.to_csv(filepath, index=False)



#violations aggregate by company-year
folders=["zhao/_finaldb", "zhao/_aggregate"]
items=["violations_cusip", "violations_cusip_aggregate"]
def _violations_aggregate(folders, items):

    #https://violationtracker.goodjobsfirst.org/pages/user-guide

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
        #nrows=10**7,
        #on_bad_lines='skip',
        )
    
    #lowercase col values
    for i, col in enumerate(df.columns):
        df[col]=df[col].str.lower()

    #drop na
    dropna_cols=[
        organization_id,
        contributor_cusip,
        contribution_year,
        contribution_amount_ytd,
        ]
    for i, col in enumerate(dropna_cols):
        df=df.dropna(subset=col)

    #aggregate over organization-contributor-year obs
    by=[
        organization_id,
        contributor_cusip,
        contribution_year,
        ]
    dict_agg_colfunctions={
        contribution_amount_ytd: [sum],
        organization_name: [_first_value],
        contributor_name_irs: [_first_value],
        contributor_name_rdp: [_first_value],
        contributor_address_state: [_first_value],
        contributor_address_zipcode: [_first_value],
        company_involved: [_first_value],
        }
    
    df=_groupby(df, by, dict_agg_colfunctions)

    #save
    filepath=f"{results}/{result}.csv"
    df.to_csv(filepath, index=False)










#0 dont do
#compustat name/cusip matching table
folders=["zhao/_data", "zhao/_data"]
items=["crspcompustat_2000_2023", "crspcompustat_2000_2023_linktable"]
colnames={
    "name": "conm",
    "identifier": "cusip",
    }
#_df_to_uniquecol(folders, items, colnames)
#list of unmatched names
folders=["zhao/_search", "zhao/_search"]
items=["A_search_A__company_involved", "A_search_A__company_involved_unmatched"]
colnames={
    "name": "query",
    "identifier": "CUSIP",
    }
#_df_to_unmatchedcol(folders, items, colnames)
#manually match cusip to each name
#look at "zhao/_data/crspcompustat_2000_2023_linktable"
#change col format to "Text", modify column "CUSIP" of "zhao/_search/A_search_A__company_involved_unmatched"
#put matched and unmatched together
