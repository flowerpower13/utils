

#imports
import re
import io
import numpy as np
import pandas as pd
from pathlib import Path
#import linktransformer as lt
from datetime import datetime


#functions
from _merge_utils import _pd_merge, _dfpath_to_dfon
from _pd_utils import _folder_to_filestems, \
    _lowercase_colnames_values, _todatecols_to_df, _tonumericcols_to_df, _fillnacols_to_df, \
    _groupby, _df_to_fullpanel, \
    _first_value, _firstvalue_join


#vars
from _string_utils import ENCODING
OLD_SEP="|"  
OLD_LINETERMINATOR="\r\n"
NEW_LINETERMINATOR="\n"
NEW_SEP=","
QUOTECHAR='"'
NA_VALUE=None
CURRENT_YEAR=datetime.now().year
CURRENT_YEAR_2DIGIT=CURRENT_YEAR%100
tot=13428119
buffer_size=8192


#irs code and columns
def _irs_codecolumns(resources):

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
    df=df.replace(columns_replace, NA_VALUE)

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


#_irs_newtxts
def _irs_newtxts(dict_codecolumns, results):

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
            text=NEW_SEP.join(quoted_text) + NEW_LINETERMINATOR

            #remove carriage 
            text=text.replace("\r" ,"")

            #write
            file_object.write(text)


#_irs_txt_to_dfs
folders=["zhao/data/irs", "zhao/_irs"]
items=["FullDataFile"]
def _irs_txt_to_dfs(folders, items):

    #Download form data file (entire database of Forms 8871 and Forms 8872)
    #https://forms.irs.gov/app/pod/dataDownload/dataDownload
    #https://forms.irs.gov/app/pod/dataDownload/fullData 
    #downloaded on 2023/08/09
    #read PolOrgsFileLayout.doc 

    #folders
    resources=folders[0]
    results=folders[1]

    #items
    resource=items[0]
    result=items[1]

    #variables
    len_sep=len(OLD_SEP)

    #dict code and columnns
    dict_codecolumns=_irs_codecolumns(resources)

    #create empty txts
    _irs_newtxts(dict_codecolumns, results)

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
            line=line.replace(OLD_LINETERMINATOR, "")\
                .replace(QUOTECHAR, "")
            
            #if line ends with sep
            if line.endswith(OLD_SEP):

                #remove sep
                line=line[:-len_sep]

            #values
            values=line.split(OLD_SEP)
            len_values=len(values)
            val0=values[0]

            #new line for csv
            quoted_values=[f'{QUOTECHAR}{x}{QUOTECHAR}' for x in values]
            new_line = NEW_SEP.join(quoted_values) + NEW_LINETERMINATOR 

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


#_irs_A_screen
folders=["zhao/_irs", "zhao/_irs"]
items=["A", "A_screen"]
def _irs_A_screen(folders, items):

    #folders
    resources=folders[0]
    results=folders[1]

    #items
    resource=items[0]
    result=items[1]

    #usecols
    usecols=[
        #"A__record_type",
        #"A__form_id_number",
        #"A__sched_a_id",
        "A__org_name",
        "A__ein",
        "A__contributor_name",
        "A__contributor_address_1",
        #"A__contributor_address_2",
        "A__contributor_address_city",
        "A__contributor_address_state",
        "A__contributor_address_zip_code",
        #"A__contributor_address_zip_ext",
        "A__contributor_employer",
        "A__contribution_amount",
        #"A__contributor_occupation",
        #"A__agg_contribution_ytd",
        "A__contribution_date\r", #???
        ]
    usecols=[x.capitalize() for x in usecols]

    #read
    filepath=f"{resources}/{resource}.csv"
    df=pd.read_csv(
        filepath, 
        sep=NEW_SEP,
        usecols=usecols,
        dtype="string",
        #nrows=100000,
        lineterminator=NEW_LINETERMINATOR,
        quotechar=QUOTECHAR,
        #on_bad_lines='skip',
        )

    #lowercase col names and values
    df=_lowercase_colnames_values(df)

    #???
    df=df.rename(columns={f"A__contribution_date\r": "A__contribution_date"})

    #dropna
    dropna_cols=[
        "A__ein",
        "A__contributor_name",
        "A__contribution_amount",
        "A__contribution_date", 
        ]
    df=df.dropna(subset=dropna_cols)

    #tonumeric
    tonumeric_cols=[
        "A__ein",
        "A__contribution_amount",
        ]
    df=_tonumericcols_to_df(df, tonumeric_cols)

    #dict_pivot_columns
    dict_pivot_columns={
        134220019: "daga",
        464501717: "raga",
        #521304889: "dga",
        #113655877: "rga",
        #521870839: "dlcc",
        #050532524: "rslc",
        }

    #select rows
    df=df[df["A__ein"].isin([key for key in dict_pivot_columns.keys()])]

    #todate
    todate_cols=[
        "A__contribution_date",
        ]
    errors="coerce"
    format="%Y%m%d"
    df=_todatecols_to_df(df, todate_cols, errors, format)

    #dates
    df["A__contribution_year"]=pd.DatetimeIndex(df["A__contribution_date"], ambiguous="NaT").year
    df["A__contribution_quarter"]=pd.DatetimeIndex(df["A__contribution_date"], ambiguous="NaT").quarter

    #_groupby contributor-organization-year-quarter
    by=[
        "A__ein",
        "A__contributor_name",
        "A__contribution_year",
        "A__contribution_quarter",
        ]
    dict_agg_colfunctions={
        "A__org_name": [_first_value],
        "A__contributor_address_1": [_first_value],
        "A__contributor_address_city": [_first_value],
        "A__contributor_address_state": [_first_value],
        "A__contributor_address_zip_code": [_first_value],
        "A__contributor_employer": [_first_value],
        "A__contribution_amount": ["sum"],
        "A__contribution_date": [_first_value],
        }
    df=_groupby(df, by, dict_agg_colfunctions)

    #substitute employer with nan
    dict_replace={
        "n/a": NA_VALUE,
        "na": NA_VALUE,
        "d/a": NA_VALUE,
        "da": NA_VALUE,
        }
    df["A__contributor_employer"]=df["A__contributor_employer"].replace(dict_replace)

    #isfirm
    df["A__contributor_isfirm"]=np.where(df["A__contributor_employer"].isna(), 1, 0)

    #firm
    df["A__contributor_firm"]=np.where(df["A__contributor_isfirm"]==1, df["A__contributor_name"], df["A__contributor_employer"])

    #keep firms
    df=df[df['A__contributor_isfirm']==1]

    #sortvalues
    sortvalues_cols=[
        "A__contributor_firm",
        "A__contribution_year",
        "A__contribution_quarter",
        "A__ein",
        ]
    df=df.sort_values(
        by=sortvalues_cols,
        )

    #ordered_cols
    ordered_cols=[
        "A__contributor_firm",
        "A__org_name",
        "A__ein",
        "A__contribution_amount",
        "A__contribution_year",
        "A__contribution_quarter",
        "A__contribution_date",
        "A__contributor_name",
        "A__contributor_isfirm",
        "A__contributor_employer",
        "A__contributor_address_1",
        "A__contributor_address_city",
        "A__contributor_address_state",
        "A__contributor_address_zip_code",
        ]
    df=df[ordered_cols]

    #to_csv
    filepath=f"{results}/{result}.csv"
    df.to_csv(filepath, index=False)


#_irs_B_screen
folders=["zhao/_irs", "zhao/_irs"]
items=["B", "B_screen"]
def _irs_B_screen(folders, items):

    #folders
    resources=folders[0]
    results=folders[1]

    #items
    resource=items[0]
    result=items[1]

    #usecols
    usecols=[
        #"B__record_type",
        #"B__form_id_number",
        #"B__sched_b_id",
        "B__org_name",
        "B__ein",
        "B__reciepient_name",
        "B__reciepient_address_1",
        #"B__reciepient_address_2",
        "B__reciepient_address_city",
        "B__reciepient_address_st",
        "B__reciepient_address_zip_code",
        #"B__reciepient_address_zip_ext",
        "B__reciepient_employer",
        "B__expenditure_amount",
        #"B__recipient_occupation",
        "B__expenditure_date",
        "B__expenditure_purpose\r", #???
        ]
    usecols=[x.capitalize() for x in usecols]

    #read
    filepath=f"{resources}/{resource}.csv"
    df=pd.read_csv(
        filepath, 
        sep=NEW_SEP,
        usecols=usecols,
        dtype="string",
        #nrows=100000,
        lineterminator=NEW_LINETERMINATOR,
        quotechar=QUOTECHAR,
        #on_bad_lines='skip',
        )

    #lowercase col names and values
    df=_lowercase_colnames_values(df)

    #???
    df=df.rename(columns={f"B__expenditure_purpose\r": "B__expenditure_purpose"})

    #dropna
    dropna_cols=[
        "B__ein",
        "B__reciepient_name",
        "B__expenditure_amount",
        "B__expenditure_date", 
        ]
    df=df.dropna(subset=dropna_cols)

    #tonumeric
    tonumeric_cols=[
        "B__ein",
        "B__expenditure_amount",
        ]
    df=_tonumericcols_to_df(df, tonumeric_cols)

    #dict_pivot_columns
    dict_pivot_columns={
        134220019: "daga",
        464501717: "raga",
        #521304889: "dga",
        #113655877: "rga",
        #521870839: "dlcc",
        #050532524: "rslc",
        }

    #select rows
    df=df[df["B__ein"].isin([int(key) for key in dict_pivot_columns.keys()])]

    #to date
    todate_cols=[
        "B__expenditure_date",
        ]
    errors="coerce"
    format="%Y%m%d"
    df=_todatecols_to_df(df, todate_cols, errors, format)

    #dates
    df["B__expenditure_year"]=pd.DatetimeIndex(df["B__expenditure_date"], ambiguous="NaT").year
    df["B__expenditure_quarter"]=pd.DatetimeIndex(df["B__expenditure_date"], ambiguous="NaT").quarter

    #_groupby recipient-organization-year-quarter
    by=[
        "B__ein",
        "B__reciepient_name",
        "B__expenditure_year",
        "B__expenditure_quarter",
        ]
    dict_agg_colfunctions={
        "B__org_name": [_first_value],
        "B__reciepient_address_1": [_first_value],
        "B__reciepient_address_city": [_first_value],
        "B__reciepient_address_st": [_first_value],
        "B__reciepient_address_zip_code": [_first_value],
        "B__reciepient_employer": [_first_value],
        "B__expenditure_amount": ["sum"],
        "B__expenditure_date": [_firstvalue_join],
        }
    df=_groupby(df, by, dict_agg_colfunctions)

    #substitute employer with nan
    dict_replace={
        "n/a": NA_VALUE,
        "na": NA_VALUE,
        "d/a": NA_VALUE,
        "da": NA_VALUE,
        }
    df["B__reciepient_employer"]=df["B__reciepient_employer"].replace(dict_replace)

    #isfirm
    df["B__reciepient_isfirm"]=np.where(df["B__reciepient_employer"].isna(), 1, 0)

    #firm
    df["B__reciepient_firm"]=np.where(df["B__reciepient_isfirm"]==1, df["B__reciepient_name"], df["B__reciepient_employer"])

    #keep firms
    #df=df[df['B__reciepient_isfirm']==1]

    #sortvalues
    sortvalues_cols=[
        "B__reciepient_firm",
        "B__expenditure_year",
        "B__expenditure_quarter",
        "B__ein",
        ]
    df=df.sort_values(
        by=sortvalues_cols,
        )

    #ordered_cols
    ordered_cols=[
        "B__reciepient_firm",
        "B__org_name",
        "B__ein",
        "B__expenditure_amount",
        "B__expenditure_year",
        "B__expenditure_quarter",
        "B__expenditure_date",
        "B__reciepient_name",
        "B__reciepient_isfirm",
        "B__reciepient_employer",
        "B__reciepient_address_1",
        "B__reciepient_address_city",
        "B__reciepient_address_st",
        "B__reciepient_address_zip_code",
        ]
    df=df[ordered_cols]

    #to_csv
    filepath=f"{results}/{result}.csv"
    df.to_csv(filepath, index=False)


#violtrack screen
folders=["zhao/data/violation_tracker", "zhao/_violtrack"]
items=["ViolationTracker_basic_28jul23", "_violtrack_screen"]
def _violtrack_screen(folders, items):

    #folders
    resources=folders[0]
    results=folders[1]

    #items
    resource=items[0]
    result=items[1]

    #usecols
    usecols=[
        "agency",
        "agency_code",
        #"pen_year",
        #"company",
        #"city",
        #"county",
        #"street_address",
        #"state",
        #"zip",
        #"description",
        #"naics",
        #"info_source",
        #"notes",
        "unique_id",
        "current_parent_name",
        #"current_parent_ownership_structure",
        #"current_parent_stock_ticker",
        #"current_parent_CIK",
        "current_parent_ISIN",
        "current_parent_HQ_country",
        "current_parent_HQ_state",
        "current_parent_specific_industry",
        "current_parent_major_industry",
        #"load_day",
        "penalty",
        #"sub_penalty",
        #"penalty_adjusted",
        #"orig_id",
        #"civil_criminal",
        #"npa_dpa",
        "offense_group",
        #"primary_offense",
        #"secondary_offense",
        #"naics_tr",
        #"case_id",
        #"facility_name",
        "penalty_date",
        #"govt_level",
        #"case_category",
        #"court",
        #"litigation_case_title",
        #"lawsuit_resolution",
        #"pacer_link",
        #"reporting_date_parent",
        #"hist_ISIN",
        #"hist_cik",
        #"history_recap",
        #"echo_case_url",
        ]

    #read
    filepath=f"{resources}/{resource}.csv"
    df=pd.read_csv(
        filepath, 
        usecols=usecols,
        dtype="string",
        #nrows=1000,
        )

    #lowercase col names and values
    df=_lowercase_colnames_values(df)

    #tonumeric
    tonumeric_cols=[
        "penalty",
        ]
    df=_tonumericcols_to_df(df, tonumeric_cols)

    #dropna
    dropna_cols=[
        "current_parent_name",
        "penalty",
        "penalty_date",
        ]
    df=df.dropna(subset=dropna_cols)

    #todate
    todate_cols=[
        "penalty_date",
        ]
    errors="coerce"
    format="%Y%m%d"
    df=_todatecols_to_df(df, todate_cols, errors, format)

    #dates
    df["penalty_year"]=pd.DatetimeIndex(df["penalty_date"], ambiguous="NaT").year
    df["penalty_quarter"]=pd.DatetimeIndex(df["penalty_date"], ambiguous="NaT").quarter

    #_groupby company-agency-year-quarter
    by=[
        "agency_code",
        "current_parent_name",
        "penalty_year",
        "penalty_quarter",
        ]
    dict_agg_colfunctions={
        "agency": [_firstvalue_join],
        "unique_id": [_firstvalue_join],
        "current_parent_ISIN": [_firstvalue_join],
        "current_parent_HQ_country": [_firstvalue_join],
        "current_parent_HQ_state": [_firstvalue_join],
        "current_parent_specific_industry": [_firstvalue_join],
        "current_parent_major_industry": [_firstvalue_join],
        "penalty": ["sum"],
        "offense_group": [_firstvalue_join],
        "penalty_date": [_firstvalue_join],
        }
    df=_groupby(df, by, dict_agg_colfunctions)

    #sortvalues
    sortvalues_cols=[
        "current_parent_name",
        "penalty_year",
        "penalty_quarter",
        "agency_code",
        ]
    df=df.sort_values(
        by=sortvalues_cols,
        )

    #ordered_cols
    ordered_cols=[
        "current_parent_name",
        "agency",
        "agency_code",
        "offense_group",
        "penalty",
        "penalty_year",
        "penalty_quarter",
        "penalty_date",
        "current_parent_ISIN",
        "current_parent_HQ_country",
        "current_parent_HQ_state",
        "current_parent_specific_industry",
        "current_parent_major_industry",
        "unique_id",
        ]
    df=df[ordered_cols]

    #to_csv
    filepath=f"{results}/{result}.csv"
    df.to_csv(filepath, index=False)


#_rdp_ids
folders=["zhao/_merge"]
items=["A_violtrack_ids"]
left_path="zhao/_irs/A_ids"
right_path="zhao/_violtrack/violtrack_ids"
def _rdp_ids(folders, items, left_path, right_path):

    #folders items
    results=folders[0]
    result=items[0]

    #read_csv
    left_ons=right_ons=["OAPermID"]
    left=_dfpath_to_dfon(left_path, left_ons)
    right=_dfpath_to_dfon(right_path, right_ons)

    #concat
    objs=[left, right]
    axis="index"
    join="outer"
    df=pd.concat(
        objs=objs,
        axis=axis,
        join=join,
        )

    #drop_duplicates
    df=df.drop_duplicates(subset="OAPermID")

    #create timevars
    df["year"]=np.nan
    df["quarter"]=np.nan

    #tonumeric
    tonumeric_cols=[
        "OAPermID",
        "year",
        "quarter",
        ]
    df=_tonumericcols_to_df(df, tonumeric_cols)

    #list_pivot_columns
    list_pivot_columns=[
        "OAPermID",
        "year",
        "quarter",
        ]

    #df_pivot
    df_pivot=df[list_pivot_columns]
    
    #_df_to_fullpanel
    companyid="OAPermID"
    timevars={
        "year": (2000, 2023+1),
        "quarter": (1, 4+1),
        }
    fillna_cols=[]
    df_fullpanel=_df_to_fullpanel(df_pivot, companyid, timevars, fillna_cols)

    #df_withoutdups
    df_withoutdups=df.drop_duplicates(subset="OAPermID")
    df_withoutdups=df_withoutdups.drop(
        [
            "year",
            "quarter",
            ],
        axis=1,
        )

    #merge df_fullpanel and df_withoutdups
    left=df_fullpanel
    right=df_withoutdups
    how="left"
    on="OAPermID"
    suffixes=('_left', '_right')
    indicator=f"_merge_dups"
    validate="m:1"
    df=pd.merge(
        left=left,
        right=right,
        how=how,
        on=on,
        suffixes=suffixes,
        indicator=indicator,
        validate=validate,
        )
    
    #to_csv
    filepath=f"{results}/{result}.csv"
    df.to_csv(filepath, index=False)

    #sortvalues
    sortvalues_cols=[
        "OAPermID",
        "year",
        "quarter",
        ]
    df=df.sort_values(
        by=sortvalues_cols,
        )

    #ordered_cols
    ordered_cols=[
        "OAPermID",
        "CommonName",
        "year",
        "quarter",
        "Gics",
        "PrimaryRIC",
        "OwnershipExists",
        "OrganisationStatus",
        "MktCapCompanyUsd",
        "RCSFilingCountryLeaf",
        "UltimateParentCompanyOAPermID",
        "DTSubjectName",
        "UltimateParentOrganisationName",
        "DTSimpleType",
        "RCSOrganisationSubTypeLeaf",
        "PEBackedStatus",
        "RCSCountryHeadquartersLeaf",
        ]
    df=df[ordered_cols]
    
    #to_csv
    filepath=f"{results}/{result}.csv"
    df.to_csv(filepath, index=False)
    #'''


#_irs_A_aggregate
folders=["zhao/_irs", "zhao/_irs"]
items=["A_ids", "A_aggregate"]
def _irs_A_aggregate(folders, items):

    #folders
    resources=folders[0]
    results=folders[1]

    #items
    resource=items[0]
    result=items[1]

    #usecols
    usecols=[
        #irs
        "A__contributor_firm",
        "A__org_name",
        "A__ein",
        "A__contribution_amount",
        "A__contribution_year",
        "A__contribution_quarter",
        "A__contribution_date",
        "A__contributor_name",
        "A__contributor_isfirm",
        "A__contributor_employer",
        "A__contributor_address_1",
        "A__contributor_address_city",
        "A__contributor_address_state",
        "A__contributor_address_zip_code",
        #refinitiv
        "CommonName",
        "OAPermID",
        ]

    #read
    filepath=f"{resources}/{resource}.csv"
    df=pd.read_csv(
        filepath, 
        usecols=usecols,
        dtype="string",
        #nrows=1000,
        )

    #lowercase col names and values
    df=_lowercase_colnames_values(df)

    #dropna
    dropna_cols=[
        "A__ein",
        "OAPermID",
        "A__contribution_amount",
        "A__contribution_year",
        "A__contribution_quarter"
        ]
    df=df.dropna(subset=dropna_cols)

    #tonumeric
    tonumeric_cols=[
        "A__ein",
        "OAPermID",
        "A__contribution_amount",
        ]
    df=_tonumericcols_to_df(df, tonumeric_cols)

    #_groupby contributor-organization-year-quarter
    by=[
        "A__ein",
        "OAPermID",
        "A__contribution_year",
        "A__contribution_quarter",
        ]
    dict_agg_colfunctions={
        "A__contributor_firm": [_firstvalue_join],
        "A__org_name": [_firstvalue_join],
        "A__contribution_amount": ["sum"],
        "A__contribution_date": [_firstvalue_join],
        "A__contributor_name": [_firstvalue_join],
        "A__contributor_isfirm": [_firstvalue_join],
        "A__contributor_employer": [_firstvalue_join],
        "A__contributor_address_1": [_firstvalue_join],
        "A__contributor_address_city": [_firstvalue_join],
        "A__contributor_address_state": [_firstvalue_join],
        "A__contributor_address_zip_code": [_firstvalue_join],
        #refinitiv
        "CommonName": [_firstvalue_join],
        }
    df=_groupby(df, by, dict_agg_colfunctions)

    #dict_pivot_columns
    dict_pivot_columns={
        134220019: "daga",
        464501717: "raga",
        #521304889: "dga",
        #113655877: "rga",
        #521870839: "dlcc",
        #050532524: "rslc",
        }
    
    #df_pivot
    index=[
        "OAPermID",
        "A__contribution_year",
        "A__contribution_quarter",
        ]
    values="A__contribution_amount"
    columns="A__ein"
    aggfunc="sum"
    fill_value=0
    df_pivot=pd.pivot_table(
        data=df,
        values=values,
        index=index,
        columns=columns,  
        aggfunc=aggfunc,
        fill_value=fill_value,
        )      
    #reset index
    df_pivot=df_pivot.reset_index()   
    #rename
    df_pivot=df_pivot.rename(columns=dict_pivot_columns)
    
    #list_pivot_columns
    list_pivot_columns=list(dict_pivot_columns.values())

    #_df_to_fullpanel
    companyid="OAPermID"
    timevars={
        "A__contribution_year": (2000, 2023+1),
        "A__contribution_quarter": (1, 4+1),
        }
    fillna_cols=list_pivot_columns
    df_fullpanel=_df_to_fullpanel(df_pivot, companyid, timevars, fillna_cols)

    #df_withoutdups
    df_withoutdups=df.drop_duplicates(subset="OAPermID")
    df_withoutdups=df_withoutdups.drop(
        [
            "A__ein",
            "A__org_name",
            "A__contribution_amount",
            "A__contribution_year",
            "A__contribution_quarter",
            "A__contribution_date"
            ],
        axis=1,
        )

    #merge df_fullpanel and df_withoutdups
    left=df_fullpanel
    right=df_withoutdups
    how="left"
    on="OAPermID"
    suffixes=('_left', '_right')
    indicator=f"_merge_dups"
    validate="m:1"
    df=pd.merge(
        left=left,
        right=right,
        how=how,
        on=on,
        suffixes=suffixes,
        indicator=indicator,
        validate=validate,
        )

    #sortvalues
    sortvalues_cols=[
        "OAPermID",
        "A__contribution_year",
        "A__contribution_quarter",
        ]
    df=df.sort_values(
        by=sortvalues_cols,
        )

    #ordered_cols
    ordered_cols= [
        "OAPermID",
        "CommonName",
        "A__contributor_firm",
        "A__contribution_year",
        "A__contribution_quarter",
        ] + list_pivot_columns + [
        "A__contributor_name",
        "A__contributor_isfirm",
        "A__contributor_employer",
        "A__contributor_address_1",
        "A__contributor_address_city",
        "A__contributor_address_state",
        "A__contributor_address_zip_code",
        ]
    df=df[ordered_cols]

    #to_csv
    filepath=f"{results}/{result}.csv"
    df.to_csv(filepath, index=False)
    #'''


#_violtrack_aggregate
folders=["zhao/_violtrack", "zhao/_violtrack"]
items=["violtrack_ids", "violtrack_aggregate"]
def _violtrack_aggregate(folders, items):

    #folders
    resources=folders[0]
    results=folders[1]

    #items
    resource=items[0]
    result=items[1]

    #usecols
    usecols=[
        #violtrack
        "current_parent_name",
        "agency",
        "agency_code",
        "offense_group",
        "penalty",
        "penalty_year",
        "penalty_quarter",
        "penalty_date",
        "current_parent_ISIN",
        "current_parent_HQ_country",
        "current_parent_HQ_state",
        "current_parent_specific_industry",
        "current_parent_major_industry",
        "unique_id",
        #refinitiv
        "CommonName",
        "OAPermID",
        ]

    #read
    filepath=f"{resources}/{resource}.csv"
    df=pd.read_csv(
        filepath, 
        usecols=usecols,
        dtype="string",
        #nrows=1000,
        )

    #lowercase col names and values
    df=_lowercase_colnames_values(df)

    #dropna
    dropna_cols=[
        "OAPermID",
        "penalty",
        "penalty_year",
        "penalty_quarter"
        ]
    df=df.dropna(subset=dropna_cols)

    #tonumeric
    tonumeric_cols=[
        "OAPermID",
        "penalty",
        "penalty_year",
        "penalty_quarter"
        ]
    df=_tonumericcols_to_df(df, tonumeric_cols)

    #_groupby company-agency-year-quarter
    by=[
        "agency_code",
        "OAPermID",
        "penalty_year",
        "penalty_quarter",
        ]
    dict_agg_colfunctions={
        "agency": [_firstvalue_join],
        "unique_id": [_firstvalue_join],
        "current_parent_name": [_firstvalue_join],
        "current_parent_ISIN": [_firstvalue_join],
        "current_parent_HQ_country": [_firstvalue_join],
        "current_parent_HQ_state": [_firstvalue_join],
        "current_parent_specific_industry": [_firstvalue_join],
        "current_parent_major_industry": [_firstvalue_join],
        "penalty": ["sum"],
        "offense_group": [_firstvalue_join],
        "penalty_date": [_firstvalue_join],
        #refinitiv
        "CommonName": [_firstvalue_join],
        }
    df=_groupby(df, by, dict_agg_colfunctions)
    
    #pivot
    index=[
        "OAPermID",
        "penalty_year",
        "penalty_quarter",
        ]
    values="penalty"
    columns="agency_code"
    aggfunc="sum"
    fill_value=0
    df_pivot=pd.pivot_table(
        data=df,
        values=values,
        index=index,
        columns=columns,  
        aggfunc=aggfunc,
        fill_value=fill_value,
        )      
    #reset index
    df_pivot=df_pivot.reset_index()   
    
    #list_pivot_columns
    list_pivot_columns=df["agency_code"].unique()
    list_pivot_columns=list(np.sort(list_pivot_columns))

    #_df_to_fullpanel
    companyid="OAPermID"
    timevars={
        "penalty_year": (2000, 2023+1),
        "penalty_quarter": (1, 4+1),
        }
    fillna_cols=list_pivot_columns
    df_fullpanel=_df_to_fullpanel(df_pivot, companyid, timevars, fillna_cols)

    #df_withoutdups
    df_withoutdups=df.drop_duplicates(subset="OAPermID")
    df_withoutdups=df_withoutdups.drop(
        [
            "penalty",
            "penalty_year",
            "penalty_quarter",
            "penalty_date",
            "agency",
            "unique_id",
            "offense_group",
            ],
        axis=1,
        )

    #merge df_fullpanel and df_withoutdups
    left=df_fullpanel
    right=df_withoutdups
    how="left"
    on="OAPermID"
    suffixes=('_left', '_right')
    indicator=f"_merge_dups"
    validate="m:1"
    df=pd.merge(
        left=left,
        right=right,
        how=how,
        on=on,
        suffixes=suffixes,
        indicator=indicator,
        validate=validate,
        )    

    #sortvalues
    sortvalues_cols=[
        "OAPermID",
        "penalty_year",
        "penalty_quarter",
        ]
    df=df.sort_values(
        by=sortvalues_cols,
        )

    #ordered_cols
    ordered_cols= [
        "OAPermID",
        "CommonName",
        "current_parent_name",
        "penalty_year",
        "penalty_quarter",
        ] + list_pivot_columns + [
        "current_parent_ISIN",
        "current_parent_HQ_country",
        "current_parent_HQ_state",
        "current_parent_specific_industry",
        "current_parent_major_industry",
        ]
    df=df[ordered_cols]

    #to_csv
    filepath=f"{results}/{result}.csv"
    df.to_csv(filepath, index=False)

    #df_list_pivot_columns
    df_list_pivot_columns=pd.DataFrame()
    df_list_pivot_columns["violtrack_list_pivot_columns"]=list_pivot_columns
    filepath=f"zhao/_violtrack/violtrack_list_pivot_columns.csv"
    df_list_pivot_columns.to_csv(filepath, index=False)
    #'''


#_rdp_aggregate
folders=["zhao/_merge", "zhao/_merge"]
items=["rdp_ids_A_aggregate_violtrack_aggregate", "rdp_aggregate"]
def _rdp_aggregate(folders, items):

    #folders
    resources=folders[0]
    results=folders[1]

    #items
    resource=items[0]
    result=items[1]

    #A_list_pivot_columns
    A_dict_pivot_columns={
        134220019: "daga",
        464501717: "raga",
        #521304889: "dga",
        #113655877: "rga",
        #521870839: "dlcc",
        #050532524: "rslc",
        }
    A_list_pivot_columns=list(A_dict_pivot_columns.values())

    #violtrack_list_pivot_columns
    filepath=f"zhao/_violtrack/violtrack_list_pivot_columns.csv"
    df=pd.read_csv(filepath, dtype="string")
    violtrack_list_pivot_columns=df["violtrack_list_pivot_columns"].tolist()

    #usecols
    usecols=[
        #A
        "OAPermID",
        "CommonName",
        "year",
        "quarter",
        "A__contributor_firm",
        "A__contribution_year",
        "A__contribution_quarter",
        ] + A_list_pivot_columns + [
        "A__contributor_name",
        "A__contributor_isfirm",
        "A__contributor_employer",
        "A__contributor_address_1",
        "A__contributor_address_city",
        "A__contributor_address_state",
        "A__contributor_address_zip_code",
        #violtrack
        "OAPermID",
        "CommonName",
        "current_parent_name",
        "penalty_year",
        "penalty_quarter",
        ] + violtrack_list_pivot_columns + [
        "current_parent_ISIN",
        "current_parent_HQ_country",
        "current_parent_HQ_state",
        "current_parent_specific_industry",
        "current_parent_major_industry",
        #refinitiv
        "Gics",
        "PrimaryRIC",
        "OwnershipExists",
        "OrganisationStatus",
        "MktCapCompanyUsd",
        "RCSFilingCountryLeaf",
        "UltimateParentCompanyOAPermID",
        "DTSubjectName",
        "UltimateParentOrganisationName",
        "DTSimpleType",
        "RCSOrganisationSubTypeLeaf",
        "PEBackedStatus",
        "RCSCountryHeadquartersLeaf",
        ]

    #read
    filepath=f"{resources}/{resource}.csv"
    df=pd.read_csv(
        filepath, 
        usecols=usecols,
        dtype="string",
        nrows=1000,
        )

    #lowercase col names and values
    df=_lowercase_colnames_values(df)

    #dropna
    dropna_cols=[
        "OAPermID",
        "year",
        "quarter"
        ]
    df=df.dropna(subset=dropna_cols)

    #tonumeric
    tonumeric_cols=[
        "OAPermID",
        "year",
        "quarter",
        ] + A_list_pivot_columns + violtrack_list_pivot_columns
    df=_tonumericcols_to_df(df, tonumeric_cols)

    #list_pivot_columns
    list_pivot_columns = A_list_pivot_columns + violtrack_list_pivot_columns

    fillna_cols=list_pivot_columns
    df=_fillnacols_to_df(df, fillna_cols, value=0)

    #agencies_sum
    df["agencies_sum"]=df[violtrack_list_pivot_columns].sum(axis=1)

    #AG_sum
    df["AG_sum"]=df[violtrack_list_pivot_columns].filter(like='-ag').sum(axis=1)

    #non_AG_sum
    df["non_AG_sum"] = df["agencies_sum"] - df["AG_sum"]

    #disclosure shocks, regdata

    list_newvars=[
        "agencies_sum",
        "AG_sum",
        "non_AG_sum",
        ]
    
    #sortvalues
    sortvalues_cols=[
        "OAPermID",
        "year",
        "quarter",
        ]
    df=df.sort_values(
        by=sortvalues_cols,
        )

    #ordered_cols
    ordered_cols=[
        "OAPermID",
        "CommonName",
        "A__contributor_firm",
        "current_parent_name",
        "year",
        "quarter",
        ] + A_list_pivot_columns + list_newvars + violtrack_list_pivot_columns + [
        #A
        "A__contributor_name",
        "A__contributor_isfirm",
        "A__contributor_employer",
        "A__contributor_address_1",
        "A__contributor_address_city",
        "A__contributor_address_state",
        "A__contributor_address_zip_code",
        #violtrack
        "current_parent_ISIN",
        "current_parent_HQ_country",
        "current_parent_HQ_state",
        "current_parent_specific_industry",
        "current_parent_major_industry",
        #refinitiv
        "Gics",
        "PrimaryRIC",
        "OwnershipExists",
        "OrganisationStatus",
        "MktCapCompanyUsd",
        "RCSFilingCountryLeaf",
        "UltimateParentCompanyOAPermID",
        "DTSubjectName",
        "UltimateParentOrganisationName",
        "DTSimpleType",
        "RCSOrganisationSubTypeLeaf",
        "PEBackedStatus",
        "RCSCountryHeadquartersLeaf",
        ]
    df=df[ordered_cols]

    #to_csv
    filepath=f"{results}/{result}.csv"
    df.to_csv(filepath, index=False)
    #'''



#_irs_txt_to_dfs
folders=["zhao/data/irs", "zhao/_irs"]
items=["FullDataFile"]
#_irs_txt_to_dfs(folders, items)


#_irs_A_screen
folders=["zhao/_irs", "zhao/_irs"]
items=["A", "A_screen"]
#_irs_A_screen(folders, items)


#_irs_B_screen
folders=["zhao/_irs", "zhao/_irs"]
items=["B", "B_screen"]
#_irs_B_screen(folders, items)


#_violtrack_screen
folders=["zhao/data/violation_tracker", "zhao/_violtrack"]
items=["ViolationTracker_basic_28sep23", "violtrack_screen"]
#_violtrack_screen(folders, items)


#_search _irs_A_screen 
folders=["zhao/_irs", "zhao/_irs"]
items=["A_screen", "A_screen_search"]
colname="A__contributor_firm"
#_search(folders, items, colname)


#_search _violtrack_screen 
folders=["zhao/_violtrack", "zhao/_violtrack"]
items=["violtrack_screen", "violtrack_screen_search"]
colname="current_parent_name"
#_search(folders, items, colname)


#merge A_screen with A_screen_search_A__contributor_firm
folders=["zhao/_irs"]
items=["A_ids"]
left_path="zhao/_irs/A_screen"
left_ons=["A__contributor_firm"]
right_path="zhao/_irs/A_screen_search_A__contributor_firm"
right_ons=["query"]
how="left"
validate="m:1"
#_pd_merge(folders, items, left_path, left_ons, right_path, right_ons, how, validate)


#merge violtrack_screen with violtrack_screen_search_current_parent_name
folders=["zhao/_violtrack"]
items=["violtrack_ids"]
left_path="zhao/_violtrack/violtrack_screen"
left_ons=["current_parent_name"]
right_path="zhao/_violtrack/violtrack_screen_search_current_parent_name"
right_ons=["query"]
how="left"
validate="m:1"
#_pd_merge(folders, items, left_path, left_ons, right_path, right_ons, how, validate)


#_rdp_ids
folders=["zhao/_merge"]
items=["rdp_ids"]
left_path="zhao/_irs/A_ids"
right_path="zhao/_violtrack/violtrack_ids"
#_rdp_ids(folders, items, left_path, right_path)


#_irs_A_aggregate
folders=["zhao/_irs", "zhao/_irs"]
items=["A_ids", "A_aggregate"]
#_irs_A_aggregate(folders, items)


#_violtrack_aggregate
folders=["zhao/_violtrack", "zhao/_violtrack"]
items=["violtrack_ids", "violtrack_aggregate"]
#_violtrack_aggregate(folders, items)


#merge rdp_ids and A_aggregate
folders=["zhao/_merge"]
items=["rdp_ids_A_aggregate"]
left_path="zhao/_merge/rdp_ids"
left_ons=["OAPermID", "CommonName", "year", "quarter"]
right_path="zhao/_irs/A_aggregate"
right_ons=["OAPermID", "CommonName", "A__contribution_year", "A__contribution_quarter"]
how="outer"
validate="1:1"
#_pd_merge(folders, items, left_path, left_ons, right_path, right_ons, how, validate)


#merge rdp_ids_A_aggregate and violtrack_aggregate
folders=["zhao/_merge"]
items=["rdp_ids_A_aggregate_violtrack_aggregate"]
left_path="zhao/_merge/rdp_ids_A_aggregate"
left_ons=["OAPermID", "CommonName", "year", "quarter"]
right_path="zhao/_violtrack/violtrack_aggregate"
right_ons=["OAPermID", "CommonName", "penalty_year", "penalty_quarter"]
how="outer"
validate="1:1"
#_pd_merge(folders, items, left_path, left_ons, right_path, right_ons, how, validate)


#_rdp_aggregate
folders=["zhao/_merge", "zhao/_merge"]
items=["rdp_ids_A_aggregate_violtrack_aggregate", "rdp_aggregate"]
_rdp_aggregate(folders, items)


#compustat?




#print
print("done")









'''
#to do
des stats
regression
presentation


motivation e.g., headlines huge settlements, companies care
table opaque states bigger
less slides, less words
wood: separating, cirmuvent 
data and sample slide
activism, not donation
What is California's, or other state-level LOBBYING disclosure?
before 2018, check form 990 schedule B
state AGs departures as exogenous shock to connection
employ state level PACs, for a subsample
cross-section state-level corruption, DOJ, cases
can firms move hq on putpose? forum shopping
political exposure when industry is not regulated, e.g., bitcoin
timing, quarter -4 wrt election date
donation_t-1
reg neg ext on PACs
will violation info be on 8-k?
career concerns?


year=1990
https://www.followthemoney.org/show-me?dt=1&y={y},{y+1},{y+2},{y+3}&c-exi=1&c-r-oc=Z10#[{1|gro=f-s,f-eid,c-t-id,d-id,d-par,d-empl,d-occupation

#negative/positive consequences
fatalities: https://www.osha.gov/fatalities, https://www.osha.gov/fatalities/reports/archive
injury: https://www.osha.gov/Establishment-Specific-Injury-and-Illness-Data, https://www.osha.gov/severeinjury
odi: https://www.osha.gov/ords/odi/establishment_search.html
chemical: https://www.osha.gov/opengov/health-samples, https://www.osha.gov/chemicaldata/

'''
