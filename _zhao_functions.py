

#imports
import re
import io
import numpy as np
import pandas as pd
from pathlib import Path
from datetime import datetime


#functions
from _pd_utils import _groupby, _folder_to_filestems


#vars
from _string_utils import ENCODING
SEP="|"  
LINETERMINATOR="\n"
NEW_SEP=","
QUOTECHAR='"'
NA_VALUE=None
CURRENT_YEAR=datetime.now().year
CURRENT_YEAR_2DIGIT=CURRENT_YEAR%100
tot=13428119
buffer_size=8192


#vars donations
#org
organization_name="a__org_name"
organization_id="a__ein"
pivot_columns={
    "134220019": "democratic_ag",
    "464501717": "republican_ag",
    }
#contributors
contributor_id="cusip"
contributor_name_irs="a__contributor_name"
company_name_rdp="dtsubjectname"
company_businessentity="businessentity"
company_ric="ric"
company_isin="issueisin"
company_cusip="cusip"
company_oapermid="oapermid"
contributor_address_state="a__contributor_address_state"
contributor_address_zipcode="a__contributor_address_zip_code"
contributor_employer="a__contributor_employer"
contributor_employer_new="a__contributor_employer_new"
donor_isfirm="donor_isfirm"
company_involved="a__company_involved"
#contribution
contribution_date="a__contribution_date"
contribution_year="a__contribution_year"
contribution_amount_ytd="a__agg_contribution_ytd"
contributor_list_firstvalue=[
    contributor_name_irs,
    company_name_rdp,
    company_businessentity,
    company_isin,
    #company_cusip,
    company_ric,
    company_oapermid,
    #contributor_address_state,
    #contributor_address_zipcode,
    contributor_employer,
    contributor_employer_new,
    donor_isfirm,
    company_involved,
    ]


#vars violations
viol_parent_id="current_parent_ISIN"
viol_penalty_year="pen_year"
viol_penalty_amount="penalty"
viol_subpenalty_amount="sub_penalty"
viol_penalty_adjusted_amount="penalty_adjusted"
viol_initiation_year_true="initiation_year_true"
viol_initiation_lag="initiation_lag"
initiation_year_inferred_mean="initiation_year_inferred_mean"
initiation_year_inferred_median="initiation_year_inferred_median"


#vars echo
echo_parent_id="cusip"
echo_initiation_year="echo_initiation_year"
echo_penalty_year="settlement_fy"
echo_initiation_lag="echo_initiation_lag"
echo_penalty_amount="fed_penalty_assessed_amt"
echo_activity_type_code="activity_type_code"


#vars osha
osha_parent_id="cusip"
osha_initiation_year="osha_initiation_year"
osha_penalty_year="osha_penalty_year"
osha_penalty_amount="initial_penalty"
osha_initiation_lag="osha_initiation_lag"
osha_list_firstvalue=[
    osha_penalty_year,
    osha_initiation_lag,
    "fta_issuance_year",
    company_name_rdp,
    company_businessentity,
    company_isin,
    #company_cusip,
    company_ric,
    company_oapermid,
    ]


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


#irs create empty txts
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
            text=NEW_SEP.join(quoted_text) + LINETERMINATOR

            #remove carriage 
            text=text.replace("\r" ,"")

            #write
            file_object.write(text)


#irs from txt txt to dfs
folders=["zhao/data/irs", "zhao/_irs"]
items=["FullDataFile"]
def _irs_txt_to_dfs(folders, items):

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


#first value (if same) or join (if different)
def _firstvalue_join(series):

    unique_values = series.unique()
    
    if len(unique_values) == 1:
        return series.iloc[0]
    else:
        return "||".join(map(str, series))


#keep most recent obs
def _groupby_mostrecent(df, by_groupby, datevar):

    #by sortvalues
    by_sortvalues = by_groupby + [datevar]

    #dropna
    df=df.dropna(subset=by_sortvalues)

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


#irs contributors screen
folders=["zhao/_irs", "zhao/_irs"]
items=["A", "A_screen"]
def _irs_contributors_screen(folders, items):

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
        #nrows=1000,
        lineterminator=LINETERMINATOR,
        quotechar=QUOTECHAR,
        #on_bad_lines='skip',
        )
    
    #lowercase
    df.columns=df.columns.str.lower()

    #remove carriage
    df.columns=[col.replace("\r", '') for col in df.columns]

    #lowercase col values
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
    errors="coerce"
    format="%Y%m%d"
    df[contribution_date]=pd.to_datetime(
        df[contribution_date], 
        errors=errors,
        format=format,
        )

    #initiation year
    df[contribution_year]=pd.DatetimeIndex(df[contribution_date], ambiguous="NaT").year

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
        "n/a": NA_VALUE,
        "na": NA_VALUE,
        "not available": NA_VALUE,
        "not applicable": NA_VALUE,
        "not employed": NA_VALUE,
        "not a contribution": NA_VALUE,
        "in-kind": NA_VALUE,
        "self": NA_VALUE,
        "self-employed": NA_VALUE,
        "self-employed-employed": NA_VALUE,
        "self employed": NA_VALUE,
        "retired": NA_VALUE,
        "attorney": NA_VALUE,
        "vice president": NA_VALUE,
        "teacher": NA_VALUE,
        "founder": NA_VALUE,
        }
    df[contributor_employer_new]=df[contributor_employer].replace(dict_replace)

    #init series
    x0=df[contributor_name_irs]
    x1=df[contributor_employer_new]

    condlist=[
        x1.isna(),
        x1.notna(),
        ]   
    choicelist0=[
        x0, 
        x1,
        ]
    choicelist1=[
        1, 
        0,
        ]
    y0=np.select(condlist, choicelist0, default="error")
    y1=np.select(condlist, choicelist1, default="error")

    #outcome series
    df[company_involved]=y0
    df[donor_isfirm]=y1

    #save
    filepath=f"{results}/{result}.csv"
    df.to_csv(filepath, index=False)


#echo facilities screen
folders=["zhao/data/epa", "zhao/_epa"]
items=["CASE_FACILITIES", "CASE_FACILITIES_screen"]
def _echo_facilities_screen(folders, items):

    #https://echo.epa.gov/files/echodownloads/case_downloads.zip
    #https://echo.epa.gov/tools/data-downloads/icis-fec-download-summary

    #folders
    resources=folders[0]
    results=folders[1]

    #items
    resource=items[0]
    result=items[1]

    #usecols
    usecols=[
        "CASE_NUMBER", 
        "ACTIVITY_ID", 
        "REGISTRY_ID",
        "FACILITY_NAME",
        #"LOCATION_ADDRESS",
        #"CITY",
        #"STATE_CODE",
        #"ZIP",
        #"PRIMARY_SIC_CODE",
        #"PRIMARY_NAICS_CODE",
        ]

    #read
    filepath=f"{resources}/{resource}.csv"
    df=pd.read_csv(
        filepath,
        usecols=usecols,
        dtype="string",
        #nrows=1000,
        )

    #lowercase col values
    for i, col in enumerate(df.columns):
        df[col]=df[col].str.lower()

    #dropna
    dropna_cols=[
        "CASE_NUMBER", 
        "REGISTRY_ID", 
        "ACTIVITY_ID", 
        ]
    df=df.dropna(subset=dropna_cols)

    #to numeric
    tonumeric_cols=[
        "REGISTRY_ID",
        "ACTIVITY_ID",
        ]
    for i, col in enumerate(tonumeric_cols):
        df[col]=pd.to_numeric(df[col])

    #sortvalues
    sortvalues_cols=[
        "CASE_NUMBER", 
        "ACTIVITY_ID", 
        "REGISTRY_ID",
        "FACILITY_NAME",
        ]
    df=df.sort_values(by=sortvalues_cols)

    #drop duplicates
    dropdups_cols=[
        "CASE_NUMBER", 
        "ACTIVITY_ID", 
        "REGISTRY_ID",
        ]
    df=df.drop_duplicates(subset=dropdups_cols)

    #ordered
    ordered_cols=usecols
    df=df[ordered_cols]

    #save
    filepath=f"{results}/{result}.csv"
    df.to_csv(filepath, index=False)


#initiation lag
def _initiation_lag(row, col0, col1):

    #rows
    row0=row[col0]
    row1=row[col1]

    #initiation lag
    initiation_lag=row0-row1

    #if
    if initiation_lag<0:

        #initiation lag
        initiation_lag=None

    #return
    return initiation_lag


#echo enforcements screen
folders=["zhao/data/epa", "zhao/_epa"]
items=["CASE_ENFORCEMENT_CONCLUSIONS", "CASE_ENFORCEMENT_CONCLUSIONS_screen"]
def _echo_enforcements_screen(folders, items):

    #https://echo.epa.gov/files/echodownloads/case_downloads.zip
    #https://echo.epa.gov/tools/data-downloads/icis-fec-download-summary

    #folders
    resources=folders[0]
    results=folders[1]

    #items
    resource=items[0]
    result=items[1]

    #usecols
    usecols=[
        #id
        "CASE_NUMBER", 
        "ACTIVITY_ID",

        #conclusion
        #"ENF_CONCLUSION_ID",
        #"ENF_CONCLUSION_NMBR",
        #"ENF_CONCLUSION_ACTION_CODE",
        #"ENF_CONCLUSION_ACTION_CODE",
        #"ENF_CONCLUSION_NAME",

        #date
        #"SETTLEMENT_LODGED_DATE",
        #"SETTLEMENT_ENTERED_DATE",
        "SETTLEMENT_FY",

        #other
        #"PRIMARY_LAW",
        #"REGION_CODE",
        "ACTIVITY_TYPE_CODE",

        #penalty
        "FED_PENALTY_ASSESSED_AMT",
        #"STATE_LOCAL_PENALTY_AMT",
        #"SEP_AMT",
        #"COMPLIANCE_ACTION_COST",
        #"COST_RECOVERY_AWARDED_AMT",
        ]

    #read
    filepath=f"{resources}/{resource}.csv"
    df=pd.read_csv(
        filepath,
        usecols=usecols,
        dtype="string",
        #nrows=1000,
        )

    #lowercase col values
    for i, col in enumerate(df.columns):
        df[col]=df[col].str.lower()

    #fillna
    fillna_cols=[
        "FED_PENALTY_ASSESSED_AMT",
        ]
    for i, col in enumerate(fillna_cols):
        df[col]=pd.to_numeric(df[col])
        df[col]=df[col].fillna(0)

    #dropna
    dropna_cols=[ 
        "CASE_NUMBER", 
        "ACTIVITY_ID",
        "FED_PENALTY_ASSESSED_AMT",
        "SETTLEMENT_FY",
        ]
    df=df.dropna(subset=dropna_cols)

    #to numeric
    tonumeric_cols=[
        "ACTIVITY_ID",
        "SETTLEMENT_FY",
        ]
    for i, col in enumerate(tonumeric_cols):
        df[col]=pd.to_numeric(df[col])

    #sortvalues
    sortvalues_cols=[
        "CASE_NUMBER", 
        "ACTIVITY_ID",
        "SETTLEMENT_FY",
        ]
    ascending=[
        True,
        True,
        False,
        ]
    df=df.sort_values(
        by=sortvalues_cols,
        ascending=ascending,
        )
    
    #groupby
    by=[
        "CASE_NUMBER",
        "ACTIVITY_ID",
        ]
    dict_agg_colfunctions={
        "FED_PENALTY_ASSESSED_AMT": [np.sum],
        "SETTLEMENT_FY": [_first_value],
        "ACTIVITY_TYPE_CODE": [_first_value],
        }
    df=_groupby(df, by, dict_agg_colfunctions)

    #ordered
    ordered_cols=[
        "CASE_NUMBER", 
        "ACTIVITY_ID",
        "SETTLEMENT_FY",
        "ACTIVITY_TYPE_CODE",
        "FED_PENALTY_ASSESSED_AMT",
        ]
    df=df[ordered_cols]

    #save
    filepath=f"{results}/{result}.csv"
    df.to_csv(filepath, index=False)


#echo enforcements screen
folders=["zhao/data/epa", "zhao/_epa"]
items=["CASE_MILESTONES", "CASE_MILESTONES_screen"]
def _echo_milestones_screen(folders, items):

    #https://echo.epa.gov/files/echodownloads/case_downloads.zip
    #https://echo.epa.gov/tools/data-downloads/icis-fec-download-summary

    #folders
    resources=folders[0]
    results=folders[1]

    #items
    resource=items[0]
    result=items[1]

    #usecols
    usecols=[
        #id
        "CASE_NUMBER", 
        "ACTIVITY_ID",

        #date
        "ACTUAL_DATE",

        #other
        #"SUB_ACTIVITY_TYPE_CODE",
        #"SUB_ACTIVITY_TYPE_DESC",
        ]

    #read
    filepath=f"{resources}/{resource}.csv"
    df=pd.read_csv(
        filepath,
        usecols=usecols,
        dtype="string",
        #nrows=1000,
        )

    #lowercase col values
    for i, col in enumerate(df.columns):
        df[col]=df[col].str.lower()

    #dropna
    dropna_cols=[ 
        "CASE_NUMBER", 
        "ACTIVITY_ID",
        "ACTUAL_DATE",
        ]
    df=df.dropna(subset=dropna_cols)

    #to numeric
    tonumeric_cols=[
        "ACTIVITY_ID",
        ]
    for i, col in enumerate(tonumeric_cols):
        df[col]=pd.to_numeric(df[col])

    #to date
    errors="coerce"
    format="%m/%d/%Y"
    todate_cols=[
        "ACTUAL_DATE",
        ]
    for i, col in enumerate(todate_cols):
        df[col]=pd.to_datetime(
            df[col],
            errors=errors,
            format=format,
            )

    #sortvalues
    sortvalues_cols=[
        "CASE_NUMBER", 
        "ACTIVITY_ID",
        "ACTUAL_DATE",
        ]
    ascending=[
        True,
        True,
        True,
        ]
    df=df.sort_values(
        by=sortvalues_cols,
        ascending=ascending,
        )
    
    #groupby
    by=[
        "CASE_NUMBER",
        "ACTIVITY_ID",
        ]
    dict_agg_colfunctions={
        "ACTUAL_DATE": [_first_value],
        }
    df=_groupby(df, by, dict_agg_colfunctions)

    #initiation year
    df[echo_initiation_year]=pd.DatetimeIndex(df["ACTUAL_DATE"], ambiguous="NaT").year

    #ordered
    ordered_cols=[
        "CASE_NUMBER", 
        "ACTIVITY_ID",
        "ACTUAL_DATE",
        echo_initiation_year,
        ]
    df=df[ordered_cols]

    #save
    filepath=f"{results}/{result}.csv"
    df.to_csv(filepath, index=False)


#echo tri screen
folders=["zhao/data/epa", "zhao/_epa"]
items=["tri", "tri_screen"]
def _echo_tri_screen(folders, items):
 
    #https://enviro.epa.gov/facts/tri/form_ra_download.html
    #select "Reporting Year", "Greater than", "1960"
    #select "usecols" below

    #folders
    resources=folders[0]
    results=folders[1]

    #items
    resource=items[0]
    result=items[1]

    #usecols
    usecols=[
        "EPA_REGISTRY_ID",
        "REPORTING_YEAR",
        "FACILITY_NAME",
        "PARENT_CO_NAME",
        ]

    #read
    filepath=f"{resources}/{resource}.csv"
    df=pd.read_csv(
        filepath,
        usecols=usecols,
        dtype="string",
        #nrows=1000,
        )

    #lowercase col values
    for i, col in enumerate(df.columns):
        df[col]=df[col].str.lower()

    #dropna
    dropna_cols=[
        "EPA_REGISTRY_ID",
        "REPORTING_YEAR",
        "PARENT_CO_NAME",
        ]
    df=df.dropna(subset=dropna_cols)

    #to numeric
    tonumeric_cols=[
        "EPA_REGISTRY_ID",
        "REPORTING_YEAR",
        ]
    for i, col in enumerate(tonumeric_cols):
        df[col]=pd.to_numeric(df[col])

    #sortvalues
    sortvalues_cols=[
        "EPA_REGISTRY_ID",
        "REPORTING_YEAR",
        ]
    df=df.sort_values(by=sortvalues_cols)

    #ordered
    ordered_cols=usecols
    df=df[ordered_cols]

    #save
    filepath=f"{results}/{result}.csv"
    df.to_csv(filepath, index=False)


#osha violation screen
folders=["zhao/data/osha", "zhao/_osha"]
items=["osha_violation", "osha_violation_screen"]
def _osha_violation_screen(folders, items):
 
    #https://enforcedata.dol.gov/views/data_catalogs.php - OSHA
    #https://enforcedata.dol.gov/views/data_dictionary.php - OSHA

    #folders
    resources=folders[0]
    results=folders[1]

    #items
    resource=items[0]
    result=items[1]

    #filestems
    folderpath=f"{resources}/{resource}"
    files, filestems = _folder_to_filestems(folderpath)

    #n obs
    n_obs=len(filestems)

    #init frames
    frames=[None]*n_obs

    #for
    for i, filestem in enumerate(filestems):
        
        #usecols
        usecols=[
            "activity_nr",
            "citation_id",
            "issuance_date",
            #"abate_date",
            #"contest_date",
            #"final_order_date",
            
            "initial_penalty",
            #"current_penalty",

            #"rec", #why initiated
            #"nr_instances",
            #"viol_type",
            "gravity",
            "nr_exposed", #nr employess exposed
            #"abate_complete",

            "fta_issuance_date",
            #"fta_contest_date",
            #"fta_final_order_date",            
            "fta_penalty",
            #"fta_insp_nr", #failure to abate

            #"delete_flag",
            #"standard",
            #"emphasis",
            #"hazcat",
            #"hazsub1",
            #"hazsub2",
            #"hazsub3",
            #"hazsub4",
            #"hazsub5",
            ]
        #read
        filepath=f"{resources}/{resource}/{filestem}.csv"
        df_i=pd.read_csv(
            filepath,
            usecols=usecols,
            dtype="string",
            #nrows=1000,
            )
        
        #update frames
        frames[i]=df_i
        
    #concat
    df=pd.concat(frames)

    #save
    filepath=f"{results}/{resource}_all.csv"
    df.to_csv(filepath, index=False)

    #lowercase col values
    for i, col in enumerate(df.columns):
        df[col]=df[col].str.lower()

    #fillna
    fillna_cols=[
        "initial_penalty",
        "fta_penalty",
        ]
    for i, col in enumerate(fillna_cols):
        df[col]=pd.to_numeric(df[col])
        df[col]=df[col].fillna(0)

    #dropna
    dropna_cols=[
        "activity_nr",
        "initial_penalty",
        ]
    df=df.dropna(subset=dropna_cols)

    #to numeric
    errors="raise"
    tonumeric_cols=[
        "activity_nr",
        "gravity",
        "nr_exposed",
        ]
    for i, col in enumerate(tonumeric_cols):
        df[col]=pd.to_numeric(
            df[col],
            errors=errors,
            )

    #to numeric
    errors="coerce"
    format="%Y-%m-%d"
    todate_cols=[
        "issuance_date", 
        "fta_issuance_date",
        ]
    for i, col in enumerate(todate_cols):
        df[col]=pd.to_datetime(
            df[col],
            errors=errors,
            format=format,
            ).dt.strftime(format)

    #issuance year
    df["issuance_year"]=pd.DatetimeIndex(df["issuance_date"], ambiguous="NaT").year
    df["fta_issuance_year"]=pd.DatetimeIndex(df["fta_issuance_date"], ambiguous="NaT").year

    #sortvalues
    sortvalues_cols=[
        "activity_nr",
        "citation_id",
        ]
    df=df.sort_values(by=sortvalues_cols)

    #groupby
    by=[
        "activity_nr",
        ]
    dict_agg_colfunctions={
        "initial_penalty": [np.sum],            
        "fta_penalty": [np.sum],
        "gravity": [np.sum],
        "nr_exposed": [np.sum], 
        "citation_id":  [_firstvalue_join],
        "issuance_date": [_firstvalue_join],
        "fta_issuance_date": [_firstvalue_join],
        "issuance_year": [_firstvalue_join],
        "fta_issuance_year": [_firstvalue_join],
        }
    df=_groupby(df, by, dict_agg_colfunctions)

    #ordered
    ordered_cols=[
        "activity_nr",
        "citation_id",
        "issuance_date",
        "issuance_year",
        "fta_issuance_date",
        "fta_issuance_year",
        "initial_penalty",
        "gravity",
        "nr_exposed", 
        ]
    df=df[ordered_cols]

    #save
    filepath=f"{results}/{result}.csv"
    df.to_csv(filepath, index=False)


#osha inspection screen
folders=["zhao/data/osha", "zhao/_osha"]
items=["osha_inspection", "osha_inspection_screen"]
def _osha_inspection_screen(folders, items):
 
    #https://enforcedata.dol.gov/views/data_catalogs.php - OSHA
    #https://enforcedata.dol.gov/views/data_dictionary.php - OSHA

    #folders
    resources=folders[0]
    results=folders[1]

    #items
    resource=items[0]
    result=items[1]

    #filestems
    folderpath=f"{resources}/{resource}"
    files, filestems = _folder_to_filestems(folderpath)

    #n obs
    n_obs=len(filestems)

    #init frames
    frames=[None]*n_obs

    #for
    for i, filestem in enumerate(filestems):
        
        #usecols
        usecols=[
            "activity_nr",
            "estab_name",
            "open_date",
            #"close_conf_date",
            "close_case_date",

            "nr_in_estab",
            #"union_status",

            #"adv_notice",
            #"insp_scope",
            #"insp_type",
            #"why_no_insp",

            #"owner_type",
            #"owner_code",

            #"migrant",   
            #"reporting_id",
            #"host_est_key",

            #"sic_code",
            #"naics_code",

            #"site_address",
            #"site_city",
            #"site_state",
            #"site_zip",
            #"mail_city",
            #"mail_state",
            #"mail_zip",
            #"mail_street",

            #"health_const",
            #"health_manuf",
            #"health_marit",
            #"safety_const",
            #"safety_manuf",
            #"safety_marit",
            #"safety_hlth",
            ]

        #read
        filepath=f"{resources}/{resource}/{filestem}.csv"
        df_i=pd.read_csv(
            filepath,
            usecols=usecols,
            dtype="string",
            #nrows=1000,
            )
        
        #update frames
        frames[i]=df_i
        
    #concat
    df=pd.concat(frames)

    #save
    filepath=f"{results}/{resource}_all.csv"
    df.to_csv(filepath, index=False)

    #lowercase col values
    for i, col in enumerate(df.columns):
        df[col]=df[col].str.lower()

    #dropna
    dropna_cols=[
        "activity_nr",
        "estab_name",
        "open_date",
        "close_case_date",
        ]
    df=df.dropna(subset=dropna_cols)

    #to numeric
    errors="raise"
    tonumeric_cols=[
        "activity_nr",
        "nr_in_estab",
        ]
    for i, col in enumerate(tonumeric_cols):
        df[col]=pd.to_numeric(
            df[col],
            errors=errors,
            )

    #to date
    errors="coerce"
    format="%Y-%m-%d"
    todate_cols=[
        "open_date",
        "close_case_date",
        ]
    for i, col in enumerate(todate_cols):
        df[col] = pd.to_datetime(
            df[col],
            errors=errors,
            format=format,
            )
        
    #initiation year
    df[osha_initiation_year]=pd.DatetimeIndex(df["open_date"], ambiguous="NaT").year

    #penalty year
    df[osha_penalty_year]=pd.DatetimeIndex(df["close_case_date"], ambiguous="NaT").year

    #initiation lag
    col0=osha_penalty_year
    col1=osha_initiation_year
    df[osha_initiation_lag]=df.apply(_initiation_lag, axis=1, args=(col0, col1))

    #sortvalues
    sortvalues_cols=[
        "activity_nr",
        ]
    df=df.sort_values(by=sortvalues_cols)

    #ordered
    ordered_cols=[
        "activity_nr",
        "estab_name",
        "open_date",
        "close_case_date",
        osha_initiation_year,
        osha_penalty_year,
        osha_initiation_lag,
        "nr_in_estab",
        ]
    df=df[ordered_cols]

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


#irs donations aggregate
folders=["zhao/_irs", "zhao/_irs"]
items=["donations_ids", "donations_ids_aggregate"]
def _irs_contributors_aggregate(folders, items):

    #folders
    resources=folders[0]
    results=folders[1]

    #items
    resource=items[0]
    result=items[1]

    #usecols
    cols_id=[
        organization_id,
        contributor_id,
        contribution_year,
        contribution_amount_ytd,
        ]
    usecols=cols_id+contributor_list_firstvalue

    #read
    filepath=f"{resources}/{resource}.csv"
    df=pd.read_csv(
        filepath, 
        usecols=usecols,
        dtype="string",
        #nrows=1000,
        )

    #lowercase col values
    for i, col in enumerate(df.columns):
        df[col]=df[col].str.lower()

    #drop na
    dropna_cols=[
        organization_id,
        contributor_id,
        contribution_year,
        contribution_amount_ytd,
        ]
    df=df.dropna(subset=dropna_cols)

    #to numeric
    tonumeric_cols=[
        contribution_amount_ytd,
        ]
    for i, col in enumerate(tonumeric_cols):
        df[col]=pd.to_numeric(df[col])

    #first value dict
    dict_firstvalue={x: [_firstvalue_join] for x in contributor_list_firstvalue}
    #aggregate over organization-contributor-year obs
    by=[
        organization_id,
        contributor_id,
        contribution_year,
        ]
    dict_agg_colfunctions={
        contribution_amount_ytd: [np.sum],
        }
    dict_agg_colfunctions=dict_firstvalue|dict_agg_colfunctions
    df=_groupby(df, by, dict_agg_colfunctions)

    #pivot
    index=[
        contributor_id,
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
    list_pivot_columns=list(pivot_columns.values())
    pivot_df=pivot_df.rename(columns=pivot_columns)

    #full panel
    col_id=contributor_id
    col_year=contribution_year
    start_year=2000
    stop_year=2023
    df_fullpanel=_df_to_fullpanel(pivot_df, col_id, col_year, start_year, stop_year)

    #df without dups
    df_withoutdups=df.drop_duplicates(subset=contributor_id)
    df_withoutdups=df_withoutdups.drop([contribution_year], axis=1)

    #args
    suffixes=('_left', '_right')
    indicator=f"_merge_dups"
    #merge
    df=pd.merge(
        left=df_fullpanel,
        right=df_withoutdups,
        how="left",
        on=contributor_id,
        suffixes=suffixes,
        indicator=indicator,
        validate="m:1",
        )

    #ordered
    relevant_cols=[col_id, col_year] + list_pivot_columns
    ordered_cols= relevant_cols + contributor_list_firstvalue
    df=df[ordered_cols]

    #save
    filepath=f"{results}/{result}.csv"
    df.to_csv(filepath, index=False)
    #'''


#viol choice function echo
def _viol_choice_funct_echo(series):

    #len year
    len_year=4

    #series
    series=series.str[3:(3+len_year)]

    #return
    return series


#viol choice function pacer
def _viol_choice_funct_pacer(series):

    #series
    series=series.str[0:(0+4)]

    #return
    return series


#viol case id to year
def _viol_caseid_to_year(df, cond_var, choice_var, _choice_funct, select_var):

    #cond series
    cond_series=df[cond_var]

    #choice series
    choice_series=df[choice_var]

    #condlist
    condlist=[
        cond_series.isna(),
        cond_series.notna(),
        ]   
    
    #choicelist
    choicelist=[
        NA_VALUE, 
        _choice_funct(choice_series),
        ]
    
    #select series
    select_series=np.select(
        condlist,
        choicelist,
        default="error",
        )

    #create col
    df[select_var]=select_series

    #return
    return df


#viol initiation year
def _viol_initiation_year(row, col0, col1):

    #char
    char=":"

    #len year echo
    len_year0=4

    #len year pacer
    len_year1=2

    #rows
    row0=row[col0]
    row1=row[col1]

    #both missing
    if (pd.isna(row0)) and (pd.isna(row1)):
        
        #year
        year=None

    #not missing row0
    elif pd.notna(row0):

        #year
        year=int(row0)

    #not missing row1
    elif pd.notna(row1):

        #any list
        contains_2digit=(sum(1 for char in row1 if char.isdigit()) >= 2)

        #if
        if not contains_2digit:

            #year
            year=None
    
        #elif
        elif contains_2digit:

            #if
            if char not in row1:

                #first char is digit
                firstchar_isdigit=row1[0:1].isdigit()
                
                #if
                if firstchar_isdigit:

                    #idx
                    idx=0

                    #year 2digit
                    year_2digit=row1[idx:(idx+len_year1)]

                #elif
                elif not firstchar_isdigit:
                        
                    #idx
                    idx=1

                    #year 2digit
                    year_2digit=row1[idx:(idx+len_year1)]

            #elif
            elif char in row1:

                #idx
                idx=2

                #year 2digit
                year_2digit=row1[idx:(idx+len_year1)]

            #int
            int_year_2digit=int(year_2digit)

            #if
            if int_year_2digit>CURRENT_YEAR_2DIGIT:

                #year base
                year_base=19

            #elif
            elif int_year_2digit<=CURRENT_YEAR_2DIGIT:

                #year base
                year_base=20
        
            #year
            year=f"{year_base}{year_2digit}"
            
            #year
            year=int(year)

    #return
    return year


#viol initiation year infer
def _viol_initiation_year_infer(row, col0, col1, var0, var1):

    #rows
    row0=row[col0]
    row1=row[col1]

    #if
    if pd.notna(row0):
        
        #year
        year_mean=row0
        year_median=row0

        #year
        year_mean=int(year_mean)
        year_median=int(year_median)

    #elif
    elif pd.isna(row0):

        #if
        if pd.isna(row1):

            #year
            year_mean=None
            year_median=None

        elif pd.notna(row1):

            #year
            year_mean=row1-var0
            year_median=row1-var1  

            #year
            year_mean=int(year_mean)
            year_median=int(year_median)

    #return
    return year_mean, year_median


#viol aggregate
folders=["zhao/data/violation_tracker", "zhao/_violations"]
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

    #to numeric
    tonumeric_cols=[
        viol_penalty_year,
        viol_penalty_amount,
        viol_subpenalty_amount,
        viol_penalty_adjusted_amount,
        ]
    for i, col in enumerate(tonumeric_cols):
        df[col]=pd.to_numeric(df[col])

    #choice var
    choice_var="case_id"

    #initiation year echo
    cond_var="echo_case_url"
    select_var="initiation_year_echo"
    _choice_funct=_viol_choice_funct_echo
    df=_viol_caseid_to_year(df, cond_var, choice_var, _choice_funct, select_var)

    #initiation year pacer
    cond_var="pacer_link"
    select_var="initiation_year_pacer"
    _choice_funct=_viol_choice_funct_pacer
    df=_viol_caseid_to_year(df, cond_var, choice_var, _choice_funct, select_var)

    #initiation year true
    col0="initiation_year_echo"
    col1="initiation_year_pacer"
    df[viol_initiation_year_true]=df.apply(_viol_initiation_year, axis=1, args=(col0, col1))

    #initiation lag
    col0=viol_penalty_year
    col1=viol_initiation_year_true
    df[viol_initiation_lag]=df.apply(_initiation_lag, axis=1, args=(col0, col1))

    #stat initiation lag
    mean_initiation_lag=int(df[viol_initiation_lag].mean())
    median_initiation_lag=int(df[viol_initiation_lag].median())

    #report and infer init year
    col0=viol_initiation_year_true
    col1=viol_penalty_year
    var0=mean_initiation_lag
    var1=median_initiation_lag
    df[[initiation_year_inferred_mean, initiation_year_inferred_median]]=df.apply(
        _viol_initiation_year_infer,
        axis=1,
        result_type="expand",
        args=(col0, col1, var0, var1),
        )

    #drop na
    dropna_cols=[
        viol_parent_id,
        viol_penalty_year,
        initiation_year_inferred_mean,
        #initiation_year_inferred_median,
        viol_penalty_amount,
        ]
    df=df.dropna(subset=dropna_cols)

    #first value dict
    list_firstvalue=[x for x in df.columns if x not in dropna_cols]
    dict_firstvalue={x: [_firstvalue_join] for x in list_firstvalue}

    #aggregate over company-year obs
    by=[
        viol_parent_id,
        initiation_year_inferred_mean,
        ]
    dict_agg_colfunctions={
        viol_penalty_amount: [np.sum],
        viol_subpenalty_amount: [np.sum],
        viol_penalty_adjusted_amount: [np.sum],
        }
    dict_agg_colfunctions=dict_firstvalue|dict_agg_colfunctions
    df=_groupby(df, by, dict_agg_colfunctions)

    #save
    filepath=f"{results}/{result}.csv"
    df.to_csv(filepath, index=False)


#echo aggregate
folders=["zhao/_epa", "zhao/_epa"]
items=["echo_ids", "echo_ids_aggregate"]
def _echo_aggregate(folders, items):

    #folders
    resources=folders[0]
    results=folders[1]

    #items
    resource=items[0]
    result=items[1]

    #usecols
    usecols=[
        "case_number",
        "activity_id",
        echo_parent_id,
        echo_initiation_year,
        echo_penalty_year,
        echo_penalty_amount,
        echo_activity_type_code,
        "parent_co_name",
        company_name_rdp,
        company_businessentity,
        company_isin,
        #company_cusip,
        company_ric,
        company_oapermid,
        ]

    #read
    filepath=f"{resources}/{resource}.csv"
    df=pd.read_csv(
        filepath, 
        usecols=usecols,
        dtype="string",
        #nrows=1000,
        )
    
    #lowercase col values
    for i, col in enumerate(df.columns):
        df[col]=df[col].str.lower()

    #drop na
    dropna_cols=[
        echo_parent_id,
        echo_initiation_year,
        echo_penalty_year,
        echo_penalty_amount,
        ]
    df=df.dropna(subset=dropna_cols)

    #to numeric
    tonumeric_cols=[
        echo_initiation_year,
        echo_penalty_year,
        echo_penalty_amount,
        ]
    for i, col in enumerate(tonumeric_cols):
        df[col]=pd.to_numeric(df[col])

    #initiation lag
    col0=echo_penalty_year
    col1=echo_initiation_year
    df[echo_initiation_lag]=df.apply(_initiation_lag, axis=1, args=(col0, col1))

    #aggregate over case-company-init year obs
    by=[
        "case_number",
        "activity_id",
        echo_parent_id,
        echo_initiation_year,
        ]
    dict_agg_colfunctions={
        echo_penalty_amount: [_firstvalue_join],
        #"case_number": [_firstvalue_join],
        echo_penalty_year: [_firstvalue_join],
        echo_initiation_lag: [_firstvalue_join],
        echo_activity_type_code: [_firstvalue_join],
        "parent_co_name": [_firstvalue_join],
        company_name_rdp: [_firstvalue_join],
        company_businessentity: [_firstvalue_join],
        company_isin: [_firstvalue_join],
        #company_cusip: [_firstvalue_join],
        company_ric: [_firstvalue_join],
        company_oapermid: [_firstvalue_join],
        }
    df=_groupby(df, by, dict_agg_colfunctions)

    #aggregate over company-init year obs
    by=[
        echo_parent_id,
        echo_initiation_year,
        ]
    dict_agg_colfunctions={
        echo_penalty_amount: [np.sum],
        "case_number": [_firstvalue_join],
        echo_penalty_year: [_firstvalue_join],
        echo_initiation_lag: [_firstvalue_join],
        echo_activity_type_code: [_firstvalue_join],
        "parent_co_name": [_firstvalue_join],
        company_name_rdp: [_firstvalue_join],
        company_businessentity: [_firstvalue_join],
        company_isin: [_firstvalue_join],
        #company_cusip: [_firstvalue_join],
        company_ric: [_firstvalue_join],
        company_oapermid: [_firstvalue_join],
        }
    df=_groupby(df, by, dict_agg_colfunctions)

    #sortvalues
    sortvalues_cols=[
        "case_number",
        echo_parent_id,
        echo_initiation_year,
        ]
    df=df.sort_values(by=sortvalues_cols)

    #ordered
    ordered_cols=[
        "case_number",
        echo_parent_id,
        echo_initiation_year,
        echo_penalty_year,
        echo_initiation_lag,
        echo_penalty_amount,
        echo_activity_type_code,
        "parent_co_name",
        company_name_rdp,
        company_businessentity,
        company_isin,
        #company_cusip,
        company_ric,
        company_oapermid,
        ]
    df=df[ordered_cols]

    #save
    filepath=f"{results}/{result}.csv"
    df.to_csv(filepath, index=False)


#osha aggregate
folders=["zhao/_osha", "zhao/_osha"]
items=["osha_ids", "osha_ids_aggregate"]
def _osha_aggregate(folders, items):

    #folders
    resources=folders[0]
    results=folders[1]

    #items
    resource=items[0]
    result=items[1]

    #usecols
    cols_id=[
        osha_parent_id,
        osha_initiation_year,
        osha_penalty_year,
        osha_penalty_amount,
        "fta_issuance_year",
        "fta_penalty",
        "gravity",
        "nr_exposure",
        "nr_in_estab",
        ]
    usecols=cols_id+osha_list_firstvalue

    #read
    filepath=f"{resources}/{resource}.csv"
    df=pd.read_csv(
        filepath, 
        usecols=usecols,
        dtype="string",
        #nrows=1000,
        )
    
    #lowercase col values
    for i, col in enumerate(df.columns):
        df[col]=df[col].str.lower()

    #drop na
    dropna_cols=[
        osha_parent_id,
        osha_initiation_year,
        osha_penalty_year,
        osha_penalty_amount,
        ]
    df=df.dropna(subset=dropna_cols)

    #to numeric
    tonumeric_cols=[
        osha_penalty_amount,
        "gravity",
        "nr_exposure",
        "nr_in_estab",
        ]
    for i, col in enumerate(tonumeric_cols):
        df[col]=pd.to_numeric(df[col])

    #first value dict
    dict_firstvalue={x: [_firstvalue_join] for x in osha_list_firstvalue}

    #aggregate over company-init year obs
    by=[
        osha_parent_id,
        osha_initiation_year,
        ]
    dict_agg_colfunctions={
        osha_penalty_amount: [np.sum],
        "fta_penalty": [np.sum],
        "gravity": [np.sum],
        "nr_exposure": [np.sum],
        "nr_in_estab": [np.sum],
        }
    dict_agg_colfunctions=dict_firstvalue|dict_agg_colfunctions
    df=_groupby(df, by, dict_agg_colfunctions)

    #ordered
    ordered_cols=usecols
    df=df[ordered_cols]

    #save
    filepath=f"{results}/{result}.csv"
    df.to_csv(filepath, index=False)


#crspcompustat drop dups
folders=["zhao/data/crspcocompustat", "zhao/_crspcocompustat"]
items=["crspcompustat_2000_2023", "crspcompustat_2000_2023_dropdups"]
#_crspcompustat_dropdups(folders, items)
def _crspcompustat_dropdups(folders, items):

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
        )
    
    #drop duplicates
    df=df.drop_duplicates(subset=["cusip", "fyear"])

    #save
    filepath=f"{results}/{result}.csv"
    df.to_csv(filepath, index=False)

