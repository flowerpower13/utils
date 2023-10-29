

#imports
import re
import io
import numpy as np
import pandas as pd
from pathlib import Path
from datetime import datetime


#functions
from _industry_famafrench import _filepath_to_mapping
from _pd_utils import _folder_to_filestems, \
    _lowercase_colnames_values, _todatecols_to_df, _tonumericcols_to_df, _fillnacols_to_df, \
    _groupby, _df_to_fullpanel, \
    _first_value, _firstvalue_join


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


#vars refinitiv
company_name="dtsubjectname"
company_businessentity="businessentity"
company_ric="ric"
company_isin="issueisin"
company_cusip="cusip"
company_oapermid="oapermid"


#vars donations
#organization
organization_name="a__org_name"
organization_id="a__ein"
pivot_columns={
    "134220019": "amount_democratic",
    "464501717": "amount_republican",
    }
#contributors
contributor_id="cusip"
contributor_name="a__contributor_name"
contributor_employer="a__contributor_employer"
contributor_employer_new="a__contributor_employer_new"
contributor_donor_isfirm="a__donor_isfirm"
contributor_company_involved="a__contributor_company_involved"
#contribution
contribution_date="a__contribution_date"
contribution_year="a__contribution_year"
contribution_amount_ytd="a__agg_contribution_ytd"


#vars violation_tracker
violtrack_parent_id="violtrack_parent_id"
violtrack_initiation_year="violtrack_initiation_year"
violtrack_penalty_year="pen_year"
violtrack_initiation_lag="violtrack_initiation_lag"
violtrack_penalty_amount="penalty"
violtrack_docket_number="case_id"
violtrack_pacer_link="pacer_link"

#vars echo
echo_parent_id="cusip"
echo_initiation_year="echo_initiation_year"
echo_penalty_year="settlement_fy"
echo_initiation_lag="echo_initiation_lag"
echo_penalty_amount="fed_penalty_assessed_amt"
echo_activity_type_code="activity_type_code"


#vars osha
osha_parent_id="cusip"
osha_initiation_year="open_date"
osha_penalty_year="close_case_date"
osha_initiation_lag="osha_initiation_lag"
osha_penalty_amount="initial_penalty"


#vars crspcompustat
#https://wrds-www.wharton.upenn.edu/pages/get-data/center-research-security-prices-crsp/annual-update/crspcompustat-merged/fundamentals-annual/
#panel
crspcomp_cusip="cusip" #https://wrds-www.wharton.upenn.edu/data-dictionary/form_metadata/crsp_a_ccm_ccmfunda_identifyinginformation/CUSIP/
crspcomp_fyear="fyear" #https://wrds-www.wharton.upenn.edu/data-dictionary/form_metadata/crsp_a_ccm_ccmfunda_companydescriptor/FYEAR/
#id
crspcomp_cik="cik" #https://wrds-www.wharton.upenn.edu/data-dictionary/form_metadata/crsp_a_ccm_ccmfunda_identifyinginformation/CIK/
crspcomp_ein="ein" #https://wrds-www.wharton.upenn.edu/data-dictionary/form_metadata/crsp_a_ccm_ccmfunda_extra8_identifyinginformationcont/EIN/
crspcomp_name="conm" #https://wrds-www.wharton.upenn.edu/data-dictionary/form_metadata/crsp_a_ccm_ccmfunda_identifyinginformation/CONM/
crspcomp_state="state" #https://wrds-www.wharton.upenn.edu/data-dictionary/form_metadata/crsp_a_ccm_ccmfunda_extra8_identifyinginformationcont/STATE/
crspcomp_incorp="incorp" #https://wrds-www.wharton.upenn.edu/data-dictionary/form_metadata/crsp_a_ccm_ccmfunda_extra8_identifyinginformationcont/INCORP/
#balance sheet
crspcomp_assets="at" #https://wrds-www.wharton.upenn.edu/data-dictionary/form_metadata/crsp_a_ccm_ccmfunda_balancesheetitems/AT/
crspcomp_liabilities="lt" #https://wrds-www.wharton.upenn.edu/data-dictionary/form_metadata/crsp_a_ccm_ccmfunda_balancesheetitems/LT/
crspcomp_bookequity="seq" #https://wrds-www.wharton.upenn.edu/data-dictionary/form_metadata/crsp_a_ccm_ccmfunda_balancesheetitems/SEQ/
crspcomp_mktequity="mkvalt" #https://wrds-www.wharton.upenn.edu/data-dictionary/form_metadata/crsp_a_ccm_ccmfunda_supplementaldataitems/MKVALT/
crspcomp_sharesoutstanding="csho" #https://wrds-www.wharton.upenn.edu/data-dictionary/form_metadata/crsp_a_ccm_ccmfunda_miscellaneousitems/CSHO/
crspcomp_dividends="dvt" #https://wrds-www.wharton.upenn.edu/data-dictionary/form_metadata/crsp_a_ccm_ccmfunda_incomestatementitems/DVT/
#income statement
crspcomp_revenues="revt" ##https://wrds-www.wharton.upenn.edu/data-dictionary/form_metadata/crsp_a_ccm_ccmfunda_incomestatementitems/REVT/
crspcomp_cogs="cogs" #https://wrds-www.wharton.upenn.edu/data-dictionary/form_metadata/crsp_a_ccm_ccmfunda_incomestatementitems/COGS/
crspcomp_oibdp="oibdp" #https://wrds-www.wharton.upenn.edu/data-dictionary/form_metadata/crsp_a_ccm_ccmfunda_incomestatementitems/OIBDP/
crspcomp_da="dp" #https://wrds-www.wharton.upenn.edu/data-dictionary/form_metadata/crsp_a_ccm_ccmfunda_incomestatementitems/DP/
crspcomp_netincome="ni" #https://wrds-www.wharton.upenn.edu/data-dictionary/form_metadata/crsp_a_ccm_ccmfunda_incomestatementitems/NI/
#industry
crspcomp_sic="sic" #https://wrds-www.wharton.upenn.edu/data-dictionary/form_metadata/crsp_a_ccm_ccmfunda_extra8_identifyinginformationcont/SIC/
crspcomp_naics="naics" #https://wrds-www.wharton.upenn.edu/data-dictionary/form_metadata/crsp_a_ccm_ccmfunda_extra8_identifyinginformationcont/NAICS/
crspcomp_gics="gind" #https://wrds-www.wharton.upenn.edu/data-dictionary/form_metadata/crsp_a_ccm_ccmfunda_extra8_identifyinginformationcont/GIND/
#size, lev, btm, roe, industry

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

    #usecols
    usecols=[
        organization_name,
        organization_id,
        contributor_name,
        contributor_employer,
        f"{contribution_date}\r", #???
        contribution_amount_ytd,
        ]
    usecols=[x.capitalize() for x in usecols]

    #read
    filepath=f"{resources}/{resource}.csv"
    df=pd.read_csv(
        filepath, 
        sep=NEW_SEP,
        usecols=usecols,
        dtype="string",
        #nrows=1000,
        lineterminator=LINETERMINATOR,
        quotechar=QUOTECHAR,
        #on_bad_lines='skip',
        )

    #lowercase col names and values
    df=_lowercase_colnames_values(df)

    #???
    df=df.rename(columns={f"{contribution_date}\r": contribution_date})

    #dropna
    dropna_cols=[
        organization_id,
        contribution_date, 
        contribution_amount_ytd,
        ]
    df=df.dropna(subset=dropna_cols)

    #to numeric
    tonumeric_cols=[
        organization_id,
        contribution_amount_ytd,
        ]
    df=_tonumericcols_to_df(df, tonumeric_cols)

    #to date
    todate_cols=[
        contribution_date,
        ]
    errors="coerce"
    format="%Y%m%d"
    df=_todatecols_to_df(df, todate_cols, errors, format)

    #keep only attorneys general associations (resp. Dem and Rep)
    s=df[organization_id]
    df=df[
        (s==134220019) | (s==464501717)
        ]

    #initiation year
    df[contribution_year]=pd.DatetimeIndex(df[contribution_date], ambiguous="NaT").year

    #sortvalues
    sortvalues_cols=[
        contributor_name,
        organization_id,
        contribution_year,
        contribution_date,
        ]
    ascending=[
        True,
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
        organization_id,
        contributor_name,
        contribution_year,
        ]
    dict_agg_colfunctions={
        organization_name: [_first_value],
        contributor_employer: [_first_value],
        contribution_date: [_first_value],
        contribution_amount_ytd: [_first_value],
        }
    df=_groupby(df, by, dict_agg_colfunctions)

    #substitute employer with nan
    dict_replace={
        "n/a": NA_VALUE,
        "d/a": NA_VALUE,
        "na": NA_VALUE,
        "not available": NA_VALUE,
        "not applicable": NA_VALUE,
        "same": NA_VALUE,
        "none": NA_VALUE,
        "not employed": NA_VALUE,
        "not a contribution": NA_VALUE,
        "in-kind": NA_VALUE,
        "self": NA_VALUE,
        "self-employed": NA_VALUE,
        "self - employed": NA_VALUE,
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
    x0=df[contributor_name]
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
    df[contributor_company_involved]=y0
    df[contributor_donor_isfirm]=y1

    ordered_cols=[
        organization_name,
        organization_id,
        contributor_name,
        contributor_employer,
        contributor_employer_new,
        contributor_donor_isfirm,
        contributor_company_involved,
        contribution_date,
        contribution_year,
        contribution_amount_ytd,
        ]
    df=df[ordered_cols]

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
        "case_number", 
        "activity_id", 
        "registry_id", 
        "facility_name",

        #"LOCATION_ADDRESS",
        #"CITY",
        #"STATE_CODE",
        #"ZIP",
        #"PRIMARY_SIC_CODE",
        #"PRIMARY_NAICS_CODE",
        ]
    usecols=[x.upper() for x in usecols]

    #read
    filepath=f"{resources}/{resource}.csv"
    df=pd.read_csv(
        filepath,
        usecols=usecols,
        dtype="string",
        #nrows=1000,
        )

    df=_lowercase_colnames_values(df)

    #dropna
    dropna_cols=[
        "case_number", 
        "activity_id", 
        "registry_id", 
        ]
    df=df.dropna(subset=dropna_cols)

    #to numeric
    tonumeric_cols=[
        "activity_id", 
        "registry_id", 
        ]
    df=_tonumericcols_to_df(df, tonumeric_cols)

    #sortvalues
    sortvalues_cols=[
        "case_number", 
        "activity_id", 
        "registry_id", 
        "facility_name",
        ]
    df=df.sort_values(by=sortvalues_cols)

    #drop duplicates
    dropdups_cols=[
        "case_number", 
        "activity_id", 
        "registry_id", 
        ]
    df=df.drop_duplicates(subset=dropdups_cols)

    #ordered
    ordered_cols=[
        "case_number", 
        "activity_id", 
        "registry_id", 
        "facility_name",
        ]
    df=df[ordered_cols]

    #save
    filepath=f"{results}/{result}.csv"
    df.to_csv(filepath, index=False)


#initiation lag
def _initiation_lag(row, col0, col1):

    #values
    value0=row[col0]
    value1=row[col1]

    #initiation lag
    initiation_lag=value0-value1

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
        "case_number", 
        "activity_id",

        #conclusion
        #"ENF_CONCLUSION_ID",
        #"ENF_CONCLUSION_NMBR",
        #"ENF_CONCLUSION_ACTION_CODE",
        #"ENF_CONCLUSION_ACTION_CODE",
        #"ENF_CONCLUSION_NAME",

        #date
        #"SETTLEMENT_LODGED_DATE",
        #"SETTLEMENT_ENTERED_DATE",
        "settlement_fy",

        #other
        #"PRIMARY_LAW",
        #"REGION_CODE",
        "activity_type_code",

        #penalty
        "fed_penalty_assessed_amt",
        #"STATE_LOCAL_PENALTY_AMT",
        #"SEP_AMT",
        #"COMPLIANCE_ACTION_COST",
        #"COST_RECOVERY_AWARDED_AMT",
        ]
    usecols=[x.upper() for x in usecols]

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

    #fillna
    fillna_cols=[
        "fed_penalty_assessed_amt",
        ]
    df=_fillnacols_to_df(df, fillna_cols)

    #dropna
    dropna_cols=[ 
        "case_number", 
        "activity_id",
        "fed_penalty_assessed_amt",
        "settlement_fy",
        ]
    df=df.dropna(subset=dropna_cols)

    #to numeric
    tonumeric_cols=[
        "activity_id",
        "settlement_fy",
        ]
    df=_tonumericcols_to_df(df, tonumeric_cols)
        
    #sortvalues
    sortvalues_cols=[
        "case_number", 
        "activity_id",
        "settlement_fy",
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
        "case_number",
        "activity_id",
        ]
    dict_agg_colfunctions={
        "fed_penalty_assessed_amt": [np.sum],
        "settlement_fy": [_first_value],
        "activity_type_code": [_first_value],
        }
    df=_groupby(df, by, dict_agg_colfunctions)

    #ordered
    ordered_cols=[
        "case_number", 
        "activity_id",
        "settlement_fy",
        "activity_type_code",
        "fed_penalty_assessed_amt",
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
        "case_number", 
        "activity_id",

        #date
        "actual_date",

        #other
        #"SUB_activity_type_code",
        #"SUB_ACTIVITY_TYPE_DESC",
        ]
    usecols=[x.upper() for x in usecols]

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
        "case_number", 
        "activity_id",
        "actual_date",
        ]
    df=df.dropna(subset=dropna_cols)

    #to numeric
    tonumeric_cols=[
        "activity_id",
        ]
    df=_tonumericcols_to_df(df, tonumeric_cols)

    #to date
    todate_cols=[
        "actual_date",
        ]
    errors="coerce"
    format="%m/%d/%Y"
    df=_todatecols_to_df(df, todate_cols, errors, format)

    #sortvalues
    sortvalues_cols=[
        "case_number", 
        "activity_id",
        "actual_date",
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
        "case_number",
        "activity_id",
        ]
    dict_agg_colfunctions={
        "actual_date": [_first_value],
        }
    df=_groupby(df, by, dict_agg_colfunctions)

    #initiation year
    df[echo_initiation_year]=pd.DatetimeIndex(df["actual_date"], ambiguous="NaT").year

    #ordered
    ordered_cols=[
        "case_number", 
        "activity_id",
        "actual_date",
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
        "epa_registry_id",
        "reporting_year",
        "facility_name",
        "parent_co_name",
        ]
    usecols=[x.upper() for x in usecols]

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
        "epa_registry_id",
        "reporting_year",
        "parent_co_name",
        ]
    df=df.dropna(subset=dropna_cols)

    #to numeric
    tonumeric_cols=[
        "epa_registry_id",
        "reporting_year",
        ]
    df=_tonumericcols_to_df(df, tonumeric_cols)

    #sortvalues
    sortvalues_cols=[
        "epa_registry_id",
        "reporting_year",
        ]
    df=df.sort_values(by=sortvalues_cols)

    #ordered
    ordered_cols=[
        "epa_registry_id",
        "reporting_year",
        "facility_name",
        "parent_co_name",
        ]
    df=df[ordered_cols]

    #save
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
        "current_parent_name",
        "current_parent_ISIN",
        violtrack_penalty_year,
        violtrack_penalty_amount,
        violtrack_docket_number,
        violtrack_pacer_link,

        #company
        "company",
        "current_parent_ownership_structure",
        "current_parent_CIK",

        #case
        "offense_group", #e.g., employment-related offenses
        "case_category", #private litigation or agency action
        "agency_code", #e.g., MULTI-AG
        "govt_level", #federal/state
        "court", #e.g., Northern District of Illinois
        "litigation_case_title", #e.g., Gerlib, et al v. R R Donnelley & Sons, et al
        "lawsuit_resolution", #settlement or verdict
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

    #to numeric
    tonumeric_cols=[
        violtrack_penalty_year,
        violtrack_penalty_amount,
        ]
    df=_tonumericcols_to_df(df, tonumeric_cols)

    #drop na
    dropna_cols=[
        "current_parent_name",
        violtrack_penalty_year,
        violtrack_penalty_amount,
        violtrack_docket_number,
        violtrack_pacer_link,
        ]
    df=df.dropna(subset=dropna_cols)

    #initiation year
    df[violtrack_initiation_year]=df[violtrack_docket_number].apply(_violtrack_initiation_year)

    #drop na
    dropna_cols=[
        violtrack_initiation_year,
        ]
    df=df.dropna(subset=dropna_cols)
 
    #initiation lag
    col0=violtrack_penalty_year
    col1=violtrack_initiation_year
    df[violtrack_initiation_lag]=df.apply(_initiation_lag, axis=1, args=(col0, col1))

    ordered_cols=[
        "current_parent_name",
        "current_parent_isin",
        violtrack_initiation_year,
        violtrack_penalty_year,
        violtrack_initiation_lag,
        violtrack_penalty_amount,
        violtrack_docket_number,
        violtrack_pacer_link,

        #company
        "company",
        "current_parent_ownership_structure",
        "current_parent_cik",

        #case
        "offense_group", #e.g., employment-related offenses
        "case_category", #private litigation or agency action
        "agency_code", #e.g., MULTI-AG
        "govt_level", #federal/state
        "court", #e.g., Northern District of Illinois
        "litigation_case_title", #e.g., Gerlib, et al v. R R Donnelley & Sons, et al
        "lawsuit_resolution", #settlement or verdict
        ]
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

    #lowercase col names and values
    df=_lowercase_colnames_values(df)

    #fillna
    fillna_cols=[
        "initial_penalty",
        "fta_penalty",
        ]
    df=_fillnacols_to_df(df, fillna_cols)

    #dropna
    dropna_cols=[
        "activity_nr",
        "initial_penalty",
        ]
    df=df.dropna(subset=dropna_cols)

    #to numeric
    tonumeric_cols=[
        "activity_nr",
        "gravity",
        "nr_exposed",
        ]
    df=_tonumericcols_to_df(df, tonumeric_cols)

    #to date
    todate_cols=[
        "issuance_date", 
        "fta_issuance_date",
        ]
    errors="coerce"
    format="%Y-%m-%d"
    df=_todatecols_to_df(df, todate_cols, errors, format)

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
        "initial_penalty",
        "fta_issuance_date",
        "fta_issuance_year",
        "fta_penalty"
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

    #lowercase col names and values
    df=_lowercase_colnames_values(df)

    #dropna
    dropna_cols=[
        "activity_nr",
        "estab_name",
        "open_date",
        "close_case_date",
        ]
    df=df.dropna(subset=dropna_cols)

    #to numeric
    tonumeric_cols=[
        "activity_nr",
        "nr_in_estab",
        ]
    df=_tonumericcols_to_df(df, tonumeric_cols)

    #to date
    todate_cols=[
        "open_date",
        "close_case_date",
        ]
    errors="coerce"
    format="%Y-%m-%d"
    df=_todatecols_to_df(df, todate_cols, errors, format)
        
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
        osha_initiation_year,
        osha_penalty_year,
        osha_initiation_lag,
        "nr_in_estab",
        ]
    df=df[ordered_cols]

    #save
    filepath=f"{results}/{result}.csv"
    df.to_csv(filepath, index=False)


#crspcompustat screen
folders=["zhao/data/crspcompustat", "zhao/_crspcompustat"]
items=["crspcompustat_2000_2023", "crspcompustat_2000_2023_screen"]
def _crspcompustat_screen(folders, items):

    #folders
    resources=folders[0]
    results=folders[1]

    #items
    resource=items[0]
    result=items[1]

    #usecols
    usecols=[
        crspcomp_cusip,
        crspcomp_fyear,
        crspcomp_cik,
        crspcomp_ein,
        crspcomp_name,
        crspcomp_state,
        crspcomp_incorp,
        crspcomp_assets,
        crspcomp_liabilities,
        crspcomp_bookequity,
        crspcomp_mktequity,
        crspcomp_sharesoutstanding,
        crspcomp_dividends,
        crspcomp_revenues,
        crspcomp_cogs,
        crspcomp_oibdp,
        crspcomp_da,
        crspcomp_netincome,
        crspcomp_sic,
        crspcomp_naics,
        crspcomp_gics,
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
        crspcomp_cusip, 
        crspcomp_fyear, 
        ]
    df=df.dropna(subset=dropna_cols)

    #drop duplicates
    dropdups_cols=[
        crspcomp_cusip, 
        crspcomp_fyear, 
        ]
    df=df.drop_duplicates(subset=dropdups_cols)

    #ordered
    ordered_cols=usecols
    df=df[ordered_cols]

    #save
    filepath=f"{results}/{result}.csv"
    df.to_csv(filepath, index=False)


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
    usecols=[
        organization_id,
        contributor_id,
        contributor_name,
        contribution_year,
        contribution_amount_ytd,
        contributor_employer,
        contributor_employer_new,
        contributor_donor_isfirm,
        contributor_company_involved,

        #refinitiv
        company_name,
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

    #lowercase col names and values
    df=_lowercase_colnames_values(df)

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
        contribution_year,
        contribution_amount_ytd,
        ]
    df=_tonumericcols_to_df(df, tonumeric_cols)

    #aggregate over organization-contributor-year obs
    by=[
        organization_id,
        contributor_id,
        contribution_year,
        ]
    dict_agg_colfunctions={
        contribution_amount_ytd: [np.sum],
        contributor_name: [_firstvalue_join],
        contributor_employer: [_firstvalue_join],
        contributor_employer_new: [_firstvalue_join],
        contributor_donor_isfirm: [_firstvalue_join],
        contributor_company_involved: [_firstvalue_join],
        company_name: [_firstvalue_join],
        company_businessentity: [_firstvalue_join],
        company_isin: [_firstvalue_join],
        #company_cusip: [_firstvalue_join],
        company_ric: [_firstvalue_join],
        company_oapermid: [_firstvalue_join],
        }
    df=_groupby(df, by, dict_agg_colfunctions)

   #pivot
    index=[
        contributor_id,
        contribution_year,
        ]
    df_pivot=pd.pivot_table(
        data=df,
        values=contribution_amount_ytd,
        index=index,
        columns=organization_id,  
        aggfunc=np.sum,
        fill_value=0,
        )      
    #reset index
    df_pivot=df_pivot.reset_index()   
    #rename
    list_pivot_columns=list(pivot_columns.values())
    df_pivot=df_pivot.rename(columns=pivot_columns)

    #df without dups
    df_withoutdups=df.drop_duplicates(subset=contributor_id)
    df_withoutdups=df_withoutdups.drop([contribution_year], axis=1)

    #args
    suffixes=('_left', '_right')
    indicator=f"_merge_dups"
    #merge
    df=pd.merge(
        left=df_pivot,
        right=df_withoutdups,
        how="left",
        on=contributor_id,
        suffixes=suffixes,
        indicator=indicator,
        validate="m:1",
        )
    
    #full panel
    cols_id=[
        contributor_id,
        contribution_year,
        ]
    years=[
        2000,
        2023,
        ]
    fillna_cols=list_pivot_columns
    df=_df_to_fullpanel(df, cols_id, years, fillna_cols)

    #ordered
    ordered_cols= [
        contributor_name,
        contributor_id,
        contribution_year
        ] + list_pivot_columns + [
        contributor_employer,
        contributor_employer_new,
        contributor_donor_isfirm,
        contributor_company_involved,
        company_name,
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

        #refinitiv
        company_name,
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
    
    #lowercase col names and values
    df=_lowercase_colnames_values(df)

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
    df=_tonumericcols_to_df(df, tonumeric_cols)

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

        #refinitiv
        company_name: [_firstvalue_join],
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

        #refinitiv
        company_name: [_firstvalue_join],
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

    #full panel
    cols_id=[
        echo_parent_id,
        echo_initiation_year,
        ]
    years=[
        2000,
        2023,
        ]
    fillna_cols=[
        echo_penalty_amount,
        ]
    df=_df_to_fullpanel(df, cols_id, years, fillna_cols)

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

        #refinitiv
        company_name,
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


#viol initiation year
def _violtrack_initiation_year(value):

    #char
    char=":"

    #len year
    len_year=2

    #cut value
    value=value[:4]

    #at least two digits in value
    contains_2digit=(sum(1 for char in value if char.isdigit()) >= 2)

    #if
    if not contains_2digit:

        #year
        year=None

    #elif
    elif contains_2digit:

        #if
        if char not in value:

            #first char is digit
            firstchar_isdigit=value[0:1].isdigit()
            
            #if
            if firstchar_isdigit:

                #idx
                idx=0

                #year 2digit
                year_2digit=value[idx:(idx+len_year)]

            #elif
            elif not firstchar_isdigit:
                    
                #idx
                idx=1

                #year 2digit
                year_2digit=value[idx:(idx+len_year)]

        #elif
        elif char in value:

            #idx
            idx=2

            #year 2digit
            year_2digit=value[idx:(idx+len_year)]

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


#keep internal isin, if not present keep external
def _violtrack_keepisin(row, col0, col1):

    #values
    value0=row[col0]
    value1=row[col1]

    #if
    if pd.notna(value0):

        #value
        value=value0

    #elif
    elif pd.isna(value0):

        #if
        if pd.notna(value1):

            #value
            value=value1

        #elif
        elif pd.isna(value0):

            #value
            value=None

    #return 
    return value


#violtrack aggregate
folders=["zhao/data/violation_tracker", "zhao/_violtrack"]
items=["ViolationTracker_basic_28jul23", "_violtrack_ids_aggregate"]
def _violtrack_aggregate(folders, items):

    #folders
    resources=folders[0]
    results=folders[1]

    #items
    resource=items[0]
    result=items[1]

    #usecols
    usecols=[
        violtrack_initiation_year,
        violtrack_penalty_year,
        violtrack_initiation_lag,
        violtrack_penalty_amount,
        violtrack_docket_number,
        violtrack_pacer_link,

        #company
        "company",
        "current_parent_name",
        "current_parent_isin",
        "current_parent_ownership_structure",
        "current_parent_cik",

        #case
        "offense_group", #e.g., employment-related offenses
        "case_category", #private litigation or agency action
        "agency_code", #e.g., MULTI-AG
        "govt_level", #federal/state
        "court", #e.g., Northern District of Illinois
        "litigation_case_title", #e.g., Gerlib, et al v. R R Donnelley & Sons, et al
        "lawsuit_resolution", #settlement or verdict

        #refinitiv        
        company_name,
        company_businessentity,
        company_isin,
        company_cusip,
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

    #lowercase col names and values
    df=_lowercase_colnames_values(df)

    #to numeric
    tonumeric_cols=[
        violtrack_initiation_year,
        violtrack_penalty_year,
        violtrack_initiation_lag,
        violtrack_penalty_amount,
        ]
    df=_tonumericcols_to_df(df, tonumeric_cols)

    #keep internal isin, if not present keep external
    col0="current_parent_isin"
    col1=company_isin
    df[violtrack_parent_id]=df.apply(_violtrack_keepisin, axis=1, args=(col0, col1))

    #drop na
    dropna_cols=[
        violtrack_parent_id,
        violtrack_initiation_year,
        violtrack_penalty_year,
        violtrack_initiation_lag,
        violtrack_penalty_amount,
        ]
    df=df.dropna(subset=dropna_cols)

    #aggregate over company-year obs
    by=[
        violtrack_parent_id,
        violtrack_initiation_year,
        ]
    dict_agg_colfunctions={
        violtrack_penalty_amount: [np.sum],
        violtrack_penalty_year: [_firstvalue_join],
        violtrack_initiation_lag: [_firstvalue_join],
        violtrack_docket_number: [_firstvalue_join],
        violtrack_pacer_link: [_firstvalue_join],

        #company
        "company": [_firstvalue_join],
        "current_parent_name": [_firstvalue_join],
        "current_parent_isin": [_firstvalue_join],
        "current_parent_ownership_structure": [_firstvalue_join],
        "current_parent_cik": [_firstvalue_join],

        #case
        "offense_group": [_firstvalue_join],
        "case_category": [_firstvalue_join],
        "agency_code": [_firstvalue_join],
        "govt_level": [_firstvalue_join],
        "court": [_firstvalue_join],
        "litigation_case_title": [_firstvalue_join],
        "lawsuit_resolution": [_firstvalue_join],

        #refinitiv
        company_name: [_firstvalue_join],
        company_businessentity: [_firstvalue_join],
        company_isin: [_firstvalue_join],
        company_cusip: [_firstvalue_join],
        company_ric: [_firstvalue_join],
        company_oapermid: [_firstvalue_join],
        }
    df=_groupby(df, by, dict_agg_colfunctions)

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
    df=_df_to_fullpanel(df, cols_id, years, fillna_cols)

    ordered_cols=[
        violtrack_parent_id,
        violtrack_initiation_year,
        violtrack_penalty_year,
        violtrack_initiation_lag,
        violtrack_penalty_amount,
        violtrack_docket_number,
        violtrack_pacer_link,

        #company
        "company",
        "current_parent_name",
        "current_parent_isin",
        "current_parent_ownership_structure",
        "current_parent_cik",

        #case
        "offense_group", #e.g., employment-related offenses
        "case_category", #private litigation or agency action
        "agency_code", #e.g., MULTI-AG
        "govt_level", #federal/state
        "court", #e.g., Northern District of Illinois
        "litigation_case_title", #e.g., Gerlib, et al v. R R Donnelley & Sons, et al
        "lawsuit_resolution", #settlement or verdict

        #refinitiv
        company_name,
        company_businessentity,
        #company_isin,
        company_cusip,
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
    usecols=[
        osha_parent_id,
        osha_initiation_year,
        osha_penalty_year,
        osha_initiation_lag,
        osha_penalty_amount,
        "fta_issuance_year",
        "fta_penalty",
        "gravity",
        "nr_exposure",
        "nr_in_estab",

        #refinitiv
        company_name,
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
    
    #lowercase col names and values
    df=_lowercase_colnames_values(df)

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
    df=_tonumericcols_to_df(df, tonumeric_cols)

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
        osha_penalty_year: [_firstvalue_join],
        osha_initiation_lag: [_firstvalue_join],
        "fta_issuance_year": [_firstvalue_join],

        #refinitiv
        company_name: [_firstvalue_join],
        company_businessentity: [_firstvalue_join],
        company_isin: [_firstvalue_join],
        #company_cusip: [_firstvalue_join],
        company_ric: [_firstvalue_join],
        company_oapermid: [_firstvalue_join],
        }
    df=_groupby(df, by, dict_agg_colfunctions)

    #full panel
    cols_id=[
        osha_parent_id,
        osha_initiation_year,
        ]
    years=[
        2000,
        2023,
        ]
    fillna_cols=[
        osha_penalty_amount,
        "fta_penalty",
        "gravity",
        "nr_exposure",
        "nr_in_estab",
        ]
    df=_df_to_fullpanel(df, cols_id, years, fillna_cols)

    #ordered
    ordered_cols=usecols
    df=df[ordered_cols]

    #save
    filepath=f"{results}/{result}.csv"
    df.to_csv(filepath, index=False)


#ln vars
def _ln_vars(df, oldvars):

    #to numeric
    tonumeric_cols=oldvars
    df=_tonumericcols_to_df(df, tonumeric_cols)

    #init
    newvars=[None]*len(oldvars)

    #for
    for i, col in enumerate(oldvars):

        #newvar
        newvar=f"ln_{col}"

        #gen
        df[newvar]=np.log1p(df[col])

        #update
        newvars[i]=newvar

    #return
    return df, newvars


#lagged vars
def _lag_vars(df, oldvars):

    #periods
    periods=1

    #to numeric
    tonumeric_cols=oldvars
    df=_tonumericcols_to_df(df, tonumeric_cols)

    #init
    newvars=[None]*len(oldvars)

    #for
    for i, col in enumerate(oldvars):

        #newvar
        newvar=f"lag_{col}"

        #gen
        df[newvar]=df[col].shift(periods=periods, fill_value=0)

        #update
        newvars[i]=newvar

    #return
    return df, newvars


#change vars
def _change_vars(df, oldvars):

    #periods
    periods=1

    #to numeric
    tonumeric_cols=oldvars
    df=_tonumericcols_to_df(df, tonumeric_cols)

    #init
    newvars=[None]*len(oldvars)

    #for
    for i, col in enumerate(oldvars):

        #newvar
        newvar=f"change_{col}"

        #gen
        df[newvar]=df[col] - df[col].shift(periods=periods, fill_value=0)

        #update
        newvars[i]=newvar

    #return
    return df, newvars


#gen dummy if positive
def _dummyifpositive_vars(df, oldvars):

    #to numeric
    tonumeric_cols=oldvars
    df=_tonumericcols_to_df(df, tonumeric_cols)

    #init
    newvars=[None]*len(oldvars)

    #for
    for i, col in enumerate(oldvars):

        #x
        x=df[col]

        #condlist
        condlist=[
            x<0,
            x.isna(),
            x==0,
            x>0,
            ]   
        
        #choicelist
        choicelist=[
            "error", 
            np.nan,
            0,
            1,
            ]
        
        #y
        y=np.select(condlist, choicelist, default="error")

        #newvar
        newvar=col.replace("amount", "dummy")

        #gen
        df[newvar]=y

        #update
        newvars[i]=newvar

    #return
    return df, newvars


#post year dummies
def _post_year_dummies(df, start_year, stop_year):

    #to numeric
    tonumeric_cols=[
        crspcomp_fyear,
        ]
    df=_tonumericcols_to_df(df, tonumeric_cols)

    #years
    years=list(range(start_year, (stop_year+1)))

    #init vars
    post_year_dummies=[None]*len(years)

    #for
    for j, year in enumerate(years):

        #var name
        post_year_dummy=f"post{year}"

        #gen var
        df[post_year_dummy]=np.where(df[crspcomp_fyear] >= year, 1, 0)

        #update var
        post_year_dummies[j]=post_year_dummy

    #return
    return df, post_year_dummies


#donations newvars
def _donations_newvars(df):

    #amount_both
    df["amount_both"]=df["amount_democratic"] + df["amount_republican"]

    #oldvars
    oldvars=[
        "amount_democratic",
        "amount_republican",
        "amount_both",
        ]
    df, newvars = _dummyifpositive_vars(df, oldvars)

    
    #ln
    oldvars=[
        #amount
        "amount_democratic",
        "amount_republican",
        "amount_both",
        ]
    df, ln_vars = _ln_vars(df, oldvars)

    #lagged
    oldvars=[
        #amount
        "amount_democratic",
        "amount_republican",
        "amount_both",

        #ln amount
        "ln_amount_democratic",
        "ln_amount_republican",
        "ln_amount_both",

        #dummy
        "dummy_democratic",
        "dummy_republican", 
        "dummy_both",
        ]
    df, lag_vars = _lag_vars(df, oldvars)

    #change
    oldvars=[
        #amount
        "amount_democratic",
        "amount_republican",
        "amount_both",

        #ln amount
        "ln_amount_democratic",
        "ln_amount_republican",
        "ln_amount_both",
        ]
    df, change_vars = _change_vars(df, oldvars)

    #donation vars
    donation_vars=[
        #amount
        "amount_democratic",
        "amount_republican",
        "amount_both",

        #dummy
        "dummy_democratic",
        "dummy_republican", 
        "dummy_both",
        ] + ln_vars + lag_vars + change_vars

    #post year + interaction
    start_year=2015
    stop_year=2019
    level_vars=donation_vars
    df, post_year_dummies = _post_year_dummies(df, start_year, stop_year)

    #return
    return df, donation_vars, post_year_dummies


#stagdid
def _stagdid(df, unit_var, time_var, treatment_switch_dummies):

    #init
    newvars=list()

    #for
    for i, treatment_switch_dummy in enumerate(treatment_switch_dummies):

        #newvars
        treatment_firstswitch_dummy=f"{treatment_switch_dummy}_treatment_firstswitch_dummy"
        treatment_dummy=f"{treatment_switch_dummy}_treatment_dummy"
        group_var=f"{treatment_switch_dummy}_group"
        group_dummy=f"{treatment_switch_dummy}_group_dummy"

        #sortvalues
        sortvalues_cols=[
            unit_var,
            time_var,
            ]
        df=df.sort_values(by=sortvalues_cols)

        #min indices
        min_indices=df[df[treatment_switch_dummy] == 1].groupby(unit_var).apply(lambda x: x.index.min())

        #treatment firstswitch dummy
        df[treatment_firstswitch_dummy]=0
        df.loc[min_indices, treatment_firstswitch_dummy]=1

        #treatment dummy
        df[treatment_dummy]=df.groupby(unit_var)[treatment_switch_dummy].cummax()

        #group var
        grouped=df[df[treatment_firstswitch_dummy] == 1].groupby(unit_var)[time_var].sum()

        #reset index
        grouped=grouped.reset_index()

        #rename
        grouped=grouped.rename(columns={time_var: group_var})

        #merge
        df=pd.merge(
            left=df,
            right=grouped,
            how="left",
            on=unit_var,
            validate="m:1"
            )
        
        #fillna
        df[group_var]=df[group_var].fillna(0).astype(int)

        #gen group dummies
        '''df_pivot=pd.pivot_table(
            data=df,
            values=treatment_firstswitch_dummy,
            index=unit_var,
            columns=time_var,
            aggfunc=np.sum,
            fill_value=0,
            )

        #rename cols
        df_pivot.columns=[f"{group_dummy}_{x}" for x in df_pivot.columns]

        #reset index
        df_pivot=df_pivot.reset_index()

        #group dummies
        group_dummies=list(df_pivot.columns)

        # Merge the pivoted data back into the original DataFrame
        df=pd.merge(
            left=df,
            right=df_pivot,
            how="left",
            on=unit_var,
            validate="m:1"
            )
        #'''

        #newvars
        newvars_i=[
            treatment_firstswitch_dummy,
            treatment_dummy,
            group_var,
            ] #+ group_dummies

        #update
        newvars.extend(newvars_i)

    #return
    return df, newvars


#echo newvars
def _echo_newvars(df):

    #rename
    df=df.rename(columns={
        echo_penalty_amount: "echo_penalty_amount",
        echo_penalty_year: "echo_penalty_year",
        })
    
    #dummy enforcement action
    df["echo_enforcement_dummy"]=np.where(df["echo_penalty_year"].notna(), 1, 0)

    #dummy vars
    oldvars=["echo_penalty_amount",]
    df, newvars = _dummyifpositive_vars(df, oldvars)

    #ln
    oldvars=["echo_penalty_amount"]
    df, newvars = _ln_vars(df, oldvars)

    #lag
    oldvars=[
        "echo_enforcement_dummy",
        "echo_penalty_dummy",
        "echo_penalty_amount",
        "ln_echo_penalty_amount",
        ]
    df, newvars = _lag_vars(df, oldvars)

    #stagdid 
    unit_var=crspcomp_cusip
    time_var=crspcomp_fyear
    treatment_switch_dummies=[
        "echo_enforcement_dummy",
        "echo_penalty_dummy",
        ]
    df, stagdid_vars = _stagdid(df, unit_var, time_var, treatment_switch_dummies)

    #echo vars
    echo_vars=[
        #dummy
        "echo_enforcement_dummy",
        "echo_penalty_dummy",

        #amount
        "echo_penalty_amount",
        "ln_echo_penalty_amount",

        #lag
        "lag_echo_enforcement_dummy",
        "lag_echo_penalty_dummy",
        "lag_echo_penalty_amount",
        "lag_ln_echo_penalty_amount",

        #years
        "echo_initiation_year",
        "echo_penalty_year",
        "echo_initiation_lag",
        "case_number",
        ] + stagdid_vars

    #return
    return df, echo_vars


#divide vars
def _divide_vars(df, newvar, numerator, denominator):

    #to numeric
    tonumeric_cols=[
        numerator,
        denominator,
        ]
    df=_tonumericcols_to_df(df, tonumeric_cols)

    #condlist
    condlist=[
        df[numerator].isna(),
        df[denominator].isna(),
        df[denominator]==0,
        df[denominator]!=0,
        ]

    #choicelist
    choicelist=[
        None,
        None,
        None,
        df[numerator] / df[denominator],
        ]
    #y
    y=np.select(condlist, choicelist, default="error")

    #newvar
    df[newvar]=y

    #return
    return df


#crspcompustat newvars
def _crspcompustat_newvars(df):

    #to numeric
    tonumeric_cols=[
        crspcomp_assets,
        ]
    df=_tonumericcols_to_df(df, tonumeric_cols)

    #firm size
    df["firm_size"]=np.log1p(df[crspcomp_assets])

    #leverage_ratio
    df=_divide_vars(df, "leverage_ratio", crspcomp_liabilities, crspcomp_assets)

    #roa
    df=_divide_vars(df, "roa", crspcomp_netincome, crspcomp_assets)

    #roe
    df=_divide_vars(df, "roe", crspcomp_netincome, crspcomp_bookequity)

    #mtb
    df=_divide_vars(df, "mtb", crspcomp_mktequity, crspcomp_bookequity)

    #crspcompustat vars
    crspcompustat_vars=[
        "firm_size",
        "leverage_ratio",
        "roa",
        "roe",
        "mtb",
        crspcomp_state,
        crspcomp_incorp,
        ]

    #return
    return df, crspcompustat_vars


#industry famafrench
def _sic_to_famafrench(value, mapping):

    #default value, e.g., 49 "other"
    default_val=max(int(value) for value in mapping.values())

    #if
    if pd.isna(value):

        #new
        newval=None

    #elif
    elif pd.notna(value):

        #newval
        newval=default_val

        #if
        for i, (sic_range, ff_code) in enumerate(mapping.items()):

            #if
            if value in sic_range:

                #newval
                newval=ff_code

        newval=int(newval)

    #return
    return newval


#industry print stats
def _industry_print_stats(df, unit_var, drop_basedon, industry_col):
    
    #n_unique_industries
    n_unique_industries=df[industry_col].nunique()

    #n_unique_firms
    n_unique_firms=df[unit_var].nunique()

    #n_firm_wdummies
    filtered_df=df[df[drop_basedon]>0]
    n_firms_wdummies=filtered_df[unit_var].nunique()

    #fraction_firms_wdummies
    fraction_firms_wdummies=n_firms_wdummies/n_unique_firms

    #n_obs_wdummies
    n_obs_wdummies=df[drop_basedon].sum()

    #n_obs
    n_obs=len(df)

    #fraction_dummies
    fraction_dummies=n_obs_wdummies/n_obs


    #print
    print("n_unique_industries:", n_unique_industries)

    print("n_firms_wdummies:", n_firms_wdummies)
    print("n_unique_firms:", n_unique_firms)
    print("fraction_firms_wdummies:", fraction_firms_wdummies)

    print("n_obs_wdummies:", n_obs_wdummies)
    print("n_obs:", n_obs)
    print("fraction_dummies:", fraction_dummies)


#drop industry based on dummy
def _industry_drop(df, unit_var, drop_basedon, industry_col):

    #print
    _industry_print_stats(df, unit_var, drop_basedon, industry_col)

    #grouped
    grouped=df.groupby(industry_col)[drop_basedon].sum()

    #industries_to_keep
    industries_to_keep=grouped[grouped>0].index.tolist()

    #industries_to_drop
    unique_industries=list(df[industry_col].unique())
    industries_to_drop=[x for x in unique_industries if x not in industries_to_keep]
    print("industries_to_drop:", industries_to_drop)

    #isin
    df=df[df[industry_col].isin(industries_to_keep)]

    print(df)

    #print
    _industry_print_stats(df, unit_var, drop_basedon, industry_col)

    #return
    return df


#drop industry
def _industry_newvars(df, filepath):

    #mapping
    mapping=_filepath_to_mapping(filepath)

    #industry col
    n=int(''.join(char for char in filepath if char.isdigit()))
    industry_col=f"industry_famafrench{n}"

    #to numeric
    tonumeric_cols=[
        crspcomp_sic,
        ]
    df=_tonumericcols_to_df(df, tonumeric_cols)

    #gen var
    oldvalues=df[crspcomp_sic].values
    df[industry_col]=np.array([_sic_to_famafrench(x, mapping) for x in oldvalues])

    #drop industries
    unit_var=crspcomp_cusip
    drop_basedon="echo_enforcement_dummy"
    df=_industry_drop(df, unit_var, drop_basedon, industry_col)

    #newvars
    newvars=[
        crspcomp_sic,
        industry_col,
        ] 

    #return
    return df, newvars


#donations_echo_crspcompustat screen
folders=["zhao/_merge", "zhao/_merge"]
items=["crspcompustat_donations_echo", "crspcompustat_donations_echo_screen"]
def _crspcompustat_donations_echo_screen(folders, items):

    #folders
    resources=folders[0]
    results=folders[1]

    #items
    resource=items[0]
    result=items[1]

    contributor_id
    #usecols
    usecols=[
        #irs
        contributor_name,
        "dtsubjectname_left",
        "dtsubjectname_right",
        crspcomp_name,
        crspcomp_state,
        crspcomp_incorp,
        crspcomp_cusip,
        crspcomp_fyear,
        "amount_democratic",
        "amount_republican",

        #echo
        echo_initiation_year,
        echo_penalty_year,
        echo_initiation_lag,
        echo_penalty_amount,
        "case_number",

        #refinitiv
        crspcomp_assets,
        crspcomp_liabilities,
        crspcomp_bookequity,
        crspcomp_mktequity,
        crspcomp_sharesoutstanding,
        crspcomp_dividends,
        crspcomp_revenues,
        crspcomp_cogs,
        crspcomp_oibdp,
        crspcomp_da,
        crspcomp_netincome,
        crspcomp_sic,
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

    #fillna
    fillna_cols=[
        "amount_democratic",
        "amount_republican",
        echo_penalty_amount,
        ]
    df=_fillnacols_to_df(df, fillna_cols)

    #sortvalues
    sortvalues_cols=[
        crspcomp_cusip, 
        crspcomp_fyear, 
        ]
    df=df.sort_values(by=sortvalues_cols)

    #donations newvars
    df, donation_vars, post_year_dummies = _donations_newvars(df)

    #echo newvars
    df, echo_vars = _echo_newvars(df)

    #crspcompustat newvars
    df, crspcompustat_vars = _crspcompustat_newvars(df)

    #industry 
    filepath="_resources/Siccodes49.txt"
    df, industry_vars = _industry_newvars(df, filepath)

    #ordered
    ordered_cols=[
        contributor_name,
        "dtsubjectname_left",
        "dtsubjectname_right",
        crspcomp_name,
        crspcomp_cusip,
        crspcomp_fyear,
        ]  +\
        donation_vars + post_year_dummies +\
        echo_vars + crspcompustat_vars + industry_vars
    df=df[ordered_cols]

    print("saving")

    #save
    filepath=f"{results}/{result}.csv"
    df.to_csv(
        filepath, 
        index=False,
        #compression="gzip",
        )


#_crspcompustat_donations_echo_screen(folders, items)
print("done")


'''
policy unceratinty: reg returns on Baker and Bloom

FDA
https://open.fda.gov/
https://datadashboard.fda.gov/ora/index.htm

state level lobbying data


Who votes for AG? What is the electoral system? Need to check
Exploit “close” elections (won by a small margin)? Think about it
How many times can be re-elected? Need to check
Should be “regulatory risk” rather than cost? Just a definition, we could change
Julien: do we know that all the money to these originations go to ags? Do they also go to congressmen? Usually no, but check again
How important is election/re-election in your story? Think about it
Exploit “career concerns”, i.e. AGs who later became politicians/governors? Think about it

winning reelection or election to governor is definitely the incentive for AG. so helping firm can benefit from getting the money but may be perceived as bad politician/lawyer. challenging federal law could be perceived as beneficial to local firms and economy. so that's the tension.



https://www.fec.gov/legal-resources/court-cases/speechnoworg-v-fec/#:~:text=On%20March%2026%2C%202010%2C%20the,them%2C%20violate%20the%20First%20Amendment.
court found that such "nominally independent" organizations are "uniquely positioned to serve as conduits for corruption both in terms of the sale of access and the circumvention of the soft money ban."

https://www.brennancenter.org/issues/ensure-every-american-can-vote/voting-reform/state-voting-laws
https://ballotpedia.org/Main_Page
https://ballotpedia.org/Voting_laws_in_the_United_States#State_election_laws
https://heinonline.org/HOL/Welcome
state attorneys general
state statutes historical archives
national survey of state laws
voter laws

osha state plan staggered adoption
cross sectional: firms in red state more like to donate

#literature
yu2011
study OSHA, state plans
AGs pacs

#data
governors and rslc assn
aggregate at 6th cusip, and merge
private firms too, total assets, industry of the firm, revenues, income, debt
AGs pacs

#analysis
after legal/natural shock (alekseev2022), enforc ratio (choy2023)  -> higher donation
aga vs AGs pacs


#Martens
will violation info be on 8-k?

#Zhao
epa firms, how many public firms receive enforcement actions, look at literature
facility level analylsis, Violation Tracker or linktransformer

#Pope
Countercyclical lobbying? lobby more rep when switch
Big 4 and ESG disclosure

#Ciani
opiods sold by local pharmacies - walmart, some place expected a fine, but did not receive it, gravity of the violation
#'''
