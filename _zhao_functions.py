

#imports
import re
import io
import numpy as np
import pandas as pd
from pathlib import Path
from datetime import datetime


#functions
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


sic_to_ff_mapping={
    range(100, 1000): 'NoDur',
    range(2000, 2400): 'NoDur',
    range(2700, 2750): 'NoDur',
    range(2770, 2800): 'NoDur',
    range(3100, 3200): 'NoDur',
    range(3940, 3990): 'NoDur',
    
    range(2500, 2520): 'Durbl',
    range(2590, 2600): 'Durbl',
    range(3630, 3660): 'Durbl',
    range(3710, 3712): 'Durbl',
    range(3714, 3715): 'Durbl',
    range(3716, 3717): 'Durbl',
    range(3750, 3752): 'Durbl',
    range(3792, 3793): 'Durbl',
    range(3900, 3940): 'Durbl',
    range(3990, 4000): 'Durbl',
    
    range(2520, 2590): 'Manuf',
    range(2600, 2700): 'Manuf',
    range(2750, 2770): 'Manuf',
    range(3000, 3100): 'Manuf',
    range(3200, 3570): 'Manuf',
    range(3580, 3630): 'Manuf',
    range(3700, 3710): 'Manuf',
    range(3712, 3714): 'Manuf',
    range(3715, 3716): 'Manuf',
    range(3717, 3750): 'Manuf',
    range(3752, 3792): 'Manuf',
    range(3793, 3800): 'Manuf',
    range(3830, 3840): 'Manuf',
    range(3860, 3900): 'Manuf',
    
    range(1200, 1400): 'Enrgy',
    range(2900, 3000): 'Enrgy',
    
    range(2800, 2830): 'Chems',
    range(2840, 2900): 'Chems',
    
    range(3570, 3580): 'BusEq',
    range(3660, 3693): 'BusEq',
    range(3694, 3700): 'BusEq',
    range(3810, 3830): 'BusEq',
    range(7370, 7380): 'BusEq',
    
    range(4800, 4900): 'Telcm',
    
    range(4900, 4950): 'Utils',
    
    range(5000, 6000): 'Shops',
    range(7200, 7300): 'Shops',
    range(7600, 7700): 'Shops',
    
    range(2830, 2840): 'Hlth',
    range(3693, 3694): 'Hlth',
    range(3840, 3860): 'Hlth',
    range(8000, 8100): 'Hlth',
    
    range(6000, 7000): 'Money',
    }


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
    errors="raise"
    df=_tonumericcols_to_df(df, tonumeric_cols, errors)

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
    errors="raise"
    df=_tonumericcols_to_df(df, tonumeric_cols, errors)

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
    errors="raise"
    df=_tonumericcols_to_df(df, tonumeric_cols, errors)
        
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
    errors="raise"
    df=_tonumericcols_to_df(df, tonumeric_cols, errors)

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
    errors="raise"
    df=_tonumericcols_to_df(df, tonumeric_cols, errors)

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
    errors="raise"
    df=_tonumericcols_to_df(df, tonumeric_cols, errors)

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
    errors="raise"
    df=_tonumericcols_to_df(df, tonumeric_cols, errors)

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
    errors="raise"
    df=_tonumericcols_to_df(df, tonumeric_cols, errors)

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
    errors="raise"
    df=_tonumericcols_to_df(df, tonumeric_cols, errors)

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
    pivot_df=pd.pivot_table(
        data=df,
        values=contribution_amount_ytd,
        index=index,
        columns=organization_id,  
        aggfunc=np.sum,
        fill_value=0,
        )      
    #reset index
    pivot_df=pivot_df.reset_index()   
    #rename
    list_pivot_columns=list(pivot_columns.values())
    pivot_df=pivot_df.rename(columns=pivot_columns)

    #df without dups
    df_withoutdups=df.drop_duplicates(subset=contributor_id)
    df_withoutdups=df_withoutdups.drop([contribution_year], axis=1)

    #args
    suffixes=('_left', '_right')
    indicator=f"_merge_dups"
    #merge
    df=pd.merge(
        left=pivot_df,
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
    errors="raise"
    df=_tonumericcols_to_df(df, tonumeric_cols, errors)

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
    errors="raise"
    df=_tonumericcols_to_df(df, tonumeric_cols, errors)

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
    errors="raise"
    df=_tonumericcols_to_df(df, tonumeric_cols, errors)

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
    errors="raise"
    _tonumericcols_to_df(df, tonumeric_cols, errors)

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
    errors="raise"
    _tonumericcols_to_df(df, tonumeric_cols, errors)

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
    errors="raise"
    _tonumericcols_to_df(df, tonumeric_cols, errors)

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
    errors="raise"
    _tonumericcols_to_df(df, tonumeric_cols, errors)

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


#gen aggregate pastn
def _aggregate_pastn(df, col_identifier, oldvar, agg_funct, n_shifts):

    #to numeric
    tonumeric_cols=[
        oldvar,
        ]
    errors="raise"
    _tonumericcols_to_df(df, tonumeric_cols, errors)

    #init cols
    var_shifted_cols=[None]*n_shifts

    #for
    for i in range(n_shifts):

        #periods
        periods=i+1

        #col name
        var_shifted=f"{oldvar}_shift{periods}"

        #gen new col
        df[var_shifted]=df.groupby(col_identifier)[oldvar].shift(periods=periods, fill_value=0)

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


#post interaction
def _post_vars(df, start_year, stop_year, level_vars):

    #to numeric
    tonumeric_cols=[
        crspcomp_fyear,
        ] + level_vars
    errors="raise"
    _tonumericcols_to_df(df, tonumeric_cols, errors)

    #years
    years=list(range(start_year, (stop_year+1)))

    #init vars
    post_year_dummies=[None]*len(years)
    interact_vars=[None]*(len(level_vars)*len(years))

    #newdict
    newdict=dict()

    #init i
    i=0

    #for
    for j, year in enumerate(years):

        #var name
        post_year_dummy=f"post{year}"

        #newdict
        newdict[post_year_dummy]=np.where(df[crspcomp_fyear] >= year, 1, 0)

        #update var
        post_year_dummies[j]=post_year_dummy

        #interactions
        for k, col in enumerate(level_vars):  

            #var name
            interact_var=f"{post_year_dummy}x{col}"

            #newdict
            newdict[interact_var]=newdict[post_year_dummy]*df[col]

            #update var
            interact_vars[i]=interact_var

            #update i
            i+=1

    #new df
    new_df=pd.DataFrame(newdict)

    #concat
    df=pd.concat([df, new_df], axis=1)

    #return
    return df, post_year_dummies, interact_vars


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

    #agreggate past
    col_identifier=crspcomp_cusip
    n_shifts=3
    tuples=[
        #amount
        ("amount_democratic", np.sum, n_shifts),
        ("amount_republican", np.sum, n_shifts),
        ("amount_both", np.sum, n_shifts),
        #dummy
        ("dummy_democratic", np.prod, n_shifts),
        ("dummy_republican", np.prod, n_shifts),
        ("dummy_both", np.prod, n_shifts),
        ]
    for i, (oldvar, agg_funct, n_shifts) in enumerate(tuples):
        df=_aggregate_pastn(df, col_identifier, oldvar, agg_funct, n_shifts)
    
    #ln
    oldvars=[
        #amount
        "amount_democratic",
        "amount_republican",
        "amount_both",

        #amount past
        "amount_democratic_past3",
        "amount_republican_past3",
        "amount_both_past3",
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

        #amount past
        "amount_democratic_past3",
        "amount_republican_past3",
        "amount_both_past3",

        #dummy
        "dummy_democratic",
        "dummy_republican", 
        "dummy_both",

        #dummy past
        "dummy_democratic_past3",
        "dummy_republican_past3",
        "dummy_both_past3",
        ] + ln_vars + lag_vars + change_vars

    #post year + interation
    start_year=2015
    stop_year=2019
    level_vars=donation_vars
    df, post_year_dummies, donation_interact_vars = _post_vars(df, start_year, stop_year, level_vars)

    #return
    return df, donation_vars, post_year_dummies, donation_interact_vars


#echo newvars
def _echo_newvars(df):

    df=df.rename(columns={
        echo_penalty_amount: "echo_penalty_amount",
        echo_penalty_year: "echo_penalty_year",
        })

    #dummy vars
    oldvars=[
        "echo_penalty_amount",
        ]
    df, newvars = _dummyifpositive_vars(df, oldvars)

    #ln
    oldvars=["echo_penalty_amount"]
    df, newvars = _ln_vars(df, oldvars)

    #dummy enforcement action
    df["echo_enforcement_dummy"]=np.where(df["echo_penalty_year"].notna(), 1, 0)

    #staggered treated XXX


    #echo vars
    echo_vars=[
        #amount
        "echo_enforcement_dummy",
        "echo_penalty_dummy",
        "echo_penalty_amount",

        #staggered
        #"echo_enforcement_treated",

        #years
        "echo_initiation_year",
        "echo_penalty_year",
        "echo_initiation_lag",
        "case_number",
        ] + newvars

    #return
    return df, echo_vars


def _divide_vars(df, newvar, numerator, denominator):

    #to numeric
    tonumeric_cols=[
        numerator,
        denominator,
        ]
    errors="raise"
    _tonumericcols_to_df(df, tonumeric_cols, errors)

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
    errors="raise"
    _tonumericcols_to_df(df, tonumeric_cols, errors)

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
def _industry_famafrench(value):

    #https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/Data_Library/det_12_ind_port.html

    #if
    if pd.isna(value):

        #new
        newval=None

    #elif
    elif pd.notna(value):

        #newval
        newval="Other"

        #if
        for i, (sic_range, ff_name) in enumerate(sic_to_ff_mapping.items()):

            #if
            if value in sic_range:

                #newval
                newval=ff_name

        #lowercase
        newval=newval.lower()

    #return
    return newval


#industry drop
def _industry_drop(df, col, industry_col):

    #industry_counts
    industry_counts=df.groupby(industry_col)[col].value_counts().unstack(fill_value=0)

    #industries_to_drop
    industries_to_drop=industry_counts.index[industry_counts[1] == 0]

    #keep if not in
    df=df[~df[industry_col].isin(industries_to_drop)]

    #return
    return df


#gen dummies
def _gen_dummies(df, col, prefix):

    #get dummies
    dummies=pd.get_dummies(
        df[col],
        prefix=prefix,
        prefix_sep="_",
        drop_first=True,
        )

    #colnames
    dummies_cols=list(dummies.columns)

    #concat
    df=pd.concat([df, dummies], axis=1)

    #return
    return df, dummies_cols


#drop industry
def _industry_newvars(df):

    #industry col
    industry_col="industry_famafrench"

    #to numeric
    tonumeric_cols=[
        crspcomp_sic,
        ]
    errors="raise"
    _tonumericcols_to_df(df, tonumeric_cols, errors)

    #gen
    oldvalues=df[crspcomp_sic].values
    df[industry_col]=np.array([_industry_famafrench(x) for x in oldvalues])

    #irs drop
    col="dummy_both"
    df=_industry_drop(df, col, industry_col)

    #echo drop
    col="echo_enforcement_dummy"
    df=_industry_drop(df, col, industry_col)

    #industry dummies
    col=industry_col
    prefix="industry_ff_dummy"
    df, dummies_cols = _gen_dummies(df, col, prefix)

    #newvars
    newvars=[
        crspcomp_sic,
        industry_col,
        ] + dummies_cols

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
    df, donation_vars, post_year_dummies, donation_interact_vars = _donations_newvars(df)

    #echo newvars
    df, echo_vars = _echo_newvars(df)

    #crspcompustat newvars
    df, crspcompustat_vars = _crspcompustat_newvars(df)

    #industry
    df, industry_vars = _industry_newvars(df)

    #year dummies
    col=crspcomp_fyear
    prefix="year_dummy"
    df, year_dummies = _gen_dummies(df, col, prefix)

    #firm dummies
    col=crspcomp_cusip
    prefix="firm_dummy"
    #df, firm_dummies = _gen_dummies(df, col, prefix)

    #state dummies
    col=crspcomp_state
    prefix="state_dummy"
    #df, state_dummies = _gen_dummies(df, col, prefix)

    #incorp dummies
    col=crspcomp_incorp
    prefix="incorp_dummy"
    #df, incorp_dummies = _gen_dummies(df, col, prefix)

    #ordered
    ordered_cols=[
        contributor_name,
        "dtsubjectname_left",
        "dtsubjectname_right",
        crspcomp_name,
        crspcomp_cusip,
        crspcomp_fyear,
        ]  +\
        donation_vars + post_year_dummies + donation_interact_vars +\
        echo_vars + crspcompustat_vars + industry_vars +\
        year_dummies #+ firm_dummies + state_dummies + incorp_dummies
    df=df[ordered_cols]

    print("saving")

    #save
    filepath=f"{results}/{result}.csv"
    df.to_csv(
        filepath, 
        index=False,
        #compression="gzip",
        )


_crspcompustat_donations_echo_screen(folders, items)
print("done")


#staggered did
#y=change donations, right=treated

#https://github.com/EddieYang211/ebal-py
#entropy balancing: based on covariates

