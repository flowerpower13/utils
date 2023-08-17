

#imports
import re
import io
import numpy as np
import pandas as pd
from datetime import datetime


#functions
from _pd_utils import _groupby


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
donor_isfirm="donor_isfirm"
company_involved="a__company_involved"
contributor_list_firstvalue=[
    contributor_name_irs,
    contributor_name_rdp,
    #contributor_isin,
    contributor_cusip,
    contributor_ric,
    contributor_oapermid,
    contributor_address_state,
    contributor_address_zipcode,
    contributor_employer,
    contributor_employer_new,
    donor_isfirm,
    company_involved,
    ]
select_list=[
    #name
    "DTSubjectName",
    "CommonName",

    #identifier
    "CUSIP",
    #"IssueISIN",
    "RIC",
    "PrimaryRIC",
    "IssuerOAPermID",
    "PermID",
    "OAPermID",
    "Orgid",
    "TickerSymbol",

    #asset
    "AssetState", #active ('DC' if true)
    "BusinessEntity", #organization type
    "RCSOrganisationSubTypeLeaf",
    "OrganisationStatus", #listed
    "RCSAssetCategoryLeaf", #asset type (e.g., 'Ordinary Shares')

    #exchange
    "ExchangeName",
    "ExchangeCode",
    ]
contributor_list_firstvalue = contributor_list_firstvalue + [x.lower() for x in select_list] 
#contribution
contribution_date="a__contribution_date"
contribution_year="a__contribution_year"
contribution_amount_ytd="a__agg_contribution_ytd"


#var violations
viol_parent_isin="current_parent_ISIN"
viol_penalty_year="pen_year"
viol_penalty_amount="penalty"
viol_subpenalty_amount="sub_penalty"
viol_penalty_adjusted_amount="penalty_adjusted"
viol_initiation_year_true="initiation_year_true"
viol_initiation_lag="initiation_lag"
initiation_year_inferred_mean="initiation_year_inferred_mean"
initiation_year_inferred_median="initiation_year_inferred_median"


#vars echo and tri
echo_parent_isin="issueisin"
echo_penalty_date="XXX"
echo_penalty_year="echo_penalty_year"
echo_penalty_amount="total_penalty_assessed_amt"
echo_case_id="case_number"
echo_initiation_year="echo_initiation_year"
echo_initiation_lag="echo_initiation_lag"


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


#from IRS txt to dfs
folders=["zhao/data/irs", "zhao/_irs"]
items=["FullDataFile"]
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


#contributors screen by ein, keep companies name
folders=["zhao/_irs", "zhao/_irs"]
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
        #nrows=100000,
        lineterminator=LINETERMINATOR,
        quotechar=QUOTECHAR,
        #on_bad_lines='skip',
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


#facilities screen
folders=["zhao/data/epa", "zhao/_epa"]
items=["CASE_FACILITIES", "CASE_FACILITIES_screen"]
def _facilities_screen(folders, items):

    #https://echo.epa.gov/files/echodownloads/case_downloads.zip
    #https://echo.epa.gov/tools/data-downloads/icis-fec-download-summary

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

    #dropna
    dropna_cols=[
        "ACTIVITY_ID", 
        "CASE_NUMBER", 
        "REGISTRY_ID", 
        ]
    df=df.dropna(subset=dropna_cols)

    #drop duplicates
    dropdups_col=[
        "ACTIVITY_ID", 
        "CASE_NUMBER", 
        ]
    #df=df.drop_duplicates(subset=dropdups_col)

    #sort values
    sortvalues_cols=[
        "ACTIVITY_ID", 
        "CASE_NUMBER", 
        "REGISTRY_ID", 
        ]
    df=df.sort_values(by=sortvalues_cols)

    #save
    filepath=f"{results}/{result}.csv"
    df.to_csv(filepath, index=False)


#enforcements screen
folders=["zhao/data/epa", "zhao/_epa"]
items=["CASE_ENFORCEMENTS", "CASE_ENFORCEMENTS_screen"]
def _enforcements_screen(folders, items):

    #https://echo.epa.gov/files/echodownloads/case_downloads.zip
    #https://echo.epa.gov/tools/data-downloads/icis-fec-download-summary

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

    #fillna
    s=pd.to_numeric(df["TOTAL_PENALTY_ASSESSED_AMT"], errors="raise")
    s=s.fillna(0)
    df["TOTAL_PENALTY_ASSESSED_AMT"]=s

    #dropna
    dropna_cols=[
        "ACTIVITY_ID", 
        "CASE_NUMBER", 
        "TOTAL_PENALTY_ASSESSED_AMT", 
        ]
    df=df.dropna(subset=dropna_cols)
    
    sortvalues_cols=[
        "ACTIVITY_ID", 
        "CASE_NUMBER", 
        "TOTAL_PENALTY_ASSESSED_AMT", 
        ]
    df=df.sort_values(by=sortvalues_cols)

    #save
    filepath=f"{results}/{result}.csv"
    df.to_csv(filepath, index=False)


#tri screen
folders=["zhao/data/epa", "zhao/_epa"]
items=["tri", "tri_screen"]
def _tri_screen(folders, items):
 
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
        "PARENT_CO_NAME",
        ]
    df=df.dropna(subset=dropna_cols)

    #drop duplicates
    dropdups_col=[
        "EPA_REGISTRY_ID",
        ]
    df=df.drop_duplicates(subset=dropdups_col)

    #sort values
    sortvalues_cols=[
        "EPA_REGISTRY_ID",
        "FACILITY_NAME",
        "PARENT_CO_NAME", 
        ]
    df=df.sort_values(by=sortvalues_cols)

    #order
    df=df[usecols]

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


#aggregate donations
folders=["zhao/_irs", "zhao/_irs"]
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
    df=df.dropna(subset=dropna_cols)

    #to numeric
    df[contribution_amount_ytd]=pd.to_numeric(df[contribution_amount_ytd])

    #first value dict
    dict_firstvalue={x: [_firstvalue_join] for x in contributor_list_firstvalue}
    #aggregate over organization-contributor-year obs
    by=[
        organization_id,
        contributor_isin,
        contribution_year,
        ]
    dict_agg_colfunctions={
        contribution_amount_ytd: [sum],
        }
    dict_agg_colfunctions=dict_firstvalue|dict_agg_colfunctions
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
    list_pivot_columns=list(pivot_columns.values())
    pivot_df=pivot_df.rename(columns=pivot_columns)

    #full panel
    col_id=contributor_isin
    col_year=contribution_year
    start_year=2000
    stop_year=2023
    df_fullpanel=_df_to_fullpanel(pivot_df, col_id, col_year, start_year, stop_year)

    #df without dups
    df_withoutdups=df.drop_duplicates(subset=contributor_isin)
    df_withoutdups=df_withoutdups.drop([contribution_year], axis=1)

    #args
    indicator=f"_merge_dups"
    suffixes=('_left', '_right')
    #merge
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
    ordered_cols=[col_id, col_year] + list_pivot_columns + contributor_list_firstvalue
    df=df[ordered_cols]

    #save
    filepath=f"{results}/{result}.csv"
    df.to_csv(filepath, index=False)
    #'''


#choice function echo
def _choice_funct_echo(series):

    #len year
    len_year=4

    #series
    series=series.str[3:(3+len_year)]

    #return
    return series


#choice function pacer
def _choice_funct_pacer(series):

    #series
    series=series.str[0:(0+4)]

    #return
    return series


#case id to year
def _caseid_to_year(df, cond_var, choice_var, _choice_funct, select_var):

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


#initiation year
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


#initiation year infer
def _initiation_year_infer(row, col0, col1, var0, var1):

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


#aggregate violations
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
        #nrows=100000,
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
        df[col]=pd.to_numeric(df[col], errors="raise")

    #choice var
    choice_var="case_id"

    #initiation year echo
    cond_var="echo_case_url"
    select_var="initiation_year_echo"
    _choice_funct=_choice_funct_echo
    df=_caseid_to_year(df, cond_var, choice_var, _choice_funct, select_var)

    #initiation year pacer
    cond_var="pacer_link"
    select_var="initiation_year_pacer"
    _choice_funct=_choice_funct_pacer
    df=_caseid_to_year(df, cond_var, choice_var, _choice_funct, select_var)

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
        _initiation_year_infer,
        axis=1,
        result_type="expand",
        args=(col0, col1, var0, var1),
        )

    #drop na
    dropna_cols=[
        viol_parent_isin,
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
        viol_parent_isin,
        initiation_year_inferred_mean,
        ]
    dict_agg_colfunctions={
        viol_penalty_amount: [sum],
        viol_subpenalty_amount: [sum],
        viol_penalty_adjusted_amount: [sum],
        }
    dict_agg_colfunctions=dict_firstvalue|dict_agg_colfunctions
    df=_groupby(df, by, dict_agg_colfunctions)

    #save
    filepath=f"{results}/{result}.csv"
    df.to_csv(filepath, index=False)


#echo initiation year
def _echo_initiation_year(value):

    #year
    year=value[3:(3+4)]
    
    #if
    if year.isdigit():
        
        #year
        year=int(year)

        #if
        if year>CURRENT_YEAR:
            
            #year
            year=None  

    #elif
    elif not year.isdigit():

        #yeare
        year=None

    #return 
    return year


#aggregate echo
folders=["zhao/_epa", "zhao/_epa"]
items=["echo_ids", "echo_ids_aggregate"]
def _echo_aggregate(folders, items):

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
        #nrows=100000,
        )
    
    #lowercase col values
    for i, col in enumerate(df.columns):
        df[col]=df[col].str.lower()

    #to numeric
    tonumeric_cols=[
        echo_penalty_amount,
        ]
    for i, col in enumerate(tonumeric_cols):
        df[col]=pd.to_numeric(df[col], errors="raise")

    #to datetime
    df[echo_penalty_date]=pd.to_datetime(df[echo_penalty_date], format="%m/%d/%Y")
    df[echo_penalty_year]=pd.DatetimeIndex(df[echo_penalty_date]).year


    #initiation year
    df[echo_initiation_year]=df[echo_case_id].apply(_echo_initiation_year)

    #initiation lag
    col0=echo_penalty_year
    col1=echo_initiation_year
    df[echo_initiation_lag]=df.apply(_initiation_lag, axis=1, args=(col0, col1))

    #drop na
    dropna_cols=[
        echo_parent_isin,
        echo_initiation_year,
        echo_penalty_amount,
        ]
    df=df.dropna(subset=dropna_cols)

    #first value dict
    list_firstvalue=[x for x in df.columns if x not in dropna_cols]
    dict_firstvalue={x: [_firstvalue_join] for x in list_firstvalue}

    #aggregate over company-init year obs
    by=[
        echo_parent_isin,
        echo_initiation_year,
        ]
    dict_agg_colfunctions={
        echo_penalty_amount: [sum],
        }
    dict_agg_colfunctions=dict_firstvalue|dict_agg_colfunctions
    df=_groupby(df, by, dict_agg_colfunctions)

    #save
    filepath=f"{results}/{result}.csv"
    df.to_csv(filepath, index=False)


#drop dups from crspcompustat
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

