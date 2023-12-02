#_echo_facilities_screen
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


#_initiation_lag
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


#_echo_enforcements_screen
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


#_echo_milestones_screen
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
    df["echo_initiation_year"]=pd.DatetimeIndex(df["actual_date"], ambiguous="NaT").year

    #ordered
    ordered_cols=[
        "case_number", 
        "activity_id",
        "actual_date",
        "echo_initiation_year",
        ]
    df=df[ordered_cols]

    #save
    filepath=f"{results}/{result}.csv"
    df.to_csv(filepath, index=False)


#_echo_tri_screen
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
        "parent_co_db_num",
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
        "pen_year",
        "penalty",
        "case_id",
        "pacer_link",

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
        "pen_year",
        "penalty",
        ]
    df=_tonumericcols_to_df(df, tonumeric_cols)

    #drop na
    dropna_cols=[
        "current_parent_name",
        "pen_year",
        "penalty",
        "case_id",
        "pacer_link",
        ]
    df=df.dropna(subset=dropna_cols)

    #initiation year
    df["violtrack_initiation_year"]=df["case_id"].apply(_violtrack_initiation_year)

    #drop na
    dropna_cols=[
        "violtrack_initiation_year",
        ]
    df=df.dropna(subset=dropna_cols)
 
    #initiation lag
    col0="pen_year"
    col1="violtrack_initiation_year"
    df["violtrack_initiation_lag"]=df.apply(_initiation_lag, axis=1, args=(col0, col1))

    ordered_cols=[
        "current_parent_name",
        "current_parent_isin",
        "violtrack_initiation_year",
        "pen_year",
        "violtrack_initiation_lag",
        "penalty",
        "case_id",
        "pacer_link",

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
            "nr_exposed", #nr employees exposed
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


#_osha_inspection_screen
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
    df["open_date"]=pd.DatetimeIndex(df["open_date"], ambiguous="NaT").year

    #penalty year
    df["close_case_date"]=pd.DatetimeIndex(df["close_case_date"], ambiguous="NaT").year

    #initiation lag
    col0="close_case_date"
    col1="open_date"
    df["osha_initiation_lag"]=df.apply(_initiation_lag, axis=1, args=(col0, col1))

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
        "osha_initiation_lag",
        "nr_in_estab",
        ]
    df=df[ordered_cols]

    #save
    filepath=f"{results}/{result}.csv"
    df.to_csv(filepath, index=False)


#_crspcompustat_screen
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
        "cusip",
        "fyear",
        "cik",
        "ein",
        "conm",
        "state",
        "incorp",
        "at",
        "lt",
        "seq",
        "mkvalt",
        "csho",
        "dvt",
        "revt",
        "cogs",
        "oibdp",
        "dp",
        "ni",
        "sic",
        "naics",
        "gind",
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
        "cusip", 
        "fyear", 
        ]
    df=df.dropna(subset=dropna_cols)

    #drop duplicates
    dropdups_cols=[
        "cusip", 
        "fyear", 
        ]
    df=df.drop_duplicates(subset=dropdups_cols)

    #ordered
    ordered_cols=usecols
    df=df[ordered_cols]

    #save
    filepath=f"{results}/{result}.csv"
    df.to_csv(filepath, index=False)


#_irs_A_aggregate
folders=["zhao/_irs", "zhao/_irs"]
items=["donations_ids", "donations_ids_aggregate"]
def _irs_A_aggregate(folders, items):

    #folders
    resources=folders[0]
    results=folders[1]

    #items
    resource=items[0]
    result=items[1]

    #usecols
    usecols=[
        "a__ein",
        "cusip",
        "a__contributor_name",
        "a__contribution_year",
        "a__agg_contribution_ytd",
        "a__contributor_employer",
        "a__contributor_employer_new",
        "a__contributor_isfirm",
        "a__contributor_company_involved",

        #refinitiv
        "dtsubjectname",
        "businessentity",
        "issueisin",
        #"cusip",
        "ric",
        "oapermid",
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
        "a__ein",
        "cusip",
        "a__contribution_year",
        "a__agg_contribution_ytd",
        ]
    df=df.dropna(subset=dropna_cols)

    #to numeric
    tonumeric_cols=[
        "a__contribution_year",
        "a__agg_contribution_ytd",
        ]
    df=_tonumericcols_to_df(df, tonumeric_cols)

    #aggregate over organization-contributor-year obs
    by=[
        "a__ein",
        "cusip",
        "a__contribution_year",
        ]
    dict_agg_colfunctions={
        "a__agg_contribution_ytd": [np.sum],
        "a__contributor_name": [_firstvalue_join],
        "a__contributor_employer": [_firstvalue_join],
        "a__contributor_employer_new": [_firstvalue_join],
        "a__contributor_isfirm": [_firstvalue_join],
        "a__contributor_company_involved": [_firstvalue_join],
        "dtsubjectname": [_firstvalue_join],
        "businessentity": [_firstvalue_join],
        "issueisin": [_firstvalue_join],
        #"cusip": [_firstvalue_join],
        "ric": [_firstvalue_join],
        "oapermid": [_firstvalue_join],
        }
    df=_groupby(df, by, dict_agg_colfunctions)

   #pivot
    index=[
        "cusip",
        "a__contribution_year",
        ]
    df_pivot=pd.pivot_table(
        data=df,
        values="a__agg_contribution_ytd",
        index=index,
        columns="a__ein",  
        aggfunc=np.sum,
        fill_value=0,
        )      
    #reset index
    df_pivot=df_pivot.reset_index()   
    #rename
    list_pivot_columns=list(pivot_columns.values())
    df_pivot=df_pivot.rename(columns=pivot_columns)

    #df without dups
    df_withoutdups=df.drop_duplicates(subset="cusip")
    df_withoutdups=df_withoutdups.drop(["a__contribution_year"], axis=1)

    #args
    suffixes=('_left', '_right')
    indicator=f"_merge_dups"
    #merge
    df=pd.merge(
        left=df_pivot,
        right=df_withoutdups,
        how="left",
        on="cusip",
        suffixes=suffixes,
        indicator=indicator,
        validate="m:1",
        )
    
    #full panel
    cols_id=[
        "cusip",
        "a__contribution_year",
        ]
    years=[
        2000,
        2023,
        ]
    fillna_cols=list_pivot_columns
    df=_df_to_fullpanel(df, cols_id, years, fillna_cols)

    #ordered
    ordered_cols= [
        "a__contributor_name",
        "cusip",
        "a__contribution_year"
        ] + list_pivot_columns + [
        "a__contributor_employer",
        "a__contributor_employer_new",
        "a__contributor_isfirm",
        "a__contributor_company_involved",
        "dtsubjectname",
        "businessentity",
        "issueisin",
        #"cusip",
        "ric",
        "oapermid", 
        ]
    df=df[ordered_cols]

    #save
    filepath=f"{results}/{result}.csv"
    df.to_csv(filepath, index=False)


#echo aggregate_echo_aggregate
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
        "cusip",
        "echo_initiation_year",
        "settlement_fy",
        "fed_penalty_assessed_amt",
        "activity_type_code",
        "parent_co_name",

        #refinitiv
        "dtsubjectname",
        "businessentity",
        "issueisin",
        #"cusip",
        "ric",
        "oapermid",
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
        "cusip",
        "echo_initiation_year",
        "settlement_fy",
        "fed_penalty_assessed_amt",
        ]
    df=df.dropna(subset=dropna_cols)

    #to numeric
    tonumeric_cols=[
        "echo_initiation_year",
        "settlement_fy",
        "fed_penalty_assessed_amt",
        ]
    df=_tonumericcols_to_df(df, tonumeric_cols)

    #initiation lag
    col0="settlement_fy"
    col1="echo_initiation_year"
    df["echo_initiation_lag"]=df.apply(_initiation_lag, axis=1, args=(col0, col1))

    #aggregate over case-company-init year obs
    by=[
        "case_number",
        "activity_id",
        "cusip",
        "echo_initiation_year",
        ]
    dict_agg_colfunctions={
        "fed_penalty_assessed_amt": [_firstvalue_join],
        #"case_number": [_firstvalue_join],
        "settlement_fy": [_firstvalue_join],
        "echo_initiation_lag": [_firstvalue_join],
        "activity_type_code": [_firstvalue_join],
        "parent_co_name": [_firstvalue_join],

        #refinitiv
        "dtsubjectname": [_firstvalue_join],
        "businessentity": [_firstvalue_join],
        "issueisin": [_firstvalue_join],
        #"cusip": [_firstvalue_join],
        "ric": [_firstvalue_join],
        "oapermid": [_firstvalue_join],
        }
    df=_groupby(df, by, dict_agg_colfunctions)

    #aggregate over company-init year obs
    by=[
        "cusip",
        "echo_initiation_year",
        ]
    dict_agg_colfunctions={
        "fed_penalty_assessed_amt": [np.sum],
        "case_number": [_firstvalue_join],
        "settlement_fy": [_firstvalue_join],
        "echo_initiation_lag": [_firstvalue_join],
        "activity_type_code": [_firstvalue_join],
        "parent_co_name": [_firstvalue_join],

        #refinitiv
        "dtsubjectname": [_firstvalue_join],
        "businessentity": [_firstvalue_join],
        "issueisin": [_firstvalue_join],
        #"cusip": [_firstvalue_join],
        "ric": [_firstvalue_join],
        "oapermid": [_firstvalue_join],
        }
    df=_groupby(df, by, dict_agg_colfunctions)

    #sortvalues
    sortvalues_cols=[
        "case_number",
        "cusip",
        "echo_initiation_year",
        ]
    df=df.sort_values(by=sortvalues_cols)

    #full panel
    cols_id=[
        "cusip",
        "echo_initiation_year",
        ]
    years=[
        2000,
        2023,
        ]
    fillna_cols=[
        "fed_penalty_assessed_amt",
        ]
    df=_df_to_fullpanel(df, cols_id, years, fillna_cols)

    #ordered
    ordered_cols=[
        "case_number",
        "cusip",
        "echo_initiation_year",
        "settlement_fy",
        "echo_initiation_lag",
        "fed_penalty_assessed_amt",
        "activity_type_code",
        "parent_co_name",

        #refinitiv
        "dtsubjectname",
        "businessentity",
        "issueisin",
        #"cusip",
        "ric",
        "oapermid",
        ]
    df=df[ordered_cols]

    #save
    filepath=f"{results}/{result}.csv"
    df.to_csv(filepath, index=False)


#_violtrack_initiation_year
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


#_violtrack_keepisin
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


#_violtrack_aggregate
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
        "violtrack_initiation_year",
        "pen_year",
        "violtrack_initiation_lag",
        "penalty",
        "case_id",
        "pacer_link",

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
        "dtsubjectname",
        "businessentity",
        "issueisin",
        "cusip",
        "ric",
        "oapermid",
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
        "violtrack_initiation_year",
        "pen_year",
        "violtrack_initiation_lag",
        "penalty",
        ]
    df=_tonumericcols_to_df(df, tonumeric_cols)

    #keep internal isin, if not present keep external
    col0="current_parent_isin"
    col1="issueisin"
    df["violtrack_parent_id"]=df.apply(_violtrack_keepisin, axis=1, args=(col0, col1))

    #drop na
    dropna_cols=[
        "violtrack_parent_id",
        "violtrack_initiation_year",
        "pen_year",
        "violtrack_initiation_lag",
        "penalty",
        ]
    df=df.dropna(subset=dropna_cols)

    #aggregate over company-year obs
    by=[
        "violtrack_parent_id",
        "violtrack_initiation_year",
        ]
    dict_agg_colfunctions={
        "penalty": [np.sum],
        "pen_year": [_firstvalue_join],
        "violtrack_initiation_lag": [_firstvalue_join],
        "case_id": [_firstvalue_join],
        "pacer_link": [_firstvalue_join],

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
        "dtsubjectname": [_firstvalue_join],
        "businessentity": [_firstvalue_join],
        "issueisin": [_firstvalue_join],
        "cusip": [_firstvalue_join],
        "ric": [_firstvalue_join],
        "oapermid": [_firstvalue_join],
        }
    df=_groupby(df, by, dict_agg_colfunctions)

    cols_id=[
        "violtrack_parent_id",
        "violtrack_initiation_year",
        ]
    years=[
        2000,
        2023,
        ]
    fillna_cols=[
        "penalty",
        ]
    df=_df_to_fullpanel(df, cols_id, years, fillna_cols)

    ordered_cols=[
        "violtrack_parent_id",
        "violtrack_initiation_year",
        "pen_year",
        "violtrack_initiation_lag",
        "penalty",
        "case_id",
        "pacer_link",

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
        "dtsubjectname",
        "businessentity",
        #"issueisin",
        "cusip",
        "ric",
        "oapermid",
        ]
    df=df[ordered_cols]

    #save
    filepath=f"{results}/{result}.csv"
    df.to_csv(filepath, index=False)


#_osha_aggregate
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
        "cusip",
        "open_date",
        "close_case_date",
        "osha_initiation_lag",
        "initial_penalty",
        "fta_issuance_year",
        "fta_penalty",
        "gravity",
        "nr_exposure",
        "nr_in_estab",

        #refinitiv
        "dtsubjectname",
        "businessentity",
        "issueisin",
        #"cusip",
        "ric",
        "oapermid",
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
        "cusip",
        "open_date",
        "close_case_date",
        "initial_penalty",
        ]
    df=df.dropna(subset=dropna_cols)

    #to numeric
    tonumeric_cols=[
        "initial_penalty",
        "gravity",
        "nr_exposure",
        "nr_in_estab",
        ]
    df=_tonumericcols_to_df(df, tonumeric_cols)

    #aggregate over company-init year obs
    by=[
        "cusip",
        "open_date",
        ]
    dict_agg_colfunctions={
        "initial_penalty": [np.sum],
        "fta_penalty": [np.sum],
        "gravity": [np.sum],
        "nr_exposure": [np.sum],
        "nr_in_estab": [np.sum],
        "close_case_date": [_firstvalue_join],
        "osha_initiation_lag": [_firstvalue_join],
        "fta_issuance_year": [_firstvalue_join],

        #refinitiv
        "dtsubjectname": [_firstvalue_join],
        "businessentity": [_firstvalue_join],
        "issueisin": [_firstvalue_join],
        #"cusip": [_firstvalue_join],
        "ric": [_firstvalue_join],
        "oapermid": [_firstvalue_join],
        }
    df=_groupby(df, by, dict_agg_colfunctions)

    #full panel
    cols_id=[
        "cusip",
        "open_date",
        ]
    years=[
        2000,
        2023,
        ]
    fillna_cols=[
        "initial_penalty",
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


#_ln_vars
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


#_lag_vars
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


#_change_vars
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


#_dummyifpositive_vars
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


#_post_year_dummies
def _post_year_dummies(df, start_year, stop_year):

    #to numeric
    tonumeric_cols=[
        "fyear",
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
        df[post_year_dummy]=np.where(df["fyear"] >= year, 1, 0)

        #update var
        post_year_dummies[j]=post_year_dummy

    #return
    return df, post_year_dummies


#_donations_newvars
def _donations_newvars(df):

    #amount_both
    df["amount_both"]=df["daga"] + df["raga"]

    #oldvars
    oldvars=[
        "daga",
        "raga",
        "amount_both",
        ]
    df, newvars = _dummyifpositive_vars(df, oldvars)

    
    #ln
    oldvars=[
        #amount
        "daga",
        "raga",
        "amount_both",
        ]
    df, ln_vars = _ln_vars(df, oldvars)

    #lagged
    oldvars=[
        #amount
        "daga",
        "raga",
        "amount_both",

        #ln amount
        "ln_daga",
        "ln_raga",
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
        "daga",
        "raga",
        "amount_both",

        #ln amount
        "ln_daga",
        "ln_raga",
        "ln_amount_both",
        ]
    df, change_vars = _change_vars(df, oldvars)

    #donation vars
    donation_vars=[
        #amount
        "daga",
        "raga",
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


#_stagdid
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


#_echo_newvars
def _echo_newvars(df):
    
    #dummy enforcement action
    df["echo_enforcement_dummy"]=np.where(df["settlement_fy"].notna(), 1, 0)

    #dummy vars
    oldvars=["fed_penalty_assessed_amt",]
    df, newvars = _dummyifpositive_vars(df, oldvars)

    #ln
    oldvars=["fed_penalty_assessed_amt"]
    df, newvars = _ln_vars(df, oldvars)

    #lag
    oldvars=[
        "echo_enforcement_dummy",
        "echo_penalty_dummy",
        "fed_penalty_assessed_amt",
        "ln_echo_penalty_amount",
        ]
    df, newvars = _lag_vars(df, oldvars)

    #stagdid 
    unit_var="cusip"
    time_var="fyear"
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
        "fed_penalty_assessed_amt",
        "ln_echo_penalty_amount",

        #lag
        "lag_echo_enforcement_dummy",
        "lag_echo_penalty_dummy",
        "lag_echo_penalty_amount",
        "lag_ln_echo_penalty_amount",

        #years
        "echo_initiation_year",
        "settlement_fy",
        "echo_initiation_lag",
        "case_number",
        ] + stagdid_vars

    #return
    return df, echo_vars


#_divide_vars
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


#_crspcompustat_newvars
def _crspcompustat_newvars(df):

    #to numeric
    tonumeric_cols=[
        "at",
        ]
    df=_tonumericcols_to_df(df, tonumeric_cols)

    #firm size
    df["firm_size"]=np.log1p(df["at"])

    #leverage_ratio
    df=_divide_vars(df, "leverage_ratio", "lt", "at")

    #roa
    df=_divide_vars(df, "roa", "ni", "at")

    #roe
    df=_divide_vars(df, "roe", "ni", "seq")

    #mtb
    df=_divide_vars(df, "mtb", "mkvalt", "seq")

    #crspcompustat vars
    crspcompustat_vars=[
        "firm_size",
        "leverage_ratio",
        "roa",
        "roe",
        "mtb",
        "state",
        "incorp",
        ]

    #return
    return df, crspcompustat_vars


#_sic_to_famafrench
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


#_industry_print_stats
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


#_industry_drop
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


#_industry_newvars
def _industry_newvars(df, filepath):

    #mapping
    mapping=_filepath_to_mapping(filepath)

    #industry col
    n=int(''.join(char for char in filepath if char.isdigit()))
    industry_col=f"industry_famafrench{n}"

    #to numeric
    tonumeric_cols=[
        "sic",
        ]
    df=_tonumericcols_to_df(df, tonumeric_cols)

    #gen var
    oldvalues=df["sic"].values
    df[industry_col]=np.array([_sic_to_famafrench(x, mapping) for x in oldvalues])

    #drop industries
    unit_var="cusip"
    drop_basedon="echo_enforcement_dummy"
    df=_industry_drop(df, unit_var, drop_basedon, industry_col)

    #newvars
    newvars=[
        "sic",
        industry_col,
        ] 

    #return
    return df, newvars


#_crspcompustat_donations_echo_screen
folders=["zhao/_merge", "zhao/_merge"]
items=["crspcompustat_donations_echo", "crspcompustat_donations_echo_screen"]
def _crspcompustat_donations_echo_screen(folders, items):

    #folders
    resources=folders[0]
    results=folders[1]

    #items
    resource=items[0]
    result=items[1]

    "cusip"
    #usecols
    usecols=[
        #irs
        "a__contributor_name",
        "dtsubjectname_left",
        "dtsubjectname_right",
        "conm",
        "state",
        "incorp",
        "cusip",
        "fyear",
        "daga",
        "raga",

        #echo
        "echo_initiation_year",
        "settlement_fy",
        "echo_initiation_lag",
        "fed_penalty_assessed_amt",
        "case_number",

        #refinitiv
        "at",
        "lt",
        "seq",
        "mkvalt",
        "csho",
        "dvt",
        "revt",
        "cogs",
        "oibdp",
        "dp",
        "ni",
        "sic",
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
        "daga",
        "raga",
        "fed_penalty_assessed_amt",
        ]
    df=_fillnacols_to_df(df, fillna_cols)

    #sortvalues
    sortvalues_cols=[
        "cusip", 
        "fyear", 
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
        "a__contributor_name",
        "dtsubjectname_left",
        "dtsubjectname_right",
        "conm",
        "cusip",
        "fyear",
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




#echo facilities screen
folders=["zhao/data/epa", "zhao/_epa"]
items=["CASE_FACILITIES", "CASE_FACILITIES_screen"]
#_echo_facilities_screen(folders, items)


#echo enforcements screen
folders=["zhao/data/epa", "zhao/_epa"]
items=["CASE_ENFORCEMENT_CONCLUSIONS", "CASE_ENFORCEMENT_CONCLUSIONS_screen"]
#_echo_enforcements_screen(folders, items)


#echo milestones screen
folders=["zhao/data/epa", "zhao/_epa"]
items=["CASE_MILESTONES", "CASE_MILESTONES_screen"]
#_echo_milestones_screen(folders, items)


#echo tri screen
folders=["zhao/data/epa", "zhao/_epa"]
items=["TRI", "TRI_screen"]
#_echo_tri_screen(folders, items)


#merge echo facilities with echo enforcements
folders=["zhao/_epa"]
items=["CASE_FACILITIES_screen_CASE_ENFORCEMENT_CONCLUSIONS_screen"]
left_path="zhao/_epa/CASE_FACILITIES_screen"
left_ons=["case_number", "activity_id",]
right_path="zhao/_epa/CASE_ENFORCEMENT_CONCLUSIONS_screen"
right_ons=["case_number", "activity_id"]
how="left"
validate="m:1"
#_pd_merge(folders, items, left_path, left_ons, right_path, right_ons, how, validate)


#merge echo facilities_enforcements with echo milestones
folders=["zhao/_epa"]
items=["CASE_FACILITIES_screen_CASE_ENFORCEMENT_CONCLUSIONS_screen_CASE_MILESTONES_screen"]
left_path="zhao/_epa/CASE_FACILITIES_screen_CASE_ENFORCEMENT_CONCLUSIONS_screen"
left_ons=["case_number", "activity_id",]
right_path="zhao/_epa/CASE_MILESTONES_screen"
right_ons=["case_number", "activity_id"]
how="left"
validate="m:1"
#_pd_merge(folders, items, left_path, left_ons, right_path, right_ons, how, validate)


#merge echo facilities_enforcements_milestones with echo tri, by nearest years
folders=["zhao/_epa"]
items=["CASE_FACILITIES_screen_CASE_ENFORCEMENT_CONCLUSIONS_screen_CASE_MILESTONES_screen_TRI_screen"]
left_path="zhao/_epa/CASE_FACILITIES_screen_CASE_ENFORCEMENT_CONCLUSIONS_screen_CASE_MILESTONES_screen"
left_bys=["registry_id"]
left_on="echo_initiation_year"
right_path="zhao/_epa/TRI_screen"
right_bys=["epa_registry_id"]
right_on="reporting_year"
#_pd_merge_asof(folders, items, left_path, left_bys, left_on, right_path, right_bys, right_on)




#crspcompustat screen
folders=["zhao/data/crspcompustat", "zhao/_crspcompustat"]
items=["crspcompustat_2000_2023", "crspcompustat_2000_2023_screen"]
#_crspcompustat_screen(folders, items)


#search irs donations ids
folders=["zhao/_irs", "zhao/_irs"]
items=["A_screen", "A_screen_search"]
colname="a__contributor_company_involved"
#_search(folders, items, colname)


#search echo facilities_enforcements_milestones_tri ids
folders=["zhao/_epa", "zhao/_epa"]
items=["CASE_FACILITIES_screen_CASE_ENFORCEMENT_CONCLUSIONS_screen_CASE_MILESTONES_screen_TRI_screen", "CASE_FACILITIES_screen_CASE_ENFORCEMENT_CONCLUSIONS_screen_CASE_MILESTONES_screen_TRI_screen_search"]
colname="parent_co_name"
#_search(folders, items, colname)


#search violtrack ids
folders=["zhao/_violtrack", "zhao/_violtrack"]
items=["violtrack_screen", "violtrack_screen_search"]
colname="current_parent_name"
#_search(folders, items, colname)


#merge irs donations with ids
folders=["zhao/_irs"]
items=["donations_ids"]
left_path="zhao/_irs/A_screen"
left_ons=["a__contributor_company_involved"]
right_path="zhao/_irs/A_screen_search_a__contributor_company_involved"
right_ons=["query"]
how="left"
validate="m:1"
#_pd_merge(folders, items, left_path, left_ons, right_path, right_ons, how, validate)


#merge echo facilities_enforcements_tri with ids
folders=["zhao/_epa"]
items=["echo_ids"]
left_path="zhao/_epa/CASE_FACILITIES_screen_CASE_ENFORCEMENT_CONCLUSIONS_screen_CASE_MILESTONES_screen_TRI_screen"
left_ons=["parent_co_name"]
right_path="zhao/_epa/CASE_FACILITIES_screen_CASE_ENFORCEMENT_CONCLUSIONS_screen_CASE_MILESTONES_screen_TRI_screen_search_parent_co_name"
right_ons=["query"]
how="left"
validate="m:1"
#_pd_merge(folders, items, left_path, left_ons, right_path, right_ons, how, validate)


#merge violtrack with ids
folders=["zhao/_violtrack"]
items=["violtrack_ids"]
left_path="zhao/_violtrack/violtrack_screen"
left_ons=["current_parent_name"]
right_path="zhao/_violtrack/violtrack_screen_search_current_parent_name"
right_ons=["query"]
how="left"
validate="m:1"
#_pd_merge(folders, items, left_path, left_ons, right_path, right_ons, how, validate)


#_irs_A_aggregate
folders=["zhao/_irs", "zhao/_irs"]
items=["donations_ids", "donations_ids_aggregate"]
#_irs_A_aggregate(folders, items)


#echo aggregate
folders=["zhao/_epa", "zhao/_epa"]
items=["echo_ids", "echo_ids_aggregate"]
#_echo_aggregate(folders, items)


#violtrack aggregate
folders=["zhao/_violtrack", "zhao/_violtrack"]
items=["violtrack_ids", "violtrack_ids_aggregate"]
#_violtrack_aggregate(folders, items)


#merge irs donations_ids_aggregate with echo_ids_aggregate
folders=["zhao/_merge"]
items=["donations_echo"]
left_path="zhao/_irs/donations_ids_aggregate"
left_ons=["cusip", "a__contribution_year"]
right_path="zhao/_epa/echo_ids_aggregate"
right_ons=["cusip", "echo_initiation_year"]
how="outer"
validate="1:1"
#_pd_merge(folders, items, left_path, left_ons, right_path, right_ons, how, validate)


#merge irs donations_ids_aggregate with violtrack_ids_aggregate


#merge crspcompustat with donations_echo 
folders=["zhao/_merge"]
items=["crspcompustat_donations_echo"]
left_path="zhao/_crspcompustat/crspcompustat_2000_2023_screen"
left_ons=["cusip", "fyear"]
right_path="zhao/_merge/donations_echo"
right_ons=["cusip", "a__contribution_year"]
how="left"
validate="1:1"
#_pd_merge(folders, items, left_path, left_ons, right_path, right_ons, how, validate)


#merge crspcompustat with donations_violtrack 





#crspcompustat_donations_echo screen
folders=["zhao/_merge", "zhao/_merge"]
items=["crspcompustat_donations_echo", "crspcompustat_donations_echo_screen"]
#_crspcompustat_donations_echo_screen(folders, items)


#_crspcompustat_donations_violtrack screen
