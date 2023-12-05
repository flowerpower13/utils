
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
