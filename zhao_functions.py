

#imports
import pandas as pd
from pathlib import Path
from zipfile import ZipFile


#functions
from _merge_utils import _pd_merge, _dfpath_to_dfon
from _pd_utils import _folder_to_filestems, _lowercase_colnames_values, _tonumericcols_to_df, _todatecols_to_df, \
    _groupby, _firstvalue, _firstvalue_join, _fullpanel


#_cfi
#https://cfinst.github.io/downloads/cfi-laws-database_Disclosure.zip
#https://www.ncsl.org/elections-and-campaigns/independent-expenditure-disclosure-requirements, https://aboutpoliticalads.org/
folders=["zhao/data/cfi/cfi-laws-database_Disclosure", "zhao/_cfi"]
items=["Laws_03_Disclosure_2", "cfi"]
def _cfi(folders, items):

    #folders
    resources=folders[0]
    results=folders[1]

    #items
    resource=items[0]
    result=items[1]

    #usecols
    usecols=[
        "State",
        "Year",
        "IE_Filers_Corporation",
        "IE_Filers_Lobbyist",    
        ]

    #read_csv
    filepath=f"{resources}/{resource}.csv"
    sep=","
    df=pd.read_csv(
        filepath, 
        sep=sep,
        usecols=usecols,
        dtype="string",
        #nrows=10000,
        #encoding="utf-8",
        #on_bad_lines="skip",
        )

    #_lowercase_colnames_values
    df=_lowercase_colnames_values(df)

    #dropna

    #tonumeric

    #todate
    todate_cols=[
        "Year",
        ]
    errors="coerce"
    format="%Y"
    df=_todatecols_to_df(df, todate_cols, errors, format)
    #year
    df["Year"]=pd.DatetimeIndex(df["Year"], ambiguous="NaT").year

    #dropobs
    df=df[df["State"]!="us"]
    df=df[df["Year"]>=2010]

    #dict_replace
    dict_replace={
        "yes": "1",
        "no": "0",
        }
    df[["IE_Filers_Corporation", "IE_Filers_Lobbyist"]]=df[["IE_Filers_Corporation", "IE_Filers_Lobbyist"]].replace(dict_replace)
    dict_replace={
        'al': '01', 'ak': '02', 'az': '04', 'ar': '05', 'ca': '06', 'co': '08',
        'ct': '09', 'de': '10', 'fl': '12', 'ga': '13', 'hi': '15', 'id': '16',
        'il': '17', 'in': '18', 'ia': '19', 'ks': '20', 'ky': '21', 'la': '22',
        'me': '23', 'md': '24', 'ma': '25', 'mi': '26', 'mn': '27', 'ms': '28',
        'mo': '29', 'mt': '30', 'ne': '31', 'nv': '32', 'nh': '33', 'nj': '34',
        'nm': '35', 'ny': '36', 'nc': '37', 'nd': '38', 'oh': '39', 'ok': '40',
        'or': '41', 'pa': '42', 'ri': '44', 'sc': '45', 'sd': '46', 'tn': '47',
        'tx': '48', 'ut': '49', 'vt': '50', 'va': '51', 'wa': '53', 'wv': '54',
        'wi': '55', 'wy': '56',
        }
    df["State"]=df["State"].replace(dict_replace)

    #tonumeric
    tonumeric_cols=[
        "State",
        "IE_Filers_Corporation",
        "IE_Filers_Lobbyist",
        ]
    errors="raise"
    df=_tonumericcols_to_df(df, tonumeric_cols, errors)

    #sortvalues
    sortvalues_cols=[
        "State",
        "Year",
        ]
    df=df.sort_values(
        by=sortvalues_cols,
        )

    #start, end
    start, end = 1996, 2023
    all_states = df["State"].unique()
    all_years = range(start, end + 1)

    #fill missing years
    df=df.set_index(["State", "Year"])
    df=df.sort_index()
    index = pd.MultiIndex.from_product([all_states, all_years], names=['State', 'Year'])
    df=df.reindex(index)
    df=df.fillna(method='ffill')
    df=df.reset_index()

    #adjustments?
    '''adjustment_tuples=[
        ("al", 2022, 0),
        ]
    for i, adj_tuple in enumerate(adjustment_tuples):
        (state, year, dummy) = adj_tuple
        df.loc[(df["State"] == state) & (df["Year"] == year), "IE_Filers_Corporation"] = dummy'''

    #sortvalues
    sortvalues_cols=[
        "State",
        "Year",
        ]
    df=df.sort_values(
        by=sortvalues_cols,
        )

    #ordered_cols
    ordered_cols=[
        "State",
        "Year",
        "IE_Filers_Corporation",
        "IE_Filers_Lobbyist",  
        ]
    df=df[ordered_cols]

    #to_csv
    filepath=f"{results}/{result}.csv"
    df.to_csv(filepath, index=False)
    #'''
    

#_mit_year
#https://dataverse.harvard.edu/dataverse/medsl
#https://github.com/orgs/MEDSL/repositories?type=all
#http://doi.org/10.7910/DVN/GSZG1O
#http://doi.org/10.7910/DVN/ZFXEJU
#http://doi.org/10.7910/DVN/OKL2K1
folders=["zhao/data/mit/2016", "zhao/_mit"]
items=["2016-precinct-state", "mit_2016"]
def _mit_year(folders, items):

    #folders
    resources=folders[0]
    results=folders[1]

    #items
    resource=items[0]
    result=items[1]

    #read_csv
    filepath=f"{resources}/{resource}.csv"
    sep=","
    df=pd.read_csv(
        filepath, 
        sep=sep,
        #usecols=usecols,
        dtype="string",
        #nrows=300000,
        encoding="ISO-8859-1",
        #on_bad_lines="skip",
        )
    
    #rename
    df=df.rename(columns={"party": "party_detailed"})

    #usecols
    usecols=[
        "state_fips",
        "candidate",
        "office",
        "district",
        "writein",
        "party_detailed",
        "votes",
        ]
    df=df[usecols]

    #_lowercase_colnames_values
    df=_lowercase_colnames_values(df)

    #dropna
    dropna_cols=[
        "state_fips",
        "candidate",
        "office",
        "district",
        "votes",
        ]
    df=df.dropna(subset=dropna_cols)

    #str_replace
    df["candidate"]=df["candidate"].str.replace("representative ", "")
    df["district"]=df["district"].str.replace("district ", "")

    #tonumeric
    tonumeric_cols=[
        "state_fips",
        "district",
        "votes",
        ]
    errors="coerce"
    df=_tonumericcols_to_df(df, tonumeric_cols, errors)

    #todate

    #dropobs
    df=df[df["candidate"]!="unopposed"]
    df=df[df["candidate"]!="for"]
    df=df[df["candidate"]!="undervotes"]
    df=df[df["office"]=="state house"]
    df=df[df["writein"]!="true"]

    #_groupby
    by=[
        "state_fips",
        "district",
        "candidate",
        ]
    dict_agg_colfunctions={
        "votes": ["sum"],
        "party_detailed": [_firstvalue],
        }
    df_district_candidate=_groupby(df, by, dict_agg_colfunctions)

    #sortvalues
    sortvalues_cols=[
        "state_fips",
        "district",
        "votes",
        ]
    df_district_candidate=df_district_candidate.sort_values(
        by=sortvalues_cols,
        ascending=[True, True, False],
        )

    #_groupby
    by=[
        "state_fips",
        "district",
        ]
    dict_agg_colfunctions={
        "candidate": [_firstvalue],
        "votes": [_firstvalue],
        "party_detailed": [_firstvalue]
        }
    df_district=_groupby(df_district_candidate, by, dict_agg_colfunctions)

    #grouped
    grouped=df_district_candidate.groupby(["state_fips", "district"]).\
        agg(
                {"votes": 
                    [lambda x: x.iloc[1] if len(x) > 1 else None, 
                    "sum"
                    ]
                }
            )
    grouped=grouped.reset_index()
    grouped.columns = ["state_fips", "district", "votes_second", "votes_total"]
    df_district=pd.merge(df_district, grouped, on=["state_fips", "district"], how='left')

    #year
    year=result[-4:]
    df["year"]=int(year)

    #sortvalues
    sortvalues_cols=[
        "state_fips",
        "district",
        ]
    df=df.sort_values(
        by=sortvalues_cols,
        )

    #ordered_cols
    ordered_cols=[
        "state_fips",
        "candidate",
        "district",
        "party_detailed",
        "votes",
        "votes_second",
        "votes_total",
        "year",
        ]
    df=df[ordered_cols]

    #to_csv
    filepath=f"{results}/{result}.csv"
    df.to_csv(filepath, index=False)

    #return
    return df
    #'''


#_mit_2022
#https://github.com/MEDSL/2022-elections-official
folders=["zhao/data/mit/2022/individual_states", "zhao/_mit"]
items=["mit_2022"]
def _mit_2022(folders, items):

    #folders
    resources=folders[0]
    results=folders[1]

    #items
    result=items[0]

    #_folder_to_filestems
    files, filestems = _folder_to_filestems(resources)

    #n obs
    n_obs=len(filestems)

    #ordered frames and cols
    frames=[None]*n_obs

    #for
    for i, filestem in enumerate(filestems):

        #filepath
        filepath=f"{resources}/{filestem}.zip"

        #rawresults
        rawresults=f"{results}/2022/raw"

        #cleanresults
        cleanresults=f"{results}/2022/clean"

        #rawfile
        rawfile=f"{rawresults}/{filestem}.csv"

        with ZipFile(filepath, "r") as zip_ref:

            #tempfile
            templist=zip_ref.namelist()
            tempfile=templist[0]

            #extract
            zip_ref.extract(tempfile)

            #p
            p=Path(tempfile)

            #rename
            p.rename(rawfile)
        
        #_mit
        folders=[rawresults, cleanresults]
        items=[filestem, filestem]
        df_i=_mit_year(folders, items) 

        #ordered cols
        frames[i]=df_i

    #concat
    df=pd.concat(frames)

    #sortvalues
    sortvalues_cols=[
        "state_fips",
        "district",
        ]
    df=df.sort_values(
        by=sortvalues_cols,
        )

    #to_csv
    filepath=f"{results}/{result}.csv"
    df.to_csv(filepath, index=False)  
    #'''


#_mit
folders=["zhao/_mit", "zhao/_mit"]
items=["mit"]
def _mit(folders, items):

    #folders
    resources=folders[0]
    results=folders[1]

    #items
    result=items[0]

    #_folder_to_filestems
    filestems=[
        "mit_2016",
        "mit_2018",
        "mit_2020",
        "mit_2022",
        ]
    
    #n obs
    n_obs=len(filestems)

    #ordered frames and cols
    frames=[None]*n_obs

    #for
    for i, filestem in enumerate(filestems):

        #usecols
        usecols=[
            "state_fips",
            "candidate",
            "district",
            "party_detailed",
            "votes",
            "votes_second",
            "votes_total",
            "year",
            ]
        
        #read_csv
        filepath=f"{resources}/{filestem}.csv"
        sep=","
        df_i=pd.read_csv(
            filepath, 
            sep=sep,
            usecols=usecols,
            dtype="string",
            #nrows=10000,
            #encoding="utf-8",
            #on_bad_lines="skip",
            )

        #ordered cols
        frames[i]=df_i

    #concat
    df=pd.concat(frames)

    #state_fips_district
    df["state_fips_district"]=df["state_fips"]+"_"+df["district"]

    #tonumeric
    tonumeric_cols=[
        "year",
        ]
    errors="raise"
    df=_tonumericcols_to_df(df, tonumeric_cols, errors)

    #_fullpanel
    idvar="state_fips_district"
    timevar="year"
    timevar_range=(2016, 2022+1)
    fillna_cols=[]
    df=_fullpanel(df, idvar, timevar, timevar_range, fillna_cols)

    #tonumeric
    tonumeric_cols=[
        "state_fips",
        "district",
        ]
    errors="raise"
    df=_tonumericcols_to_df(df, tonumeric_cols, errors)

    #sortvalues
    sortvalues_cols=[
        "state_fips",
        "district",
        "year",
        ]
    df=df.sort_values(
        by=sortvalues_cols,
        )
    
    #ordered_cols
    ordered_cols=[
        "state_fips",
        "district",
        "year",
        "candidate",
        "party_detailed",
        "votes",
        "votes_second",
        "votes_total",
        ]
    df=df[ordered_cols]

    #to_csv
    filepath=f"{results}/{result}.csv"
    df.to_csv(filepath, index=False)  
    #'''


#_mit(folders, items)


#_qcew_i
#https://www.bls.gov/cew/downloadable-data-files.htm
#https://data.bls.gov/cew/data/files/2022/csv/2022_annual_singlefile.zip
#https://www.bls.gov/cew/about-data/downloadable-file-layouts/annual/naics-based-annual-layout.htm
#https://www.bls.gov/cew/classifications/ownerships/ownership-titles.htm
def _qcew_i(folders, items):

    #folders
    resources=folders[0]
    results=folders[1]

    #items
    resource=items[0]
    result=items[1]

    #usecols
    usecols=[
        "area_fips",
        "own_code",
        "industry_code",
        "year",
        "annual_avg_estabs",
        "annual_avg_emplvl",
        "total_annual_wages",
        ]

    #read_csv
    filepath=f"{resources}/{resource}.csv"
    sep=","
    df=pd.read_csv(
        filepath, 
        sep=sep,
        usecols=usecols,
        dtype="string",
        #nrows=10000,
        #encoding="utf-8",
        #on_bad_lines="skip",
        )

    #_lowercase_colnames_values
    df=_lowercase_colnames_values(df)

    #dropna
    dropna_cols=[
        "area_fips",
        "own_code",
        "industry_code",
        "year",
        ]
    df=df.dropna(subset=dropna_cols)

    #dropobs
    df=df[~df["area_fips"].str.endswith("000")]
    df=df[df["area_fips"].str.match("^\d+$")]
    #https://www.bls.gov/cew/classifications/ownerships/ownership-titles.htm
    df=df[df["own_code"].isin(["5", "3", "2", "1"])] #5 Private, 3 Local Government, 2 State Government, 1 Federal Government 
    df=df[df["industry_code"]=="10"]

    #tonumeric
    tonumeric_cols=[
        "area_fips",
        "own_code",
        "year",
        "annual_avg_estabs",
        "annual_avg_emplvl",
        "total_annual_wages",
        ]
    errors="raise"
    df=_tonumericcols_to_df(df, tonumeric_cols, errors)
    #todate

    #_groupby

    #pivot
    values=[
        "annual_avg_estabs",
        "annual_avg_emplvl",
        "total_annual_wages",
        ]
    index=[
        "area_fips",
        "year",
        ]
    columns="own_code"
    aggfunc="first"
    fill_value=0
    df_pivot=pd.pivot_table(
        data=df,
        values=values,
        index=index,
        columns=columns,  
        aggfunc=aggfunc,
        fill_value=fill_value,
        )      
    df_pivot.columns=[f"{col[0]}_{col[1]}" for col in df_pivot.columns]
    df_pivot=df_pivot.reset_index() 
    list_pivotcolumns=list(df_pivot.columns)
    df=df_pivot.copy()

    #sortvalues
    sortvalues_cols=[
        "area_fips",
        "year",
        ]
    df=df.sort_values(
        by=sortvalues_cols,
        )

    #ordered_cols
    ordered_cols=list_pivotcolumns
    df=df[ordered_cols]

    #to_csv
    filepath=f"{results}/{result}.csv"
    df.to_csv(filepath, index=False)

    #return
    return df
    #'''


#_qcew
folders=["zhao/data/qcew", "zhao/_qcew"]
items=["qcew"]
def _qcew(folders, items):

    #folders
    resources=folders[0]
    results=folders[1]

    #items
    result=items[0]

    #_folder_to_filestems
    files, filestems = _folder_to_filestems(resources)

    #n obs
    n_obs=len(filestems)

    #ordered frames and cols
    frames=[None]*n_obs

    #for
    for i, filestem in enumerate(filestems):

        #filepath
        filepath=f"{resources}/{filestem}.zip"

        #rawresults
        rawresults=f"{results}/raw"

        #cleanresults
        cleanresults=f"{results}/clean"

        #rawfile
        rawfile=f"{rawresults}/{filestem}.csv"

        with ZipFile(filepath, "r") as zip_ref:

            #tempfile
            templist=zip_ref.namelist()
            tempfile=templist[0]

            #extract
            zip_ref.extract(tempfile)

            #p
            p=Path(tempfile)

            #rename
            p.rename(rawfile)
        
        #open
        folders=[rawresults, cleanresults]
        items=[filestem, filestem]
        df_i=_qcew_i(folders, items) 

        #ordered cols
        frames[i]=df_i

    #concat
    df=pd.concat(frames)

    #sortvalues
    sortvalues_cols=[
        "area_fips",
        "year",
        ]
    df=df.sort_values(
        by=sortvalues_cols,
        )

    #to_csv
    filepath=f"{results}/{result}.csv"
    df.to_csv(filepath, index=False)  
    #'''


#_soi_i
#https://www.irs.gov/statistics/soi-tax-stats-individual-income-tax-statistics-zip-code-data-soi, not include agi, 
def _soi_i(folders, items):

    #folders
    resources=folders[0]
    results=folders[1]

    #items
    resource=items[0]
    result=items[1]

    #usecols
    usecols=[
        "STATEFIPS",
        "ZIPCODE",
        "A00100", #Adjust gross income
        ]

    #read_csv
    filepath=f"{resources}/{resource}.csv"
    sep=","
    df=pd.read_csv(
        filepath, 
        sep=sep,
        usecols=usecols,
        dtype="string",
        #nrows=10000,
        encoding="latin1",
        #on_bad_lines="skip",
        )

    #_lowercase_colnames_values
    df=_lowercase_colnames_values(df)

    #dropna
    dropna_cols=[
        "STATEFIPS",
        "ZIPCODE",
        ]
    df=df.dropna(subset=dropna_cols)

    #tonumeric
    tonumeric_cols=[
        "STATEFIPS",
        "ZIPCODE",
        ]
    errors="raise"
    df=_tonumericcols_to_df(df, tonumeric_cols, errors)

    #dropobs
    df=df=df[df["ZIPCODE"]!=0]

    #todate

    #_groupby

    #pivot

    #year
    year=f"20{resource[0:2]}"
    df["year"]=year

    #sortvalues
    sortvalues_cols=[
        "STATEFIPS",
        "ZIPCODE",
        ]
    df=df.sort_values(
        by=sortvalues_cols,
        )

    #ordered_cols
    ordered_cols=[
        "STATEFIPS",
        "ZIPCODE",
        "year",
        "A00100",
        ]
    df=df[ordered_cols]

    #to_csv
    filepath=f"{results}/{result}.csv"
    df.to_csv(filepath, index=False)

    #return
    return df
    #'''


#_soi
folders=["zhao/data/soi", "zhao/_soi"]
items=["soi"]
def _soi(folders, items):

    #folders
    resources=folders[0]
    results=folders[1]

    #items
    result=items[0]

    #_folder_to_filestems
    files, filestems = _folder_to_filestems(resources)

    #n obs
    n_obs=len(filestems)

    #ordered frames and cols
    frames=[None]*n_obs

    #for
    for i, filestem in enumerate(filestems):

        #filepath
        filepath=f"{resources}/{filestem}.csv"

        #cleanresults
        cleanresults=f"{results}/clean"
        
        #open
        folders=[resources, cleanresults]
        items=[filestem, filestem]
        df_i=_soi_i(folders, items) 

        #ordered cols
        frames[i]=df_i

    #concat
    df=pd.concat(frames)

    #sortvalues
    sortvalues_cols=[
        "STATEFIPS",
        "ZIPCODE",
        "year",
        ]
    df=df.sort_values(
        by=sortvalues_cols,
        )

    #to_csv
    filepath=f"{results}/{result}.csv"
    df.to_csv(filepath, index=False)  
    #'''


#_burningglass
folders=["zhao/data/burningglass", "zhao/_burningglass"]
items=["burningglass", "burningglass"]
def _burningglass(folders, items):

    #folders
    resources=folders[0]
    results=folders[1]

    #items
    resource=items[0]
    result=items[1]

    #usecols
    usecols=[
        "fips",
        "year",
        "totaljob",
        "totalcorpjob",
        ]

    #read_csv
    filepath=f"{resources}/{resource}.csv"
    sep=","
    df=pd.read_csv(
        filepath, 
        sep=sep,
        usecols=usecols,
        dtype="string",
        #nrows=10000,
        #encoding="latin1",
        #on_bad_lines="skip",
        )

    #_lowercase_colnames_values
    df=_lowercase_colnames_values(df)

    #dropna
    dropna_cols=[
        "fips",
        "year",
        ]
    df=df.dropna(subset=dropna_cols)

    #tonumeric
    tonumeric_cols=[
        "fips",
        "year",
        "totaljob",
        "totalcorpjob",
        ]
    errors="raise"
    df=_tonumericcols_to_df(df, tonumeric_cols, errors)

    #dropobs

    #todate

    #_groupby

    #pivot

    #sortvalues
    sortvalues_cols=[
        "fips",
        "year",
        ]
    df=df.sort_values(
        by=sortvalues_cols,
        )

    #ordered_cols
    ordered_cols=[
        "fips",
        "year",
        "totaljob",
        "totalcorpjob",
        ]
    df=df[ordered_cols]

    #to_csv
    filepath=f"{results}/{result}.csv"
    df.to_csv(filepath, index=False)
    #'''


#_osha_inspection_i
#https://enforcedata.dol.gov/views/data_summary.php
#https://enforcedata.dol.gov/views/data_dictionary.php
def _osha_inspection_i(folders, items):

    #folders
    resources=folders[0]
    results=folders[1]

    #items
    resource=items[0]
    result=items[1]

    #usecols
    usecols=[
        "activity_nr",
        "host_est_key",
        "estab_name",
        "nr_in_estab",
        "open_date",
        "owner_type", #A=Private. B=LocalGovt. C=StateGovt. D=Federal
        "site_zip",
        ]

    #read_csv
    filepath=f"{resources}/{resource}.csv"
    sep=","
    df=pd.read_csv(
        filepath, 
        sep=sep,
        usecols=usecols,
        dtype="string",
        #nrows=1000,
        #encoding="latin1",
        #on_bad_lines="skip",
        )

    #_lowercase_colnames_values
    df=_lowercase_colnames_values(df)

    #dropna
    dropna_cols=[
        "activity_nr",
        "open_date",
        "site_zip",
        ]
    df=df.dropna(subset=dropna_cols)

    #tonumeric
    tonumeric_cols=[
        "activity_nr",
        "site_zip",
        ]
    errors="raise"
    df=_tonumericcols_to_df(df, tonumeric_cols, errors)

    #todate
    todate_cols=[
        "open_date",
        ]
    errors="raise"
    format="%Y-%m-%d"
    df=_todatecols_to_df(df, todate_cols, errors, format)
    #year
    df["year"]=pd.DatetimeIndex(df["open_date"], ambiguous="NaT").year

    #dropobs
    df=df[df["year"]>=2010]

    #_groupby

    #sortvalues
    sortvalues_cols=[
        "activity_nr",
        ]
    df=df.sort_values(
        by=sortvalues_cols,
        )

    #ordered_cols
    ordered_cols=[
        "activity_nr",
        "host_est_key",
        "estab_name",
        "nr_in_estab",
        "open_date",
        "owner_type",
        "site_zip",
        "year",
        ]
    df=df[ordered_cols]

    #to_csv
    filepath=f"{results}/{result}.csv"
    df.to_csv(filepath, index=False)

    #return
    return df
    #'''


#_osha_inspection
folders=["zhao/data/osha_inspection", "zhao/_osha_inspection"]
items=["osha_inspection"]
def _osha_inspection(folders, items):

    #folders
    resources=folders[0]
    results=folders[1]

    #items
    result=items[0]

    #_folder_to_filestems
    files, filestems = _folder_to_filestems(resources)

    #n obs
    n_obs=len(filestems)

    #ordered frames and cols
    frames=[None]*n_obs

    #for
    for i, filestem in enumerate(filestems):

        #filepath
        filepath=f"{resources}/{filestem}.csv"

        #cleanresults
        cleanresults=f"{results}/clean"
        
        #open
        folders=[resources, cleanresults]
        items=[filestem, filestem]
        df_i=_osha_inspection_i(folders, items) 

        #ordered cols
        frames[i]=df_i

    #concat
    df=pd.concat(frames)

    #sortvalues
    sortvalues_cols=[
        "activity_nr",
        ]
    df=df.sort_values(
        by=sortvalues_cols,
        )

    #to_csv
    filepath=f"{results}/{result}.csv"
    df.to_csv(filepath, index=False)  
    #'''


#_osha_violation_i
#https://enforcedata.dol.gov/views/data_summary.php
#https://enforcedata.dol.gov/views/data_dictionary.php
def _osha_violation_i(folders, items):

    #folders
    resources=folders[0]
    results=folders[1]

    #items
    resource=items[0]
    result=items[1]

    #usecols
    usecols=[
        "activity_nr",
        "current_penalty",
        "nr_exposed",
        "fta_penalty", #failure to abate
        "gravity",
        "initial_penalty",
        "nr_instances",
        ]

    #read_csv
    filepath=f"{resources}/{resource}.csv"
    sep=","
    df=pd.read_csv(
        filepath, 
        sep=sep,
        usecols=usecols,
        dtype="string",
        #nrows=1000,
        #encoding="latin1",
        #on_bad_lines="skip",
        )

    #_lowercase_colnames_values
    df=_lowercase_colnames_values(df)

    #dropna
    dropna_cols=[
        "activity_nr",
        ]
    df=df.dropna(subset=dropna_cols)

    #tonumeric
    tonumeric_cols=[
        "activity_nr",
        "current_penalty",
        "nr_exposed",
        "fta_penalty",
        "gravity",
        "initial_penalty",
        "nr_instances",
        ]
    errors="raise"
    df=_tonumericcols_to_df(df, tonumeric_cols, errors)

    #dropobs

    #todate

    #_groupby
    by=[
        "activity_nr",
        ]
    dict_agg_colfunctions={
        "current_penalty": ["sum"],
        "nr_exposed": ["sum"],
        "fta_penalty": ["sum"], 
        "gravity": ["sum"],
        "initial_penalty": ["sum"],
        "nr_instances": ["sum"],
        }
    df=_groupby(df, by, dict_agg_colfunctions)

    #sortvalues
    sortvalues_cols=[
        "activity_nr",
        ]
    df=df.sort_values(
        by=sortvalues_cols,
        )

    #ordered_cols
    ordered_cols=[
        "activity_nr",
        "current_penalty",
        "nr_exposed",
        "fta_penalty",
        "gravity",
        "initial_penalty",
        "nr_instances",
        ]
    df=df[ordered_cols]

    #to_csv
    filepath=f"{results}/{result}.csv"
    df.to_csv(filepath, index=False)

    #return
    return df
    #'''


#_osha_violation
folders=["zhao/data/osha_violation", "zhao/_osha_violation"]
items=["osha_violation"]
def _osha_violation(folders, items):

    #folders
    resources=folders[0]
    results=folders[1]

    #items
    result=items[0]

    #_folder_to_filestems
    files, filestems = _folder_to_filestems(resources)

    #n obs
    n_obs=len(filestems)

    #ordered frames and cols
    frames=[None]*n_obs

    #for
    for i, filestem in enumerate(filestems):

        #filepath
        filepath=f"{resources}/{filestem}.csv"

        #cleanresults
        cleanresults=f"{results}/clean"
        
        #open
        folders=[resources, cleanresults]
        items=[filestem, filestem]
        df_i=_osha_violation_i(folders, items) 

        #ordered cols
        frames[i]=df_i

    #concat
    df=pd.concat(frames)

    #_groupby
    by=[
        "activity_nr",
        ]
    dict_agg_colfunctions={
        "current_penalty": ["sum"],
        "nr_exposed": ["sum"],
        "fta_penalty": ["sum"], 
        "gravity": ["sum"],
        "initial_penalty": ["sum"],
        "nr_instances": ["sum"],
        }
    df=_groupby(df, by, dict_agg_colfunctions)

    #sortvalues
    sortvalues_cols=[
        "activity_nr",
        ]
    df=df.sort_values(
        by=sortvalues_cols,
        )

    #to_csv
    filepath=f"{results}/{result}.csv"
    df.to_csv(filepath, index=False)  
    #'''


#_osha_accident_injury
#https://enforcedata.dol.gov/views/data_summary.php
#https://enforcedata.dol.gov/views/data_dictionary.php
folders=["zhao/data/osha_accident_injury", "zhao/_osha_accident_injury"]
items=["osha_accident_injury", "osha_accident_injury"]
def _osha_accident_injury(folders, items):

    #folders
    resources=folders[0]
    results=folders[1]

    #items
    resource=items[0]
    result=items[1]

    #usecols
    usecols=[
        "rel_insp_nr",
        "degree_of_inj", #0=No injury found, 1=Fatality. 2=Hospitalized injuries. 3=No Hospitalized injuries
        ]

    #read_csv
    filepath=f"{resources}/{resource}.csv"
    sep=","
    df=pd.read_csv(
        filepath, 
        sep=sep,
        usecols=usecols,
        dtype="string",
        #nrows=1000,
        #encoding="latin1",
        #on_bad_lines="skip",
        )

    #_lowercase_colnames_values
    df=_lowercase_colnames_values(df)

    #dropna
    dropna_cols=[
        "rel_insp_nr",
        "degree_of_inj",
        ]
    df=df.dropna(subset=dropna_cols)

    #tonumeric
    tonumeric_cols=[
        "rel_insp_nr",
        ]
    errors="raise"
    df=_tonumericcols_to_df(df, tonumeric_cols, errors)

    #dropobs

    #todate

    #pivot
    values=None
    index="rel_insp_nr"
    columns="degree_of_inj"
    aggfunc="size"
    fill_value=0
    df_pivot=pd.pivot_table(
        data=df,
        values=values,
        index=index,
        columns=columns,  
        aggfunc=aggfunc,
        fill_value=fill_value,
        )  
    df_pivot.columns=[f"degree_of_inj_{col}" for col in df_pivot.columns]
    df_pivot=df_pivot.reset_index()  
    list_pivotcolumns=list(df_pivot.columns)  
    df=df_pivot.copy()

    #_groupby

    #sortvalues
    sortvalues_cols=[
        "rel_insp_nr",
        ]
    df=df.sort_values(
        by=sortvalues_cols,
        )

    #ordered_cols
    ordered_cols=list_pivotcolumns
    df=df[ordered_cols]

    #to_csv
    filepath=f"{results}/{result}.csv"
    df.to_csv(filepath, index=False)

    #return
    return df
    #'''





#_osha
#https://enforcedata.dol.gov/views/data_summary.php
#https://enforcedata.dol.gov/views/data_dictionary.php
folders=["zhao/_osha", "zhao/_osha"]
items=["osha_inspection_osha_violation_osha_accident_injury_zip", "osha"]
def _osha(folders, items):

    #folders
    resources=folders[0]
    results=folders[1]

    #items
    resource=items[0]
    result=items[1]

    #usecols
    usecols=[
        "activity_nr",
        #"host_est_key",
        "estab_name",
        "nr_in_estab",
        "owner_type",
        "site_zip",
        "year",
        "current_penalty",
        "nr_exposed",
        "fta_penalty",
        "gravity",
        "initial_penalty",
        "nr_instances",
        "degree_of_inj_0",
        "degree_of_inj_1",
        "degree_of_inj_2",
        "degree_of_inj_3",
        ]

    #read_csv
    filepath=f"{resources}/{resource}.csv"
    sep=","
    df=pd.read_csv(
        filepath, 
        sep=sep,
        usecols=usecols,
        dtype="string",
        #nrows=1000,
        #encoding="latin1",
        #on_bad_lines="skip",
        )

    #_lowercase_colnames_values
    df=_lowercase_colnames_values(df)

    #dropna
    dropna_cols=[
        "activity_nr",
        "site_zip",
        "year",
        ]
    df=df.dropna(subset=dropna_cols)

    #tonumeric
    tonumeric_cols=[
        "activity_nr",
        "nr_in_estab",
        "site_zip",
        "year",
        "current_penalty",
        "nr_exposed",
        "fta_penalty",
        "gravity",
        "initial_penalty",
        "nr_instances",
        "degree_of_inj_0",
        "degree_of_inj_1",
        "degree_of_inj_2",
        "degree_of_inj_3",
        "district" # district
        ]
    errors="raise"
    df=_tonumericcols_to_df(df, tonumeric_cols, errors)

    #todate

    #owner_types
    owner_types=["a", "b", "c", "d"]
    for i, owner_type in enumerate(owner_types):

        #dropobs
        df_i=df[df["owner_type"]==owner_type]

        #_groupby
        by=[
            "district" # district
            "year",
            ]
        dict_agg_colfunctions={
            "nr_in_estab": ["sum"],
            "current_penalty": ["sum"],
            "nr_exposed": ["sum"],
            "fta_penalty": ["sum"],
            "gravity": ["sum"],
            "initial_penalty": ["sum"],
            "nr_instances": ["sum"],
            "degree_of_inj_0": ["sum"],
            "degree_of_inj_1": ["sum"],
            "degree_of_inj_2": ["sum"],
            "degree_of_inj_3": ["sum"],
            "population": ["sum"],
            "density": ["sum"],
            }
        df_i=_groupby(df_i, by, dict_agg_colfunctions)

        #sortvalues
        sortvalues_cols=[
            "district" # district
            "year",
            ]
        df_i=df_i.sort_values(
            by=sortvalues_cols,
            )

        #ordered_cols
        ordered_cols=[
            "district" # district
            "year",
            "nr_in_estab",
            "current_penalty",
            "nr_exposed",
            "fta_penalty",
            "gravity",
            "initial_penalty",
            "nr_instances",
            "degree_of_inj_0",
            "degree_of_inj_1",
            "degree_of_inj_2",
            "degree_of_inj_3",
            "population",
            "density",
            ]
        df_i=df_i[ordered_cols]

        #to_csv
        filepath=f"{results}/{result}_{owner_type}.csv"
        df_i.to_csv(filepath, index=False)

    #_groupby
    by=[
        "district" # district
        "year",
        ]
    dict_agg_colfunctions={
        "nr_in_estab": ["sum"],
        "current_penalty": ["sum"],
        "nr_exposed": ["sum"],
        "fta_penalty": ["sum"],
        "gravity": ["sum"],
        "initial_penalty": ["sum"],
        "nr_instances": ["sum"],
        "degree_of_inj_0": ["sum"],
        "degree_of_inj_1": ["sum"],
        "degree_of_inj_2": ["sum"],
        "degree_of_inj_3": ["sum"],
        "population": ["sum"],
        "density": ["sum"],
        }
    df=_groupby(df, by, dict_agg_colfunctions)

    #sortvalues
    sortvalues_cols=[
        "district" # district
        "year",
        ]
    df=df.sort_values(
        by=sortvalues_cols,
        )

    #ordered_cols
    ordered_cols=[
        "district" # district
        "year",
        "nr_in_estab",
        "current_penalty",
        "nr_exposed",
        "fta_penalty",
        "gravity",
        "initial_penalty",
        "nr_instances",
        "degree_of_inj_0",
        "degree_of_inj_1",
        "degree_of_inj_2",
        "degree_of_inj_3",
        "population",
        "density",
        ]
    df=df[ordered_cols]

    #to_csv
    filepath=f"{results}/{result}.csv"
    df.to_csv(filepath, index=False)

    #'''





#run


#_cfi        
folders=["zhao/data/cfi/cfi-laws-database_Disclosure", "zhao/_cfi"]
items=["Laws_03_Disclosure_2", "cfi"]
#_cfi(folders, items)


#_mit_year - 2016
#http://doi.org/10.7910/DVN/GSZG1O
folders=["zhao/data/mit/2016", "zhao/_mit"]
items=["2016-precinct-state", "mit_2016"]
_mit_year(folders, items)


#_mit_year - 2018
#http://doi.org/10.7910/DVN/ZFXEJU
folders=["zhao/data/mit/2018", "zhao/_mit"]
items=["STATE_precinct_general", "mit_2018"]
#_mit_year(folders, items)


#_mit_year - 2020
#http://doi.org/10.7910/DVN/OKL2K1
folders=["zhao/data/mit/2020", "zhao/_mit"]
items=["STATE_precinct_general", "mit_2020"]
#_mit_year(folders, items)


#_mit_2022
#https://github.com/MEDSL/2022-elections-official
folders=["zhao/data/mit/2022/individual_states", "zhao/_mit"]
items=["mit_2022"]
#_mit_2022(folders, items)





#_soi
folders=["zhao/data/soi", "zhao/_soi"]
items=["soi"]
#_soi(folders, items)


#_burningglass
folders=["zhao/data/burningglass", "zhao/_burningglass"]
items=["burningglass", "burningglass"]
#_burningglass(folders, items)


#_osha_inspection
folders=["zhao/data/osha_inspection", "zhao/_osha_inspection"]
items=["osha_inspection"]
#_osha_inspection(folders, items)


#_osha_violation
folders=["zhao/data/osha_violation", "zhao/_osha_violation"]
items=["osha_violation"]
#_osha_violation(folders, items)


#_osha_accident_injury
folders=["zhao/data/osha_accident_injury", "zhao/_osha_accident_injury"]
items=["osha_accident_injury", "osha_accident_injury"]
#_osha_accident_injury(folders, items)


#merge osha_inspection with osha_violation
folders=["zhao/_osha"]
items=["osha_inspection_osha_violation"]
left_path="zhao/_osha_inspection/osha_inspection"
left_ons=["activity_nr"]
right_path="zhao/_osha_violation/osha_violation"
right_ons=["activity_nr"]
how="left"
validate="1:1"
#_pd_merge(folders, items, left_path, left_ons, right_path, right_ons, how, validate)


#merge osha_inspection_osha_violation with osha_accident_injury
folders=["zhao/_osha"]
items=["osha_inspection_osha_violation_osha_accident_injury"]
left_path="zhao/_osha/osha_inspection_osha_violation"
left_ons=["activity_nr"]
right_path="zhao/_osha_accident_injury/osha_accident_injury"
right_ons=["rel_insp_nr"]
how="left"
validate="1:1"
#_pd_merge(folders, items, left_path, left_ons, right_path, right_ons, how, validate)








#print
print("done")




'''
#to do
analysis
introduction
stanford, lott


#real effects of campaign finance disclosure
jobs = election x contested x powerful mp x opaqueness  


#census by zip coode
https://www.census.gov/data/developers/data-sets/cbp-zbp/zbp-api.html
https://www2.census.gov/


eg. zip cali
https://sdmg.senate.ca.gov/zipcodedirectory


#uszips
#https://simplemaps.com/data/us-zips
#https://transition.fcc.gov/oet/info/maps/census/fips/fips.txt



#_icis_air
https://echo.epa.gov/files/echodownloads/ICIS-AIR_downloads.zip
https://echo.epa.gov/tools/data-downloads/icis-air-download-summary


#_icis_fec
https://echo.epa.gov/files/echodownloads/case_downloads.zip
https://echo.epa.gov/tools/data-downloads/icis-fec-download-summary


#_epa_air
https://echo.epa.gov/files/echodownloads/POLL_RPT_COMBINED_EMISSIONS.zip
https://echo.epa.gov/tools/data-downloads/air-emissions-download-summary


_tri_ra
https://enviro.epa.gov/facts/tri/form_ra_download.html, select all "total_release"
https://enviro.epa.gov/enviro/ef_metadata_html.ef_metadata_table?p_table_name=TRI_FORM_R&p_topic=TRI


#_ghgrp
https://enviro.epa.gov/envirofacts/ghg/search
https://www.epa.gov/enviro/greenhouse-gas-search-user-guide


#electproject
https://www.electproject.org/
https://dataverse.harvard.edu/dataverse/electionscience





alec
Columbia Academics Commons future charities regulation
https://onlinelibrary.wiley.com/doi/10.1111/1475-679X.12417
https://papers.ssrn.com/sol3/papers.cfm?abstract_id=3273502
giil horowitz ucla law school
Cinthia Schuman Ottinger
Marion Fremont-Smith
Debbie Archambeault
https://nonprofitquarterly.org/shifting-boundaries-nonprofit-regulation-enforcement-conversation-cindy-m-lott/
bonta scholars
 


'''
