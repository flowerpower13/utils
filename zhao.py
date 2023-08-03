

#imports
import io
import numpy as np
import pandas as pd


#functions
from _pd_utils import _groupby, _df_to_uniquecol
from _merge_utils import _fuzzymatch, _pd_merge
from _rdp import _search


#rdp
import eikon as ek
import refinitiv.dataplatform as rdp
import refinitiv.data as rd
from refinitiv.data.content import search
#right click on import "SymbolTypes", "Go to Definition"
from refinitiv.dataplatform.content.symbology.symbol_type import SymbolTypes
appkey="7203cad580454a948f17be1b595ef4884be257be"
ek.set_app_key(app_key=appkey)
rdp.open_desktop_session(app_key=appkey)
rd.open_session(app_key=appkey)



#variables
from _string_utils import encoding
#read PolOrgsFileLayout.doc
#codes=["H", "1", "D", "R", "E", "2", "A", "B", "F"] 
#read FullDataFile.txt
sep="|"  
lineterminator="\n"
tot=12748446
buffer_size=8192
#new csv separator
new_sep=","
quotechar='"'


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
            encoding=encoding,
            errors="strict",
            ) as file_object:

            #text
            text=new_sep.join(f'{quotechar}{x}{quotechar}' for x in columns) + lineterminator #add lineterminator
            file_object.write(text)



#irstxt to dfs
#folders=["_donations", "_irstxt_to_dfs"]
#items=["FullDataFile"]
def _irstxt_to_dfs(folders, items):

    #Download form data file (entire database of Forms 8871 and Forms 8872)
    #https://forms.irs.gov/app/pod/dataDownload/dataDownload
    #https://forms.irs.gov/app/pod/dataDownload/fullData 


    resources=folders[0]
    results=folders[1]

    resource=items[0]
    result=items[1]

    #variables
    len_sep=len(sep)

    #dict code and columnns
    dict_codecolumns=_codecolumns(resources)

    #create empty txts
    _new_txts(dict_codecolumns, results)

    #read
    filepath=f"{resources}/{resource}.txt"
    with open(
        file=filepath,
        mode="r",
        encoding=encoding,
        errors="strict",
        ) as file_object:

        for i, line in enumerate(file_object):

            #remove lineterminator character
            line=line.replace(lineterminator, "")

            #if line ends with sep
            if line.endswith(sep):

                #remove sep
                line=line[:-len_sep]

            #values
            values=line.split(sep)
            len_values=len(values)
            val0=values[0]

            #new line for csv
            new_line=new_sep.join(f'{quotechar}{x}{quotechar}' for x in values) + lineterminator #add lineterminator

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

                            text=new_line.encode(encoding)
                            buffered_file.write(text)

            #print
            print(f"{i}/{tot} - done")

            #try
            #if i==2: break


#folders=["_irstxt_to_dfs", "_unique_donors"]
#items=["A", "_unique_donors"]
def _unique_donors(folders, items):
    resources=folders[0]
    results=folders[1]

    resource=items[0]
    result=items[1]

    #read
    filepath=f"{resources}/{resource}.csv"
    df=pd.read_csv(
        filepath, 
        dtype="string",
        on_bad_lines='skip',
        )

    columns=["A__org_name", "A__contributor_name", "A__contributor_employer"]
    df=df[columns]

    #n_obs
    n_obs=len(columns)

    #col
    colname="donor"
    orig_col="original_col"

    #for
    frames=[None]*n_obs
    for i, col in enumerate(columns):

        #data
        data=df[col].to_list()

        #crate df_frame
        df_frame=pd.DataFrame(data, columns=[colname])

        #original column
        df_frame[orig_col]=col

        #updateframe
        frames[i]=df_frame
        
    #create df
    df=pd.concat(frames)    

    #drop na
    df=df.dropna()

    #unique values
    df=df.drop_duplicates(subset=colname)

    #lowercase
    df[colname]=df[colname].str.lower()

    #sort   
    df=df.sort_values(by=[orig_col, colname])

    #save
    filepath=f"{results}/{result}.csv"
    df.to_csv(filepath, index=False)


#contributors screen by ein
def _contributors_screen(folders, items):
    resources=folders[0]
    results=folders[1]

    resource=items[0]
    result=items[1]
    result_comp=items[2]

    #read
    filepath=f"{resources}/{resource}.csv"
    df=pd.read_csv(
        filepath, 
        dtype="string",
        #nrows=10**6,
        na_values=[""],
        keep_default_na=False,
        on_bad_lines='skip',
        )
    
    #lowercase col values
    for i, col in enumerate(df.columns):
        df[col]=df[col].str.lower()

    #vars
    organization_id="A__ein"
    contributor="A__contributor_name"
    employer="A__contributor_employer"
    company_involved="A__company_involved"
    employer_new="A__contributor_employer_new"

    #drop na
    df=df.dropna(subset=[organization_id])
    df=df.dropna(subset=[contributor])
    df=df.dropna(subset=[contributor, employer])

    #keep only attorneys general associations
    s=pd.to_numeric(df[organization_id])
    df=df[
        (s==134220019) | (s==464501717)
        ]
    
    #fillna
    nan="nan"
    df=df.fillna(nan)

    #substitute employer with nan
    dict_replace={
        "n/a": nan,
        "na": nan,
        "not available": nan,
        "not applicable": nan,
        "retired": nan,
        "attorney": nan,
        "vice president": nan,
        }
    df[employer_new]=df[employer].replace(dict_replace)

    #init series
    x0=df[contributor]
    x1=df[employer_new]

    condlist=[
        (x1==nan),
        (x1!=nan),
        ]   
    choicelist=[
        x0, 
        x1,
        ]
    condlist=[cond.astype("bool") for cond in condlist]
    y=np.select(condlist, choicelist, default="error")

    #outcome series
    df[company_involved]=y

    #save
    filepath=f"{results}/{result}.csv"
    df.to_csv(filepath, index=False)


#contributors aggregate by firm-year
def _contributors_aggregate(folders, items):
    resources=folders[0]
    results=folders[1]

    resource=items[0]
    result=items[1]

    #read
    filepath=f"{resources}/{resource}.csv"
    df=pd.read_csv(
        filepath, 
        dtype="string",
        #nrows=10**7,
        na_values=[""],
        keep_default_na=False,
        on_bad_lines='skip',
        )
    
    #lowercase col values
    for i, col in enumerate(df.columns):
        df[col]=df[col].str.lower()

    #old vars
    organization="A__ein"
    contributor="cusip"
    contributor_employer="A__contributor_employer"
    contribution_amount="A__contribution_amount"
    contribution_date="A__contribution_date"
    agg_contribution_ytd="A__agg_contribution_ytd"

    #fillna
    df=df.fillna("nan")

    #drop duplicates
    cols_duplicates=[organization, contributor, agg_contribution_ytd, contribution_date]
    df=df.drop_duplicates(subset=cols_duplicates)

    #date variables
    contributiondate_format="contribution_date"
    contribution_year="contribution_year"
    df[contributiondate_format]=pd.to_datetime(df[contribution_date], format="%Y%m%d")
    df[contribution_year]=pd.DatetimeIndex(df[contributiondate_format]).year
    #df["contribution_month"]=pd.DatetimeIndex(df[contributiondate_format]).month
    #df["contribution_day"]=pd.DatetimeIndex(df[contributiondate_format]).day

    #sort by name and date 
    cols_sort=[organization, contributiondate_format]
    df=df.sort_values(by=cols_sort)

    #groupby contributor-527-year
    df[contribution_amount]=pd.to_numeric(df[contribution_amount], errors="ignore")
    by=[contributor, organization, contribution_year]
    dict_agg_colfunctions={
        contribution_amount: [sum],
        }
    df=_groupby(df, by, dict_agg_colfunctions)

    #sort by name and date 
    cols_sort=[contributor, organization, "contribution_year"]
    df=df.sort_values(by=cols_sort)

    #save
    filepath=f"{results}/{result}.csv"
    df.to_csv(filepath
              , index=False)



#download data from https://forms.irs.gov/app/pod/dataDownload/dataDownload
folders=["zhao/_donations", "zhao/_irstxt_to_dfs"]
items=["FullDataFile", "_irstxt_to_dfs"]
#_irstxt_to_dfs(folders, items)


#https://medium.com/analytics-vidhya/supplier-name-standardization-using-unsupervised-learning-adb27bed9e0d
folders=["zhao/_irstxt_to_dfs", "zhao/_contributors_screen"]
items=["A", "A_screen", "companies"]
#_contributors_screen(folders, items)


#search companies cusips (donations)
folders=["zhao/_contributors_screen", "zhao/_search"]
items=["A_screen", "A_search"]
colname="A__company_involved"
#_search(folders, items, colname)


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
items=["A_search", "A_search_unmatched"]
colnames={
    "name": "A__company_involved",
    "identifier": "CUSIP",
    }
#_df_to_unmatchedcol(folders, items, colnames)


#manually match cusip to each name




#add cusip to donations
folders=["zhao/_finaldb"]
items=["_finaldb_donations_link"]
left="_contributors_screen/A_screen"
left_vars=["cusip"]
right="XXX"
right_vars=["cusip"]
how="inner"
validate="1:1"
#'''
#_pd_merge(folders, items, left, left_vars, right, right_vars, how, validate)


folders=["zhao/_finaldb", "zhao/_contributors_aggregate"]
items=["_finaldb_donations_link", "A_aggregate"]
#_contributors_aggregate(folders, items)


#merge compustat with donations


#add cusip to violations


#merge compustat/donations to violations


#litigation





rdp.close_session()
rd.close_session()
print("_rdp - done")


print("done")





#CONGRESSDATA APP
#https://cspp.ippsr.msu.edu/congress/


#LOBBYVIEW
#https://www.lobbyview.org/


#INTEGRITY WATCH
#https://data.integritywatch.eu/login


#FMINUS
#https://fminus.org/database/








#podcast
#history of campaign finance laws (from FEC to IRS) 
# 10.1146/annurev-lawsocsci-102612-133930
# 10.1146/annurev-lawsocsci-110316-113428
# how to create a pac https://www.fec.gov/help-candidates-and-committees/
# how to start a 527 https://www.irs.gov/charities-non-profits/political-organizations/user-guides-political-organizations-filing-and-disclosure-web-site
# how to start a 501 https://www.irs.gov/charities-non-profits/educational-resources-and-guidance-for-exempt-organizations
# IRS Audit Techniques Guides  https://www.irs.gov/charities-non-profits/audit-technique-guides-atgs-and-technical-guides-tgs-for-exempt-organizations
# R&D tax credit https://www.irs.gov/businesses/research-credit


#IRS 527
#https://uscode.house.gov/ (Jump to: Title 26, Section 501, 527)
#https://www.irs.gov/charities-non-profits/political-organizations
#https://www.irs.gov/charities-non-profits/political-organizations/political-organizations-resource-materials
#https://www.irs.gov/charities-non-profits/audit-technique-guides-atgs-and-technical-guides-tgs-for-exempt-organizations


#IRS 501
#https://www.irs.gov/charities-non-profits/tax-exempt-organization-search
#https://www.irs.gov/charities-non-profits/tax-exempt-organization-search-bulk-data-downloads
#https://www.irs.gov/charities-non-profits/exempt-organizations-business-master-file-extract-eo-bmf
#https://www.irs.gov/charities-non-profits/audit-technique-guides-atgs-and-technical-guides-tgs-for-exempt-organizations


#FEC
#https://www.fec.gov/help-candidates-and-committees/
#https://www.fec.gov/data/legal/statutes/


#NBER
#https://www.nber.org/research/data/irs-form-990-data
#https://nccs.urban.org/


#CPA
#https://www.politicalaccountability.net/
#https://www.politicalaccountability.net/reports/cpa-reports/527data


#CONGRESSMEN PERSONAL DISCLOSURES
#https://disclosures-clerk.house.gov/PublicDisclosure/FinancialDisclosure
#https://www.opensecrets.org/api/?method=memPFDprofile&output=doc


#JUDGES PERSONAL DISCLOSURES
#https://pub.jefs.uscourts.gov/


#US OGE
#https://www.oge.gov/web/oge.nsf/Officials%20Individual%20Disclosures%20Search%20Collection?OpenForm


#PROCUREMENT
#http://usaspending.gov/


#OPENSECRETS
#https://www.opensecrets.org/open-data/api-documentation
#https://github.com/opensecrets/python-crpapi


#PRO PUBLICA
#https://projects.propublica.org/api-docs/congress-api/


#LDA
#https://lda.senate.gov/api/


#CONGRESS MEMBERS
#https://www.congress.gov/members


#VIOLATION TRACKER
#https://violationtracker.goodjobsfirst.org/?company_op=starts&company=&offense_group=&agency_code=OSHA
#MULTI, MN-AG, USAO


#CONGRESSMEN MISCONDUCT
#https://github.com/govtrack/misconduct/blob/master/misconduct-instances.csv






