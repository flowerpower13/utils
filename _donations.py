

#imports
import io
import numpy as np
import pandas as pd
import dask.dataframe as dd


#variables
from _string_utils import encoding
#read PolOrgsFileLayout.doc
codes=["H", "1", "D", "R", "E", "2", "A", "B", "F"] 
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
    file_path=f"{resources}/{filename}.csv"
    df=pd.read_csv(file_path, dtype="string")

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
        file_path=f"{results}/{code}.csv"
        with open(
            file=file_path,
            mode="w",
            encoding=encoding,
            errors="strict",
            ) as file_object:

            #text
            text=new_sep.join(f'{quotechar}{x}{quotechar}' for x in columns) + lineterminator #add lineterminator
            file_object.write(text)


#txt to df
def _txtcodes_to_csvs(resources, resource, results):

    #variables
    len_sep=len(sep)

    #dict code and columnns
    dict_codecolumns=_codecolumns(resources)

    #create empty txts
    _new_txts(dict_codecolumns, results)

    #read
    file_path=f"{resources}/{resource}.txt"
    with open(
        file=file_path,
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
                    file_path=f"{results}/{code}.csv"
                    with open(
                        file=file_path,
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



#irstxt to dfs
#folders=["donations", "_irstxt_to_dfs"]
#items=["FullDataFile", "_irstxt_to_dfs"]
def _irstxt_to_dfs(folders, items):
    resources=folders[0]
    results=folders[1]

    resource=items[0]
    result=items[1]

    #txt to txts
    _txtcodes_to_csvs(resources, resource, results)

    #txts to dfs





#IRS 527
#https://en.wikipedia.org/wiki/527_organization
#https://www.irs.gov/charities-non-profits/political-organizations/political-organization-filing-and-disclosure
#https://www.politicalaccountability.net/reports/cpa-reports/527data
#https://www.politicalmoneyline.com/

#lobbying
#https://lda.senate.gov/system/public/  
#https://www.lobbyview.org/

#congress
#https://projects.propublica.org/api-docs/congress-api/

#personal disclosure
#https://disclosures-clerk.house.gov/PublicDisclosure/FinancialDisclosure
#https://github.com/govtrack/misconduct/blob/master/misconduct-instances.csv

#procurement data
#usaspending.gov

#Charities
#https://www.irs.gov/charities-non-profits/tax-exempt-organization-search
#https://www.foundationsearch.com/

#FEC
#https://www.fec.gov/data/browse-data/?tab=bulk-data

#Climate change litigation   
#https://climate.law.columbia.edu/content/climate-change-litigation

#EU
#https://www.europarl.europa.eu/thinktank/en/document/IPOL_STU(2021)694836
#https://appf.europa.eu/appf/en/guidance/donations-and-contributions 
#https://appf.europa.eu/appf/en/donations-and-contributions 
#https://parlamento18.camera.it/199 





folders=["donations", "_irstxt_to_dfs"]
items=["FullDataFile", "_irstxt_to_dfs"]
_irstxt_to_dfs(folders, items)


print("done")