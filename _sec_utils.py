
import time
import requests
import pandas as pd
from pathlib import Path


#preliminary
encoding="utf-8"


#source
#https://wrds-www.wharton.upenn.edu/pages/get-data/wrds-sec-analytics-suite/wrds-sec-filings-index-data/sec-filings-index/


#make request
def _request(i, url):
    #timeout
    timeout=10

    #headers
    HEADERS={
        "User-Agent": f"name{i} email{i}@outlook.com",
        "Accept-Encoding": "gzip, deflate",
        "Host": "www.sec.gov"
        }

    #request
    r=requests.get(url, timeout=timeout, headers=HEADERS)
    text=r.text

    #close
    r.close()

    return text

#download each filing
def _dwn(i, url, outcome, tot):
    try:    
        #requests
        text=_request(i, url)

        #open file
        with open(outcome, "w", encoding=encoding) as f:
            f.write(text)

        #converted
        converted=True

        #print
        print(f"{i}/{tot} - {url} - done")

            
    except Exception as e:
        #converted
        converted=False

        #print
        print(f"{i}/{tot} - {url} - error")
        print(e)
    
    return converted

    
#from secfile to filings
#WRDS SEC Analytics Suite - SEC Filings Index 
#https://wrds-www.wharton.upenn.edu/pages/get-data/wrds-sec-analytics-suite/wrds-sec-filings-index-data/sec-filings-index/
#https://www.sec.gov/developer
#https://www.sec.gov/edgar/sec-api-documentation
#https://www.sec.gov/os/accessing-edgar-data
#https://developer.edgar-online.com/docs
def _secfile_to_filings(folders, items, form):
    resources=folders[0]
    results=folders[1]

    resource=items[0]
    result=items[1]

    #only a form
    result=f"{result}_{form}"

    #load df
    file_path=f"{resources}/{resource}.csv"
    df=pd.read_csv(
        file_path, 
        dtype="string", 
        #nrows=pow(10, 9),
        )

    #colname filings
    colname_form="Form"
    colname_url="FName"

    #or 1. 
    #keep only if equal to requested filing (10-K only)
    df=df[df[colname_form]==form]

    #or 2. 
    #keep multiple types of filing with same root (e.g., 10-K and 10-K/A)
    #df=df.loc[df[colname_form].str.contains(filing)]

    #trials
    df=df.head(1)

    #file urls
    file_urls=df[colname_url].tolist()

    #n obs
    n_obs=len(file_urls)
    tot=n_obs-1

    #new cols
    urls=[None]*n_obs
    html_urls=[None]*n_obs
    converteds=[None]*n_obs

    #base url
    base_url="https://www.sec.gov/Archives"

    #sleep
    time_sleep=1/10

    for i, file_url in enumerate(file_urls):

        #url
        url=f"{base_url}/{file_url}"
        html_url=url.replace(".txt", "-index.htm")

        #file path
        file_stem=file_url.replace("edgar/data/","").replace("/","_")
        outcome=Path(f"{results}/{file_stem}")
        
        if not outcome.is_file():

            #download
            converted=_dwn(i, url, outcome, tot)
            
            #sleep
            time.sleep(time_sleep)

        elif outcome.is_file():
            size=outcome.stat().st_size

            if size==0:

                #download
                converted=_dwn(i, url, outcome, tot)

                #sleep
                time.sleep(time_sleep)

            elif size>0:

                #converted
                converted=True

                #print
                print(f"{i}/{tot} - {url} - already done")


        urls[i]=url
        html_urls[i]=html_url
        converteds[i]=converted

    #converteds
    df["txt_url"]=urls
    df["html_url"]=html_urls
    df["converted"]=converteds

    file_path=f"{results}/{result}.csv"
    df.to_csv(file_path, index=False)

