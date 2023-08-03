

#imports
import os
import time
import json
import requests
import pandas as pd
from pathlib import Path
from zipfile import ZipFile



#functions
from _pd_utils import _folder_to_filestems


#vars
TIME_SLEEP=1
from _appkey import PROPUBLICA_API_KEY, LDA_API_KEY
HEADERS_PROPUBLICA={
    "X-API-Key": PROPUBLICA_API_KEY,
    }
BASEURL_PROPUBLICA="https://api.propublica.org/congress/v1"
HEADERS_LDA={
    "Authorization": LDA_API_KEY,
            }
from _fec import DICT_BULKS
BASEURL_FEC="https://www.fec.gov/files/bulk-downloads"
#start and stop
START_FEC=2000
STOP_FEC=2024
STEP_FEC=2
N_OBS_FEC = int((STOP_FEC-START_FEC)/STEP_FEC) + 1


#from url to json
def _url_to_jsonobject(url, headers):

    #response
    response=requests.get(
        url,
        headers=headers,
        )

    #status code
    status_code=response.status_code

    #if
    if status_code==200:

        #response text
        response_text=response.text

        #json object
        json_object=json.loads(response_text)

    #elif 
    elif status_code!=200:

        #print
        print(f"error: {status_code}")

        #json object
        json_object=None

    return json_object


#from url to df, iter by offset
def _url_to_offset(base_url, endpoint, filepath):

    #https://projects.propublica.org/api-docs/congress-api/other/

    #empty
    offset=0
    list_endpoint_data=list()

    #initialize i
    i=0

    #while
    while True:

        #url
        url=f"{base_url}?offset={offset}"

        #response
        response=requests.get(
            url,
            headers=HEADERS_PROPUBLICA,
            )
        
        #time sleep
        time.sleep(TIME_SLEEP)

        #if
        if response.status_code==200:

            #data
            response_json=response.json()

            #results
            results=response_json["results"][0]

            #lobbying representations
            endpoint_data=results[endpoint]

            #if
            if not endpoint_data:

                #break
                break

            list_endpoint_data.extend(endpoint_data)

            #update offset
            offset += 20

        #elif
        elif response.status_code!=200:
            
            #break
            break

        #print
        first_client=endpoint_data[0]["lobbying_client"]["name"]
        print(f"{i} - {offset} retrieved - {first_client}")

        #update i
        i+=1

    #create df
    df=pd.DataFrame(data=list_endpoint_data)

    #save
    df.to_csv(filepath, index=False)
    #'''


#collect pro publica data
base_url="https://api.propublica.org/congress/v1/lobbying/latest.json"
endpoint="lobbying_representations"
filepath=f"zhao/_congress/{endpoint}.csv"
#_url_to_offset(base_url, endpoint, filepath)


#fromurlsuffix to listurls
def _urlsuffix_to_listurls(url_suffix):

    #https://www.fec.gov/data/browse-data/?tab=bulk-data

    #initialize list urls
    list_urls=[None]*N_OBS_FEC

    #for
    i=0
    for year in range(START_FEC, STOP_FEC+STEP_FEC, STEP_FEC):

        #year 2digit
        year_2digit=str(year)[2:4]

        url=f"{BASEURL_FEC}/{year}/{url_suffix}{year_2digit}.zip"
        BASEURL_FEC

        #update list urls
        list_urls[i]=url

        i+=1

    return list_urls


#response to save
def _response_to_save(response, filestem, colnames):

    #write zip
    filepath=f"{filestem}.zip"
    with open(filepath, 'wb') as file_object:
        file_object.write(response.content)

    #extract zip
    filepath=f"{filestem}.zip"
    with ZipFile(filepath, "r") as zip_ref:

        #file list
        templist=zip_ref.namelist()
        tempfile=templist[0]

        #extract
        zip_ref.extract(tempfile)

        #rename
        p=Path(tempfile)
        p.rename(f"{filestem}.txt")

    #read txt
    filepath=f"{filestem}.txt"
    df=pd.read_csv(
        filepath,
        sep="|",
        names=colnames,
        dtype="string",
        nrows=1000,
        )

    #save csv
    filepath=f"{filestem}.csv"
    df.to_csv(
        filepath,
        index=False,
        quotechar='"',
        )


#from dictbulks to csvs
#folders=["zhao/_fec"]
def _dictbulks_to_csvs(folders):

    #https://www.fec.gov/data/browse-data/?tab=bulk-data

    #folders
    resources=folders[0]

    #n obs
    n_obs=len(DICT_BULKS)*(N_OBS_FEC)
    tot=n_obs-1

    #for
    i=0
    for j, bulk in enumerate(DICT_BULKS):

        #unpack
        folder=bulk["folder"]
        url_suffix=bulk["url_suffix"]
        colnames=bulk["colnames"]

        #mkdir
        directory_path=Path(f"{resources}/{folder}")
        directory_path.mkdir(exist_ok=True)

        #list urls
        list_urls=_urlsuffix_to_listurls(url_suffix)

        #for
        for k, url in enumerate(list_urls):

            #year 
            year_2digit=url[-6:-4]

            #file stem
            filestem=f"{resources}/{folder}/{folder}_{year_2digit}"

            #filepath
            filepath=f"{filestem}.zip"
            pathlib_filepath=Path(filepath)

            #if
            if not pathlib_filepath.is_file():

                #response
                response=requests.get(url)

                #status code
                status_code=response.status_code

                #if
                if status_code==200:

                    #response to save
                    _response_to_save(response, filestem, colnames)

                    #print
                    print(f"{i}/{tot} - {j} {folder} - year {year_2digit} - {url} - done")

                #elif
                elif status_code!=200:

                    #print
                    print(f"{i}/{tot} - {j} {folder} - year {year_2digit} - {url} - error")
                    print(f"status_code: {status_code}")

            #elif
            elif pathlib_filepath.is_file():
                    
                #print
                print(f"{i}/{tot} - {j} {folder} - year {year_2digit} - {url} - already done")
             
            #update i
            i+=1


#collect FEC data
folders=["zhao/_fec"]
#_dictbulks_to_csvs(folders)




print("done")





