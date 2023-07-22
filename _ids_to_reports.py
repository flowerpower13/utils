

#imports
import time
import requests
import pandas as pd
from pathlib import Path
from bs4 import BeautifulSoup
from urllib.parse import urlparse


#functions
from _pd_utils import _pd_DataFrame
from _selenium_requests import _requests_text, _requests_dwn


#set requests variables
auth=("UnivBocc", "UbC4er53")
HEADERS={
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
    "Accept-Encoding": "gzip, deflate, br",
    "Accept-Language": "en-US,en;q=0.6",
    "Cache-Control": "max-age=0",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36",
    }
timeout=10
time_sleep=5


#find by link
def _findby_link(soup, base, results, ID):
    
    files_to_download = list()
    for link_value in soup.find_all("a"):

        #get link
        link=link_value.get("href")

        #if
        matches=[".pdf", ".doc", ".docx"]
        if any([x in link for x in matches]):

            #url
            url=f"{base}{link}"

            #image
            image=url.split("/")[-1] #pick last element

            #output
            output=Path(f"{results}/{ID}_{image}")

            files_to_download.append((url, output))

    return files_to_download
    

#find by img
def _findby_img(soup, base, results, ID):

    files_to_download = list()
    for title_value in soup.find_all("img"):

        #get title
        title=title_value.get("title")

        #if
        target="Waiting for "
        if target in title:

            #remove words
            image=title.replace(target, "")

            #replace "Y" with image number, replace "detail" with "deliver"
            #https://www.connect4.com.au/fcas/casdetail.cgi?img=Y
            #https://www.connect4.com.au/fcas/casdeliver.cgi?img=02253892
            deliver="fcas/casdetail.cgi?img=".replace("detail", "deliver")
            url=f"{base}{deliver}{image}"

            #output
            output=Path(f"{results}/{ID}_{image}.pdf")
            files_to_download.append((url, output))

    return files_to_download




#generate n random digits
#https://stackoverflow.com/questions/2673385/how-to-generate-a-random-number-with-a-specific-amount-of-digits
from random import randint
def random_with_N_digits(n):
    
    range_start = 10**(n-1)
    range_end = (10**n)-1

    return randint(range_start, range_end)


#download pdf report
def _dwn_report(i, tot, results, ID):

    #replace "X" with record number, change "random=" with 13 random digits
    #https://www.connect4.com.au/subscribers/expertreports/X/related_documents/?height=150&width=400&random=1609242245840
    #https://www.connect4.com.au/subscribers/expertreports/4450/related_documents/?height=150&width=400&random=1609242245840
    url0="https://www.connect4.com.au/subscribers/expertreports/"
    url1=ID
    url2="/related_documents/?height=150&width=400&random="
    url3=random_with_N_digits(13)
    url=f"{url0}{url1}{url2}{url3}" 
    parsed_uri=urlparse(url)
    base="{uri.scheme}://{uri.netloc}/".format(uri=parsed_uri)  

    #request text
    text=_requests_text(url, auth, HEADERS, timeout, time_sleep)

    #soup
    soup=BeautifulSoup(text, "html.parser")

    try:

        #find by link
        files_to_download=_findby_link(soup, base, results, ID)

        #if not working
        if len(files_to_download)==0:

            #find by img   
            files_to_download=_findby_img(soup, base, results, ID)

        #if working
        if len(files_to_download)!=0: 

            #save each file to output
            for j, file in enumerate(files_to_download):

                #url output
                url, output = file

                #if file NOT present
                if not output.is_file():

                    #download single report
                    _requests_dwn(url, output, auth, HEADERS, timeout, time_sleep)

                    #converted
                    converted=True

                    #print
                    print(f"{i}/{tot} - {ID} - {j}/? - {output} - done")

                #if file present
                elif output.is_file():

                    #converted
                    converted=True
            
                    #print
                    print(f"{i}/{tot} - {ID} - {j}/? - {output} - already done")

        #if still not working
        elif len(files_to_download)==0:

            #converted
            output="none"
            converted=False

            #print
            print(f"{i}/{tot} - {ID} - none")

            #return

    #except
    except Exception as e:

        #converted
        output="exception"
        converted=False

        #print
        print(e)
        print(f"{i}/{tot} - {ID} - exception")

    ID_output=str(output).replace(f"{results}\\", "").replace(".pdf", "").replace(".doc", "")
    return ID_output, converted 




        


#compute counts
#folders=["_ids", "_ids_to_reports" ]
#items=["_ids_to_reports"]
def _ids_to_reports(folders, items):
    resources=folders[0]
    results=folders[1]

    resource=items[0]
    result=items[1]

    #colname
    colname_IDs="Id"
    colname_files="file"

    #read csv
    df=pd.read_csv(f"{resources}/{resource}.csv", dtype="string")

    #lists
    df=df.sort_values(by=colname_IDs)
    IDs=df[colname_IDs].to_list()
    #IDs=IDs[3533:]

    #n obs
    n_obs=len(IDs)
    tot=n_obs-1

    #empty lists
    IDss=list()
    ID_outputs=list()
    converteds=list()

    #for
    for i, ID in enumerate(IDs):

        #download report
        ID_output, converted = _dwn_report(i, tot, results, ID)

        IDss.append(ID)
        ID_outputs.append(ID_output)
        converteds.append(converted)

    #create df
    values=[
            IDss,
            ID_outputs, 
            converteds, 
            ]
    columns=[
            "ID"
            "ID_output", 
            "converted", 
            ]
    df=_pd_DataFrame(values, columns)

    #sort
    df=df.sort_values(by="ID_output")

    #save
    file_path=f"{results}/{result}.csv"
    df.to_csv(file_path, index=False)


