
import re
import time
import html
import requests
import numpy as np
import pandas as pd
from pathlib import Path
from bs4 import BeautifulSoup
from nltk import corpus, tokenize


#functions
from _pd_utils import _pd_DataFrame


#preliminary
encoding="utf-8"
words=set(corpus.words.words())

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
#folders=["_secfile", "_secfile_to_filings"]
#items=["_secfile", "_secfile_to_filings"]
#forms=["DEFM14A", "DEF 14A", "DEFS14A", "DEFR14A"]
def _secfile_to_filings(folders, items, forms):
    resources=folders[0]
    results=folders[1]

    resource=items[0]
    result=items[1]

    #only a form
    string_form="_".join(forms)
    result=f"{result}_{string_form}"

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

    #keep only if belongs to list
    df=df[df[colname_form].isin(forms)]

    #trials
    #df=df.head(1)

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


def _getDistinctDocumentsIndexes(textLines):

    documentsIndexes = [] 

    for lineIndex, line in enumerate(textLines):

        if line.startswith("<document>"):
            startLine = lineIndex

        elif line.startswith("</document>"):
            documentsIndexes.append((startLine,lineIndex))

    return documentsIndexes


def _isAnHtmlDocument(text):

    return "<html>" in text or '<?xml version="1.0" encoding="utf-8"?><html' in text


def _removeHeadingTags(text):

    headingEnd = text.find("</head><body")

    if headingEnd >= 0:
        text = text[headingEnd:]

    xmlHeading = text.find("</ix:header>")

    if xmlHeading >= 0:
        text = text[xmlHeading:]

    return text


def markupToText(rawText):

    #lowercase
    rawText=rawText.lower()
    textLines = rawText.splitlines()
    documentsIndexes = _getDistinctDocumentsIndexes(textLines)
    documentsIndexes = [doc for doc in documentsIndexes if _isAnHtmlDocument("".join(textLines[doc[0]:doc[1]]))]
    
    cleanedText = ""

    for doc in documentsIndexes:

        documentText = "\n".join(textLines[doc[0]:doc[1]])
        documentText = html.unescape(documentText)
        documentText = _removeHeadingTags(documentText)
        
        soup = BeautifulSoup(documentText, 'html.parser')
        cleanedText += soup.text
     
    pattern=r'\n+'
    repl='\n'
    string=cleanedText
    text=re.sub(pattern, repl, string)

    pattern=r"[^ ^\n]+:[^ ^\n]+"
    repl=""
    string=text
    text=re.sub(pattern, repl, string)

    return text


#from markup to txt
def _markup_to_txt(file_stem, input, outcome, i, tot):
          
    try:
        #read
        with open(
            file=input, 
            mode="r",
            encoding=encoding,
            ) as f:
            text=f.read()

        text=markupToText(text)

        #write
        with open(
            file=outcome, 
            mode="w", 
            encoding=encoding,
            ) as f:
            f.write(text)

        #converted
        converted=True

        #print
        print(f"{i}/{tot} - {file_stem} - done")

    except Exception as e:

        #converted
        converted=False

        #print
        print(f"{i}/{tot} - {file_stem} - error")
        print(e)

    return converted


#from markup to txt
def _markup_to_txt(file_stem, input, outcome, i, tot):
          
    try:
        #read
        with open(
            file=input, 
            mode="r",
            encoding=encoding,
            ) as f:
            text=f.read()

        text=markupToText(text)
      
        #other
        #remove non-english words
        #word_tokens=tokenize.word_tokenize(text)
        #filtered_sentence=[w for w in word_tokens if w in words]
        #text=" ".join(filtered_sentence)
        #remove singleton letters
        #word_tokens=tokenize.word_tokenize(text)
        #filtered_sentence=[w for w in word_tokens if len(w)>1] 
        #text=" ".join(filtered_sentence)
        #remove punctuation
        #text=re.sub(r"[^\w\s]", "", text)
        #remove whitespaces
        #text=text.strip()
        #text=re.sub(r"\s+", " ", text)

        #write
        with open(
            file=outcome, 
            mode="w", 
            encoding=encoding,
            ) as f:
            f.write(text)

        #converted
        converted=True

        #print
        print(f"{i}/{tot} - {file_stem} - done")

    except Exception as e:

        #converted
        converted=False

        #print
        print(f"{i}/{tot} - {file_stem} - error")
        print(e)

    return converted


#convert html to text
#folders=["_secfile_to_filings", "_html_to_txt" ]
#items=["_html_to_txt"]
def _html_to_txt(folders, items):
    resources=folders[0]
    results=folders[1]

    result=items[0]

    #file stems
    p=Path(resources).glob('**/*')
    file_stems=[
        x.stem for x in p 
        if x.is_file() and not x.suffix==".csv"
        ]

    #obs
    n_obs=len(file_stems)
    tot=n_obs-1

    #empty lists
    converteds=[None]*n_obs

    #iterate
    for i, file_stem in enumerate(file_stems):

        #input and output outcome
        input=Path(f"{resources}/{file_stem}.txt")
        outcome=Path(f"{results}/{file_stem}.txt")
        
        #file is NOT present
        if not outcome.is_file():

            #markup to txt
            converted=_markup_to_txt(file_stem, input, outcome, i, tot)

        #file is present
        elif outcome.is_file():
            
            converted=True

            print(f"{i}/{tot} - {file_stem} - already done")

        #fill values    
        converteds[i]=converted

    #create df
    values=[
        file_stems,
        converteds,
        ]
    columns=[
        "file_stem", 
        "converted",
        ]
    df=_pd_DataFrame(values, columns) 

    #save
    file_path=f"{results}/{result}.csv"
    df.to_csv(file_path, index=False)

