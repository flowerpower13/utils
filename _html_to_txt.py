
import re
import html
import numpy as np
import pandas as pd
from pathlib import Path
from bs4 import BeautifulSoup


#functions
from _pd_DataFrame import _pd_DataFrame


#preliminaries
encoding="utf-8"


def _getDistinctDocumentsIndexes(textLines):
    documentsIndexes = [] 
    for lineIndex, line in enumerate(textLines):
        if line.startswith("<DOCUMENT>"):
            startLine = lineIndex
        elif line.startswith("</DOCUMENT>"):
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
def _markup_to_txt(text):
    #take only text
    text=markupToText(text)

    #lowercase
    text=text.lower()
    
    '''
    #remove non-english words
    words=set(corpus.words.words())
    word_tokens=tokenize.word_tokenize(text)
    filtered_sentence=[w for w in word_tokens if w in words]
    text=" ".join(filtered_sentence)

    #remove singleton letters
    word_tokens=tokenize.word_tokenize(text)
    filtered_sentence=[w for w in word_tokens if len(w)>1] 
    #text=" ".join(filtered_sentence)

    #remove punctuation
    #text=re.sub(r"[^\w\s]", "", text)

    #remove whitespaces
    #text=text.strip()
    #text=re.sub(r"\s+", " ", text)
    #'''

    return text


#convert html to text
def _html_to_txt(folders, items):
    resources=folders[0]
    results=folders[1]

    result=items[0]

    p=Path(resources).glob('**/*')
    files=[x for x in p if x.is_file() and not x.name==f"{resources}.csv"]

    n_obs=len(files)
    tot=n_obs-1

    file_stems=[None]*n_obs
    converteds=[None]*n_obs

    for i, file in enumerate(files):
        file_stem=file.stem

        file_path=f"{results}/{file_stem}.txt"
        path=Path(file_path)
        
        if not path.is_file():
            try:
                file_path=f"{resources}/{file_stem}.txt"
                with open(
                    file=file_path, 
                    mode="r",
                    encoding=encoding,
                    ) as file_object:
                    text=file_object.read()

                text=_markup_to_txt(text)

                file_path=f"{results}/{file_stem}.txt"
                with open(
                    file=file_path, 
                    mode="w", 
                    encoding=encoding,
                    ) as file_object:
                    file_object.write(text)

                converted=True

                print(f"{i}/{tot} - {file_stem} - done")

            except Exception as e:
                converted=False

                print(f"{i}/{tot} - {file_stem} - error")
                print(e)
        elif path.is_file():
            converted=True

            print(f"{i}/{tot} - {file_stem} - already done")

            
        file_stems[i]=file_stem
        converteds[i]=converted

    values=[
        file_stems,
        converteds,
        ]
    columns=[
        "file_stem", 
        "converted",
        ]

    df=_pd_DataFrame(values, columns) 

    file_path=f"{results}/{result}.csv"
    df.to_csv(file_path, index=False)

