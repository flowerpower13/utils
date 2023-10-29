

#virtual env
'''
#create
virtualenv nagler

#activate
.\nagler\Scripts\Activate.ps1

#close
deactivate
'''


#import
import pandas as pd


#function


def _screengvkey():

    #read_csv args
    filepath="nagler/_examples/_finaldb.csv"
    usecols=[
        "file_stem",
        "gvkey",
        "prisk",
        ]
    df=pd.read_csv(
        filepath,
        usecols=usecols, 
        dtype="string",
        )
    
    #to_numeric
    df["gvkey"]=pd.to_numeric(df["gvkey"])

    #right
    filepath="nagler/_examples/gvkeys_srisk.csv"
    usecols=[
        "gvkey",
        "gsubind",
        "sic",
        "iso",
        ]
    right=pd.read_csv(
        filepath,
        usecols=usecols, 
        dtype="string",
        )
    
    #to_numeric
    right["gvkey"]=pd.to_numeric(right["gvkey"])
    isin_values=right["gvkey"].to_list()

    #isin
    df=df[df["gvkey"].isin(isin_values)]

    #merge
    how="left"
    left_ons=["gvkey"]
    right_ons=["gvkey"]
    suffixes=('_left', '_right')
    indicator=f"_merge"
    validate="m:1"
    df=pd.merge(
        left=df,
        right=right,
        how=how,
        left_on=left_ons,
        right_on=right_ons,
        suffixes=suffixes,
        indicator=indicator,
        validate=validate,
        )

    #to_numeric
    df["prisk"]=pd.to_numeric(df["prisk"])

    #sort
    df=df.sort_values(
        by="prisk",
        ascending=False,
        )
    
    #reset_index
    df=df.reset_index(drop=True)

    #try
    #df=df.head(10)

    #to_csv
    filepath="nagler/_examples/_finaldb_screengvkey.csv"
    df.to_csv(
        filepath,
        index=False,
        )

    #return
    return df


#text_to_extractlist
def text_to_extractlist(text, gram, n_chars=200):

    #init extract
    extract_list = []

    #index
    index=0

    #while
    while index < len(text):

        #find
        index = text.find(gram, index)

        #if
        if index == -1:

            #break
            break

        #index
        start_index = max(0, index - n_chars)
        end_index = min(
            index + len(gram) + n_chars, 
            len(text),
            )

        #text
        extract_text = text[start_index:end_index]

        #replace
        extract_text=extract_text.replace(gram, f"###{gram}###")

        #append
        extract_list.append(extract_text)
        index += len(gram)

    #if
    if extract_list:
        extract_list=str(extract_list)

    #elif
    elif not extract_list:
        extract_list=None

    #return
    return extract_list


#find examples
def _find_examples():

    #_screengvkey
    df=_screengvkey()

    #resources
    resources="nagler/_pdfs_to_txts"

    #grams
    grams=[
        "domestic debt",
        "fiscal policy",
        "government debt",
        "public debt",
        "public sector",
        "sovereign debt",
        "sovereign default",
        ]

    #for
    for j, gram in enumerate(grams):
        df[gram]=None

    #tot
    tot=len(df)-1

    #for
    for index, row in df.iterrows():

        filestem=row["file_stem"]

        #filepath
        filepath=f"{resources}/{filestem}.txt"

        #open
        with open(
            file=filepath, 
            mode="r",
            ) as file:
            text=file.read()

        #for
        for j, gram in enumerate(grams):

            #text_to_extractlist
            extract_list=text_to_extractlist(text, gram)

            #loc
            df.loc[index, gram]=extract_list

        #print
        print(f"{index}/{tot} - {filestem} - done")


    #to_csv
    filepath="nagler/_examples/_finaldb_screengvkey_examples.csv"
    df.to_csv(
        filepath,
        index=False,
        )
    

#run
_find_examples()




print("done")

