

#import
import pandas as pd
from pathlib import Path



#functions
from _concat import _folder_to_filestems
from _string_utils import _txt_to_tokens, _tokensbags_to_scores
from _pd_utils import _pd_DataFrame, _dict_to_valscols


#variables
from _dict_bags import dict_bags
error="???"


def _libtxt_to_libdf(file_path):

    #read
    with open(
        file_path, 
        mode='r', 
        encoding="utf-8-sig", 
        errors="ignore",
        ) as file_object:
        text=file_object.read()
    
    #text
    list_tokens, n_words, list_bigrams, n_bigrams = _txt_to_tokens(text)

    #empty lists
    tot=n_bigrams-1
    TFs=[None]*n_bigrams

    for i, bigram in enumerate(list_bigrams):

        #TF
        count=list_bigrams.count(bigram)
        TF=count/n_bigrams

        #update
        TFs[i]=TF

        print(f"{i}/{tot} - {bigram} - done")

    #create df
    values=[
        list_bigrams, 
        TFs, 
        ]
    columns=[
        "bigram", 
        "TF", 
        ]
    df=_pd_DataFrame(values, columns)

    #remove duplicates
    df=df.drop_duplicates(subset="bigram")

    #sort
    df=df.sort_values(by="bigram")

    #set bigrams
    set_bigrams=set(list_bigrams)

    return df, set_bigrams


def _topicbigrams(folders, items):
    resources=folders[0]
    results=folders[1]

    resource_p=items["_topicbigrams_p"][0]
    resource_n=items["_topicbigrams_n"][0]
    result_pn=items["_topicbigrams_p"][1]
    result_np=items["_topicbigrams_n"][1]

    #file path
    file_path_p=f"{resources}/{resource_p}.txt"
    file_path_n=f"{resources}/{resource_n}.txt"

    df_p, set_bigrams_p =_libtxt_to_libdf(file_path_p)
    df_n, set_bigrams_n =_libtxt_to_libdf(file_path_n)

    df_pn=df_p[~df_p["bigram"].isin(set_bigrams_n)]
    df_np=df_n[~df_n["bigram"].isin(set_bigrams_p)]

    #save
    file_path_pn=f"{results}/{result_pn}.csv"
    df_pn.to_csv(file_path_pn, index=False)
    file_path_np=f"{results}/{result_np}.csv"
    df_np.to_csv(file_path_np, index=False)


#txt to hassan scores
def _txt_to_hassan(text):

    #text
    list_tokens, n_words, list_bigrams, n_bigrams = _txt_to_tokens(text)

    #list topic bigrams
    df_pn=pd.read_csv("_topicbigrams/_topicbigrams_pn.csv", dtype="string")
    set_topicbigrams_pn=set(df_pn["bigram"])
    df_np=pd.read_csv("_topicbigrams/_topicbigrams_np.csv", dtype="string")
    set_topicbigrams_np=set(df_np["bigram"])

    #list synonyms and sentiment
    set_synonyms_uncertainty=dict_bags["synonyms_uncertainty"]
    set_loughran_positive=dict_bags["loughran_positive"]
    set_loughran_negative=dict_bags["loughran_negative"]

    #sum
    PRisk_sum=0
    Risk_sum=0
    NPRisk_sum=0
    PSentiment_sum=0
    Sentiment_sum=0
    NPSentiment=0

    #for
    for i, bigram in enumerate(list_bigrams):

        #if contained in topic_pn
        if bigram in set_topicbigrams_pn:
            x=1
            #TF
            TF_pn=df_pn["TF"].to_numpy()[df_pn["bigram"].to_numpy()==bigram].item()

            #if within 10 words from uncertainty!!!!!!!!!!!!!!!!!!!!!!!!!
            #

           #if contained in uncertainty
            if bigram in set_synonyms_uncertainty:
                r=1
            else:
                r=0
     
            #if within 10 words from sentiment!!!!!!!!!!!!!!!!!!!!!!!!!
            #


            #if contained in sentiment
            if bigram in set_loughran_positive:
                s=+1
            elif bigram in set_loughran_negative:
                s=-1
            else:
                s=0

        else:

            #dummy x
            x=0

        #update i
        PRisk_i=x*y*TF_pn
        Risk_i=r
        #NPRisk_i=
        #PSentiment_i=x*TF_pn*s
        Sentiment_i=s
        #NPSentiment_i=

        #update sum
        PRisk_sum+=PRisk_i
        Risk_sum+=Risk_i
        #NPRisk_sum+=NPRisk_i
        #PSentiment_sum+=PSentiment_i
        Sentiment_sum+=Sentiment_i
        #NPSentiment_sum+=NPSentiment_i
    
    PRisk=PRisk_sum/n_bigrams
    Risk=Risk_sum/n_bigrams
    NPRisk=0
    PSentiment=0
    Sentiment=Sentiment_sum/n_bigrams
    NPSentiment=0

    #create empty dict for storing data
    dict_data={
        "PRisk":        PRisk,
        "Risk":         Risk,
        "NPRisk":       NPRisk,
        "PSentiment":   PSentiment,
        "Sentiment":    Sentiment,
        "NPSentiment":  NPSentiment,
        "n_bigrams":    n_bigrams,
        "n_words":      n_words,
        }

    #print(dict_data)
    
    return dict_data


#from file to converted
def _file_to_converted(file, file_stem, output, i, tot):
    try:
        with open(
            file=file, 
            mode='r', 
            encoding="utf-8", 
            #errors="ignore", 
            ) as f:
            text=f.read()

        if text==error:

            #converted
            converted=False

            #print
            print(f"{i}/{tot} - {file_stem} - error")

        elif text!=error:
            #converted
            converted=True

            #values and columns
            values=[
                [file_stem],
                ]
            columns=[
                "file_stem", 
                ]

            #txt to count
            dict_data=_txt_to_hassan(text)  
            Vs, Ks = _dict_to_valscols(dict_data)
            values+=Vs
            columns+=Ks

            #create df
            df=_pd_DataFrame(values, columns)
            df.to_csv(output, index=False)

            #print(df)

            #print
            print(f"{i}/{tot} - {file_stem} - done")

    except Exception as e:

        #converted
        converted=False

        #print
        print(f"{i}/{tot} - {file_stem} - exception")
        print(e)
    
    return converted


#compute hassan scores
'''
folders=["_pdfs_to_txts", "_txts_to_hassan"]
items=["_topicbigrams/_topicbigrams", "_txts_to_hassan"]
'''
def _txts_to_hassan(folders, items):
    resources=folders[0]
    results=folders[1]

    resource=items[0]
    result=items[1]

    #colname
    colname_filestems="file_stem"

    #resources
    files, file_stems=_folder_to_filestems(resources)

    #n obs
    n_obs=len(files)
    tot=n_obs-1

    #empty lists
    file_stems=[None]*n_obs
    converteds=[None]*n_obs

    for i, file in enumerate(files):
        file_stem=file.stem

        output=Path(f"{results}/{file_stem}.csv")

        #file is NOT present
        if not output.is_file():

            #converted
            converted=_file_to_converted(file, file_stem, output, i, tot)

        #file is present
        elif output.is_file():

            #converted
            converted=True

            #print
            print(f"{i}/{tot} - {file_stem} - already done")

        #fill lists
        file_stems[i]=file_stem
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

    #sort
    df=df.sort_values(by=colname_filestems)

    #save
    file_path=f"{results}/{result}.csv"
    df.to_csv(file_path, index=False)


