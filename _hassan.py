

#import
import collections
import pandas as pd
import re
from pathlib import Path


#functions
from _concat import _folder_to_filestems
from _pd_utils import _pd_DataFrame, _dict_to_valscols, _csv_to_dictdf
from _string_utils import _txt_to_tokens, _remove_partsofspeech


#variables
error="???"
#list bigrams pn
file_path="_topicbigrams/_topicbigrams_pn.csv"
index_col="bigram"
dict_name="topicbigrams_pn"
#list bigrams np
file_path="_topicbigrams/_topicbigrams_np.csv"
index_col="bigram"
dict_name="topicbigrams_np"


from _hassan_vars import dict_bags
set_synonyms_uncertainty=dict_bags["synonyms_uncertainty"]
set_loughran_positive=dict_bags["loughran_positive"]
set_loughran_negative=dict_bags["loughran_negative"]




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

    #remove parts of speech
    list_cleanbigrams=_remove_partsofspeech(list_tokens, list_bigrams)

    #count frequencies
    dict_counts=collections.Counter(list_cleanbigrams)
    unique_cleanbigrams=list(dict_counts.keys())
    counts=list(dict_counts.values())

    #TFs
    sum_counts=sum(counts)
    TFs=[x/sum_counts for x in counts]

    print(sum(TFs))

    #create df
    values=[
        unique_cleanbigrams, 
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

    #set of bigrams
    set_cleanbigrams=set(list_cleanbigrams)

    return df, set_cleanbigrams


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

    #from library text to df
    df_p, set_bigrams_p =_libtxt_to_libdf(file_path_p)
    df_n, set_bigrams_n =_libtxt_to_libdf(file_path_n)

    #isin
    df_pn=df_p[~df_p["bigram"].isin(set_bigrams_n)]
    df_np=df_n[~df_n["bigram"].isin(set_bigrams_p)]

    #save
    file_path_pn=f"{results}/{result_pn}.csv"
    df_pn.to_csv(file_path_pn, index=False)
    file_path_np=f"{results}/{result_np}.csv"
    df_np.to_csv(file_path_np, index=False)




#binary search
from bisect import bisect_left
def BinarySearch(elements_list, element_to_find):
    i = bisect_left(elements_list, element_to_find)
    if i != len(elements_list) and elements_list[i] == element_to_find:
        return i
    else:
        return -1
    

#bigram in context
def _bigram_in_context(bigram_index, context_indexes, window_size):
    occurrences_found = 0
    skip = False
    for index in range(bigram_index-window_size, bigram_index+window_size+1):
        if index <= bigram_index:
            if BinarySearch(context_indexes, index) >= 0:
                occurrences_found += 1   
    return occurrences_found


#generate context indexes
def _gen_context_indexes(context_bag, list_tokens):
    context_indexes = list()
    for word in context_bag:
        for i, token in enumerate(list_tokens):
            if token == word:
                context_indexes.append(i)
    context_indexes.sort() 
    return context_indexes


#txt to hassan scores
window_size=3
def _txt_to_hassan(text, dict_bigrams):

    #variables
    dict_topicbigrams_pn=dict_bigrams["dict_topicbigrams_pn"]
    dict_topicbigrams_np=dict_bigrams["dict_topicbigrams_np"]

    #text
    list_tokens, n_words, list_bigrams, n_bigrams = _txt_to_tokens(text)
    print("list tokens \n", list_tokens)
    print("list bigrams \n", list_bigrams)

    #sum
    PRisk_sum=0
    Risk_sum=0
    NPRisk_sum=0
    PSentiment_sum=0
    Sentiment_sum=0
    NPSentiment_sum=0
    
    context_indexes_uncertainty = _gen_context_indexes(set_synonyms_uncertainty, list_tokens) #list_bigrams
    context_indexes_positive = _gen_context_indexes(set_loughran_positive, list_tokens)    
    context_indexes_negative = _gen_context_indexes(set_loughran_negative, list_tokens)     
    print(context_indexes_uncertainty)
    print(context_indexes_positive)

    #for
    for i, bigram in enumerate(list_bigrams):

        #if contained in P\N
        if bigram in dict_topicbigrams_pn:
            indicator_pn=1
            TF_pn=dict_topicbigrams_pn[bigram]["TF"]
        else:
            indicator_pn=0
            TF_pn=0

        #if contained in N\P
        if bigram in dict_topicbigrams_np:
            indicator_np=1
            TF_np=dict_topicbigrams_np[bigram]["TF"]
        else:
            indicator_np=0
            TF_np=0

        #if within 20 words from uncertainty!!!!!!!!!!!!!!!!!!!!!!!!!
        words_count = _bigram_in_context(i, context_indexes_uncertainty, window_size)
        print(bigram, words_count)
        if words_count > 0:
            indicator_within_uncertainty=1
        else:
            indicator_within_uncertainty=0

        #if contained in uncertainty
        if bigram in set_synonyms_uncertainty:
            indicator_uncertainty=1
        else:
            indicator_uncertainty=0
    
        #if within 20 words from sentiment!!!!!!!!!!!!!!!!!!!!!!!!!
        sum_within_positive=_bigram_in_context(i, context_indexes_positive, window_size)
        sum_within_negative=_bigram_in_context(i, context_indexes_negative, window_size)
        categorical_within_sentiment = sum_within_positive - sum_within_negative
        #print("positive words count \n", bigram, categorical_within_sentiment)

        #if contained in sentiment
        if bigram in set_loughran_positive:
            categorical_sentiment=+1
        elif bigram in set_loughran_negative:
            categorical_sentiment=-1
        else:
            categorical_sentiment=0

        #update i
        PRisk_i=indicator_pn*indicator_within_uncertainty*TF_pn
        Risk_i=indicator_uncertainty
        NPRisk_i=indicator_np*indicator_within_uncertainty*TF_np
        PSentiment_i=indicator_pn*TF_pn*categorical_within_sentiment
        Sentiment_i=categorical_sentiment
        NPSentiment_i=indicator_np*TF_np*categorical_within_sentiment

        #update sum
        PRisk_sum+=PRisk_i
        Risk_sum+=Risk_i
        NPRisk_sum+=NPRisk_i
        PSentiment_sum+=PSentiment_i
        Sentiment_sum+=Sentiment_i
        NPSentiment_sum+=NPSentiment_i
    
    PRisk=PRisk_sum/n_bigrams
    Risk=Risk_sum/n_bigrams
    NPRisk=NPRisk_sum/n_bigrams
    PSentiment=PSentiment_sum/n_bigrams
    Sentiment=Sentiment_sum/n_bigrams
    NPSentiment=PSentiment_sum/n_bigrams

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

text="blaone blatwo blathree blafour uncertain blafive blasix blaseven blaeight blanine"
#_txt_to_hassan(text)





#from file to converted
def _file_to_converted(file, file_stem, output, i, tot, dict_bigrams):
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
            dict_data=_txt_to_hassan(text, dict_bigrams)  
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
#folders=["_pdfs_to_txts", "_txts_to_hassan"]
#items=["_txts_to_hassan"]
def _txts_to_hassan(folders, items):
    resources=folders[0]
    results=folders[1]

    resource=items[0]
    result=items[1]

    #variables
    dict_topicbigrams_pn=_csv_to_dictdf(file_path, index_col, dict_name)
    dict_topicbigrams_np=_csv_to_dictdf(file_path, index_col, dict_name)

    dict_bigrams={
        **dict_topicbigrams_pn
        **dict_topicbigrams_np
        }

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
            converted=_file_to_converted(file, file_stem, output, i, tot, dict_bigrams)

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


