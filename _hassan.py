

#import
import collections
import pandas as pd
import re
from pathlib import Path


#functions
from _concat import _folder_to_filestems
from _pd_utils import _pd_DataFrame, _dict_to_valscols, _csv_to_dictdf
from _string_utils import _clean_text, _txt_to_tokens, _remove_partsofspeech, _bigram_in_context, _gen_context_indexes

#variables
from _string_utils import error, marker
from _hassan_vars import get_generalsets, get_topicdicts
set_synonyms_uncertainty, set_loughran_positive, set_loughran_negative, set_sovereign = get_generalsets()
dict_topicbigrams_pn, dict_topicbigrams_np = get_topicdicts()
window_size=10


#from txts to unique txt corpus
def _txts_to_corpus(folder_corpus, path_aggregatecorpus):

    file_exclude="exclude"
    file_path=f"{file_exclude}.csv"
    df=pd.read_csv(file_path, dtype="string")
    set_exclude=set(df[file_exclude].to_list())

    #file stems
    files, file_stems = _folder_to_filestems(folder_corpus)

    #n obs
    n_obs=len(files)
    tot=n_obs-1

    #empty lists
    corpus=[None]*n_obs

    #for
    for i, file in enumerate(files):

        #file stem
        file_stem=file.stem

        #open
        with open(
            file, 
            mode='r', 
            ) as file_object:
            raw_text=file_object.read()
        
        #clean text
        text=_clean_text(raw_text)

        #add id
        text=f"{marker}{file_stem}{marker} {text}"

        #exclude some files
        if file_stem not in set_exclude:

            #update
            corpus[i]=text

            #print
            print(f"{i}/{tot} - {file_stem} - done")
        
        else:

            #print
            print(f"{i}/{tot} - {file_stem} - excluded")
  
    #aggregate texts
    corpus=[x for x in corpus if x is not None]
    text=" ".join(corpus)

    #write
    with open(
        path_aggregatecorpus,
        mode='w', 
        ) as f:
        f.write(text)

    return text


#from training library text to csv of bigrams-TFs
def _libtxt_to_libdf(resources, resource, results, result):

    #resources and results
    folder_corpus=f"{resources}/{resource}"
    path_aggregatecorpus=f"{results}/{result}.txt"

    text=_txts_to_corpus(folder_corpus, path_aggregatecorpus)

    #text
    list_tokens, n_words, list_bigrams, n_bigrams = _txt_to_tokens(text)

    #remove parts of speech
    list_cleanbigrams=_remove_partsofspeech(list_tokens, list_bigrams)

    #count frequencies
    dict_counts=collections.Counter(list_cleanbigrams)

    #create df
    data=list(dict_counts.items())
    df=pd.DataFrame(data, columns=["bigram", "count"])
    sum_counts=sum(dict_counts.values())
    df["TF"]=df["count"]/sum_counts

    #sort P and N
    df=df.sort_values(by="TF", ascending=False)

    #save
    file_path=f"{results}/{result}.csv"
    df.to_csv(file_path, index=False)

    #set bigrams
    set_bigrams=set(dict_counts.keys())

    return df, set_bigrams


#from txt files to topic TFs
def _topicbigrams(folders, items):
    resources=folders[0]
    results=folders[1]

    p="p"
    n="n"

    #resources
    resource_p=items[p][0]
    resource_n=items[n][0]

    #results
    result_p=items[p][1]
    result_n=items[n][1]

    #from library text to df
    df_p, set_bigrams_p = _libtxt_to_libdf(resources, resource_p, results, result_p)
    df_n, set_bigrams_n = _libtxt_to_libdf(resources, resource_n, results, result_n)

    #isin
    df_pn=df_p[~df_p["bigram"].isin(set_bigrams_n)]
    df_np=df_n[~df_n["bigram"].isin(set_bigrams_p)]

    #file paths
    result_pn=items[p][2]
    result_np=items[n][2]
    filepath_pn=f"{results}/{result_pn}.csv"
    filepath_np=f"{results}/{result_np}.csv"

    #save
    df_pn.to_csv(filepath_pn, index=False)
    df_np.to_csv(filepath_np, index=False)


#txt to hassan scores
def _txt_to_hassan(text):

    #text
    list_tokens, n_words, list_bigrams, n_bigrams = _txt_to_tokens(text)
    print("list tokens \n", list_tokens)
    print("list bigrams \n", list_bigrams)

    #initialize sum
    #exposure and risk
    PSimpleExposure_sum=0
    PExposure_sum=0
    PRisk_sum=0
    Risk_sum=0
    NPRisk_sum=0
    #sentiment
    PPositiveSentiment_sum=0
    PNegativeSentiment_sum=0
    PSentiment_sum=0
    PositiveSentiment_sum=0
    NegativeSentiment_sum=0
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

        #if within 20 words from uncertainty
        words_count = _bigram_in_context(i, context_indexes_uncertainty, window_size)
        #print(bigram, words_count)
        if words_count > 0:
            indicator_within_uncertainty=1
        else:
            indicator_within_uncertainty=0

        #if contained in uncertainty
        if bigram in set_synonyms_uncertainty:
            indicator_uncertainty=1
        else:
            indicator_uncertainty=0
    
        #if within 20 words from sentiment
        sum_within_positive=_bigram_in_context(i, context_indexes_positive, window_size)
        sum_within_negative=_bigram_in_context(i, context_indexes_negative, window_size)
        print(bigram, sum_within_positive)

        #if contained in sentiment
        if bigram in set_loughran_positive:
            indicator_positive=1
            indicator_negative=0
        elif bigram in set_loughran_negative:
            indicator_positive=0
            indicator_negative=1
        else:
            indicator_positive=0
            indicator_negative=0

        #update i
        #exposure and risk
        PSimpleExposure_i=      indicator_pn
        PExposure_i=            indicator_pn*TF_pn
        PRisk_i=                indicator_pn*indicator_within_uncertainty*TF_pn
        Risk_i=                 indicator_uncertainty
        NPRisk_i=               indicator_np*indicator_within_uncertainty*TF_np
        #   
        PPositiveSentiment_i=   indicator_pn*TF_pn*sum_within_positive
        PNegativeSentiment_i=   indicator_pn*TF_pn*sum_within_negative
        PSentiment_i=           indicator_pn*TF_pn*(sum_within_positive - sum_within_negative)
        PositiveSentiment_i=    indicator_positive
        NegativeSentiment_i=    indicator_negative
        Sentiment_i=            (indicator_positive - indicator_negative)
        NPSentiment_i=          indicator_np*TF_np*(sum_within_positive - sum_within_negative)

        #update sum
        #exposure and risk
        PSimpleExposure_sum     +=PSimpleExposure_i
        PExposure_sum           +=PExposure_i
        PRisk_sum               +=PRisk_i
        Risk_sum                +=Risk_i
        NPRisk_sum              +=NPRisk_i
        #sentiment
        PPositiveSentiment_sum  +=PPositiveSentiment_i
        PNegativeSentiment_sum  +=PNegativeSentiment_i
        PSentiment_sum          +=PSentiment_i
        PositiveSentiment_sum   +=PositiveSentiment_i
        NegativeSentiment_sum   +=NegativeSentiment_i
        Sentiment_sum           +=Sentiment_i
        NPSentiment_sum         +=NPSentiment_i
    
    #file-level score
    #exposure and risk
    PSimpleExposure     =PSimpleExposure_sum/n_bigrams
    PExposure           =PExposure_sum/n_bigrams
    PRisk               =PRisk_sum/n_bigrams
    Risk                =Risk_sum/n_bigrams
    NPRisk              =NPRisk_sum/n_bigrams
    #sentiment
    PPositiveSentiment  =PPositiveSentiment_sum/n_bigrams
    PNegativeSentiment  =PNegativeSentiment_sum/n_bigrams
    PSentiment          =PSentiment_sum/n_bigrams
    PositiveSentiment   =PositiveSentiment_sum/n_bigrams
    NegativeSentiment   =NegativeSentiment_sum/n_bigrams
    Sentiment           =Sentiment_sum/n_bigrams
    NPSentiment         =PSentiment_sum/n_bigrams

    #create empty dict for storing data
    dict_data={
        #exposure and risk
        "PSimpleExposure":      PSimpleExposure,
        "PExposure":            PExposure,
        "PRisk":                PRisk,
        "Risk":                 Risk,
        "NPRisk":               NPRisk,
        #sentiment
        "PPositiveSentiment":   PPositiveSentiment,
        "PNegativeSentiment":   PNegativeSentiment,
        "PSentiment":           PSentiment,
        "PositiveSentiment":    PositiveSentiment,
        "NegativeSentiment":    NegativeSentiment,
        "Sentiment":            Sentiment,
        "NPSentiment":          NPSentiment,
        #n bigrams and words
        "n_bigrams":            n_bigrams,
        "n_words":              n_words,
        }
    #print(dict_data)
    
    return dict_data


#from file to converted
def _file_to_converted(file, file_stem, output, i, tot):
    try:
        with open(
            file=file, 
            mode='r', 
            ) as file_object:
            text=file_object.read()

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
#folders=["_pdfs_to_txts", "_txts_to_hassan"]
#items=["_txts_to_hassan"]
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


