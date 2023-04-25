

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
window_size=10
from _string_utils import error, marker, encoding, errors
from _hassan_vars import get_generalsets
set_synonyms_uncertainty, set_loughran_positive, set_loughran_negative, set_sovereign = get_generalsets()
from _hassan_vars import get_topicdicts
dict_topicbigrams_pn, dict_topicbigrams_np = get_topicdicts()


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
            file=file, 
            mode='r',
            encoding="latin-1",
            errors=errors,
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
        
        elif file_stem in set_exclude:

            #print
            print(f"{i}/{tot} - {file_stem} - excluded")
  
    #aggregate texts
    corpus=[x for x in corpus if x is not None]
    text=" ".join(corpus)

    #write
    with open(
        file=path_aggregatecorpus,
        mode='w', 
        encoding=encoding,
        errors=errors,
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


#indicator bigram in set values
def _bigram_in_set(gram0, gram1, set_values):

    #expressions
    gram0_exp = (gram0 in set_values)
    gram1_exp = (gram1 in set_values)

    #if bigram contained in set values
    if gram0_exp or gram1_exp:
        indicator=1
    elif not (gram0_exp or gram1_exp):
        indicator=0

    return indicator


#txt to hassan scores
def _txt_to_hassan(list_tokens, n_words, list_bigrams, n_bigrams):

    #initialize sum
    #exposure and risk
    P_SimpleExposure_sum=0
    P_Exposure_sum=0
    P_Risk_sum=0
    Risk_sum=0
    NP_Risk_sum=0
    #sentiment
    P_PositiveSentiment_sum=0
    P_NegativeSentiment_sum=0
    P_Sentiment_sum=0
    PositiveSentiment_sum=0
    NegativeSentiment_sum=0
    Sentiment_sum=0
    NP_Sentiment_sum=0

    context_indexes_uncertainty = _gen_context_indexes(set_synonyms_uncertainty, list_tokens) 
    context_indexes_positive = _gen_context_indexes(set_loughran_positive, list_tokens)    
    context_indexes_negative = _gen_context_indexes(set_loughran_negative, list_tokens)     

    #for
    for i, bigram in enumerate(list_bigrams):

        #if contained in P\N
        if bigram in dict_topicbigrams_pn:
            indicator_pn=1
            TF_pn=dict_topicbigrams_pn[bigram]["TF"]
        elif not (bigram in dict_topicbigrams_pn):
            indicator_pn=0
            TF_pn=0

        #if contained in N\P
        if bigram in dict_topicbigrams_np:
            indicator_np=1
            TF_np=dict_topicbigrams_np[bigram]["TF"]
        elif not (bigram in dict_topicbigrams_np):
            indicator_np=0
            TF_np=0

        #if within 20 words from uncertainty
        words_count = _bigram_in_context(i, context_indexes_uncertainty, window_size)
        
        if words_count > 0:
            indicator_within_uncertainty=1
        elif  words_count == 0:
            indicator_within_uncertainty=0

        #if within 20 words from sentiment
        sum_within_positive=_bigram_in_context(i, context_indexes_positive, window_size)
        sum_within_negative=_bigram_in_context(i, context_indexes_negative, window_size)

        #monograms grom bigram
        [gram0, gram1] = bigram.split()

        #bigram indicator uncertainty
        indicator_uncertainty=_bigram_in_set(gram0, gram1, set_synonyms_uncertainty)

        #bigram indicator uncertainty
        indicator_positive=_bigram_in_set(gram0, gram1, set_loughran_positive)
        indicator_negative=_bigram_in_set(gram0, gram1, set_loughran_negative)

        #update sum
        #exposure and risk
        P_SimpleExposure_sum        +=indicator_pn
        P_Exposure_sum              +=indicator_pn*TF_pn
        P_Risk_sum                  +=indicator_pn*indicator_within_uncertainty*TF_pn
        Risk_sum                    +=indicator_uncertainty
        NP_Risk_sum                 +=indicator_np*indicator_within_uncertainty*TF_np
        #sentime        
        P_PositiveSentiment_sum     +=indicator_pn*TF_pn*sum_within_positive
        P_NegativeSentiment_sum     +=indicator_pn*TF_pn*sum_within_negative
        P_Sentiment_sum             +=indicator_pn*TF_pn*(sum_within_positive - sum_within_negative)
        PositiveSentiment_sum       +=indicator_positive
        NegativeSentiment_sum       +=indicator_negative
        Sentiment_sum               +=(indicator_positive - indicator_negative)
        NP_Sentiment_sum            +=indicator_np*TF_np*(sum_within_positive - sum_within_negative)

    #create empty dict for storing data
    dict_data={

        #exposure and risk
        "P_SimpleExposure":      P_SimpleExposure_sum/n_bigrams,
        "P_Exposure":            P_Exposure_sum/n_bigrams,
        "P_Risk":                P_Risk_sum/n_bigrams,
        "Risk":                 Risk_sum/n_bigrams,
        "NP_Risk":               NP_Risk_sum/n_bigrams,
        #sentiment
        "P_PositiveSentiment":   P_PositiveSentiment_sum/n_bigrams,
        "P_NegativeSentiment":   P_NegativeSentiment_sum/n_bigrams,
        "P_Sentiment":           P_Sentiment_sum/n_bigrams,
        "PositiveSentiment":    PositiveSentiment_sum/n_bigrams,
        "NegativeSentiment":    NegativeSentiment_sum/n_bigrams,
        "Sentiment":            Sentiment_sum/n_bigrams,
        "NP_Sentiment":          NP_Sentiment_sum/n_bigrams,
        #n bigrams and words
        "n_bigrams":            n_bigrams,
        "n_words":              n_words,
        
        }

    return dict_data


#from file to converted
def _file_to_converted(file, file_stem, output, i, tot):

    with open(
        file=file, 
        mode='r', 
        encoding=encoding,
        errors=errors,
        ) as file_object:
        text=file_object.read()

    if text==error:

        #converted
        converted=False

        #print
        print(f"{i}/{tot} - {file_stem} - error")

    elif text!=error:

        #text
        list_tokens, n_words, list_bigrams, n_bigrams = _txt_to_tokens(text)

        if n_words>=50:

            #values and columns
            values=[
                [file_stem],
                ]
            columns=[
                "file_stem", 
                ]

            #txt to count
            dict_data=_txt_to_hassan(list_tokens, n_words, list_bigrams, n_bigrams)  
            Vs, Ks = _dict_to_valscols(dict_data)
            values+=Vs
            columns+=Ks

            #create df
            df=_pd_DataFrame(values, columns)
            df.to_csv(output, index=False)

            #converted
            converted=True

            #print
            print(f"{i}/{tot} - {file_stem} - done")

        elif n_words<50:

            #converted
            converted=False

            #print
            print(f"{i}/{tot} - {file_stem} - blank <50 words")

    return converted


#compute hassan scores
#folders=["_pdfs_to_txts", "_txts_to_hassan"]
#items=["_txts_to_hassan"]
def _txts_to_hassan(folders, items, start, stop):
    resources=folders[0]
    results=folders[1]

    result=items[0]

    #colname
    colname_filestems="file_stem"

    #resources
    files, file_stems=_folder_to_filestems(resources)

    #start and stop
    end=len(files)
    files=files[start:end]
    file_stems=file_stems[start:end]

    #n obs
    n_obs=len(files)
    tot=n_obs-1

    #empty lists
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


