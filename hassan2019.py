
import re
import pandas as pd


#functions
from _string_utils import _text_to_dicttext


#copy
items=["Loughran-McDonald_MasterDictionary_1993-2021.csv", "riskwords.txt", "covid.csv", "political_bigrams.csv"]
dict_sentiment_risk_covid, dict_politicalbigrams=_firmlevelrisk_dictbags(items)


def import_sentiment(file_path):
    #df
    df=pd.read_csv(file_path, encoding='utf-8')

    #set
    tokeep=['Word', 'Positive']
    set_positive=set([x['Word'].lower() for idx, x in df[tokeep].iterrows()
                    if x['Positive'] > 0])
    tokeep=['Word', 'Negative']
    set_negative=set([x['Word'].lower() for idx, x in df[tokeep].iterrows()
                    if x['Negative'] > 0])

    #dict
    dict_sentiment={
        "sentiment_positive": set_positive, 
        "sentiment_negative": set_negative,
        }

    return dict_sentiment


def import_risk(file_path):
    #set
    set_risk=set()
    with open(file_path, 'r') as inp:
        for line in inp:
            split = line.split(' ')
            for syn in split:
                set_risk.add(re.sub('\n', '', syn))
    
    #dict
    dict_risk={"risk": set_risk}

    return dict_risk


def import_covid(file_path):
    #df
    df=pd.read_csv(file_path, encoding='utf-8')

    #list
    list_covid=df["col"].to_list()

    #set
    set_covid = set([re.sub('[^a-z ]', '', x.lower()) for x in list_covid])

    #dict
    dict_covid={"covid": set_covid}

    return dict_covid


def import_politicalbigrams(file_path):
    #df
    df=pd.read_csv(file_path, encoding='utf-8')

    df=df.assign(bigram=df['bigram'].str.replace('_', ' '))
    df=df.rename(columns={'politicaltbb': 'tfidf'})
    df=df.set_index("bigram")

    #dict
    dict_politicalbigrams=df.to_dict(orient='index')

    return dict_politicalbigrams


#FIRM LEVEL RISK SCORES
#https://doi.org/10.1093/qje/qjz021
#https://www.firmlevelrisk.com/
#https://www.firmlevelrisk.com/download
#https://github.com/mschwedeler/firmlevelrisk
#https://github.com/mschwedeler/PAC
#sentiment: https://sraf.nd.edu/loughranmcdonald-master-dictionary/
#riskwords: oxford Dictionary
#political_bigrams: tf*idf score on 100 newspapers articles per month between July 2005 and June 2020 + textbook 
def _firmlevelrisk_dictbags(items):
    file_sentiment=items[0]
    file_risk=items[1]
    file_covid=items[2]
    file_politicalbigrams=items[3]

    dict_sentiment=import_sentiment(file_sentiment)
    dict_risk=import_risk(file_risk)
    dict_covid=import_covid(file_covid)
    dict_politicalbigrams=import_politicalbigrams(file_politicalbigrams)

    dict_bags = dict_sentiment|dict_risk|dict_covid

    return dict_bags, dict_politicalbigrams


def _flr_basicscore(list_tokens, dict_bags, key):
    basic_score=len([x for x in list_tokens if x in dict_bags[key]])

    return basic_score


def _flr_condscore(window_words, dict_bags, key, tfidf):
    window_words
    cond_score=len([x for x in window_words if x in dict_bags[key]])
    dummy_condscore=(len([x for x in window_words if x in dict_bags[key]]) > 0)

    cond_score*=tfidf
    dummy_condscore*=tfidf

    return cond_score, dummy_condscore


def _firmlevelrisk_scores(text, dict_bags, dict_politicalbigrams):

    list_tokens, word_graph, prefix_trie, n_words, list_windows = _text_to_dicttext(text)

    dict_data={}

    keys=["sentiment_positive", "sentiment_negative", "risk", "covid"]
    for i, key in enumerate(keys):
        dict_data[key]=_flr_basicscore(list_tokens, dict_bags, key)

        P_key=f"P_{key}"
        P_dummy_key=f"P_dummy_{key}"
        dict_data[P_key]=0
        dict_data[P_dummy_key]=0


    P_tfidf=0
    for i, window in enumerate(list_windows):
        middle_bigram=window[10]

        if not middle_bigram in dict_politicalbigrams:
            continue

        window_words=set([y for x in window for y in x.split()])

        #tfidf
        tfidf=dict_politicalbigrams[middle_bigram]["tfidf"]
        P_tfidf+=tfidf
        
        #P scores
        for j, key in enumerate(keys):
            P_key=f"P_{key}"
            P_dummy_key=f"P_dummy_{key}"
            cond_score, dummy_condscore = _flr_condscore(window_words, dict_bags, key, tfidf)
            dict_data[P_key]+=cond_score
            dict_data[P_dummy_key]+=dummy_condscore

    #P tfidf
    dict_data["P_tfidf"]=P_tfidf

    #scale
    for i, (key, value) in enumerate(dict_data.items()):
        dict_data[key]=value*10000*(1/n_words)

    return dict_data

