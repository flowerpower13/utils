
import re
import pandas as pd
from nltk import tokenize


#functions
from _string_utils import _text_to_dicttext, _wordbag_in_wordgraph, _find_from_prefix_trie

from _string_utils import _txt_to_tokens, _tokensbags_to_scores


def _txt_to_countgraph(text, dict_bags):

    #from string to list tokens
    list_tokens, word_graph, prefix_trie, n_words, list_windows = _text_to_dicttext(text)

    #create empty dict for storing data
    dict_data={}

    #marker
    marker="*"

    #iter through single bags
    for i, (key_bag, value_bag) in enumerate(dict_bags.items()):
        #print(i, (key_bag, value_bag))

        score_bag=0
        #iter through single words (in a bag)
        for j, word_bag in enumerate(value_bag):
            #print(j, word_bag)

            if not word_bag.endswith(marker):
                tokens=tokenize.word_tokenize(word_bag)
                x=_wordbag_in_wordgraph(tokens, word_graph)
                #print("x - ", x)

            elif word_bag.endswith(marker):
                d=_find_from_prefix_trie(word_bag, word_graph, prefix_trie)
                x=sum(d.values())
                #print("x - ", x)

            #update bag-level score
            score_bag+=x

        #fill dict with bag's score
        dict_data[key_bag]=score_bag
        #fill dict with bag's len
        dict_data[f"n_{key_bag}"]=len(value_bag)

    dict_data["n_words"]=n_words
    #print(dict_data)
    
    return dict_data


#txt to bags' scores
def _txt_to_count(text, dict_bags):

    #clean, tokenize, and count n words in text
    list_tokens, n_words = _txt_to_tokens(text)

    #create empty dict for storing data
    dict_data={}

    #window size (count within n before, n after)
    window_sizes=[3, 5, 10]

    #context bag
    contextbag_keys=["epu_uncertainty", "epu_group2"]

    #iter through window sizes
    for i, window_size in enumerate(window_sizes):

        #iter through context bags
        for j, contextbag_key in enumerate(contextbag_keys):

            #context bag's value
            contextbag_value=dict_bags[contextbag_key]

            #iter through target bags
            for k, (targetbag_key, targetbag_value) in enumerate(dict_bags.items()):

                #compute bag's score
                total, contextual = _tokensbags_to_scores(list_tokens, targetbag_value, contextbag_value, window_size)

                #fill dict with total occurrences
                dict_data[targetbag_key]=total
                #fill dict with contextual occurrences
                dict_data[f"{targetbag_key}_{contextbag_key}_{window_size}"]=contextual

    dict_data["n_words"]=n_words
    #print(dict_data)
    
    return dict_data










