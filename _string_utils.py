
#preliminary
#import nltk
#nltk.download()
#nltk.download('stopwords')
#nltk.download('punkt')
#nltk.download('words')

import re
from nltk.util import ngrams
from pytrie import StringTrie
from nltk import corpus, tokenize


#variables
marker="###"


#clean text
def _clean_text(text):
    #remove text btw markers
    text=re.sub(f'{marker}.*?{marker}', '', text)

    #lowercase
    text=text.lower()

    #remove non-characters
    text=re.sub(r'[^a-zA-Z ]', '', text)
    #remove whitespaces
    text=text.strip()
    text=re.sub(r"\s+", ' ', text)

    #remove punctuation
    #text=re.sub(r'[^\w\s]', '', text)
    #remove newline
    #text=text.replace("\n", " ")

    return text


#from txt to list tokens and n words
def _txt_to_tokens(text):

    text=_clean_text(text)
    
    list_tokens=tokenize.word_tokenize(text)
    n_words=len(list_tokens)

    return list_tokens, n_words


#from stringlist to proper set of keywords
def _stringlist_to_set(text):
    #lowercase
    text=text.lower()

    #remove whitespaces
    text=text.strip()
    text=re.sub(r"\s+", ' ', text)

    #remove newline
    text=text.replace("\n", " ")

    #from list to set
    sep=", "
    keywords_list=text.split(sep)
    keywords_set={x for x in keywords_list}

    return keywords_set


#from good/bad keywords to bag/n_bagwords
def _keywords_to_dictbag(good_keywords, bad_keywords):
    good_keywords_set=_stringlist_to_set(good_keywords)
    bad_keywords_set=_stringlist_to_set(bad_keywords)

    bag_topic={x for x in good_keywords_set if x not in bad_keywords_set}

    return bag_topic


#from dict topics to dict bags
def _dicttopics_to_dictbags(dict_topics):
    dict_bags={}
    for j, (key_topic, value_topic) in enumerate(dict_topics.items()):

        good_keywords=dict_topics[key_topic]["good_keywords"]
        bad_keywords=dict_topics[key_topic]["bad_keywords"]

        dict_bags[key_topic]=_keywords_to_dictbag(good_keywords, bad_keywords)
    
    return dict_bags


#create directed graph of text words
def _create_wordgraph(list_tokens):

    word_graph = dict()
    i = 0

    while i < len(list_tokens):

        if list_tokens[i] not in word_graph:
            word_graph[list_tokens[i]] = [1, dict()]

        else:
            (word_graph[list_tokens[i]])[0] += 1
        
        if i < len(list_tokens) - 1:

            if list_tokens[i+1] not in word_graph[list_tokens[i]][1]:
                ((word_graph[list_tokens[i]])[1])[list_tokens[i+1]] = 0
            ((word_graph[list_tokens[i]])[1])[list_tokens[i+1]] += 1
        
        i = i + 1 
    
    return word_graph


#create prefix trie
def _prefix_trie(word_graph):

    prefix_trie=StringTrie()

    for key in word_graph.keys():
        prefix_trie[key]=key
    
    return prefix_trie


#find prefix
def _find_from_prefix_trie(keyword, word_graph, prefix_trie):
    keyword = keyword[0:-1]
    values = prefix_trie.values(keyword)

    if len(values) > 0:

        result = dict()

        for keyword_found in values:
            result[keyword_found] = (word_graph[keyword_found])[0]

        return result

    else:

        return dict()


#find tokens word in text's directed graph
def _wordbag_in_wordgraph(tokens, word_graph):
    if len(tokens) == 0:

        return 0

    elif len(tokens) == 1 and tokens[0] in word_graph:

        return (word_graph[tokens[0]])[0]

    elif tokens[0] in word_graph and tokens[1] in word_graph[tokens[0]][1]:

        return min(word_graph[tokens[0]][0], _wordbag_in_wordgraph(tokens[1:], word_graph))

    else:
        return 0


#bigrams + windows of n bigrams
def _list_windows(list_tokens, ngram, window_size):

    listtuples_ngrams=list(ngrams(list_tokens, ngram)) 
    list_ngrams=[" ".join(x) for x in listtuples_ngrams]

    list_windows=list(zip(*[list_ngrams[i:] for i in range(window_size+1)]))

    return list_windows


#from string to list tokens
def _text_to_dicttext(text, ngram=2, window_size=20):
    text=_clean_text(text)

    #split text into words
    list_tokens=tokenize.word_tokenize(text)

    #remove stopwords
    stopwords=set(corpus.stopwords.words('english'))
    list_tokens=[x for x in list_tokens if x not in stopwords]

    #word graph
    word_graph=_create_wordgraph(list_tokens)

    #prefix trie
    prefix_trie=_prefix_trie(word_graph)

    #total n of words
    n_words=len(list_tokens)

    #list of ngram windows
    list_windows=_list_windows(list_tokens, ngram, window_size)

    return list_tokens, word_graph, prefix_trie, n_words, list_windows


#CONTEXTUAL SEARCH
def create_ngrams_indexes(list_tokens, ngram):
    ngrams_list = list(ngrams(list_tokens, ngram)) 
    ngrams_list = [' '.join(ngram_tuple) for ngram_tuple in ngrams_list]
    ngrams_indexes = dict()
    indexes_ngrams = dict()
    for index, word in enumerate(ngrams_list):
        if word not in ngrams_indexes:
            ngrams_indexes[word] = list()
            
        ngrams_indexes[word].append(index)
        indexes_ngrams[index] = word

    return ngrams_indexes, indexes_ngrams


def search_ngrams_in_window(left_window, right_window, ngrams2_indexes_container, ngrams_2):
    ngrams_in_window = dict()
    for ngram_size in ngrams2_indexes_container.keys():
        indexes_ngrams = ngrams2_indexes_container[ngram_size][1]
        for index in range(left_window, right_window + 1):
            if index in indexes_ngrams:
                ngram_in_position = indexes_ngrams[index]
                if ngram_in_position in ngrams_2:
                    if ngram_in_position not in ngrams_in_window:
                        ngrams_in_window[ngram_in_position] = 0
                    ngrams_in_window[ngram_in_position] += 1 
    return ngrams_in_window


def cross_ngrams_search(list_tokens, ngrams_1, ngrams_2, window_size):

    max_ngrams1_len = get_max_ngram(ngrams_1)
    ngrams1_indexes_container = generate_multi_ngrams_indexes(max_ngrams1_len, list_tokens)

    max_ngrams2_len = get_max_ngram(ngrams_2)
    ngrams2_indexes_container = generate_multi_ngrams_indexes(max_ngrams2_len, list_tokens)

    ngrams2_found_near_ngrams1 = dict()
    for ngram_word in ngrams_1:
        ngram_word_len = len(ngram_word.split())
        ngrams_indexes = ngrams1_indexes_container[ngram_word_len][0]
        if ngram_word in ngrams_indexes:
            for index in ngrams_indexes[ngram_word]:
                left_window = index - window_size
                right_window = index + window_size + len(ngram_word) - 1
                if ngram_word not in ngrams2_found_near_ngrams1:
                    ngrams2_found_near_ngrams1[ngram_word] = list()
                ngrams2_found_near_ngrams1[ngram_word].append(search_ngrams_in_window(left_window, right_window, ngrams2_indexes_container, ngrams_2))

    return ngrams2_found_near_ngrams1


def get_max_ngram(ngram_dict):
    max_ngram_len = 0
    for ngram in ngram_dict:
        ngram_len = len(ngram.split())
        if ngram_len > max_ngram_len:
            max_ngram_len = ngram_len
    return max_ngram_len


def generate_multi_ngrams_indexes(max_ngram_len, list_tokens):
    ngrams_indexes_container = dict()
    for ngrams in range(1, max_ngram_len + 1):
        ngrams_indexes, indexes_ngrams = create_ngrams_indexes(list_tokens, ngrams)
        ngrams_indexes_container[ngrams] = (ngrams_indexes, indexes_ngrams)
    return ngrams_indexes_container


def results_cross_ngrams_search(ngrams2_found_near_ngrams1):
    total=0
    contextual=0

    for keyword in ngrams2_found_near_ngrams1.keys():
        total_occurrences = len(ngrams2_found_near_ngrams1[keyword])
        contextual_occurrences = len([l for l in ngrams2_found_near_ngrams1[keyword] if len(l) > 0])
        #print(keyword + " -> total: " + str(total_occurrences) + " contextual: " + str(contextual_occurrences))

        total+=total_occurrences
        contextual+=contextual_occurrences

    return total, contextual


#from tokens and target/context bags, compute contextual score
def _tokensbags_to_scores(list_tokens, ngrams_1, ngrams_2, window_size):

    #find cross ngrams
    ngrams2_found_near_ngrams1=cross_ngrams_search(list_tokens, ngrams_1, ngrams_2, window_size)

    #find total and contextual occurrences
    total, contextual=results_cross_ngrams_search(ngrams2_found_near_ngrams1)  

    return total, contextual


