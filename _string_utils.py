
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

    #list of bigram windows
    list_windows=_list_windows(list_tokens, ngram, window_size)

    return list_tokens, word_graph, prefix_trie, n_words, list_windows


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