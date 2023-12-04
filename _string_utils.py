

#preliminary
#pip install nltk
#import nltk
#nltk.download('all')
#nltk.download('stopwords')
#nltk.download('punkt')
#nltk.download('words')


#imports
import re
from nltk import tokenize
from nltk.util import ngrams
from nltk.tag.perceptron import PerceptronTagger


#variables
ERROR="???"
MARKER="###"
ENCODING="utf8"
ERRORS="strict"
pretrain=PerceptronTagger()
from _hassan_vars import _get_badkeywords
tuples_replace_beforelowercase, tuples_replace_afterlowercase, bad_tokens, bad_bigrams = _get_badkeywords()
from _hassan_vars import nltk_negative_conditions, nltk_positive_conditions


#replace tuples in text
def _replace_txt(text, tuples_replace):

    #for each tuple
    for i, tuple in enumerate(tuples_replace):

        #old and new
        (old, new) = tuple

        #replace
        text=text.replace(old, new)
    
    return text



#simple clean text
def _simpleclean_text(raw_text):

    #remove text btw MARKERs
    text=re.sub(f'{MARKER}.*?{MARKER}', '', raw_text)

    #lowercase
    text=text.lower()

    #remove non-characters
    text=re.sub(r'[^a-zA-Z ]', '', text)
    
    #remove whitespaces
    text=re.sub(r"\s+", ' ', text)

    return text



#clean text
def _clean_text(raw_text):

    #remove text btw MARKERs
    text=re.sub(f'{MARKER}.*?{MARKER}', '', raw_text)

    #remove words before lowercase
    text=_replace_txt(text, tuples_replace_beforelowercase)

    #lowercase
    text=text.lower()

    #remove words after lowercase
    text=_replace_txt(text, tuples_replace_afterlowercase)

    #remove non-characters
    text=re.sub(r'[^a-zA-Z ]', '', text)
    
    #remove whitespaces
    #text=re.sub(r"\s+", ' ', text)

    return text


#from txt to list tokens and n words
def _txt_to_tokens(text):

    #list tokens
    list_tokens=tokenize.word_tokenize(text)

    #remove bad tokens
    list_tokens=[x for x in list_tokens if x not in bad_tokens]

    #list bigrams
    list_tuplesbigrams=list(ngrams(list_tokens, 2)) 
    list_bigrams=[' '.join(bigram_tuple) for bigram_tuple in list_tuplesbigrams]

    #remove bad bigrams
    list_bigrams=[x for x in list_bigrams if x not in bad_bigrams]

    #n bigrams
    n_bigrams=len(list_bigrams)

    #n words
    n_words=n_bigrams+1

    return list_tokens, n_words, list_bigrams, n_bigrams


#remove parts of speech - #https://www.nltk.org/api/nltk.tag.perceptron.html
def _remove_partsofspeech(list_tokens, list_bigrams):

    #tuples tagged tokens
    listtuples_taggedtokens=pretrain.tag(list_tokens)
    
    #empty list
    n_obs=len(list_bigrams)
    tot=n_obs-1
    list_cleanbigrams=[None]*n_obs

    #for
    for i, bigram in enumerate(list_bigrams):

        #tags
        tag0=listtuples_taggedtokens[i][1]
        tag1=listtuples_taggedtokens[i+1][1]

        #if meet conditions
        if (eval(nltk_positive_conditions)) and not (eval(nltk_negative_conditions)):
            
            #include
            list_cleanbigrams[i]=bigram 

            #print
            print(f"{i}/{tot} - {bigram}")

        elif not ( (eval(nltk_positive_conditions)) and not (eval(nltk_negative_conditions)) ):

            #print
            print(f"{i}/{tot} - {bigram} - skipped")
        
    #remove None
    list_cleanbigrams=[x for x in list_cleanbigrams if x is not None]

    #print(listtuples_taggedtokens)
    return list_cleanbigrams

'''
text="twelve percent of the black male population"
list_tokens, n_words, list_bigrams, n_bigrams = _txt_to_tokens(text)
_remove_partsofspeech(list_tokens, list_bigrams)
'''


#CONTEXTUAL SEARCH
from typing import List, Tuple
from nltk.util import ngrams
def create_ngrams_indexes(list_tokens: List[str], ngram: int) -> Tuple[dict, dict]:
    """
    This function creates a dictionary of ngrams and their corresponding indexes in the given list of tokens.
    
    Parameters:
    list_tokens (List[str]): A list of tokens to create ngrams from.
    ngram (int): The number of tokens to include in each ngram.
    
    Returns:
    Tuple[dict, dict]: A tuple of two dictionaries. The first dictionary maps each ngram to a list of its
                       corresponding indexes in the ngrams list. The second dictionary maps each index to
                       its corresponding ngram in the ngrams list.
    """
    
    # Create a list of ngrams from the list of tokens
    ngrams_list = list(ngrams(list_tokens, ngram)) 
    
    # Convert each ngram tuple to a string
    ngrams_list = [' '.join(ngram_tuple) for ngram_tuple in ngrams_list]
    
    # Create two dictionaries for mapping ngrams to their indexes and vice versa
    ngrams_indexes = dict()
    indexes_ngrams = dict()
    
    # Iterate through the ngrams list
    for index, word in enumerate(ngrams_list):
        
        # If the ngram has not been encountered yet, create an empty list for its indexes
        if word not in ngrams_indexes:
            ngrams_indexes[word] = list()
        
        # Append the current index to the list of indexes for the current ngram
        ngrams_indexes[word].append(index)
        
        # Map the current index to its corresponding ngram
        indexes_ngrams[index] = word
    
    # Return the two dictionaries as a tuple
    return ngrams_indexes, indexes_ngrams

#Given two windows, this function finds all ngrams of different sizes that appear in the windows.
#It uses an index of ngrams to look up ngrams quickly.
def search_ngrams_in_window(left_window, right_window, ngrams2_indexes_container, ngrams_2):
    # This dictionary will store the ngrams found in the windows along with their frequencies.
    ngrams_in_window = dict()
    
    # Iterate over all ngram sizes.
    for ngram_size in ngrams2_indexes_container.keys():
        # Get the dictionary that maps indexes to ngrams for the current ngram size.
        indexes_ngrams = ngrams2_indexes_container[ngram_size][1]
        
        # Iterate over all positions in the windows.
        for index in range(left_window, right_window + 1):
            # If the current position has an ngram of the current size.
            if index in indexes_ngrams:
                # Get the ngram at the current position.
                ngram_in_position = indexes_ngrams[index]
                
                # If the ngram is one of the target ngrams.
                if ngram_in_position in ngrams_2:
                    # If the ngram has not been seen before, add it to the dictionary.
                    if ngram_in_position not in ngrams_in_window:
                        ngrams_in_window[ngram_in_position] = 0
                    # Increment the frequency of the ngram.
                    ngrams_in_window[ngram_in_position] += 1 
                    
    # Return the dictionary of ngrams and their frequencies.
    return ngrams_in_window

def get_max_ngram(ngram_dict):
    # Initialize the maximum ngram length to 0
    max_ngram_len = 0
    # Loop through each ngram in the ngram dictionary
    for ngram in ngram_dict:
        # Get the length of the current ngram
        ngram_len = len(ngram.split())
        # If the length of the current ngram is greater than the maximum ngram length
        if ngram_len > max_ngram_len:
            # Update the maximum ngram length to the length of the current ngram
            max_ngram_len = ngram_len
    # Return the maximum ngram length
    return max_ngram_len

def generate_multi_ngrams_indexes(max_ngram_len, list_tokens):
    # Initialize an empty dictionary to hold n-grams and their indexes
    ngrams_indexes_container = dict()
    
    # Loop through each possible n-gram length from 1 up to the maximum length specified
    for ngrams in range(1, max_ngram_len + 1):
        # Use the create_ngrams_indexes function to generate the n-grams and their indexes for the current n-gram length
        ngrams_indexes, indexes_ngrams = create_ngrams_indexes(list_tokens, ngrams)
        
        # Add the n-gram indexes and index-n-gram mappings to the container dictionary
        ngrams_indexes_container[ngrams] = (ngrams_indexes, indexes_ngrams)
    
    # Return the container dictionary
    return ngrams_indexes_container

def cross_ngrams_search(list_tokens, ngrams_1, ngrams_2, window_size):
    #cotainer[2] -> ngrams_indexes=("I am" -> 1,2,3), indexes_ngrams ->(1 -> "I am","I am the")
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
                right_window = index + window_size + ngram_word_len - 1
                if ngram_word not in ngrams2_found_near_ngrams1:
                    ngrams2_found_near_ngrams1[ngram_word] = list()
                ngrams2_found_near_ngrams1[ngram_word].append(search_ngrams_in_window(left_window, right_window, ngrams2_indexes_container, ngrams_2))

    return ngrams2_found_near_ngrams1

def results_cross_ngrams_search(ngrams2_found_near_ngrams1):
    total=0       # initialize the count of total occurrences
    contextual=0  # initialize the count of contextual occurrences

    for keyword in ngrams2_found_near_ngrams1.keys():  # iterate through the keywords
        total_occurrences = len(ngrams2_found_near_ngrams1[keyword])  # count the total occurrences of the keyword
        contextual_occurrences = len([l for l in ngrams2_found_near_ngrams1[keyword] if len(l) > 0])  # count the contextual occurrences of the keyword
        
        total+=total_occurrences         # add the total occurrences to the running total count
        contextual+=contextual_occurrences  # add the contextual occurrences to the running contextual count

    return total, contextual  # return the total and contextual counts as a tuple

#from tokens and target/context bags, compute contextual score
def _tokensbags_to_scores(list_tokens, ngrams_1, ngrams_2, window_size):

    #find cross ngrams
    ngrams2_found_near_ngrams1=cross_ngrams_search(list_tokens, ngrams_1, ngrams_2, window_size)

    #find total and contextual occurrences
    total, contextual=results_cross_ngrams_search(ngrams2_found_near_ngrams1)  

    return total, contextual


#binary search
from bisect import bisect_left
def BinarySearch(elements_list, element_to_find):
    i = bisect_left(elements_list, element_to_find)
    if i != len(elements_list) and elements_list[i] == element_to_find:
        return i
    else:
        return -1


# This function generates a list of context indexes for each word in a given bag of words.
# The context indexes for a word are the indexes of that word and its neighboring words in a list of tokens.
# The list of tokens is represented as a list of strings.
def _gen_context_indexes(context_bag, list_tokens):
    # Initialize an empty list to store the context indexes for each word.
    context_indexes = list()
    
    # Loop over all the words in the context bag.
    for word in context_bag:
        # Initialize an empty set to store the indexes of the current word and its neighbors.
        word_indexes = set()
        
        # Loop over all the tokens in the list of tokens.
        for i, token in enumerate(list_tokens):
            # If the current token matches the current word, add its index and the index of its left neighbor to the set.
            if token == word:
                word_indexes.add(i)
                word_indexes.add(i-1)
        
        # If any indexes were found for the current word, sort them and add them to the list of context indexes.
        if len(word_indexes) > 0:
            sorted_context = list(word_indexes)
            sorted_context.sort()
            context_indexes.append(sorted_context)
    
    # Return the list of context indexes for all words in the context bag.
    return context_indexes


# This function counts the number of occurrences of a given bigram within a specified context.
# The bigram is represented by its starting index in a list of tokens.
# The context is represented by a list of lists of token indexes.
# The window_size parameter specifies the maximum distance (in tokens) between the bigram and any other token in the context.
def _bigram_in_context(bigram_index, context_indexes, window_size):
    # Initialize a variable to keep track of the number of occurrences found.
    occurrences_found = 0
    
    # Initialize a boolean variable to control skipping tokens that have already been counted.
    skip = False
    
    # Loop over all the contexts in the list of context_indexes.
    for context in context_indexes:
        # If the last token in the context is too far from the bigram, skip this context.
        if context[-1] < bigram_index-window_size:
            continue
        
        # Loop over all the tokens in the current context.
        for position in context:
            # If we just counted the token in the previous iteration, skip it.
            if skip:
                skip = False
                continue
            
            # If the current token is too far from the bigram, exit the loop.
            if position > bigram_index+window_size:
                break
            
            # If the current token is within the window_size distance from the bigram,
            # count it as an occurrence of the bigram and skip the next token (since it's too close).
            if position >= bigram_index-window_size and position <= bigram_index+window_size:
                occurrences_found += 1
                skip = True
            
    # Return the total number of occurrences found.
    return occurrences_found



