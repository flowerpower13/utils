

#preliminary
#import nltk
#nltk.download()
#nltk.download('stopwords')
#nltk.download('punkt')
#nltk.download('words')


#imports
import re
from nltk.util import ngrams
from nltk import tokenize


#variables
marker="###"


#replace tuples in text
def _replace_txt(text, tuples_replace):

    #for each tuple
    for i, tuple in enumerate(tuples_replace):

        #old and new
        (old, new) = tuple

        #replace
        text=text.replace(old, new)
    
    return text


#clean text
def _clean_text(text):

    #remove text btw markers
    text=re.sub(f'{marker}.*?{marker}', '', text)

    #lowercase
    text=text.lower()

    #remove non-characters
    text=re.sub(r'[^a-zA-Z ]', '', text)
    #remove whitespaces
    text=re.sub(r"\s+", ' ', text)

    return text


#from txt to list tokens and n words
def _txt_to_tokens(text):
    
    #list tokens
    list_tokens=tokenize.word_tokenize(text)

    #n words
    n_words=len(list_tokens)

    #list bigrams
    list_bigrams=list(ngrams(list_tokens, 2)) 
    list_bigrams=[' '.join(ngram_tuple) for ngram_tuple in list_bigrams]

    #n bigrams
    n_bigrams=len(list_bigrams)

    return list_tokens, n_words, list_bigrams, n_bigrams


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



# Given two windows, this function finds all ngrams of different sizes that appear in the windows.
# It uses an index of ngrams to look up ngrams quickly.
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


