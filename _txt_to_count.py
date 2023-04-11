

#functions
from _string_utils import _txt_to_tokens, _tokensbags_to_scores


#txt to bags' scores
def _txt_to_count(text, dict_bags, targetbag_keys, contextbag_keys, window_sizes):

    #clean, tokenize, and count n words in text
    list_tokens, n_words = _txt_to_tokens(text)

    #create empty dict for storing data
    dict_data={}

    #iter through target bags
    for k, targetbag_key in enumerate(targetbag_keys):

        #target bag's value
        targetbag_value=dict_bags[targetbag_key]

        #iter through context bags
        for j, contextbag_key in enumerate(contextbag_keys):
                
            #iter through window sizes
            for i, window_size in enumerate(window_sizes):

                #context bag's value
                contextbag_value=dict_bags[contextbag_key]

                #compute bag's score
                total, contextual = _tokensbags_to_scores(list_tokens, targetbag_value, contextbag_value, window_size)

                #fill dict with total occurrences
                dict_data[targetbag_key]=total
                #fill dict with contextual occurrences
                dict_data[f"{targetbag_key}__{contextbag_key}__{window_size}"]=contextual

    dict_data["n_words"]=n_words
    #print(dict_data)
    
    return dict_data


