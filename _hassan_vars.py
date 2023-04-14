

#imports
import nltk
import pandas as pd


#functions
from _pd_utils import _csv_to_dictbag, _csv_to_dictdf


#REMOVE PARTS OF SPEECH - #https://www.nltk.org/api/nltk.tag.perceptron.html
#Hassan et al. 2019 QJE - Appendix B - https://doi.org/10.1093/qje/qjz021


#see parts of speech
#nltk.help.upenn_tagset()


#non-relevant parts of speech
pronouns=["PRP", "PRP$"]
prepositions=["TO", "IN"]
adverbs=["RB", "RBR", "RBS"]
wh_adverbs=["WRB"]
determiners=["DT", "PDT"] 
foreign_words=["FW"]
adjectives=["JJ", "JJR", "JJS"]
nouns=["NN", "NNP", "NNPS", "NNS"]
verbs=["VB", "VBD", "VBG", "VBN", "VBP", "VBZ"]


#non-relevant
#pronoun
pronoun_tag0=f'(tag0=="{pronouns[0]}" or tag0=="{pronouns[1]}")'
pronoun_tag1=f'(tag1=="{pronouns[0]}" or tag1=="{pronouns[1]}")'

#preposition
preposition_tag0=f'(tag0=="{prepositions[0]}" or tag0=="{prepositions[1]}")'
preposition_tag1=f'(tag1=="{prepositions[0]}" or tag1=="{prepositions[1]}")'

#adverb
adverb_tag0=f'(tag0=="{adverbs[0]}" or tag0=="{adverbs[1]}" or tag0=="{adverbs[2]}")'
adverb_tag1=f'(tag1=="{adverbs[0]}" or tag1=="{adverbs[1]}" or tag1=="{adverbs[2]}")'

#wh adverb
wh_adverb_tag0=f'(tag0=="{wh_adverbs[0]}")'
wh_adverb_tag1=f'(tag1=="{wh_adverbs[0]}")'

#determiner
determiner_tag0=f'(tag0=="{determiners[0]}" or tag0=="{determiners[1]}")'
determiner_tag1=f'(tag1=="{determiners[0]}" or tag1=="{determiners[1]}")'

#foreign word
foreign_word_tag0=f'(tag0=="{foreign_words[0]}")'
foreign_word_tag1=f'(tag1=="{foreign_words[0]}")'

#adjective
adjective_tag0=f'(tag0=="{adjectives[0]}" or tag0=="{adjectives[1]}" or tag0=="{adjectives[2]}")'
adjective_tag1=f'(tag1=="{adjectives[0]}" or tag1=="{adjectives[1]}" or tag1=="{adjectives[2]}")'

#noun
noun_tag0=f'(tag0=="{nouns[0]}" or tag0=="{nouns[1]}" or tag0=="{nouns[2]}" or tag0=="{nouns[3]}")'
noun_tag1=f'(tag1=="{nouns[0]}" or tag1=="{nouns[1]}" or tag1=="{nouns[2]}" or tag1=="{nouns[3]}")'

#verb
verb_tag0=f'(tag0=="{verbs[0]}" or tag0=="{verbs[1]}" or tag0=="{verbs[2]}" or tag0=="{verbs[3]}" or tag0=="{verbs[4]}" or tag0=="{verbs[5]}")'
verb_tag1=f'(tag1=="{verbs[0]}" or tag1=="{verbs[1]}" or tag1=="{verbs[2]}" or tag1=="{verbs[3]}" or tag1=="{verbs[5]}" or tag1=="{verbs[5]}")'


#0, 1, 2
condition0=f"{pronoun_tag0} and {pronoun_tag1}"
condition1=f"{preposition_tag0} and {preposition_tag1}"
condition2=f"{adverb_tag0} and {adverb_tag1}"

#3 - typo in paper

#4.1 and 4.2
condition4_1=f"{preposition_tag0} and {adverb_tag1}"
condition4_2=f"{adverb_tag0} and {preposition_tag1}"
#5.1 and 5.2
condition5_1=f"{preposition_tag0} and {wh_adverb_tag1}"
condition5_2=f"{wh_adverb_tag0} and {preposition_tag1}"
#6.1 and 6.2
condition6_1=f"{preposition_tag0} and {determiner_tag1}"
condition6_2=f"{determiner_tag0} and {preposition_tag1}"
#7.1 and 7.2
condition7_1=f"{adverb_tag0} and {wh_adverb_tag1}"
condition7_2=f"{wh_adverb_tag0} and {adverb_tag1}"
#8.1 and 8.2
condition8_1=f"{adverb_tag0} and {determiner_tag1}"
condition8_2=f"{determiner_tag0} and {adverb_tag1}"
#9.1 and 9.2
condition9_1=f"{wh_adverb_tag0} and {determiner_tag1}"
condition9_2=f"{determiner_tag0} and {wh_adverb_tag1}"

#all negative conditions
nltk_negative_conditions=f'''
({condition0}) or \
({condition1}) or \
({condition2}) or \
({condition4_1}) or \
({condition4_2}) or \
({condition5_1}) or \
({condition5_2}) or \
({condition6_1}) or \
({condition6_2}) or \
({condition7_1}) or \
({condition7_2}) or \
({condition8_1}) or \
({condition8_2}) or \
({condition9_1}) or \
({condition9_2})
'''


#fiore
#0, 1, 2, 3, 
condition0=f"{foreign_word_tag0} or {foreign_word_tag1}"
condition1=f"{adjective_tag0} or {adjective_tag1}"
condition2=f"{noun_tag0} or {noun_tag1}"
condition3=f"{verb_tag0} or {verb_tag1}"

#all positive conditions
nltk_positive_conditions=f'''
({condition0}) or \
({condition1}) or \
({condition2}) or \
({condition3})
'''


#remove words before clean 
#Hassan et al. 2019 QJE- Appendix A.1 - https://doi.org/10.1093/qje/qjz021
tuples_replace_beforeclean=[
    ("Bill", "bbill"),
    ("Constitution", "cconstitution"),
    ]
tuples_replace_afterclean=[
    ("risk officer", ""),
    ("risk credit officer", ""),
    ("unknown speaker", ""),
    ("unknown participant", ""),
    ("unknown caller", ""),
    ("unknown operator", ""),
    ("unknown firm analyst", ""),
    ("in the states", ""),
    ]


#remove tokens and bigrams (also stopwords and single letters)
from nltk.corpus import stopwords
stop_words=set(stopwords.words('english'))
import string
alphabet = set(string.ascii_lowercase)
bad_tokens={
    "i", "ive", "youve", "weve", "im", "youre", "were", "id", "youd", "wed", "thats",
    *stop_words,
    *alphabet,
    }
bad_bigrams={
    "princeton university",
    "university press",
    }


#Loughran-McDonald sentiment words
#https://sraf.nd.edu/loughranmcdonald-master-dictionary/
file_path="Loughran-McDonald_MasterDictionary_1993-2021"
df=pd.read_csv(f"{file_path}.csv", dtype="string")
df=df.dropna(subset=["Word"])
def _loughran_sentiment(sentiment):

    #key
    dict_key=f"loughran_{sentiment.lower()}"

    #value
    dict_value=df.loc[df[sentiment]!="0", "Word"].str.lower().to_list()

    #dict
    loughran_sentiment={dict_key: dict_value}

    return loughran_sentiment


#general sets
def get_generalsets():

    #uncertainty
    #https://github.com/mschwedeler/firmlevelrisk/blob/master/input/riskwords/synonyms.txt
    file_path="synonyms_uncertainty"
    bad_keywords={"question", "questions", "venture"}
    set_synonyms_uncertainty=_csv_to_dictbag(file_path, bad_keywords)

    #loughran
    sentiment="Positive"
    set_loughran_positive=_loughran_sentiment(sentiment)
    sentiment="Negative"
    set_loughran_negative=_loughran_sentiment(sentiment)

    #sovereign
    file_path="sovereign"
    bad_keywords=set()
    set_sovereign=_csv_to_dictbag(file_path, bad_keywords)

    #add others here
    #

    return set_synonyms_uncertainty, set_loughran_positive, set_loughran_negative, set_sovereign


#topic dicts
def get_topicdicts():

    #index col
    index_col="bigram"

    #topic bigrams P\N
    file_path="_topicbigrams/_topicbigrams_pn.csv"
    dict_name="dict_topicbigrams_pn"
    dict_topicbigrams_pn=_csv_to_dictdf(file_path, index_col, dict_name)

    #topic bigrams N\P
    file_path="_topicbigrams/_topicbigrams_np.csv"
    dict_name="dict_topicbigrams_np"
    dict_topicbigrams_np=_csv_to_dictdf(file_path, index_col, dict_name)

    return dict_topicbigrams_pn, dict_topicbigrams_np

