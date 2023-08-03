

#imports
import nltk
import pandas as pd


#functions
from _pd_utils import _csv_to_setvalues, _csv_to_dictdf


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


#stopwords and alphabet letters
from nltk.corpus import stopwords
stop_words=set(stopwords.words('english'))
import string
alphabet=set(string.ascii_lowercase)


#add university names
#https://github.com/Hipo/university-domains-list/
def _tuples_universities():

    #imports
    import json

    #filepath
    filepath="_resources/world_universities_and_domains.json"

    #read
    with open(
        file=filepath,
        mode="r",
        encoding="utf8",
        ) as file_object:
        data = json.load(file_object)

    #tuples
    tuples_universities=[(item['name'], '') for item in data]

    return tuples_universities


#remove words, tokens, and bigrams
#Hassan et al. 2019 QJE- Appendix - https://doi.org/10.1093/qje/qjz021
def _get_badkeywords():
    #lowercase sensitive
    tuples_replace_beforelowercase=[

        #hassan
        ("Bill", "bbill"),
        ("Constitution", "cconstitution"),

        #epub-related
        ("Return to reference", ""), ("(accessed", ""), ("(all accessed", ""), ("th ed.", ""), ("Note ", ""), 
        ("Alamy Stock Photo", ""), ("Getty Images", ""), ("et al.", ""), ("Practice Quiz Questions", ""),  
        #press-specific
        ("University Press", ""), ("Princeton, NJ", ""), ("University of Chicago Press", ""), ("Los Angeles: CQ Press", ""), 
        ("Dryden Press", ""), ("Associated Press", ""), ("Beacon Press", ""), ("University of Notre Dame Press", ""),
        ("National Geographic Press", ""), ("University of Oklahoma Press", ""), ("University of Tennessee Press", ""), 
        ("Brookings Institution Press", ""), ("University of North Carolina Press", ""), ("University of Michigan Press", ""),
        ("St. Martin's Press", ""), ("Westview Press", ""), ("Penguin Press", ""), ("New York Press", ""), ("Seven Stories Press", ""), 
        ("Monthly Review Press", ""), ("Westview Press", ""), ("www.people-press.org", ""), ("SUNY Press", ""), ("Atherton Press", ""), 
        (" University of Virginia Press", ""), ("Free Press", ""),  ("Belknap Press", ""), 

        #abbas2019
        ("OUP CORRECTED PROOF", ""), ("FINAL, 01/10/19, SPi", ""), ("SPi", ""), ("Case Study", ""), 
        ("Washington, DC: International Monetary Fund", ""), ("American Economic Review", ""), 
        ("European Economic Review", ""), ("IMF Economic Review", ""),  ("Economic Review", ""), 
        ("Journal of Economic", ""), ("Journal of International", ""), ("Journal of Monetary", ""), 
        #authors
        ("Ali Abbas", ""), ("Alex Pienkowski", ""), ("Kenneth Rogoff", ""),  
        ("Carmen Reinhart", ""), ("Reinhart", ""), ("Rogoff", ""), 
        ("Eichengreen, El-Ganainy, Esteves, and Mitchener", ""), ("Arslanalp, Bergthaler, Stokoe, and Tieman", ""), 
        ("Fatás, Ghosh, Panizza, and Presbitero", ""), ("Debrun, Ostry, Willems, and Wyplosz", ""), 
        ("Jonasson, Papaioannou, and Williams", ""), ("Best, Bush, Eyraud, and Sbrancia", ""), 
        ("Ams, Baqir, Gelpern, and Trebesch", ""), ("Buchheit, Chabert, DeLong, and Zettelmeyer", ""), 
        ("Bredenkamp, Hausmann, Pienkowski, and Reinhart", ""),  

        ]

    #get university names
    tuples_universities=_tuples_universities()

    #add university names
    tuples_replace_beforelowercase=tuples_replace_beforelowercase+tuples_universities

    #lowercase indifferent
    tuples_replace_afterlowercase=[

        #hassan
        ("risk officer", ""),
        ("risk credit officer", ""),
        ("unknown speaker", ""),
        ("unknown participant", ""),
        ("unknown caller", ""),
        ("unknown operator", ""),
        ("unknown firm analyst", ""),
        ("in the states", ""),
        
        #epub-relevant
        ("see also", ""), ("see figure", ""), 
        
        #abbasa2019
        ("working paper", ""),
        ]

    #remove tokens and bigrams (also stopwords and single letters)
    bad_tokens={

        #hassan
        "i", "ive", "youve", "weve", "im", "youre", "were", "id", "youd", "wed", "thats",

        #added
        *stop_words,
        *alphabet,
        
        }
    bad_bigrams={
        
        #hassan
        "princeton university",

        }
    
    return tuples_replace_beforelowercase, tuples_replace_afterlowercase, bad_tokens, bad_bigrams


#Loughran-McDonald sentiment words
#https://sraf.nd.edu/loughranmcdonald-master-dictionary/
def _loughran_sentiment(sentiment):

    #read csv
    filepath="_resources/Loughran-McDonald_MasterDictionary_1993-2021"
    df=pd.read_csv(f"{filepath}.csv", dtype="string")

    #drop na
    df=df.dropna(subset=["Word"])

    #list values
    list_values=df.loc[df[sentiment]!="0", "Word"].str.lower().to_list()

    #set values
    set_values=set(list_values)

    return set_values


#general sets
def get_generalsets():

    #uncertainty
    #https://github.com/mschwedeler/firmlevelrisk/blob/master/input/riskwords/synonyms.txt
    filepath="_resources/synonyms_uncertainty"
    bad_keywords={"question", "questions", "venture"}
    set_synonyms_uncertainty=_csv_to_setvalues(filepath, bad_keywords)

    #loughran
    sentiment="Positive"
    set_loughran_positive=_loughran_sentiment(sentiment)
    sentiment="Negative"
    set_loughran_negative=_loughran_sentiment(sentiment)

    #sovereign
    filepath="_resources/sovereign"
    bad_keywords=set()
    set_sovereign=_csv_to_setvalues(filepath, bad_keywords)

    #add others here
    #

    return set_synonyms_uncertainty, set_loughran_positive, set_loughran_negative, set_sovereign


#topic dicts
def get_topicdicts():

    #index col
    index_col="bigram"

    #topic bigrams P\N
    filepath="_topicbigrams/_topicbigrams_pn.csv"
    dict_topicbigrams_pn=_csv_to_dictdf(filepath, index_col)

    #topic bigrams N\P
    filepath="_topicbigrams/_topicbigrams_np.csv"
    dict_topicbigrams_np=_csv_to_dictdf(filepath, index_col)

    return dict_topicbigrams_pn, dict_topicbigrams_np

