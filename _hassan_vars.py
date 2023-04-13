

#imports
import pandas as pd


#functions
from _pd_utils import _csv_to_dictbag, _csv_to_dictdf


#REMOVE PARTS OF SPEECH - #https://www.nltk.org/api/nltk.tag.perceptron.html
#Hassan et al. 2019 QJE- Appendix B - https://doi.org/10.1093/qje/qjz021


#update parts of speech
#import nltk
#nltk.help.upenn_tagset()


#parts of speech
pronouns=["PRP", "PRP$"]
prepositions=["TO", "IN"]
adverbs=["RB", "RBR", "RBS"]
wh_adverbs=["WRB"]
determiners=["DT", "PDT"] 


#parts of speech
#pronouns
pronoun_tag0=f'(tag0=="{pronouns[0]}" or tag0=="{pronouns[1]}")'
pronoun_tag1=f'(tag1=="{pronouns[0]}" or tag1=="{pronouns[1]}")'

#prepositions
preposition_tag0=f'(tag0=="{prepositions[0]}" or tag0=="{prepositions[1]}")'
preposition_tag1=f'(tag1=="{prepositions[0]}" or tag1=="{prepositions[1]}")'

#adverbs
adverb_tag0=f'(tag0=="{adverbs[0]}" or tag0=="{adverbs[1]}" or tag0=="{adverbs[2]}")'
adverb_tag1=f'(tag1=="{adverbs[0]}" or tag1=="{adverbs[1]}" or tag1=="{adverbs[2]}")'

#wh adverb
wh_adverb_tag0=f'(tag0=="{wh_adverbs[0]}")'
wh_adverb_tag1=f'(tag1=="{wh_adverbs[0]}")'

#determiner
determiner_tag0=f'(tag0=="{determiners[0]}" or tag0=="{determiners[1]}")'
determiner_tag1=f'(tag1=="{determiners[0]}" or tag1=="{determiners[1]}")'


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


#all conditions
nltk_conditions=f'''
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


#remove bad keywords from synonyms uncertainty
#Hassan et al. 2019 QJE- Appendix B - https://doi.org/10.1093/qje/qjz021
bad_sentiment=["question", "questions", "venture"]


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


#sovereign
file_path="sovereign"
set_sovereign=_csv_to_dictbag(file_path)


#synonyms_uncertainty
#https://github.com/mschwedeler/firmlevelrisk/blob/master/input/riskwords/synonyms.txt
file_path="synonyms_uncertainty"
set_synonyms_uncertainty=_csv_to_dictbag(file_path)
set_synonyms_uncertainty[file_path]=[
    x for x in set_synonyms_uncertainty[file_path] 
    if x not in bad_sentiment
    ]


#loughran
sentiment="Positive"
set_loughran_positive=_loughran_sentiment(sentiment)
sentiment="Negative"
set_loughran_negative=_loughran_sentiment(sentiment)


#dict bags
dict_bags={

    #sovereign
    **set_sovereign,
    
    #loughran
    **set_loughran_positive,
    **set_loughran_negative,

    #synonyms uncertainty
    **set_synonyms_uncertainty,

    }

