

#imports
import pandas as pd


#functions
from _pd_utils import _csv_to_dictbag


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
bad_keywords=["question", "questions", "venture"]


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
sovereign=_csv_to_dictbag(file_path)


#synonyms_uncertainty
#https://github.com/mschwedeler/firmlevelrisk/blob/master/input/riskwords/synonyms.txt
file_path="synonyms_uncertainty"
synonyms_uncertainty=_csv_to_dictbag(file_path)
synonyms_uncertainty[file_path]=[
    x for x in synonyms_uncertainty[file_path] 
    if x not in bad_keywords
    ]


#loughran
sentiment="Positive"
loughran_positive=_loughran_sentiment(sentiment)
sentiment="Negative"
loughran_negative=_loughran_sentiment(sentiment)


#dict bags
dict_bags={

    #sovereign
    **sovereign,
    
    #loughran
    **loughran_positive,
    **loughran_negative,

    #synonyms uncertainty
    **synonyms_uncertainty,

    }

