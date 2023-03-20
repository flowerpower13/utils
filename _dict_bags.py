
#import
import pandas as pd


#functions
from _pd_utils import _csv_to_dictbag



#retrieve loughran sentiment words
file_path="Loughran-McDonald_MasterDictionary_1993-2021"
df=pd.read_csv(f"{file_path}.csv", dtype="string")
def _loughran_sentiment(sentiment):

    #key
    dict_key=f"loughran_{sentiment.lower()}"

    #value
    dict_value=df.loc[df[sentiment]!= 0, "Word"].str.lower().to_list()

    #dict
    loughran_sentiment={dict_key: dict_value}

    return loughran_sentiment


#sovereign
file_path="sovereign"
sovereign=_csv_to_dictbag(file_path)
#synonyms_uncertainty
file_path="synonyms_uncertainty"
synonyms_uncertainty=_csv_to_dictbag(file_path)


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

#'''
