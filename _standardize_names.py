

#https://github.com/rahulissar/ai-supply-chain
#https://medium.com/analytics-vidhya/supplier-name-standardization-using-unsupervised-learning-adb27bed9e0d


#https://github.com/psolin/cleanco


#imports
import re
import nltk
import unicodedata
from cleanco import basename
from sklearn.feature_extraction.text import ENGLISH_STOP_WORDS


# List of keywords to help identify stop_words
vendor_stopwords=['biz', 'bv', 'co', 'comp', 'company', 
                'corp','corporation', 'dba', 
                'inc', 'incorp', 'incorporat', 
                'incorporate', 'incorporated', 'incorporation', 
                'international', 'intl', 'intnl', 
                'limited' ,'llc', 'ltd', 'llp', 
                'machines', 'pvt', 'pte', 'private', 'unknown',
                "american", "america",
                ]


# Remove vendor specific stop words
def clean_stopwords(text, english):

    if english == False:
        custom = vendor_stopwords

    else:
        custom = vendor_stopwords + list(ENGLISH_STOP_WORDS)

    for x in custom:
        pattern2 = r'\b'+x+r'\b'
        text=re.sub(pattern2,'',text)

    return text

# Trim the text to remove spaces
def clean_spaces(text):

    #white space
    text=text.replace('   ', ' ')
    text=text.replace('  ', ' ')
    text=text.strip()
    
    #too short
    if len(text) < 1:
        text='Tooshorttext'

    return text


def _standardize_query(text, lemm=False, english=True):

    #try
    try:

        text=text.replace("in-kind", "")

        #filter ascii
        text=unicodedata.normalize('NFKD', text).encode('ascii', 'ignore').decode('utf-8', 'ignore')

        #remove special characters, but not digits
        text=re.sub(r'[^a-zA-Z0-9 \-–]', ' ', text)

        #remove digits
        #text=re.sub(r'[0-9 ]', '', text)

        #lowercase
        text=text.lower()

        #clean stopwords
        text=clean_stopwords(text, english)

        #clean company name
        text=basename(text)

        #clean spaces
        text=clean_spaces(text) 


        ## Lemmatisation (convert the word into root word)
        if lemm == True:
            lem = nltk.stem.wordnet.WordNetLemmatizer()
            text=lem.lemmatize(text)
    
    #except
    except Exception as e:
        print(e)
    
    return text








# Importing the necessary libraries
from fuzzywuzzy import fuzz
import numpy as np

# Function to generate similarity matrix. Provide input as df['Column Name'] to this function
def fuzz_similarity(column):
  similarity_array = np.ones((len(column), (len(column))))*100
  for i in range(1, len(column)):
    for j in range(i):
      s1 = fuzz.token_set_ratio(column[i],column[j]) + 0.00000000001
      s2 = fuzz.partial_ratio(column[i],column[j]) + 0.00000000001
      similarity_array[i][j] = 2*s1*s2 / (s1+s2)
      
  for i in range(len(column)):
    for j in range(i+1,len(column)):
      similarity_array[i][j] = similarity_array[j][i]
      np.fill_diagonal(similarity_array, 100)
  return similarity_array












# Importing the necessary libraries
from sklearn.cluster import AffinityPropagation
import pandas as pd



def company_clusters_modified(df_left, s, colname, similarity_array):

    clusters = AffinityPropagation(
        damping=0.99,
        max_iter=10000,
        convergence_iter=15,
        copy=True,
        preference=1,
        affinity='precomputed',
        verbose=True,
        random_state=None,
        ).fit_predict(similarity_array)

    df_clusters = pd.DataFrame(list(zip(s, clusters)), columns=[colname, 'Cluster'])

    new = pd.merge(df_left, df_clusters, how="inner", on=colname)

    return new



def _series_to_df(df, colname):

    #unique and sort
    s=sorted(df[colname].unique())

    #Cleaned_name
    Cleaned_name=s.apply(_standardize_query)

    #similarity_array
    similarity_array=fuzz_similarity(Cleaned_name)

    #create df left
    d={
        colname: s,
        "Cleaned_name": Cleaned_name,
        }
    df_left=pd.DataFrame(data=d)

    #create new df
    df=company_clusters_modified(df_left, s, colname, similarity_array)
    
    #mode
    grouped = df.groupby('Cluster')[colname].apply(lambda x: x.mode()[0])
    df["std_name"]=df['Cluster'].map(grouped)

    return df



