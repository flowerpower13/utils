
from pathlib import Path


#functions
from _concat import _folder_to_filestems
from _string_utils import _dicttopics_to_dictbags
from _pd_DataFrame import _pd_DataFrame, _dict_to_valscols
from _txt_to_count import _txt_to_count, _firmlevelrisk_scores, _firmlevelrisk_dictbags


#variables
error="???"
from _dict_topics import dict_topics
dict_bags=_dicttopics_to_dictbags(dict_topics)
items=["Loughran-McDonald_MasterDictionary_1993-2021.csv", "riskwords.txt", "covid.csv", "political_bigrams.csv"]
dict_sentiment_risk_covid, dict_politicalbigrams=_firmlevelrisk_dictbags(items)


#compute counts
#folders=["_pdfs_to_txts", "_txts_to_counts"]
#items=["_txts_to_counts"]
def _txts_to_counts(folders, items):
    resources=folders[0]
    results=folders[1]

    result=items[0]

    #colname
    colname_filestems="file_stem"

    #resources
    files, file_stems=_folder_to_filestems(resources)

    n_obs=len(files)
    tot=n_obs-1

    file_stems=[None]*n_obs
    converteds=[None]*n_obs

    for i, file in enumerate(files):
        file_stem=file.stem

        output=Path(f"{results}/{file_stem}.csv")

        if not output.is_file():
            try:
                with open(
                    file=file, 
                    mode='r', 
                    encoding="utf-8", 
                    #errors="ignore", 
                    ) as f:
                    text=f.read()

                if text==error:
                    converted=False
                    print(f"{i}/{tot} - {file_stem} - error")

                elif text!=error:
                    converted=True

                    #values and columns
                    values=[
                        [file_stem],
                        ]
                    columns=[
                        "file_stem", 
                        ]

                    #functions
                    #txt to count
                    dict_data=_txt_to_count(text, dict_bags)
                    Vs, Ks = _dict_to_valscols(dict_data)
                    values+=Vs
                    columns+=Ks

                    '''dict_data=_firmlevelrisk_scores(text, dict_sentiment_risk_covid, dict_politicalbigrams)
                    Vs, Ks = _dict_to_valscols(dict_data)
                    values+=Vs
                    columns+=Ks'''

                    df=_pd_DataFrame(values, columns)
                    df.to_csv(output, index=False)

                    print(f"{i}/{tot} - {file_stem} - done")

            except Exception as e:
                converted=False
                print(f"{i}/{tot} - {file_stem} - exception")
                print(e)
    
        elif output.is_file():
            converted=True
            print(f"{i}/{tot} - {file_stem} - already done")

        file_stems[i]=file_stem
        converteds[i]=converted
  
    values=[
        file_stems, 
        converteds, 
        ]
    columns=[
        "file_stem", 
        "converted", 
        ]
    df=_pd_DataFrame(values, columns)

    #sort
    df=df.sort_values(by=colname_filestems)

    #save
    file_path=f"{results}/{result}.csv"
    df.to_csv(file_path, index=False)

