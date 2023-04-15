
#import
from pathlib import Path


#functions
from _concat import _folder_to_filestems
from _pd_utils import _pd_DataFrame, _dict_to_valscols
from _string_utils import _txt_to_tokens, _tokensbags_to_scores


#variables
from _string_utils import error


#txt to bags' scores
def _txt_to_count(text, targetbags, contextbags, window_sizes):

    #clean, tokenize, and count n words in text
    list_tokens, n_words = _txt_to_tokens(text)

    #create empty dict for storing data
    dict_data={}

    #iter through target bags
    for k, targetbag_key in enumerate(targetbags):

        #target bag's value
        targetbag_value=targetbag_key.values()

        #iter through context bags
        for j, contextbag_key in enumerate(contextbags):
                
            #iter through window sizes
            for i, window_size in enumerate(window_sizes):

                #context bag's value
                contextbag_value=contextbag_key.values()

                #compute bag's score
                total, contextual = _tokensbags_to_scores(list_tokens, targetbag_value, contextbag_value, window_size)

                #fill dict with total occurrences
                dict_data[targetbag_key]=total
                #fill dict with contextual occurrences
                dict_data[f"{targetbag_key}__{contextbag_key}__{window_size}"]=contextual

    dict_data["n_words"]=n_words
    #print(dict_data)
    
    return dict_data




#from file to converted
def _file_to_converted(file, file_stem, output, i, tot, targetbags, contextbags, window_sizes):
    try:
        with open(
            file=file, 
            mode='r', 
            encoding="utf-8", 
            #errors="ignore", 
            ) as f:
            text=f.read()

        if text==error:

            #converted
            converted=False

            #print
            print(f"{i}/{tot} - {file_stem} - error")

        elif text!=error:
            #converted
            converted=True

            #values and columns
            values=[
                [file_stem],
                ]
            columns=[
                "file_stem", 
                ]

            #txt to count
            dict_data=_txt_to_count(text, targetbags, contextbags, window_sizes)  
            Vs, Ks = _dict_to_valscols(dict_data)
            values+=Vs
            columns+=Ks

            #create df
            df=_pd_DataFrame(values, columns)
            df.to_csv(output, index=False)

            #print(df)

            #print
            print(f"{i}/{tot} - {file_stem} - done")

    except Exception as e:

        #converted
        converted=False

        #print
        print(f"{i}/{tot} - {file_stem} - exception")
        print(e)
    
    return converted


#compute counts
#folders=["_pdfs_to_txts", "_txts_to_counts"]
#items=["_txts_to_counts"]
def _txts_to_counts(folders, items, targetbags, contextbags, window_sizes):
    resources=folders[0]
    results=folders[1]

    result=items[0]

    #colname
    colname_filestems="file_stem"

    #resources
    files, file_stems=_folder_to_filestems(resources)

    #n obs
    n_obs=len(files)
    tot=n_obs-1

    #empty lists
    file_stems=[None]*n_obs
    converteds=[None]*n_obs

    for i, file in enumerate(files):
        file_stem=file.stem

        output=Path(f"{results}/{file_stem}.csv")

        #file is NOT present
        if not output.is_file():

            #converted
            converted=_file_to_converted(file, file_stem, output, i, tot, targetbags, contextbags, window_sizes)

        #file is present
        elif output.is_file():

            #converted
            converted=True

            #print
            print(f"{i}/{tot} - {file_stem} - already done")

        #fill lists
        file_stems[i]=file_stem
        converteds[i]=converted
  
    #create df
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


