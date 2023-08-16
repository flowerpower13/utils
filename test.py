

#imports
import pandas as pd


#from echo data
folders=["zhao/_data/echo", "zhao/_echo"]
items=["EXPORTER", "EXPORTER_screen"]
def _echo(folders, items):

    #folders
    resources=folders[0]
    results=folders[1]

    #items
    resource=items[0]
    result=items[1]

    #vars
    cols=[
        "REGISTRY_ID"
        "FEC_CASE_IDS", 
        "FAC_LAST_PENALTY_AMT",
        "FAC_DATE_LAST_FORMAL_ACTION",
        "FAC_DATE_LAST_PENALTY",
        "FEC_LAST_CASE_DATE",
        "FAC_DATE_LAST_FORMAL_ACT_EPA",
        ]

    #read
    filepath=f"{resources}/{resource}.csv"
    df=pd.read_csv(
        filepath,
        dtype="string",
        usecols=cols,
        encoding='utf-8',
        parse_dates=False,
        )


    #dropna
    dropna_cols=[
        "FEC_CASE_IDS", 
        "FAC_LAST_PENALTY_AMT",
        ]
    df=df.dropna(subset=dropna_cols)


    #save
    filepath=f"{results}/{result}.csv"
    df.to_csv(filepath, index=False)


_echo(folders, items)
print("done")