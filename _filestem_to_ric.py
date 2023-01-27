
import pandas as pd


#functions
from _pd_DataFrame import _pd_DataFrame

#extract ric
def _extract_ric(row):
    
    left_idx=len("yyyy-mmm-dd-")
    row_cut=row[left_idx:]
    right_idx=row_cut.find("-")+left_idx
    ric=row[left_idx:right_idx]

    return ric

#file stems to rics
#folders=["_concat", "_convert_symbols0"]
#items=["_concat", "file_stem"]
def _filestem_to_ric(folders, items):
    resources=folders[0]
    results=folders[1]

    resource=items[0]
    result=items[1]

    file_path=f"{resources}/{resource}.csv"
    df=pd.read_csv(file_path, dtype="string")

    #col names
    colname_filestem="file_stem"
    colname_timevar="timevar"
    colname_year="year"
    colname_quarter="quarter"
    colname_yearquarter="year_quarter"
    colname_ric="ric"

    #link
    link_yearquarter="Q"

    #extract timevar
    left_idx=len("yyyy-mmm-dd-")
    timevar_str=df[colname_filestem].str[0:left_idx-1]
    df[colname_timevar]=pd.to_datetime(timevar_str, format="%Y-%b-%d")

    #year, quarter, and year_quarter
    df[colname_year]=pd.DatetimeIndex(df[colname_timevar]).year
    df[colname_quarter]=pd.DatetimeIndex(df[colname_timevar]).quarter
    df[colname_yearquarter]=df[colname_year].apply(str)+link_yearquarter+df[colname_quarter].apply(str)

    #extract ric
    df[colname_ric]=df[colname_filestem].apply(_extract_ric)

    #new cols
    new_cols=[
        colname_filestem, 
        colname_timevar, 
        colname_year, 
        colname_quarter, 
        colname_yearquarter, 
        colname_ric,
        ]
    df=df[new_cols]

    file_path=f"{results}/{result}.csv"
    df.to_csv(file_path, index=False)