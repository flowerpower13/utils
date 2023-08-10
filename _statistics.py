

#import
import stargazer
import statsmodels
import numpy as np
import pandas as pd
import seaborn as sns
from pathlib import Path
import matplotlib.pyplot as plt


#functions
from _string_utils import _replace_txt


#from tablenotes to text
def _text_w_tablenotes(latex_table, tablenotes):

    old=r"\end{tabular}"
    begin=r"\begin{tablenotes}"
    end=r"\end{tablenotes}"
    new=f"{old}\n{begin}\n{tablenotes}\n{end}"

    #text
    text=latex_table.replace(old, new)

    return text


#nice table
def _nice_table(text):


    return text



#write
def _write(results, filestem, text):

    #folder stem
    folderstem="tables"

    #mkdir
    directory_path=Path(f"{results}/{folderstem}")
    directory_path.mkdir(exist_ok=True)

    #filepath
    filepath=f"{results}/{folderstem}/{filestem}.tex"

    #write
    with open(
        file=filepath,
        mode="w",
        ) as file_object:
        file_object.write(text)


#table summary
def _table_summary(df, results):

    #cols
    cols=[
        "at",
        "ni",
        ]
    
    #to numeric
    for i, col in enumerate(cols):
        df[col]=pd.to_numeric(df[col])

    #percentiles
    percentiles=[
        #0.01,
        #0.10,
        #0.25, 
        0.50, 
        #0.75,
        #0.90,
        #0.99,
        ]

    #df stats
    df_stats=df[cols].describe(percentiles=percentiles).transpose()

    #df stats columns
    df_stats.columns=[
        "N Obs.",
        "Mean",
        "Std. Dev.",
        "Min",
        #"1\%",
        #"10\%",
        #"25\%",
        "Median",
        #"75\%",
        #"90\%",
        #"99\%",
        "Max",
        ]

    #styler object
    styler_object=df_stats.style

    #format
    styler_object=styler_object.format("{:,.2f}")

    #args
    #https://pandas.pydata.org/docs/reference/api/pandas.io.formats.style.Styler.to_latex.html
    buf=None
    column_format=None
    position="!htbp"
    position_float="centering"
    hrules=True
    clines=None
    label="table_summary"
    caption="Summary Statistics"
    sparse_index=True
    sparse_columns=False
    multirow_align=None
    multicol_align=None
    siunitx=False
    environment=None
    convert_css=False

    #latex table
    latex_table=styler_object.to_latex(
        buf=buf,
        column_format=column_format,
        position=position,
        position_float=position_float,
        hrules=hrules,
        clines=clines,
        label=label,
        caption=caption,
        sparse_index=sparse_index,
        sparse_columns=sparse_columns,
        multirow_align=multirow_align,
        multicol_align=multicol_align,
        siunitx=siunitx,
        environment=environment,
        convert_css=convert_css,
        )

    #table notes
    tablenotes="lots of tablenotes"

    #text
    text=_text_w_tablenotes(latex_table, tablenotes)
    
    #change column names
    tuples_replace=[
        ("at &", "Total assets &"),
        ("ni &", "Net income &"),
        ]
    text=_replace_txt(text, tuples_replace)

    #nice looking
    text=_nice_table(text)

    #print(text)
    
    #input
    filestem="table_summary"
    _write(results, filestem, text)


#table correlation
def _table_correlation(df, results):

    #cols
    cols=[
        "at",
        "ni",
        ]
    
    #to numeric
    for i, col in enumerate(cols):
        df[col]=pd.to_numeric(df[col])

    #correlations
    df=df[cols]
    df_corr=df.corr(
        method="pearson",
        min_periods=3,
        )

    #styler object
    styler_object=df_corr.style

    #format
    styler_object=styler_object.format("{:,.2f}")

    #args
    #https://pandas.pydata.org/docs/reference/api/pandas.io.formats.style.Styler.to_latex.html
    buf=None
    column_format=None
    position="!htbp"
    position_float="centering"
    hrules=True
    clines=None
    label="table_correlation"
    caption="Pearson Correlation Matrix"
    sparse_index=True
    sparse_columns=False
    multirow_align=None
    multicol_align=None
    siunitx=False
    environment=None
    convert_css=False

    #latex table
    latex_table=styler_object.to_latex(
        buf=buf,
        column_format=column_format,
        position=position,
        position_float=position_float,
        hrules=hrules,
        clines=clines,
        label=label,
        caption=caption,
        sparse_index=sparse_index,
        sparse_columns=sparse_columns,
        multirow_align=multirow_align,
        multicol_align=multicol_align,
        siunitx=siunitx,
        environment=environment,
        convert_css=convert_css,
        )

    #table notes
    tablenotes="lots of tablenotes"

    #text
    text=_text_w_tablenotes(latex_table, tablenotes)
    
    #change column names
    tuples_replace=[
        ("at &", "Total assets &"),
        ("ni &", "Net income &"),
        ("& at", "& Total assets"),
        ("& ni", "& Net income"),
        ]
    text=_replace_txt(text, tuples_replace)

    #nice looking
    text=_nice_table(text)

    #print(text)
    
    #input
    filestem="table_correlation"
    _write(results, filestem, text)


#des stats
folders=["zhao/_finaldb", "zhao/article"]
items=["donations_cusip_compustat"]
def _des_stats(folders, items):

    #https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.describe.html
    #https://pandas.pydata.org/docs/reference/api/pandas.io.formats.style.Styler.to_latex.html
    #https://github.com/StatsReporting/stargazer

    #folders
    resources=folders[0]
    results=folders[1]

    #items
    resource=items[0]

    filepath=f"{resources}/{resource}.csv"
    df=pd.read_csv(
        filepath,
        dtype="string",
        )
    
    #table summary
    _table_summary(df, results)

    #table correlation
    _table_correlation(df, results)

    #regs


    


    

    
_des_stats(folders, items)






print("done")

