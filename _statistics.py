

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


#save table
def _save_table(results, filestem, text):

    #folderstem
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


#save figure
def _save_figure(results, filestem):

    #folderstem
    folderstem="figures"

    #mkdir
    directory_path=Path(f"{results}/{folderstem}")
    directory_path.mkdir(exist_ok=True)

    #filepath
    filepath=f"{results}/{folderstem}/{filestem}.png"

    #save
    plt.savefig(filepath)

    #show
    #plt.show()


#from table to write
def _table_to_save(df_stats, label, caption, tablenotes, tuples_replace, results): 

    #styler object
    styler_object=df_stats.style

    #format
    format_styler="{:,.0f}"
    styler_object=styler_object.format(format_styler)

    #args
    #https://pandas.pydata.org/docs/reference/api/pandas.io.formats.style.Styler.to_latex.html
    buf=None
    column_format=None
    position="!htbp"
    position_float="centering"
    hrules=True
    clines=None
    #label
    #caption
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

    #text
    text=_text_w_tablenotes(latex_table, tablenotes)
    
    #replace txt
    text=_replace_txt(text, tuples_replace)

    #save
    _save_table(results, label, text)


#table summary
def _table_summary(df, results):

    #label
    label="table_summary"
    #caption
    caption="Summary Statistics"

    #cols
    cols=[
        "at",
        "ni",
        ]
    
    #tuples replace
    tuples_replace=[
        ("at &", "Total assets &"),
        ("ni &", "Net income &"),
        ]
    
    #table notes
    tablenotes="lots of tablenotes"
    
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

    #to numeric
    for i, col in enumerate(cols):
        df[col]=pd.to_numeric(df[col])

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
    
    _table_to_save(df_stats, label, caption, tablenotes, tuples_replace, results)


#table correlation
def _table_correlation(df, results):

    #label
    label="table_correlation"
    
    #caption
    caption="Pearson Correlation Matrix"

    #cols
    cols=[
        "at",
        "ni",
        ]
    
    tuples_replace=[
        ("at &", "Total assets &"),
        ("ni &", "Net income &"),
        ("& at", "& Total assets"),
        ("& ni", "& Net income"),
        ]

    #table notes
    tablenotes="lots of tablenotes"

    #to numeric
    for i, col in enumerate(cols):
        df[col]=pd.to_numeric(df[col])

    #correlations
    df=df[cols]
    df_stats=df.corr(
        method="pearson",
        min_periods=3,
        )

    _table_to_save(df_stats, label, caption, tablenotes, tuples_replace, results)


#figure frequency
def _figure_frequency(df, results):

    filestem="frequency_at"

    # Sample data
    x = [1, 2, 3, 4, 5]
    y = [10, 20, 15, 25, 30]

    # Create the plot
    plt.plot(x, y, marker='o')
    plt.title('Sample Plot')
    plt.xlabel('X-axis')
    plt.ylabel('Y-axis')

    #save
    _save_figure(results, filestem)


#des stats
folders=["zhao/_finaldb", "zhao/article"]
items=["donations_violations_crspcompustat"]
def _generate_floats(folders, items):

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
        nrows=10000,
        )
    
    #table summary
    _table_summary(df, results)

    #table correlation
    #_table_correlation(df, results)

    #figure
    #_figure_frequency(df, results)

    #regs



_generate_floats(folders, items)
print("done")

