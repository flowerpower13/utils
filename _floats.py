

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
from _pd_utils import _tonumericcols_to_df


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


#figure frequency
def _figure_frequency(results):

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


#table summary stats
def _table_summary(df, cols, label, caption, tablenotes, tuples_replace, results):

    #to numeric
    tonumeric_cols=cols
    errors="raise"
    df=_tonumericcols_to_df(df, tonumeric_cols, errors)
    
    #percentiles
    percentiles=[
        0.50, 
        ]

    #df stats
    df=df[cols].describe(percentiles=percentiles).transpose()

    #df stats columns
    df.columns=[
        "N Obs.",
        "Mean",
        "Std. Dev.",
        "Min",
        "Median",
        "Max",
        ]
    
    #to divide
    todivide_cols=[
        "Mean",
        "Std. Dev.",
        "Min",
        "Median",
        "Max",
        ]
    todivide_by=1000
    #df[todivide_cols]=df[todivide_cols]/todivide_by
    
    #styler object
    styler_object=df.style

    #format
    format_styler="{:,.0f}"
    styler_object=styler_object.format(format_styler)

    #https://pandas.pydata.org/docs/reference/api/pandas.io.formats.style.Styler.to_latex.html
    tabular=styler_object.to_latex(hrules=True)
    
    #text
    text=(
        #begin
        "\\begin{table}[!htbp]" + "\n"
        "\\centering" + "\n"

        #caption label
        f"\\caption{{{caption}}}" + "\n"
        f"\\label{{{label}}}" + "\n"

        "\\resizebox{\\textwidth}{!}{%" + "\n"

        #tabular
        f"{tabular}" + "}" + "\n"

        #tablenotes
        "\\begin{tablenotes}" + "\n"
        f"{tablenotes}" + "\n"
        "\\end{tablenotes}" + "\n"

        #end
        "\\end{table}" + "\n"
        )

    #replace txt
    text=_replace_txt(text, tuples_replace)

    #save
    _save_table(results, label, text)


#donations summary stats
def _donations_table_summary(results):

    filepath="zhao/_irs/donations_ids_aggregate.csv"
    df=pd.read_csv(
        filepath,
        dtype="string",
        #nrows=1000,
        )

    #label
    label="donations_table_summary"

    #caption
    caption="Political Contributions to Attorneys General Associations"

    #cols
    cols=[
        "democratic_ag",
        "republican_ag",
        ]
    
    #tuples replace
    tuples_replace=[
        ("democratic_ag &", "Donation to Dem. AG assn &"),
        ("republican_ag &", "Donation to Rep. AG assn &"),
        ]
    
    #table notes
    tablenotes="The table above provide summary statistics pertaining to corporate donations directed towards a partisan attorneys general association. The variable \\textit{Donation to Dem. AG assn} captures monetary value of contributions made to the Democratic Attorneys General Association (DAGA) while \\textit{Donation to Rep. AG assn} captures the Republican counterpart."

    _table_summary(df, cols, label, caption, tablenotes, tuples_replace, results)


#echo summary stats
def _echo_table_summary(results):

    filepath="zhao/_epa/echo_ids_aggregate.csv"
    df=pd.read_csv(
        filepath,
        dtype="string",
        #nrows=1000,
        )

    #label
    label="echo_table_summary"

    #caption
    caption="EPA Enforcement"

    #cols
    cols=[
        "fed_penalty_assessed_amt",
        ]
    
    #tuples replace
    tuples_replace=[
        ("fed_penalty_assessed_amt &", "EPA Penalty Amount &"),
        ]
    
    #table notes
    tablenotes="The table above provides a summary of penalty amounts imposed by the Environmental Protection Agency (EPA) on corporations. The variable \\textit{EPA Penalty Amount} represents the monetary value of penalties levied on corporations for various environmental violations"

    _table_summary(df, cols, label, caption, tablenotes, tuples_replace, results)


#violtrack summary stats
def _violtrack_table_summary(results):

    filepath="zhao/_violtrack/violtrack_ids_aggregate.csv"
    df=pd.read_csv(
        filepath,
        dtype="string",
        #nrows=1000,
        )

    #label
    label="violtrack_table_summary"

    #caption
    caption="Court-ordered Enforcement"

    #cols
    cols=[
        "penalty",
        ]
    
    #tuples replace
    tuples_replace=[
        ("penalty &", "Court-ordered Penalty Amount &"),
        ]
    
    #table notes
    tablenotes="The table above presents information regarding penalties resulting from court-ordered enforcement, wherein district courts mandate corporations to pay fines. The variable \\textit{Court-ordered Penalty Amount} represents the monetary value of penalties imposed on corporations as a consequence of legal proceedings"

    _table_summary(df, cols, label, caption, tablenotes, tuples_replace, results)


#violtrack summary stats
def _echo_crspcomp_table_summary(results):

    filepath="zhao/_merge/donations_echo_crspcompustat.csv"
    df=pd.read_csv(
        filepath,
        dtype="string",
        #nrows=1000,
        )

    #label
    label="echo_crspcomp_table_summary"

    #caption
    caption="Donations to AG and EPA Enforcement"

    #cols
    cols=[
       "democratic_ag",
        "republican_ag",
        "fed_penalty_assessed_amt",
        "at",
        "lt",
        "mkvalt",
        "revt",
        "ni",
        ]
    
    #tuples replace
    tuples_replace=[
        ("democratic_ag &", "Donation to Dem. AG assn &"),
        ("republican_ag &", "Donation to Rep. AG assn &"),
        ("fed_penalty_assessed_amt &", "EPA Penalty Amount &"),
        ("at &", "Assets &"),
        ("lt &", "Liabilities &"),
        ("mkvalt &", "Market Value &"),
        ("revt &", "Revenues &"),
        ("ni &", "Net Income &"),
        ]
    
    #table notes
    tablenotes="The table above provides summary statistics on a sample merging donations to attorneys general associations, EPA enforcement actions, and financial data from CRSP/Compustat for firms."

    _table_summary(df, cols, label, caption, tablenotes, tuples_replace, results)




#des stats
results="zhao/article"
def _generate_floats(results):

    #https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.describe.html

    #table summary
    _donations_table_summary(results)
    _echo_table_summary(results)
    _violtrack_table_summary(results)
    _echo_crspcomp_table_summary(results)
    #_violtrack_crspcomp_table_summary(results)


    #https://github.com/StatsReporting/stargazer
    #table reg



_generate_floats(results)
print("done")

