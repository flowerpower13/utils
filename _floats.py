

#import
import stargazer
import statsmodels
import numpy as np
import pandas as pd
import seaborn as sns
from pathlib import Path
import statsmodels.api as sm
import matplotlib.pyplot as plt
from stargazer.stargazer import Stargazer, LineLocation


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
        0.90,
        0.99,
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
        "90\%",
        "99\%",
        "Max",
        ]
       
    #styler object
    styler_object=df.style

    #format
    format_styler="{:,.3f}"
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


#violtrack summary stats
def _echo_table_summary(results):

    filepath="zhao/_merge/crspcompustat_donations_echo_screen.csv"
    df=pd.read_csv(
        filepath,
        dtype="string",
        #nrows=1000,
        )

    #label
    label="echo_table_summary"

    #caption
    caption="Donations to AG assn and EPA Enforcement"

    #cols
    cols=[
        #irs
        "amount_democratic",
        "amount_republican",
        "amount_both",
        "amount_democratic_past3",
        "amount_republican_past3",
        "amount_both_past3",
        "dummy_democratic_past3",
        "dummy_republican_past3",
        "dummy_both_past3",

        #echo
        "fed_penalty_assessed_amt",
        "dummy_echo_penalty",

        #crspcompustat
        "at",
        "lt",
        "mkvalt",
        "revt",
        "ni",
        ]
    
    #tuples replace
    tuples_replace=[
        ("amount_democratic &", "Donations to Dem. AG assn &"),
        ("amount_republican &", "Donations to Rep. AG assn &"),
        ("amount_both &", "Donations to both AG assn &"),
        ("amount_democratic_past3 &", "Donations to Dem. AG assn past 3y &"),
        ("amount_republican_past3 &", "Donations to Rep. AG assn past 3y &"),
        ("amount_both_past3 &", "Donations to both AG assn past 3y &"),
        ("dummy_democratic_past3 &", "Donated to Dem. AG assn past 3y &"),
        ("dummy_republican_past3 &", "Donated to Rep. AG assn past 3y &"),
        ("dummy_both_past3 &", "Donated to both AG assn past 3y &"),

        #echo
        ("fed_penalty_assessed_amt &", "EPA Penalty Amount &"),
        ("dummy_echo_penalty", "EPA Penalty Dummy"),

        #crspcompustat
        ("at &", "Assets &"),
        ("lt &", "Liabilities &"),
        ("mkvalt &", "Market Value &"),
        ("revt &", "Revenues &"),
        ("ni &", "Net Income &"),
        ]
    
    #table notes
    tablenotes="The table above provide summary statistics for donations to attorneys general associations, EPA penalties, and company fundamentals. The variables \\textit{Donations to Dem. AG assn} and \\textit{Donations to Rep. AG assn}captures the monetary amount of the political contribution to the Democratic Attorneys General Association (DAGA) and Republican Attorneys General Association (RAGA), respectively. The variable \\textit{Donations to Dem. AG assn past 3y} captures the sum of political contributions made to DAGA in the past 3 years, while \\textit{Donations to Dem. AG assn past 3y} captures the Republican counterpart. The variable \\textit{Donated to Dem. AG assn past 3y} equals one if the commpany donated consistently to DAGA in the past 3 years, 0 otherwise. The variable \\textit{Donated to Rep. AG assn past 3y} is the Republican counterpart. The variable \\textit{EPA Penalty Amount} represents the monetary value of penalties levied on corporations by the EPA for various environmental violations."

    #table summary
    _table_summary(df, cols, label, caption, tablenotes, tuples_replace, results)


#stargazer from vars to results
def _vars_to_results(df, depvar, indepvars):

    #to numeric
    tonumeric_cols=[depvar] + indepvars
    errors="raise"
    df=_tonumericcols_to_df(df, tonumeric_cols, errors)
    
    #depvar
    Y=df[depvar]

    #indep vars
    X=df[indepvars]

    #add constant
    X=sm.add_constant(X)

    #model
    model=sm.OLS(
        endog=Y,
        exog=X,
        missing="drop",
        )

    #results
    results=model.fit()

    #return
    return results


#stargazer parameters
def _stargazer_parameters(models, label, caption, depvar_name, indepvars, tablenotes, sig_digits=3):

    #stargazer
    stargazer=Stargazer(models)

    #generate_header
    stargazer.table_label=label

    #title
    stargazer.title(caption)

    #show_header
    stargazer.show_header=True

    #show_model_numbers
    stargazer.show_model_numbers(True)

    #custom_columns

    #significance_levels

    #significant_digits
    stargazer.significant_digits(sig_digits)

    #show_confidence_intervals

    #dependent_variable_name
    stargazer.dependent_variable_name(depvar_name)

    #rename_covariates

    #covariate_order
    ordered_cols=["const"] + indepvars
    stargazer.covariate_order(ordered_cols)

    #reset_covariate_order

    #show_degrees_of_freedom
    stargazer.show_degrees_of_freedom(False)

    #custom_note_label

    #add_custom_notes
    stargazer.add_custom_notes([tablenotes])

    #add_line

    #append_notes

    #return
    return stargazer


#_preliminary_table_reg(results)
def _preliminary_table_reg(results):

    #https://github.com/StatsReporting/stargazer
    #https://github.com/StatsReporting/stargazer/blob/master/examples.ipynb

    filepath="zhao/_merge/crspcompustat_donations_echo_screen.csv"
    df=pd.read_csv(
        filepath,
        dtype="string",
        #nrows=1000,
        )

    #label
    label="preliminary_table_reg"

    #caption
    caption="Political Contributions and Regulatory Enforcement"
    
    #tuples replace
    tuples_replace=[
        ("amount_democratic &", "Donations to Dem. AG assn &"),
        ("amount_republican &", "Donations to Rep. AG assn &"),
        ("amount_both &", "Donations to both AG assn &"),
        ("amount_democratic_past3 &", "Donations to Dem. AG assn past 3y &"),
        ("amount_republican_past3 &", "Donations to Rep. AG assn past 3y &"),
        ("amount_both_past3 &", "Donations to both AG assn past 3y &"),
        ("dummy_democratic_past3 &", "Donated to Dem. AG past 3y &"),
        ("dummy_republican_past3 &", "Donated to Rep. AG past 3y &"),
        ("dummy_both_past3 &", "Donated to both AG assn past 3y &"),

        #echo
        ("fed_penalty_assessed_amt &", "EPA Penalty Amount &"),
        ("dummy_echo_penalty", "EPA Penalty Dummy"),

        #crspcompustat
        ("at &", "Assets &"),
        ("lt &", "Liabilities &"),
        ("mkvalt &", "Market Value &"),
        ("revt &", "Revenues &"),
        ("ni &", "Net Income &"),
        ]
    
    #depvar name
    depvar_name="EPA Penalty Amount"
    
    #table notes
    tablenotes="first note"

    #vars to res
    depvar="fed_penalty_assessed_amt"
    indepvars=[
        "amount_both",
        "at",
        "ni",
        ]
    res0=_vars_to_results(df, depvar, indepvars)

    #models
    models=[
        res0,
        ]

    #parameters
    stargazer=_stargazer_parameters(models, label, caption, depvar_name, indepvars, tablenotes)

    #fixed effects
    stargazer.add_line('IndustryFE', ["No"])

    #text
    text=stargazer.render_latex()

    #replace txt
    text=_replace_txt(text, tuples_replace)

    #save
    _save_table(results, label, text)


#des stats
results="zhao/article"
def _generate_floats(results):

    #https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.describe.html
    #https://github.com/StatsReporting/stargazer

    #table summary
    #_echo_table_summary(results)
    
    #table reg
    _preliminary_table_reg(results)



_generate_floats(results)
print("done")

