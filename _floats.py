

#import
import numpy as np
import pandas as pd
import seaborn as sns
from pathlib import Path
import statsmodels.api as sm
import matplotlib.pyplot as plt
from stargazer.stargazer import Stargazer


#functions
from _string_utils import _replace_txt
from _pd_utils import _tonumericcols_to_df


#vars
tuples_replace=[
    ("amount_democratic &",         "Donations to Dem. AG assn &"),
    ("amount_republican &",         "Donations to Rep. AG assn &"),
    ("amount_both &",               "Donations to both AG assn &"),
    ("dummy_democratic &",          "Donated to Dem. AG assn &"),
    ("dummy_republican &",          "Donated to Rep. AG assn &"),
    ("dummy_both &",                "Donated to any AG assn &"),
    ("amount_democratic_past3 &",   "Donations to Dem. AG assn past 3y &"),
    ("amount_republican_past3 &",   "Donations to Rep. AG assn past 3y &"),
    ("amount_both_past3 &",         "Donations to both AG assn past 3y &"),
    ("dummy_democratic_past3 &",    "Donated to Dem. AG assn past 3y &"),
    ("dummy_republican_past3 &",    "Donated to Rep. AG assn past 3y &"),
    ("dummy_both_past3 &",          "Donated to any AG assn past 3y &"),
    ("post2015x",                   "Post2015 $\\times$ "),

    #echo
    ("fed_penalty_assessed_amt &",  "EPA Penalty Amount &"),
    ("dummy_echo_enforcement &",    "EPA Enforcement Likelihood &"),
    ("dummy_echo_penalty &",        "EPA Penalty Likelihood &"),

    #crspcompustat
    ("firm_size &",                 "Firm Size &"),
    ("leverage_ratio &",            "Leverage &"),
    ("roa &",                       "ROA &"),
    ("mtb &",                       "Market-to-Book &"),
    ]


#table notes
filepath="zhao/article/tablenotes.txt"
with open(filepath, "r") as file_object:
    tablenotes=file_object.read()


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

        #table notes
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


#interact var names
def _interact_varnames(time_dummy, explanvars):

    #if
    if time_dummy==None:

        #interact vars
        interact_vars=None

    #if
    elif time_dummy!=None:

        #init
        interact_vars=[None]*len(explanvars)

        #interactions
        for i, col in enumerate(explanvars):  

            #var name
            interact_var=f"{time_dummy}x{col}"

            #update
            interact_vars[i]=interact_var

    #return
    return interact_vars


#ordered cols
def _ordered_cols(time_dummy, explanvars, controlvars):

    #if
    if time_dummy==None:

        #orderedd cols
        ordered_cols=["const"] + explanvars + controlvars

    #elif
    elif time_dummy!=None:

        interact_vars=_interact_varnames(time_dummy, explanvars)

        #orderedd cols
        ordered_cols=["const"] + interact_vars + [time_dummy] + explanvars + controlvars

    #return
    return ordered_cols
    

#stargazer from vars to results
def _vars_to_results(df, depvar, time_dummy, explanvars_i, controlvars):

    #if
    if time_dummy==None:

        #indepvars
        indepvars=explanvars_i + controlvars

    #if
    if time_dummy!=None:

        interact_vars=_interact_varnames(time_dummy, explanvars_i)

        #indepvars
        indepvars=interact_vars + [time_dummy] + explanvars_i +  controlvars

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
def _stargazer_parameters(models, label, caption, depvar_name, time_dummy, explanvars, controlvars, sig_digits=3):

    #https://github.com/StatsReporting/stargazer
    #https://github.com/StatsReporting/stargazer/blob/master/examples.ipynb

    #stargazer
    stargazer=Stargazer(models)

    #ordered cols
    ordered_cols=_ordered_cols(time_dummy, explanvars, controlvars)

    #functions
    #stargazer.title
    #stargazer.show_model_numbers
    #stargazer.custom_columns()
    #significance_levels
    #stargazer.significant_digits
    #stargazer.show_confidence_intervals
    stargazer.dependent_variable_name(depvar_name)
    #stargazer.rename_covariates
    stargazer.covariate_order(ordered_cols)
    #stargazer.reset_covariate_order
    #stargazer.show_degrees_of_freedom
    #stargazer.custom_note_label
    #stargazer.add_custom_notes
    #stargazer.add_line
    #stargazer.append_notes

    #parameters
    stargazer.title_text = caption
    #stargazer.show_header = True
    stargazer.dep_var_name = 'Dependent variable: '
    #stargazer.column_labels = None
    #stargazer.column_separators = None
    #stargazer.show_model_nums = True
    #stargazer.original_cov_names = None
    #stargazer.cov_map = None
    #stargazer.cov_spacing = None
    #stargazer.show_precision = True
    #stargazer.show_sig = True
    #stargazer.sig_levels = [0.1, 0.05, 0.01]
    stargazer.sig_digits = sig_digits
    #stargazer.confidence_intervals = False
    #stargazer.show_footer = True
    #stargazer.custom_lines = defaultdict(list)
    #stargazer.show_n = True
    #stargazer.show_r2 = True
    #stargazer.show_adj_r2 = True
    stargazer.show_residual_std_err = False
    #stargazer.show_f_statistic = True
    stargazer.show_dof = False
    stargazer.show_notes = False
    #stargazer.notes_label = 'Note:'
    #stargazer.notes_append = True 
    #stargazer.custom_notes = [] 
    #stargazer.show_stars = True
    stargazer.table_label = label

    #return
    return stargazer


#add table note
def _add_tablenotes(text, tablenotes):

    #end
    old="\\end{table}"
    new=(
        "\\begin{tablenotes}" + "\n"
        f"{tablenotes}" + "\n"
        "\\end{tablenotes}" + "\n"
        "\\end{table}" + "\n"
        )
    
    #text
    text=text.replace(old, new)

    #return
    return text


#add resizebox
def _add_resizebox(text):

    #begin
    old="\\begin{tabular}"
    new=(
        "\\resizebox{\\textwidth}{!}{%" + "\n"
        "\\begin{tabular}" + "\n"
        )
    
    #text
    text=text.replace(old, new)

    #end
    old="\\end{tabular}"
    new=(
        "\\end{tabular}" + "%" + "\n"
        "}" + "\n"
        )
    
    #text
    text=text.replace(old, new)

    #return
    return text


#stargazer to save
def _stargazer_to_save(stargazer, tuples_replace, tablenotes, results, label):

    #text
    text=stargazer.render_latex()

    #covariates name
    text=_replace_txt(text, tuples_replace)

    #table notes
    text=_add_tablenotes(text, tablenotes)

    #resize box
    text=_add_resizebox(text)
    
    #save
    _save_table(results, label, text)


#echo summary stats
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
        "dummy_democratic", 
        "dummy_republican",
        "dummy_both",
        "amount_democratic_past3",
        "amount_republican_past3",
        "amount_both_past3",
        "dummy_democratic_past3",
        "dummy_republican_past3",
        "dummy_both_past3",

        #echo
        "dummy_echo_enforcement",
        "fed_penalty_assessed_amt",
        "dummy_echo_penalty",

        #crspcompustat
        "firm_size",
        "leverage_ratio",
        "roa",
        "mtb",
        ]
    
    #table summary
    _table_summary(df, cols, label, caption, tablenotes, tuples_replace, results)


#_likelihood_table_reg
def _likelihood_table_reg(results):

    filepath="zhao/_merge/crspcompustat_donations_echo_screen.csv"
    df=pd.read_csv(
        filepath,
        dtype="string",
        #nrows=1000,
        )

    #label
    label="likelihood_table_reg"

    #caption
    caption="Political Contributions and Enforcement Likelihood"

    #depvar
    depvar="dummy_echo_enforcement"
    depvar_name="EPA Enforcement Likelihood"

    #time dummy
    time_dummy=None

    #explanatory vars
    explanvars=[
        "amount_democratic",
        "amount_republican",
        "amount_both",
        "dummy_democratic", 
        "dummy_republican",
        "dummy_both",
        ]
    
    #control vars
    controlvars=[
        "firm_size",
        "leverage_ratio",
        "roa",
        "mtb",
        ]

    #init models
    models=list()

    #res
    explanvars_i=["amount_democratic"]
    res=_vars_to_results(df, depvar, time_dummy, explanvars_i, controlvars)
    models.append(res)

    #res
    explanvars_i=["amount_republican"]
    res=_vars_to_results(df, depvar, time_dummy, explanvars_i, controlvars)
    models.append(res)

    #res
    explanvars_i=["amount_both"]
    res=_vars_to_results(df, depvar, time_dummy, explanvars_i, controlvars)
    models.append(res)

    #res
    explanvars_i=["dummy_democratic"]
    res=_vars_to_results(df, depvar, time_dummy, explanvars_i, controlvars)
    models.append(res)

    #res
    explanvars_i=["dummy_republican"]
    res=_vars_to_results(df, depvar, time_dummy, explanvars_i, controlvars)
    models.append(res)

    #res
    explanvars_i=["dummy_both"]
    res=_vars_to_results(df, depvar, time_dummy, explanvars_i, controlvars)
    models.append(res)

    #parameters
    stargazer=_stargazer_parameters(models, label, caption, depvar_name, time_dummy, explanvars, controlvars)
    stargazer.custom_columns(
        ["Full sample", "Full sample", "Full sample", "Full sample", "Full sample", "Full sample"], 
        [1]*len(models)
        )
    stargazer.add_line(
        "IndustryFE", ["No", "No", "No", "No", "No", "No"]
        )

    #stargazer to save
    _stargazer_to_save(stargazer, tuples_replace, tablenotes, results, label)


#_likelihood_post_table_reg
def _likelihood_post_table_reg(results):

    filepath="zhao/_merge/crspcompustat_donations_echo_screen.csv"
    df=pd.read_csv(
        filepath,
        dtype="string",
        #nrows=1000,
        )

    #label
    label="likelihood_post_table_reg"

    #caption
    caption="Political Contributions and Enforcement Likelihood - DiD"

    #depvar
    depvar="dummy_echo_enforcement"
    depvar_name="EPA Enforcement Likelihood"

    #time dummy
    time_dummy="post2015"

    #explanatory vars
    explanvars=[
        "amount_democratic",
        "amount_republican",
        "amount_both",
        "dummy_democratic", 
        "dummy_republican",
        "dummy_both",
        ]
    
    #control vars
    controlvars=[
        "firm_size",
        "leverage_ratio",
        "roa",
        "mtb",
        ]

    #init models
    models=list()

    #res
    explanvars_i=["amount_democratic"]
    res=_vars_to_results(df, depvar, time_dummy, explanvars_i, controlvars)
    models.append(res)

    #res
    explanvars_i=["amount_republican"]
    res=_vars_to_results(df, depvar, time_dummy, explanvars_i, controlvars)
    models.append(res)

    #res
    explanvars_i=["amount_both"]
    res=_vars_to_results(df, depvar, time_dummy, explanvars_i, controlvars)
    models.append(res)

    #res
    explanvars_i=["dummy_democratic"]
    res=_vars_to_results(df, depvar, time_dummy, explanvars_i, controlvars)
    models.append(res)

    #res
    explanvars_i=["dummy_republican"]
    res=_vars_to_results(df, depvar, time_dummy, explanvars_i, controlvars)
    models.append(res)

    #res
    explanvars_i=["dummy_both"]
    res=_vars_to_results(df, depvar, time_dummy, explanvars_i, controlvars)
    models.append(res)

    #parameters
    stargazer=_stargazer_parameters(models, label, caption, depvar_name, time_dummy, explanvars, controlvars)
    stargazer.custom_columns(
        ["Full sample", "Full sample", "Full sample", "Full sample", "Full sample", "Full sample"], 
        [1]*len(models)
        )
    stargazer.add_line(
        "IndustryFE", ["No", "No", "No", "No", "No", "No"]
        )

    #stargazer to save
    _stargazer_to_save(stargazer, tuples_replace, tablenotes, results, label)


#_severity_table_reg
def _severity_table_reg(results):

    filepath="zhao/_merge/crspcompustat_donations_echo_screen.csv"
    df=pd.read_csv(
        filepath,
        dtype="string",
        #nrows=1000,
        )

    #label
    label="severity_table_reg"

    #caption
    caption="Political Contributions and Enforcement Severity"

    #depvar
    depvar="fed_penalty_assessed_amt"
    depvar_name="EPA Penalty Amount"

    #time dummy
    time_dummy=None

    #explanatory vars
    explanvars=[
        "amount_democratic",
        "amount_republican",
        "amount_both",
        "dummy_democratic", 
        "dummy_republican",
        "dummy_both",
        ]
    
    #control vars
    controlvars=[
        "firm_size",
        "leverage_ratio",
        "roa",
        "mtb",
        ]

    #init models
    models=list()

    #res
    explanvars_i=["amount_democratic"]
    res=_vars_to_results(df, depvar, time_dummy, explanvars_i, controlvars)
    models.append(res)

    #res
    explanvars_i=["amount_republican"]
    res=_vars_to_results(df, depvar, time_dummy, explanvars_i, controlvars)
    models.append(res)

    #res
    explanvars_i=["amount_both"]
    res=_vars_to_results(df, depvar, time_dummy, explanvars_i, controlvars)
    models.append(res)

    #res
    explanvars_i=["dummy_democratic"]
    res=_vars_to_results(df, depvar, time_dummy, explanvars_i, controlvars)
    models.append(res)

    #res
    explanvars_i=["dummy_republican"]
    res=_vars_to_results(df, depvar, time_dummy, explanvars_i, controlvars)
    models.append(res)

    #res
    explanvars_i=["dummy_both"]
    res=_vars_to_results(df, depvar, time_dummy, explanvars_i, controlvars)
    models.append(res)

    #parameters
    stargazer=_stargazer_parameters(models, label, caption, depvar_name, time_dummy, explanvars, controlvars)
    stargazer.custom_columns(
        ["Full sample", "Full sample", "Full sample", "Full sample", "Full sample", "Full sample"], 
        [1]*len(models)
        )
    stargazer.add_line(
        "IndustryFE", ["No", "No", "No", "No", "No", "No"]
        )

    #stargazer to save
    _stargazer_to_save(stargazer, tuples_replace, tablenotes, results, label)


#_severity_post_table_reg
def _severity_post_table_reg(results):

    filepath="zhao/_merge/crspcompustat_donations_echo_screen.csv"
    df=pd.read_csv(
        filepath,
        dtype="string",
        #nrows=1000,
        )

    #label
    label="severity_post_table_reg"

    #caption
    caption="Political Contributions and Enforcement Severity - DiD"

    #depvar
    depvar="fed_penalty_assessed_amt"
    depvar_name="EPA Penalty Amount"

    #time dummy
    time_dummy="post2015"

    #explanatory vars
    explanvars=[
        "amount_democratic",
        "amount_republican",
        "amount_both",
        "dummy_democratic", 
        "dummy_republican",
        "dummy_both",
        ]
    
    #control vars
    controlvars=[
        "firm_size",
        "leverage_ratio",
        "roa",
        "mtb",
        ]

    #init models
    models=list()

    #res
    explanvars_i=["amount_democratic"]
    res=_vars_to_results(df, depvar, time_dummy, explanvars_i, controlvars)
    models.append(res)

    #res
    explanvars_i=["amount_republican"]
    res=_vars_to_results(df, depvar, time_dummy, explanvars_i, controlvars)
    models.append(res)

    #res
    explanvars_i=["amount_both"]
    res=_vars_to_results(df, depvar, time_dummy, explanvars_i, controlvars)
    models.append(res)

    #res
    explanvars_i=["dummy_democratic"]
    res=_vars_to_results(df, depvar, time_dummy, explanvars_i, controlvars)
    models.append(res)

    #res
    explanvars_i=["dummy_republican"]
    res=_vars_to_results(df, depvar, time_dummy, explanvars_i, controlvars)
    models.append(res)

    #res
    explanvars_i=["dummy_both"]
    res=_vars_to_results(df, depvar, time_dummy, explanvars_i, controlvars)
    models.append(res)

    #parameters
    stargazer=_stargazer_parameters(models, label, caption, depvar_name, time_dummy, explanvars, controlvars)
    stargazer.custom_columns(
        ["Full sample", "Full sample", "Full sample", "Full sample", "Full sample", "Full sample"], 
        [1]*len(models)
        )
    stargazer.add_line(
        "IndustryFE", ["No", "No", "No", "No", "No", "No"]
        )

    #stargazer to save
    _stargazer_to_save(stargazer, tuples_replace, tablenotes, results, label)


#des stats
results="zhao/article"
def _generate_floats(results):

    #https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.describe.html
    #https://github.com/StatsReporting/stargazer

    #table summary
    _echo_table_summary(results)
    
    #table regs
    _likelihood_table_reg(results)
    _likelihood_post_table_reg(results)

    _severity_table_reg(results)
    _severity_post_table_reg(results)

    pass



_generate_floats(results)
print("done")

