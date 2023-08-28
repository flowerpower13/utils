

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
    #log ln amount
    ("lag_ln_amount_democratic &",          "Donations (logs, lag 1y) to Dem. AG assn &"),
    ("lag_ln_amount_republican &",          "Donations (logs, lag 1y) to Rep. AG assn &"),
    ("lag_ln_amount_both &",                "Donations (logs, lag 1y) to both AG assn &"),

    #ln amount
    ("ln_amount_democratic &",          "Donations (logs) to Dem. AG assn &"),
    ("ln_amount_republican &",          "Donations (logs) to Rep. AG assn &"),
    ("ln_amount_both &",                "Donations (logs) to both AG assn &"),

    #amount
    ("amount_democratic &",             "Donations to Dem. AG assn &"),
    ("amount_republican &",             "Donations to Rep. AG assn &"),
    ("amount_both &",                   "Donations to both AG assn &"),

    #lag dummy
    ("lag_dummy_democratic &",              "Donated (lag 1y) to Dem. AG assn &"),
    ("lag_dummy_republican &",              "Donated (lag 1y) to Rep. AG assn &"),
    ("lag_dummy_both &",                    "Donated (lag 1y) to any AG assn &"),

    #dummy
    ("dummy_democratic &",              "Donated to Dem. AG assn &"),
    ("dummy_republican &",              "Donated to Rep. AG assn &"),
    ("dummy_both &",                    "Donated to any AG assn &"),

    #ln amount past
    ("ln_amount_democratic_past3 &",    "Donations (logs) to Dem. AG assn past 3y &"),
    ("ln_amount_republican_past3 &",    "Donations (logs) to Rep. AG assn past 3y &"),
    ("ln_amount_both_past3 &",          "Donations (logs) to both AG assn past 3y &"),

    #amount past
    ("amount_democratic_past3 &",       "Donations to Dem. AG assn past 3y &"),
    ("amount_republican_past3 &",       "Donations to Rep. AG assn past 3y &"),
    ("amount_both_past3 &",             "Donations to both AG assn past 3y &"),

    #dummy past
    ("dummy_democratic_past3 &",        "Donated to Dem. AG assn past 3y &"),
    ("dummy_republican_past3 &",        "Donated to Rep. AG assn past 3y &"),
    ("dummy_both_past3 &",              "Donated to any AG assn past 3y &"),

    #post
    ("post2000x",                       "Post2000 $\\times$ "),
    ("post2001x",                       "Post2001 $\\times$ "),
    ("post2002x",                       "Post2002 $\\times$ "),
    ("post2003x",                       "Post2003 $\\times$ "),
    ("post2004x",                       "Post2004 $\\times$ "),
    ("post2005x",                       "Post2005 $\\times$ "),
    ("post2006x",                       "Post2006 $\\times$ "),
    ("post2007x",                       "Post2007 $\\times$ "),
    ("post2008x",                       "Post2008 $\\times$ "),
    ("post2009x",                       "Post2009 $\\times$ "),
    ("post2010x",                       "Post2010 $\\times$ "),
    ("post2011x",                       "Post2011 $\\times$ "),
    ("post2012x",                       "Post2012 $\\times$ "),
    ("post2013x",                       "Post2013 $\\times$ "),
    ("post2014x",                       "Post2014 $\\times$ "),
    ("post2015x",                       "Post2015 $\\times$ "),
    ("post2016x",                       "Post2016 $\\times$ "),
    ("post2017x",                       "Post2017 $\\times$ "),
    ("post2018x",                       "Post2018 $\\times$ "),
    ("post2019x",                       "Post2019 $\\times$ "),
    ("post2020x",                       "Post2020 $\\times$ "),
    ("post2021x",                       "Post2021 $\\times$ "),
    ("post2022x",                       "Post2022 $\\times$ "),

    #echo
    ("ln_echo_penalty_amount &",        "EPA Penalty Amount (logs) &"),
    ("echo_enforcement_dummy &",        "EPA Enforcement Likelihood &"),
    ("echo_penalty_dummy &",            "EPA Penalty Likelihood &"),
    ("echo_penalty_amount &",           "EPA Penalty Amount &"),

    #crspcompustat
    ("firm_size &",                     "Firm Size &"),
    ("leverage_ratio &",                "Leverage &"),
    ("roa &",                           "ROA &"),
    ("mtb &",                           "Market-to-Book &"),
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


#echo summary stats
def _table_summaries(results):

    filepath="zhao/_merge/crspcompustat_donations_echo_screen.csv"
    df=pd.read_csv(
        filepath,
        dtype="string",
        #nrows=1000,
        )

    #label
    label="echo_tablesummary"

    #caption
    caption="Donations to AG assn and EPA Enforcement"

    #cols
    cols=[
        #irs
        #amount
        "amount_democratic",
        "amount_republican",
        "amount_both",

        #dummy
        "dummy_democratic", 
        "dummy_republican",
        "dummy_both",

        #amount past
        "amount_democratic_past3",
        "amount_republican_past3",
        "amount_both_past3",

        #dummy past
        "dummy_democratic_past3",
        "dummy_republican_past3",
        "dummy_both_past3",

        #echo
        "echo_enforcement_dummy",
        "echo_penalty_dummy",
        "echo_penalty_amount",

        #crspcompustat
        "firm_size",
        "leverage_ratio",
        "roa",
        "mtb",
        ]
    
    #getable summary
    _table_summary(df, cols, label, caption, tablenotes, tuples_replace, results)


#interact var names
def _interact_varnames(post_year_dummy, explanvars):

    #init
    interact_vars=[None]*len(explanvars)

    #interactions
    for i, col in enumerate(explanvars):  

        #var name
        interact_var=f"{post_year_dummy}x{col}"

        #update
        interact_vars[i]=interact_var

    #return
    return interact_vars


#indepvars
def _indepvars(post_year_dummy, explanvars_i, controlvars):

    #if
    if post_year_dummy==None:

        #indepvars
        indepvars=explanvars_i + controlvars

    #if
    if post_year_dummy!=None:

        #interact vars
        interact_vars=_interact_varnames(post_year_dummy, explanvars_i)

        #indepvars
        indepvars=interact_vars + [post_year_dummy] + explanvars_i +  controlvars

    #return
    return indepvars


#fe indepvars
def _fe_indepvars(df, list_fe):

    #for
    for i, dict_fe in enumerate(list_fe):

        #unpack
        present=dict_fe["present"]
        prefix=dict_fe["prefix"]

        #if
        if present=="Yes":

            #industry dummies cols
            dummies=[x for x in df.columns if x.startswith(prefix)]

            #indepvars
            indepvars=indepvars + dummies

        #elif
        elif present=="No":

            #pass
            pass
        
    #return
    return indepvars


def _sm_results(df, mod, clusters):

    #if
    if clusters==None:

        #res
        res.mod.fit()

    #elif
    elif clusters!=None:

        #https://www.statsmodels.org/dev/generated/statsmodels.regression.linear_model.OLS.fit.html

        #group
        clustering_groups = [df[col] for col in clusters]

        #res
        res=mod.fit(
            cov_type="cluster",
            cov_kwds={"groups": clustering_groups},
            )
    
    #return
    return res

#fe addline
def _fe_addline(stargazer, inputs):

    #init
    new_dictfe=dict()

    #for
    for j, input in enumerate(inputs):

        #list fe
        list_fe=input["fixedeffects"]
        
        #for
        for k, dict_fe in enumerate(list_fe):

            #unpack
            name=dict_fe["name"]
            present=dict_fe["present"]

            if name not in new_dictfe:

                #gen
                new_dictfe[name]=[None]*len(inputs)

            #update
            new_dictfe[name][j]=present

    #for
    for i, (name, presents) in enumerate(new_dictfe.items()):

        #add line
        stargazer.add_line(name, presents)

    #return
    return stargazer


#ordered cols
def _ordered_cols(post_year_dummy, explanvars, controlvars):

    #if
    if post_year_dummy==None:

        #orderedd cols
        ordered_cols=["const"] + explanvars + controlvars

    #elif
    elif post_year_dummy!=None:

        interact_vars=_interact_varnames(post_year_dummy, explanvars)

        #orderedd cols
        ordered_cols=["const"] + interact_vars + [post_year_dummy] + explanvars + controlvars

    #return
    return ordered_cols
    

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


#table reg
def _table_reg(results, post_year_dummy, explanvars, controlvars, inputs, label, caption, depvar, depvar_name):

    #n_models
    n_models=len(inputs)

    #init
    models=[None]*n_models
    subsample_names=[None]*n_models
    fe_presents=[None]*n_models

    for i, input in enumerate(inputs):

        #dict
        explanvars_i=input["explanvars_i"]
        df=input["subsample"]["subsample_df"]
        subsample_name=input["subsample"]["subsample_name"]
        clusters=input["clusters"]
        list_fe=input["fixedeffects"]

        #indepvars
        indepvars=_indepvars(post_year_dummy, explanvars_i, controlvars)

        #fe
        indepvars=_fe_indepvars(df, list_fe)

        #to numeric
        tonumeric_cols=[depvar] + indepvars
        errors="raise"
        df=_tonumericcols_to_df(df, tonumeric_cols, errors)
        
        #Y
        y=df[depvar]

        #X
        X=df[indepvars]

        #const
        X=sm.add_constant(X)

        #model
        mod=sm.OLS(
            endog=y,
            exog=X,
            missing="drop",
            )

        res=_sm_results(df, mod, clusters)

        #update
        models[i]=res
        subsample_names[i]=subsample_name

    #ordered cols
    ordered_cols=_ordered_cols(post_year_dummy, explanvars, controlvars)

    #sig_digits
    sig_digits=3

    #https://github.com/StatsReporting/stargazer
    #https://github.com/StatsReporting/stargazer/blob/master/examples.ipynb

    #stargazer
    stargazer=Stargazer(models)

    #functions
    #stargazer.title
    #stargazer.show_model_numbers
    stargazer.custom_columns(subsample_names, [1]*n_models)
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
    stargazer=_fe_addline(stargazer, inputs)
    #stargazer.append_notes

    #parameters
    stargazer.title_text = caption
    #stargazer.show_header = True
    #stargazer.dep_var_name = 'Dependent variable: '
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
    stargazer.show_f_statistic = False
    stargazer.show_dof = False
    stargazer.show_notes = False
    #stargazer.notes_label = 'Note:'
    #stargazer.notes_append = True 
    #stargazer.custom_notes = [] 
    #stargazer.show_stars = True
    stargazer.table_label = label

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


#table regs
def _table_regs(results):

    #filepath
    filepath="zhao/_merge/crspcompustat_donations_echo_screen.csv"

    #read
    df=pd.read_csv(
        filepath,
        dtype="string",
        #nrows=1000,
        )

    #time dummy
    post_year_dummy="post2015"
    post_year_dummy=None

    #explanatory vars
    explanvars=[
        #amount
        #"lag_ln_amount_democratic",
        #"lag_ln_amount_republican",
        #"lag_ln_amount_both",

        #amount past
        #"ln_amount_democratic_past3",
        #"ln_amount_republican_past3",
        #"ln_amount_both_past3",

        #dummy
        #"lag_dummy_democratic", 
        #"lag_dummy_republican",
        #"lag_dummy_both",

        #dummy past
        "dummy_democratic_past3", 
        #"dummy_republican_past3",
        #"dummy_both_past3",
        ]
    
    #control vars
    controlvars=[
        "firm_size",
        "leverage_ratio",
        "roa",
        "mtb",
        ]
    
    #subsamples
    df0=df[df["echo_enforcement_dummy"]=="1"]
    
    #inputs
    inputs=[
            {
            "explanvars_i": ["dummy_democratic_past3"],
            "subsample": {"subsample_df": df, "subsample_name": "Full sample"},
            "fixedeffects": [
                            {"name": "IndustryFE",  "present": "No", "prefix": "industry_ff_dummy"}, 
                            {"name": "YearFE",      "present": "No", "prefix": "year_dummy"},  
                            {"name": "FirmFE",      "present": "No", "prefix": "firm_dummy"}, 
                            ],
            "clusters": ["state"],
            },
        ]

    #severity
    #label
    label="echo_severity_tablereg"

    #caption
    caption="Political Contributions and Enforcement Severity - DiD"

    #depvar
    depvar="ln_echo_penalty_amount"
    depvar_name="EPA Penalty Amount (logs)"

    #table reg
    _table_reg(results, post_year_dummy, explanvars, controlvars, inputs, label, caption, depvar, depvar_name)


#gen floats
results="zhao/article"
def _generate_floats(results):

    #https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.describe.html
    #https://github.com/StatsReporting/stargazer

    #table summary
    _table_summaries(results)
    
    #table regs
    _table_regs(results)

    #figures

    pass



_generate_floats(results)
print("done")

