

#import
import numpy as np
import pandas as pd
from pathlib import Path
import statsmodels.formula.api as smf
from stargazer.stargazer import Stargazer


#functions
from _string_utils import _replace_txt
from _pd_utils import _todatecols_to_df, _tonumericcols_to_df


#vars
INTERACT=":"
obs_quarterly="117,920.00"
obs_yearly="29,480.00"
tuples_replace=[
    #A
    ("daga_raga_dummy", "Both AG Assn (dummy)"),
    ("daga_dummy", "Dem. AG Assn (dummy)"),
    ("raga_dummy", "Rep. AG Assn (dummy)"),
    ("daga_raga", "Both AG Assn (donation)"),
    ("daga", "Dem. AG Assn (donation)"),
    ("raga", "Rep. AG Assn (donation)"),

    #violtrack
    ("agencies_sum_dummy", "Enforcement (dummy)"),
    ("non_AG_sum_dummy", "Non-AG Enforcement (dummy)"),
    ("AG_sum_dummy", "AG Enforcement (dummy)"),
    ("agencies_sum", "Enforcement (penalty)"),
    ("non_AG_sum", "Non-AG Enforcement (penalty)"),
    ("AG_sum", "AG Enforcement (penalty)"),

    #obs_quarterly
    (obs_quarterly, obs_quarterly.replace(".00", "")),

    #obs_yearly
    (obs_yearly, obs_yearly.replace(".00", "")),
    ]


#tablenotes
filepath="zhao/article/tablenotes.txt"
with open(filepath, "r") as file_object:
    tablenotes=file_object.read()


#stylerobject to tabletext
def _stylerobject_to_tabletext(styler_object, caption, label, tablenotes):

    #https://pandas.pydata.org/docs/reference/api/pandas.io.formats.style.Styler.to_latex.html
    tabular=styler_object.to_latex(hrules=True)

    #text
    text=(
        #begin
        "\\begin{table}[!htbp]" "\n"
        "\\centering"           "\n"

        #caption
        f"\\caption"
        "{"
        f"{caption}"
        "}"             "\n"

        #label
        f"\\label"
        "{"
        f"{label}"
        "}"             "\n"

        #resizebox
        "\\resizebox{\\textwidth}{!}{%" "\n"

        #tabular
        f"{tabular}"    "\n"

        #resizebox
        "}"             "\n"

        #table notes
        "\\begin{tablenotes}"   "\n"
        f"{tablenotes}"         "\n"
        "\\end{tablenotes}"     "\n"

        #end table
        "\\end{table}"          "\n"
        )
    
    #return
    return text


#_save_table
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


#_table_summary
def _table_summary(df, cols, label, caption, tablenotes, tuples_replace, results):

    #to numeric
    tonumeric_cols=cols
    df=_tonumericcols_to_df(df, tonumeric_cols)
    
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

    #format styler
    format_styler="{:,.2f}"

    #format cols
    #https://pandas.pydata.org/docs/reference/api/pandas.io.formats.style.Styler.format.html
    styler_object=styler_object.format(format_styler)
    
    #tabular to tabletext
    text=_stylerobject_to_tabletext(styler_object, caption, label, tablenotes)

    #replace txt
    text=_replace_txt(text, tuples_replace)

    #save
    _save_table(results, label, text)


#_table_summaries
results="zhao/article"
def _table_summaries(results):

    #read_csv
    filepath="zhao/_merge/rdp_aggregate_quarterly.csv"
    df=pd.read_csv(
        filepath,
        dtype="string",
        #nrows=1000,
        )

    #label
    label="tablesummary_quarterly"

    #caption
    caption="Donations to AG Assn and Enforcement (Quarterly)"

    #cols
    cols=[
        #A
        "daga",
        "raga",
        "daga_dummy",
        "raga_dummy",

        #violtrack
        "agencies_sum",
        "AG_sum",
        "non_AG_sum",
        "agencies_sum_dummy",
        "AG_sum_dummy",
        "non_AG_sum_dummy",
        ]
    
    #table summary
    _table_summary(df, cols, label, caption, tablenotes, tuples_replace, results)

    #read_csv
    filepath="zhao/_merge/rdp_aggregate_yearly.csv"
    df=pd.read_csv(
        filepath,
        dtype="string",
        #nrows=1000,
        )

    #label
    label="tablesummary_yearly"

    #caption
    caption="Donations to AG Assn and Enforcement (Yearly)"

    #cols
    cols=[
        #A
        "daga",
        "raga",
        "daga_dummy",
        "raga_dummy",

        #violtrack
        "agencies_sum",
        "AG_sum",
        "non_AG_sum",
        "agencies_sum_dummy",
        "AG_sum_dummy",
        "non_AG_sum_dummy",
        ]
    
    #table summary
    _table_summary(df, cols, label, caption, tablenotes, tuples_replace, results)


#_sm_results
def _sm_results(df, depvar, indepvars_string, indepvars_list, list_fe, dict_cluster):

    #https://www.statsmodels.org/dev/generated/statsmodels.regression.linear_model.OLS.fit.html

    #formula
    formula=f"{depvar} ~ {indepvars_string}"

    #fe
    for i, dict_fe in enumerate(list_fe):

        #dict_fe
        fe_present=dict_fe["present"]
        fe_colname=dict_fe["colname"]

        #if
        if fe_present=="Yes":

            #dropna
            dropna_cols=[fe_colname]
            df=df.dropna(subset=dropna_cols)

            #factorize
            df[fe_colname]=pd.factorize(df[fe_colname])[0]

            #formula
            formula=f"{formula} + C({fe_colname})"

    #indepvars_list_indf
    indepvars_list_indf=[x for x in indepvars_list if INTERACT not in indepvars_list]

    #dropna
    dropna_cols = [depvar] + indepvars_list_indf
    df=df.dropna(subset=dropna_cols)

    #to numeric
    tonumeric_cols = [depvar] + indepvars_list_indf
    df=_tonumericcols_to_df(df, tonumeric_cols)

    #dict_cluster
    cluster_present=dict_cluster["present"]
    cluster_colname=dict_cluster["colname"]

    #if
    if cluster_present=="Yes":

        #groups
        groups=[pd.factorize(df[cluster_colname])[0]]

        #res
        res=mod.fit(
            cov_type="cluster",
            cov_kwds={"groups": groups},
            )
        
    #mod
    mod=smf.ols(
        formula=formula,
        data=df,
        )

    #res
    res=mod.fit()

    #return
    return res


#fe addline
def _addline(stargazer, inputs):

    #init
    new_dictfe=dict()
    cluster_names=[None]*len(inputs)

    #for
    for j, input in enumerate(inputs):

        #cluster_name
        cluster_name=input["cluster"]["name"]

        #update
        cluster_names[j]=cluster_name

        #list_fe
        list_fe=input["fixedeffects"]
        
        #for
        for k, dict_fe in enumerate(list_fe):

            #unpack
            name=dict_fe["name"]
            present=dict_fe["present"]

            #if
            if name not in new_dictfe:

                #gen
                new_dictfe[name]=[None]*len(inputs)

            #update
            new_dictfe[name][j]=present

    #for
    for i, (name, presents) in enumerate(new_dictfe.items()):

        #fe
        stargazer.add_line(name, presents)

    #cluster 
    stargazer.add_line("Cluster", cluster_names)    

    #return
    return stargazer

    
#add elements
def _add_elements(text, label, tablenotes):

    #label
    #end tabular
    old="\\label{empty_label}"
    new=(
        "\\label"
        "{"
        f"{label}"
        "}" 
        )
    text=text.replace(old, new)

    #begin tabular
    old="\\begin{tabular}"
    new=(
        "\\resizebox{\\textwidth}{!}{%" "\n"
        "\\begin{tabular}"              "\n"
        )
    text=text.replace(old, new)
    #end tabular
    old="\\end{tabular}"
    new=(
        "\\end{tabular}"    "%\n"
        "}"                 "\n"
        )
    text=text.replace(old, new)


    #end table
    old="\\end{table}"
    new=(
        "\\begin{tablenotes}"   "\n"
        f"{tablenotes}"         "\n"
        "\\end{tablenotes}"     "\n"
        "\\end{table}"          "\n"
        ) 
    #text
    text=text.replace(old, new)

    #return
    return text


#table reg
def _table_reg(df, results, inputs, label, caption, depvar):

    #n_models
    n_models=len(inputs)

    #init
    models=[None]*n_models
    subsample_names=[None]*n_models
    olddict=dict()

    #for
    for i, input in enumerate(inputs):

        #dict
        indepvars_string=input["indepvars"]["string"]
        indepvars_list=input["indepvars"]["list"]
        df=input["subsample"]["subsample_df"]
        subsample_name=input["subsample"]["subsample_name"]
        list_fe=input["fixedeffects"]
        dict_cluster=input["cluster"]

        #res
        res=_sm_results(df, depvar, indepvars_string, indepvars_list, list_fe, dict_cluster)

        #update
        models[i]=res
        subsample_names[i]=subsample_name

    #ordered_cols 
    ordered_cols=set(indepvars_list)

    #https://github.com/StatsReporting/stargazer
    #https://github.com/StatsReporting/stargazer/blob/master/examples.ipynb

    #stargazer
    stargazer=Stargazer(models)

    #functions
    #stargazer.title
    #stargazer.show_model_numbers
    stargazer.custom_columns(subsample_names, [1]*n_models)
    #stargazer.significance_levels
    #stargazer.significant_digits
    #stargazer.show_confidence_intervals
    depvar_name=_replace_txt(depvar, tuples_replace)
    stargazer.dependent_variable_name(depvar_name)
    #stargazer.rename_covariates
    stargazer.covariate_order(ordered_cols)
    #stargazer.reset_covariate_order
    #stargazer.show_degrees_of_freedom
    #stargazer.custom_note_label
    #stargazer.add_custom_notes
    stargazer=_addline(stargazer, inputs)
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
    sig_digits=3
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
    stargazer.table_label = "empty_label"

    #text
    text=stargazer.render_latex()

    #covariates name
    text=_replace_txt(text, tuples_replace)

    #table notes
    text=_add_elements(text, label, tablenotes)

    #save
    _save_table(results, label, text)
    #'''


#table regs
results="zhao/article"
def _table_regs(results):

    #filepath
    filepath="zhao/_merge/rdp_aggregate.csv"

    #read
    df=pd.read_csv(
        filepath,
        dtype="string",
        #nrows=1000,
        )

    #first
    inputs=[
            {
            "indepvars": {"string": "post2018", "list": ["post2018"]},
            "subsample": {"subsample_df": df, "subsample_name": "Full sample"},
            "fixedeffects": [
                            {"name": "IndustryFE",  "present": "No", "colname": "industry_famafrench49"}, 
                            {"name": "YearFE",      "present": "No", "colname": "year"},
                            ],
            "cluster": {"name": "State",  "present": "No", "colname": "state"}, 
            },
        ]
    depvar="daga_raga"
    label="first_tablereg"
    caption="Donations and Disclosure Shock"
    #table reg
    _table_reg(df, results, inputs, label, caption, depvar)


_table_summaries(results)
#_table_regs(results)
print("done")

