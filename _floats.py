

#import
import numpy as np
import pandas as pd
from pathlib import Path
from differences import ATTgt
import statsmodels.formula.api as smf
from stargazer.stargazer import Stargazer


#functions
from _string_utils import _replace_txt
from _pd_utils import _tonumericcols_to_df


#vars
INTERACT=":"
tuples_replace=[
    #change ln amount
    ("change_ln_amount_democratic",       "Donations \Delta\% to Dem. AG assn"),
    ("change_ln_amount_republican",       "Donations \Delta\% to Rep. AG assn"),
    ("change_ln_amount_both",             "Donations \Delta\% to both AG assn"),

    #ln amount
    ("ln_amount_democratic",              "Donations (logs) to Dem. AG assn"),
    ("ln_amount_republican",              "Donations (logs) to Rep. AG assn"),
    ("ln_amount_both",                    "Donations (logs) to both AG assn"),

    #change amount
    ("change_amount_democratic",          "Donations \Delta to Dem. AG assn"),
    ("change_amount_republican",          "Donations \Delta to Rep. AG assn"),
    ("change_amount_both",                "Donations \Delta to both AG assn"),

    #amount
    ("amount_democratic",                 "Donations to Dem. AG assn"),
    ("amount_republican",                 "Donations to Rep. AG assn"),
    ("amount_both",                       "Donations to both AG assn"),

    #dummy
    ("dummy_democratic",                  "Donated to Dem. AG assn"),
    ("dummy_republican",                  "Donated to Rep. AG assn"),
    ("dummy_both",                        "Donated to any AG assn"),


    #echo lag
    ("lag_ln_echo_penalty_amount",        "EPA Penalty Amount (logs, lag 1y)"),
    ("lag_echo_enforcement_dummy",        "EPA Enforcement Likelihood (lag 1y)"),
    ("lag_echo_penalty_dummy",            "EPA Penalty Likelihood (lag 1y)"),
    ("lag_echo_penalty_amount",           "EPA Penalty Amount (lag 1y)"),

    #echo
    ("ln_echo_penalty_amount",            "EPA Penalty Amount (logs)"),
    ("echo_enforcement_dummy",            "EPA Enforcement Likelihood"),
    ("echo_penalty_dummy",                "EPA Penalty Likelihood"),
    ("echo_penalty_amount",               "EPA Penalty Amount"),
    
    #crspcompustat
    ("firm_size",                         "Firm Size"),
    ("leverage_ratio",                    "Leverage"),
    ("roa",                               "ROA"),
    ("mtb",                               "Market-to-Book"),
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


#table summary stats
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
results="zhao/article"
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
def _interact_varnames(time_dummy, explanvars):

    #init
    interact_vars=[None]*len(explanvars)

    #interactions
    for i, col in enumerate(explanvars):  

        #var name
        interact_var=f"{time_dummy}{INTERACT}{col}"

        #update
        interact_vars[i]=interact_var

    #return
    return interact_vars


#indepvars
def _indepvars(time_dummy, explanvars, controlvars):

    #if
    if time_dummy=="No":

        #interact_vars
        interact_vars=list()

       #time_dummies
        time_dummies=list()

        #indepvars
        indepvars=explanvars + controlvars

    #elif
    elif time_dummy!="No":

        #interact vars
        interact_vars=_interact_varnames(time_dummy, explanvars)

        #time_dummies
        time_dummies=[time_dummy]

        #indepvars
        indepvars=interact_vars + time_dummies + explanvars +  controlvars

    #return
    return indepvars, interact_vars, time_dummies


#sm results
def _sm_results(df, depvar, indepvars, clusters, list_fe):

    #https://www.statsmodels.org/dev/generated/statsmodels.regression.linear_model.OLS.fit.html

    #join
    join_indepvars=" + ".join(indepvars)

    #formula
    formula=f"{depvar} ~ {join_indepvars}"

    #for
    for i, dict_fe in enumerate(list_fe):

        #unpack
        present=dict_fe["present"]
        colname=dict_fe["colname"]

        #if
        if present=="Yes":

            #dropna
            dropna_cols=[colname]
            df=df.dropna(subset=dropna_cols)

            #factorize
            df[colname]=pd.factorize(df[colname])[0]

            #formula
            formula=f"{formula} + C({colname})"

    #if
    if clusters!=["No"]:

        #dropna
        dropna_cols=clusters
        df=df.dropna(subset=dropna_cols)

    #indepvars truly in df
    indepvars_in_df=[x for x in indepvars if INTERACT not in x]

    #dropna
    dropna_cols=[depvar] + indepvars_in_df
    df=df.dropna(subset=dropna_cols)

    #to numeric
    tonumeric_cols=[depvar] + indepvars_in_df
    df=_tonumericcols_to_df(df, tonumeric_cols)

    #model
    mod=smf.ols(
        formula=formula,
        data=df,
        )
    
    #if
    if clusters==["No"]:

        #res
        res=mod.fit()

    #elif
    elif clusters!=["No"]:

        #groups
        groups=[pd.factorize(df[col])[0] for col in clusters]

        #res
        res=mod.fit(
            cov_type="cluster",
            cov_kwds={"groups": groups},
            )

    #return
    return res


#ordered cols
def _ordered_cols(olddict, interact_vars, time_dummies, explanvars):

    #empty
    if not olddict:
        olddict["interact_vars"]=list()
        olddict["time_dummies"]=list()
        olddict["explanvars"]=list()

    #init
    newdict=dict()

    #newlist
    interact_vars=[x for x in interact_vars if x not in olddict["interact_vars"]]
    time_dummies=[x for x in time_dummies if x not in olddict["time_dummies"]]
    explanvars=[x for x in explanvars if x not in olddict["explanvars"]]
    
    #update
    newdict["interact_vars"]=olddict["interact_vars"] + interact_vars
    newdict["time_dummies"]=olddict["time_dummies"] + time_dummies
    newdict["explanvars"]=olddict["explanvars"] + explanvars

    #return
    return newdict


#fe addline
def _addline(stargazer, inputs):

    #init
    new_dictfe=dict()
    clusters_presents=[None]*len(inputs)

    #for
    for j, input in enumerate(inputs):

        #list fe
        list_fe=input["fixedeffects"]
        clusters=input["clusters"]

        #cluster
        clusters=[x.capitalize() for x in clusters]
        clusters_present="-".join(clusters)

        #update
        clusters_presents[j]=clusters_present
        
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
    stargazer.add_line("Cluster", clusters_presents)    

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


#table reg
def _table_reg(df, results, controlvars, inputs, label, caption, depvar, depvar_name):

    #n_models
    n_models=len(inputs)

    #init
    models=[None]*n_models
    subsample_names=[None]*n_models
    olddict=dict()

    #for
    for i, input in enumerate(inputs):

        #dict
        explanvars=input["explanvars"]
        time_dummy=input["time_dummy"]
        df=input["subsample"]["subsample_df"]
        subsample_name=input["subsample"]["subsample_name"]
        list_fe=input["fixedeffects"]
        clusters=input["clusters"]

        #indepvars
        indepvars, interact_vars, time_dummies = _indepvars(time_dummy, explanvars, controlvars)

        #res
        res=_sm_results(df, depvar, indepvars, clusters, list_fe)

        #update
        models[i]=res
        subsample_names[i]=subsample_name

        #ordered cols
        olddict=_ordered_cols(olddict, interact_vars, time_dummies, explanvars)

    #unpack 
    ordered_cols=olddict["interact_vars"] + olddict["time_dummies"] + olddict["explanvars"] + controlvars


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
    #'''


#table regs
results="zhao/article"
def _table_regs(results):

    #filepath
    filepath="zhao/_merge/crspcompustat_donations_echo_screen.csv"

    #read
    df=pd.read_csv(
        filepath,
        dtype="string",
        #nrows=1000,
        )
    
    #control vars
    controlvars=[
        "firm_size",
        "leverage_ratio",
        "roa",
        "mtb",
        ]

    #inputs
    inputs=[

            {
            "explanvars": ["lag_echo_enforcement_dummy"],
            "time_dummy": "No",
            "subsample": {"subsample_df": df, "subsample_name": "Full sample"},
            "fixedeffects": [
                            {"name": "IndustryFE",  "present": "Yes", "colname": "industry_famafrench49"}, 
                            {"name": "YearFE",      "present": "Yes", "colname": "fyear"},
                            ],
            "clusters": ["state"],
            },

            {
            "explanvars": ["lag_echo_penalty_dummy"],
            "time_dummy": "No",
            "subsample": {"subsample_df": df, "subsample_name": "Full sample"},
            "fixedeffects": [
                            {"name": "IndustryFE",  "present": "Yes", "colname": "industry_famafrench49"}, 
                            {"name": "YearFE",      "present": "Yes", "colname": "fyear"},
                            ],
            "clusters": ["state"],
            },

            {
            "explanvars": ["lag_ln_echo_penalty_amount"],
            "time_dummy": "No",
            "subsample": {"subsample_df": df, "subsample_name": "Full sample"},
            "fixedeffects": [
                            {"name": "IndustryFE",  "present": "Yes", "colname": "industry_famafrench49"}, 
                            {"name": "YearFE",      "present": "Yes", "colname": "fyear"},
                            ],
            "clusters": ["state"],
            },

        ]

    #dem
    #label and caption
    label="echo_democratic_tablereg"
    caption="EPA Enforcement and Contributions - Democrat"

    #depvar
    depvar="ln_amount_democratic"
    depvar_name="Donations (logs) to Dem. AG assn"

    #table reg
    _table_reg(df, results, controlvars, inputs, label, caption, depvar, depvar_name)

    #rep
    #label and caption
    label="echo_republican_tablereg"
    caption="EPA Enforcement and Contributions - Republican"

    #depvar
    depvar="ln_amount_republican"
    depvar_name="Donations (logs) to Rep. AG assn"

    #table reg
    _table_reg(df, results, controlvars, inputs, label, caption, depvar, depvar_name)

    #both
    #label and caption
    label="echo_both_tablereg"
    caption="EPA Enforcement and Contributions - Both "

    #depvar
    depvar="ln_amount_both"
    depvar_name="Donations (logs) to both AG assn"

    #table reg
    _table_reg(df, results, controlvars, inputs, label, caption, depvar, depvar_name)





#attgt
def _csdid_attgt(df, controlvars, control_group, est_method, cluster_var, group_var, depvar):

    #https://differences.readthedocs.io/en/latest/api_reference/attgt.html#differences.attgt.attgt.ATTgt
    #https://differences.readthedocs.io/en/latest/api_reference/attgt.html#differences.attgt.attgt.ATTgt.fit

    #ids
    unit_var, time_var = "cusip", "fyear"

    #dropna
    dropna_cols=[depvar] + controlvars
    df=df.dropna(subset=dropna_cols)

    #to numeric
    tonumeric_cols=[
        time_var,
        depvar,
        group_var,
        ] + controlvars
    df=_tonumericcols_to_df(df, tonumeric_cols)

    #rename and setindex
    entity, time = "entity", "time"
    df=df.rename(columns={unit_var: entity, time_var: time})
    df=df.set_index([entity, time])

    #group var
    df[group_var] = np.where(df[group_var] == 0, np.nan, df[group_var])

    #att_gt
    att_gt = ATTgt(
        data=df,
        cohort_name=group_var,
        #strata_name: str = None,
        #base_period: str = "varying",
        #anticipation: int = 0,
        #freq: str = None
        )

    #empty
    if not controlvars:

        #formula
        formula=f"{depvar}"
    
    #elif
    elif controlvars:
    
        #formula
        join_controlvars=" + ".join(controlvars)
        formula=f"{depvar} ~ {join_controlvars}"

    #fit
    att_gt.fit(
        formula=formula,
        #weights_name: str = None,
        control_group=control_group,
        #base_delta: str | list | dict = "base",
        est_method=est_method,
        #as_repeated_cross_section: bool = None,
        #boot_iterations: int = 0,  # if > 0 mboot will be called
        #random_state: int = None,
        alpha=0.01,
        cluster_var=cluster_var,
        #split_sample_by: Callable | str | dict = None,
        #n_jobs: int = 1,
        #backend: str = "loky",
        #progress_bar: bool = True,
        )

    #return
    return att_gt


#results to latex
def _results_to_latex(df, results, caption, label, tablenotes):

    #styler object
    styler_object=df.style

    #format
    format_styler="{:,.3f}"
    #styler_object=styler_object.format(format_styler)

    #https://pandas.pydata.org/docs/reference/api/pandas.io.formats.style.Styler.to_latex.html
    tabular=styler_object.to_latex(hrules=True)

    #replace
    tabular=tabular.replace("_", " ")
    
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

    #save
    _save_table(results, label, text)


#floats
def _csdid_floats(att_gt, results):

    #https://differences.readthedocs.io/en/latest/api_reference/attgt.html#differences.attgt.attgt.ATTgt.aggregate
    #https://differences.readthedocs.io/en/latest/api_reference/attgt.html#differences.attgt.attgt.ATTgt.plot

    #aggregate params
    alpha=0.1

    #plot params
    configure_axisX={'format': 'c'}
    width=600
    height=600

    #types
    types=[
        "time",
        "event",
        "cohort",
        "simple",
        ]
    
    type_of_aggregation="not_aggregated"
    save_fname=f"{results}/figures/{type_of_aggregation}"
    plt=att_gt.plot(
        configure_axisX={'format': 'c'},
        width=width,
        height=height,
        save_fname=save_fname,
        )
    #plt.show()
    
    #for
    for i, type_of_aggregation in enumerate(types):

        #aggregate
        x=att_gt.aggregate(
            type_of_aggregation=type_of_aggregation,
            alpha=alpha,
            )
        
        #result
        df=att_gt.results(
            type_of_aggregation=type_of_aggregation,
            to_dataframe=True
            )
        
        #caption
        caption=f"ATT(g,t) aggregated at {type_of_aggregation.capitalize()}-level"

        #label
        label=type_of_aggregation

        #tablenotes
        tablenotes=f"The Average Treatment Effects on the treated groups \\textit{{g}} at time \\textit{{t}} are aggregated at {type_of_aggregation}-level."
        
        #to_latex
        _results_to_latex(df, results, caption, label, tablenotes)

        #save_fname
        save_fname=f"{results}/figures/{type_of_aggregation}"
        
        #plot
        plt=att_gt.plot(
            type_of_aggregation=type_of_aggregation,
            configure_axisX=configure_axisX,
            width=width,
            height=height,
            save_fname=save_fname,
            )
        #plt.show()

        #print
        print(f"{i} - {type_of_aggregation}")
        #'''
        

#callaway_santanna
results="zhao/article"
def _csdid(results):

    #https://differences.readthedocs.io/en/latest/notebooks/attgt.html

    #https://github.com/suahjl/paneleventstudy
    #https://github.com/d2cml-ai/csdid

    #filepath
    filepath="zhao/_merge/crspcompustat_donations_echo_screen.csv"

    #read
    df=pd.read_csv(
        filepath,
        dtype="string",
        nrows=1000,
        )
    
    #control vars
    controlvars=[
        #"firm_size",
        #"leverage_ratio",
        #"roa",
        #"mtb",
        ]
    
    #parameters
    control_group="never_treated"
    est_method="dr"
    cluster_var=None

    #group var
    group_vars=[
        "echo_enforcement_dummy_group",
        "echo_penalty_dummy_group",
        ]
    group_var="echo_enforcement_dummy_group"

    #depvars
    depvars=[
        #ln amount
        "ln_amount_democratic",
        "ln_amount_republican",
        "ln_amount_both",
        ]
    depvar="ln_amount_democratic"

    #att_gt
    att_gt=_csdid_attgt(df, controlvars, control_group, est_method, cluster_var, group_var, depvar)

    #figures  
    _csdid_floats(att_gt, results)

    #for XXX


_csdid(results)
#print("done")

