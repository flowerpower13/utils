import pandas as pd


#functions
from _merge_utils import _df_on


#extract year
def _extract_year(df, old_var, new_var):
    df[new_var]=df[old_var].str[:4]

    return df


#evolkova linktable
#https://sites.google.com/view/evolkova/data-cik-cusip-link
def _evolkova_linktable(link_table, source, left, results, result):
    right=pd.read_csv(link_table, dtype="string")
    right.columns=right.columns.str.lower()

    old_var="fdate"
    new_var="year"
    left=_extract_year(left, old_var, new_var)

    #left
    left_vars=["cik", "year"]
    left, left_on=_df_on(left, left_vars)
    #right
    right_vars=["cik", "year"]
    right, right_on=_df_on(right, right_vars)

    #right - additional
    right=right.sort_values(by=["cik"])
    right=right.drop_duplicates(subset=[right_on])

    how="left"
    validate="m:1"

    #https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.merge.html
    df=pd.merge(
        left=left,
        right=right,
        how=how,
        left_on=left_on,
        right_on=right_on,
        suffixes=('_left', '_right'),
        indicator=True,
        validate=validate,
        )

    file_path=f"{results}/{result}_{source}.csv"
    df.to_csv(file_path, index=False)


#compustat-wrds linktable
#https://wrds-www.wharton.upenn.edu/pages/get-data/center-research-security-prices-crsp/annual-update/crspcompustat-merged/compustat-crsp-link/
#https://wrds-www.wharton.upenn.edu/pages/get-data/compustat-capital-iq-standard-poors/tools/cusip-converter/
def _wrds_linktable(link_table, source, left, results, result):
    right=pd.read_csv(link_table, dtype="string")
    right.columns=right.columns.str.lower()

    #left
    left_on="cik"

    #right
    right_on="cik"

    #right - additional
    right=right.sort_values(by=["cik"])
    right=right.drop_duplicates(subset=[right_on])

    how="left"
    validate="m:1"

    #https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.merge.html
    df=pd.merge(
        left=left,
        right=right,
        how=how,
        left_on=left_on,
        right_on=right_on,
        suffixes=('_left', '_right'),
        indicator=True,
        validate=validate,
        )

    file_path=f"{results}/{result}_{source}.csv"
    df.to_csv(file_path, index=False)


def _cik_to_cusip(folders, items, filing):
    resources=folders[0]
    results=folders[1]

    resource=items[0]
    result=items[1]

    resource=f"{resource}_{filing}"
    result=f"{result}_{filing}"

    file_path=f"{resources}/{resource}.csv"
    left=pd.read_csv(file_path, dtype="string")
    left.columns=left.columns.str.lower()

    link_table="CIK_CUSIP.csv"
    source="evolkova"
    _evolkova_linktable(link_table, source, left, results, result)

    link_table="compustat-crsp-link.csv"
    source="crsp"
    _wrds_linktable(link_table, source, left, results, result)


