import pandas as pd
import numpy as np

# Sample DataFrame
data = {
    "law_case_identifier": [1, 1, 2, 3],
    "company_identifier": ["A", "A", "B", "B"],
    "penalty_amount": [100, 100, 150, 300]
}

df = pd.DataFrame(data)

from _pd_utils import _groupby

#take df first value
def _first_value(df):

    #return
    return df.iloc[0]


#aggregate over case-company-init year obs
by=[
    "law_case_identifier",
    "company_identifier",
    ]
dict_agg_colfunctions={
    "penalty_amount": [_first_value],
    }

df=_groupby(df, by, dict_agg_colfunctions)

print(df)


by=[
    "company_identifier",
    ]
dict_agg_colfunctions={
    "penalty_amount": [np.sum],
    }

df=_groupby(df, by, dict_agg_colfunctions)


print(df)