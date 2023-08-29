import pandas as pd
import numpy as np

# Sample DataFrame
data = {'firm': ['a', 'a', 'a', 
                 'b', 'b', 'b'],
        'year': [2000, 2001, 2002, 
                 2000, 2001, 2002],
        'treatment_dummy_switch': [0, 0, 1, 
                              1, 1, 0]}
df = pd.DataFrame(data)
print(df)


#oldvars
unit_var="firm"
time_var="year"
treatment_dummy_switch="treatment_dummy_switch"

#newvars
treatment_dummy_firstswitch="treatment_dummy_firstswitch"
treatment_dummy="treatment_dummy"
time_dummy="time_dummy"

#treatment dummy first switch
df[treatment_dummy_firstswitch]=(df.groupby(unit_var)[treatment_dummy_switch]
                                 .apply(lambda x: (x == 1) & (x.shift(1) != 1))
                                 .astype(int)
                                 )
#treatment dummy
df[treatment_dummy]=df.groupby(unit_var)[treatment_dummy_switch].cummax()

#time dummy
df[time_dummy]=df[treatment_dummy]

#gen group dummies
df_pivot=pd.pivot_table(
    data=df,
    values=treatment_dummy_firstswitch,
    index=unit_var,
    columns=time_var,
    aggfunc=np.sum,
    fill_value=0,
    )

#rename cols
df_pivot.columns=[f"group_{x}" for x in df_pivot.columns]

#reset index
df_pivot=df_pivot.reset_index()

#newvars
group_dummies=list(df_pivot.columns)

# Merge the pivoted data back into the original DataFrame
df=pd.merge(
    left=df,
    right=df_pivot,
    how="left",
    on=unit_var,
    validate="m:1"
    )


newvars=[
    treatment_dummy_firstswitch,
    treatment_dummy,
    time_dummy
    ] + group_dummies
