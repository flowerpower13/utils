

#imports
import pandas as pd


#functions
from _merge_utils import _pd_merge


#_add_lastname
def _add_lastname(x):

    list_y=x.split(",")
    y=list_y[0]

    return y


#_df_add
def _df_add():

    filepath="zhao/_districts/duplicate_districts.csv"
    df=pd.read_csv(
        filepath,
        dtype="string",
        )

    df["lastname"]=df["politician"].apply(_add_lastname)

    filepath="zhao/_districts/duplicate_districts_lastname.csv"
    df.to_csv(filepath, index=False)


#_df_add()


folders=["zhao/_districts"]
items=["districts_merged"]
left="zhao/_districts/duplicate_districts_lastname"
left_vars=["stalpbr", "district", "year"]
right="zhao/_districts/congress_data_2023-07-28"
right_vars=["st", "district_number", "year"]
how="left"
validate="m:1"
_pd_merge(folders, items, left, left_vars, right, right_vars, how, validate)


print("done")
