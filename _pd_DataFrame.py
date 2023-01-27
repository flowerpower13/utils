
import numpy as np
import pandas as pd


def _dict_to_valscols(dict_data):
        count_values=dict_data.values()
        count_keys=dict_data.keys()
        
        values=[[x] for x in count_values]
        keys=[x for x in count_keys]

        return values, keys


#create pd.DataFrame from values and columns as list
'''values=[
        vals0, 
        vals1, 
        ]
columns=[
        "col0", 
        "col1", 
        ]'''
def _pd_DataFrame(values, columns):

    data=np.transpose(np.array(values))
    df=pd.DataFrame(data=data, columns=columns)

    return df